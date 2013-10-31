"use strict";

var http = require("http"),
    os = require("os"),
    util = require("util"),
    zlib = require("zlib");

var async = require("async"),
    cors = require("cors"),
    d3 = require("d3"),
    express = require("express"),
    LRU = require("lru-cache"),
    mapnik = require("mapnik"),
    request = require("crequest"),
    mercator = new (require("sphericalmercator"))(),
    topojson = require("topojson");

var app = express();

app.disable("x-powered-by");
app.use(express.responseTime());
app.use(cors());

app.configure("development", function() {
  app.use(express.logger());
});

http.globalAgent.maxSockets = 200;

var SOURCES = require("./sources.json");

var CACHE_SIZE = process.env.CACHE_SIZE || 10,
    REQUEST_TIMEOUT = process.env.REQUEST_TIMEOUT || 30000;

var cache = LRU({
  max: CACHE_SIZE * 1024 * 1024,
  length: function(x) {
    return x.size();
  }
});

var locks = LRU({
  maxAge: REQUEST_TIMEOUT * 1.1 // 10% longer than REQUEST_TIMEOUT
});

var fetch = function(task, callback) {
  var key = util.format("%s/%d/%d/%d", task.source.source, task.z, task.x, task.y);

  if (locks.has(key)) {
    return setImmediate(fetch, task, callback);
  }

  var tile;
  if ((tile = cache.get(key))) {
    return callback(null, tile);
  }

  // lock the request
  locks.set(key, true);

  // TODO default source.{minzoom,maxzoom,bounds}
  var source = SOURCES[task.source.source];
  task.source.info = source;

  var xyz = mercator.xyz(source.bounds, task.z);

  if (task.z < (source.minzoom || -Infinity) ||
      task.z > (source.maxzoom || Infinity) ||
      task.x < xyz.minX ||
      task.x > xyz.maxX ||
      task.y < xyz.minY ||
      task.y > xyz.maxY) {
    // outside zoom range or bounds; skip
    return callback();
  }

  var url = source.tiles[~~(Math.random() * source.tiles.length)]
      .replace(/{z}/i, task.z)
      .replace(/{x}/i, task.x)
      .replace(/{y}/i, task.y);

  console.log("← ", url);

  return request({
    uri: url,
    encoding: null,
    headers: task.headers,
    timeout: REQUEST_TIMEOUT
  }, function(err, res, body) {
    // unlock the request
    locks.del(key);

    if (err) {
      return callback(err);
    }

    switch (res.statusCode) {
    case 200:
      switch (source.format.toLowerCase()) {
      case "pbf":
        tile = new mapnik.VectorTile(task.z, task.x, task.y);
        tile.source = task.source;
        tile.size = function() {
          return this.getData().length;
        };

        // attach TTL
        if (res.headers["cache-control"]) {
          tile.ttl = (res.headers["cache-control"] || "").split(",").map(function(c) {
            return c.trim();
          }).filter(function(c) {
            return c.match(/^max-age=/);
          }).map(function(c) {
            return +c.split("=")[1];
          })[0];
        }

        // lock the request again since we have data
        locks.set(key, true);

        return tile.setData(body, function(err) {
          // unlock the request
          locks.del(key);

          if (err) {
            return callback(err);
          }

          cache.set(key, tile);

          return callback(null, tile);
        });

      case "geojson":
        tile = {};
        tile.source = task.source;
        tile._size = 0;
        tile.size = function() {
          return this._size;
        };

        tile.toGeoJSON = function(layer) {
          if (body.features) {
            return body;
          }

          return body[layer];
        };

        // attach TTL
        if (res.headers["cache-control"]) {
          tile.ttl = (res.headers["cache-control"] || "").split(",").map(function(c) {
            return c.trim();
          }).filter(function(c) {
            return c.match(/^max-age=/);
          }).map(function(c) {
            return +c.split("=")[1];
          })[0];
        }

        locks.set(key, true);

        return async.map(source.vector_layers, function(info, next) {
          // approximate size
          tile._size += res.body.length;

          var layer = new mapnik.Layer(info.id);
          layer.srs = "+init=epsg:4326";

          var fields = Object.keys(info.fields),
              features;

          if (body.features) {
            // body is a single GeoJSON layer
            features = body.features;
          } else {
            // body contains multiple GeoJSON layers
            features = body[info.id].features;
          }

          var data = features.map(function(f) {
            var row = [JSON.stringify(f.geometry)];

            fields.map(function(k) {
              row.push(f.properties[k]);
            });

            return row;
          });

          if (features.length > 0) {
            var csv = d3.csv.formatRows([["geojson"].concat(fields)]) + "\n";
            csv += d3.csv.formatRows(data);

            try {
              layer.datasource = new mapnik.Datasource({
                type: "csv",
                inline: csv
              });
            } catch (e) {
              console.log(csv);
              return next(e);
            }
          } else {
            layer = null;
          }

          return next(null, layer);
        }, function(err, layers) {
          locks.del(key);

          if (err) {
            return callback(err);
          }

          layers = layers.filter(function(x) {
            return !!x;
          });

          tile.layers = function() {
            return layers;
          };

          cache.set(key, tile);

          return callback(null, tile);
        });

      default:
        return callback(new Error("Unsupported source format: " + source.format));
      }

      break;

    case 404:
      return callback();

    default:
      return callback(new Error(res.statusCode + ": " + body.toString()));
    }
  });
};

var getSources = function(layers) {
  return layers.split(",")
    .map(function(source) {
      var matches = source.trim().match(/([\w\.\-]+)(\[([\w\-;]+)\])?/);

      return {
        source: matches[1].trim(),
        layers: (matches[3] || "").split(";").map(function(layer) {
          return layer.trim();
        }).filter(function(x) {
          return !!x;
        })
      };
    });
};

app.get("/:layers/:z/:x/:y.vtile", function(req, res) {
  var z = +req.params.z,
      x = +req.params.x,
      y = +req.params.y;

  var sources = getSources(req.params.layers);

  // be a good citizen and pass relevant headers on
  var headers = {};

  Object.keys(req.headers).forEach(function(key) {
    if (["user-agent",
         "referer",
         "x-forwarded-for"].indexOf(key) >= 0) {
      headers[key] = req.headers[key];
    }
  });

  var tasks = sources.map(function(source) {
    return {
      z: z,
      x: x,
      y: y,
      source: source,
      headers: headers
    };
  });

  return async.map(tasks, fetch, function(err, tiles) {
    if (err) {
      console.warn(err);
      return res.send(500);
    }

    // ignore skipped tiles
    tiles = tiles.filter(function(tile) {
      return !!tile;
    });

    var sourceLayers = {};

    var map = new mapnik.Map(256, 256);

    tiles.forEach(function(tile) {
      tile.layers().filter(function(x) {
        return tile.source.layers.length === 0 ||
              tile.source.layers.indexOf(x.name) >= 0;
      }).forEach(function(layer) {
        // NOTE: if a layer with the same name appears more than once, the last
        // one present will be rendered
        sourceLayers[layer.name] = layer;
      });

      var layers = tile.source.layers;

      if (layers.length === 0) {
        layers = Object.keys(sourceLayers);
      }

      layers.forEach(function(name) {
        if (sourceLayers[name]) {
          map.add_layer(sourceLayers[name]);
        }
      });
    });

    map.srs = "+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0.0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs +over";
    map.extent = mercator.bbox(x, y, z, false, "900913");

    var dx = map.extent[2] - map.extent[0],
        dy = map.extent[3] - map.extent[1];

    map.bufferedExtent = [map.extent[0] - dx,
                          map.extent[1] - dy,
                          map.extent[2] + dx,
                          map.extent[3] + dy];

    var opts = {
      tolerance: 0,
      simplify: 0,
      simplify_algorithm: "radial-distance",
      buffer_size: map.bufferSize
    };

    return map.render(new mapnik.VectorTile(z, x, y), opts, function(err, dst) {
      if (err) {
        console.warn(err);
        return res.send(500);
      }

      return zlib.deflate(dst.getData(), function(err, data) {
        if (err) {
          console.warn(err);
        }

        var ttl = Math.min.apply(null, tiles.map(function(t) {
          // TODO default TTL?
          return t.ttl;
        }).filter(function(ttl) {
          return !!ttl;
        }));

        if (ttl === Infinity) {
          // no TTLs were provided--assume the worst
          res.set("Cache-Control", "max-age=0");
        } else {
          res.set("Cache-Control", util.format("max-age=%d", ttl));
        }

        res.set("Content-Type", "application/x-protobuf");
        res.set("Content-Encoding", "deflate");
        return res.send(data);
      });
    });
  });
});

app.get("/:layers/:z/:x/:y.json", function(req, res) {
  var z = +req.params.z,
      x = +req.params.x,
      y = +req.params.y;

  var sources = getSources(req.params.layers);

  // be a good citizen and pass relevant headers on
  var headers = {};

  Object.keys(req.headers).forEach(function(key) {
    if (["user-agent",
         "referer",
         "x-forwarded-for"].indexOf(key) >= 0) {
      headers[key] = req.headers[key];
    }
  });

  var tasks = sources.map(function(source) {
    return {
      z: z,
      x: x,
      y: y,
      source: source,
      headers: headers
    };
  });

  return async.map(tasks, fetch, function(err, tiles) {
    if (err) {
      console.warn(err);
      return res.send(500);
    }

    // ignore skipped tiles
    tiles = tiles.filter(function(tile) {
      return !!tile;
    });

    var sourceLayers = {},
        json = {};

    tiles.forEach(function(tile) {
      tile.layers().filter(function(x) {
        return tile.source.layers.length === 0 ||
              tile.source.layers.indexOf(x.name) >= 0;
      }).forEach(function(layer) {
        // NOTE: if a layer with the same name appears more than once, the last
        // one present will be rendered
        sourceLayers[layer.name] = layer;
      });

      var layers = tile.source.layers;

      if (layers.length === 0) {
        layers = Object.keys(sourceLayers);
      }

      layers.forEach(function(name) {
        if (sourceLayers[name]) {
          json[name] = tile.toGeoJSON(name);
        }
      });
    });

    var ttl = Math.min.apply(null, tiles.map(function(t) {
      // TODO default TTL?
      return t.ttl;
    }).filter(function(ttl) {
      return !!ttl;
    }));

    if (ttl === Infinity) {
      // no TTLs were provided--assume the worst
      res.set("Cache-Control", "max-age=0");
    } else {
      res.set("Cache-Control", util.format("max-age=%d", ttl));
    }

    return res.send(json);
  });
});

app.get("/:layers/:z/:x/:y.topojson", function(req, res) {
  var z = +req.params.z,
      x = +req.params.x,
      y = +req.params.y;

  var sources = getSources(req.params.layers);

  // be a good citizen and pass relevant headers on
  var headers = {};

  Object.keys(req.headers).forEach(function(key) {
    if (["user-agent",
         "referer",
         "x-forwarded-for"].indexOf(key) >= 0) {
      headers[key] = req.headers[key];
    }
  });

  var tasks = sources.map(function(source) {
    return {
      z: z,
      x: x,
      y: y,
      source: source,
      headers: headers
    };
  });

  return async.map(tasks, fetch, function(err, tiles) {
    if (err) {
      console.warn(err);
      return res.send(500);
    }

    // ignore skipped tiles
    tiles = tiles.filter(function(tile) {
      return !!tile;
    });

    var sourceLayers = {},
        json = {};

    tiles.forEach(function(tile) {
      tile.layers().filter(function(x) {
        return tile.source.layers.length === 0 ||
              tile.source.layers.indexOf(x.name) >= 0;
      }).forEach(function(layer) {
        // NOTE: if a layer with the same name appears more than once, the last
        // one present will be rendered
        sourceLayers[layer.name] = layer;
      });

      var layers = tile.source.layers;

      if (layers.length === 0) {
        layers = Object.keys(sourceLayers);
      }

      layers.forEach(function(name) {
        if (sourceLayers[name]) {
          json[name] = topojson.topology({collection: tile.toGeoJSON(name) });
        }
      });
    });

    var ttl = Math.min.apply(null, tiles.map(function(t) {
      // TODO default TTL?
      return t.ttl;
    }).filter(function(ttl) {
      return !!ttl;
    }));

    if (ttl === Infinity) {
      // no TTLs were provided--assume the worst
      res.set("Cache-Control", "max-age=0");
    } else {
      res.set("Cache-Control", util.format("max-age=%d", ttl));
    }

    return res.send(json);
  });
});

app.get("/:layers.json", function(req, res) {
  // TODO default minzoom/maxzoom/bounds
  var sources = getSources(req.params.layers),
      tileSources = sources.map(function(source) {
        return SOURCES[source.source];
      });

  // TODO sort this when multiple layers are specified in a different order
  // like "mapbox.mapbox-streets-v3[waterway;landuse]"
  var vectorLayers = sources.map(function(source) {
    return SOURCES[source.source].vector_layers.filter(function(layer) {
      return source.layers.length === 0 ||
             source.layers.indexOf(layer.id) >= 0;
    });
  }).reduce(function(a, b) {
    return a.concat(b);
  }, []);

  var minzoom = Math.min.apply(null, tileSources.map(function(x) {
    return x.minzoom;
  }));

  var maxzoom = Math.max.apply(null, tileSources.map(function(x) {
    return x.maxzoom;
  }));

  return res.send({
    "attribution": "", // TODO merge attributions
    "bounds": [ -180, -85.0511, 180, 85.0511 ], // TODO
    "center": [ -122.3782, 37.7706, 12 ],
    "format": "pbf",
    "id": "custom.custom-vtiles", // TODO
    "maskLevel": 8, // TODO how should this relate to source maskLevels?
    "maxzoom": maxzoom,
    "minzoom": minzoom,
    "name": "Tiled Vector Remix", // TODO
    "scheme": "xyz",
    "tilejson": "2.0.0",
    "tiles": [
      util.format("http://%s:%d/%s/{z}/{x}/{y}.vtile",
                  os.hostname(),
                  process.env.PORT || 8080,
                  req.params.layers)
    ],
    "vector_layers": vectorLayers
  });
});

app.get("/sources.json", function(req, res) {
  res.send(SOURCES);
});

app.listen(process.env.PORT || 8080, function() {
  console.log("Listening at http://%s:%d/", this.address().address, this.address().port);
});
