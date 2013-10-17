"use strict";

var http = require("http"),
    os = require("os"),
    util = require("util"),
    zlib = require("zlib");

var async = require("async"),
    cors = require("cors"),
    express = require("express"),
    LRU = require("lru-cache"),
    mapnik = require("mapnik"),
    Pool = require("generic-pool").Pool,
    request = require("crequest"),
    mercator = new (require("sphericalmercator"))();

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
    return x.length;
  }
});

var locks = LRU({
  maxAge: REQUEST_TIMEOUT * 1.1 // 10% longer than REQUEST_TIMEOUT
});

var pool = new Pool({
  create: function(callback) {
    var map = new mapnik.Map(256, 256);
    map.bufferSize = 0;

    return callback(null, map);
  },
  destroy: function(map) {
    // noop
  },
  // TODO (from tilelive-mapnik): need a smarter way to scale this. More maps
  // in pool seems better for PostGIS.
  max: os.cpus().length
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

  var s = SOURCES[task.source.source];
  var url = s.tiles[~~(Math.random() * s.tiles.length)]
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

    if (res.statusCode === 200) {
      tile = new mapnik.VectorTile(task.z, task.x, task.y);
      tile.source = task.source;

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
    }

    return callback(new Error(res.statusCode + ": " + body.toString()));
  });
};

var getSources = function(layers) {
  return layers.split(",")
    .map(function(source) {
      var matches = source.trim().match(/([\w\.\-]+)(\[([\w;]+)\])?/);

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

    var sourceLayers = {};

    return pool.acquire(function(err, map) {
      if (err) {
        console.warn(err);
        return res.send(500);
      }

      tiles.forEach(function(tile) {
        tile.layers().filter(function(x) {
          return tile.source.layers.indexOf(x.name) >= 0;
        }).forEach(function(layer) {
          // NOTE: if a layer with the same name appears more than once, the last
          // one present will be rendered
          sourceLayers[layer.name] = layer;
        });

        tile.source.layers.forEach(function(name) {
          map.add_layer(sourceLayers[name]);
        });
      });

      map.extent = mercator.bbox(x, y, z, false, "900913");

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
          pool.release(map);

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
});

app.get("/:layers.json", function(req, res) {
  var sources = getSources(req.params.layers);

  // TODO sort this when multiple layers are specified in a different order
  // like "mapbox.mapbox-streets-v3[waterway;landuse]"
  var vectorLayers = sources.map(function(source) {
    return SOURCES[source.source].vector_layers.filter(function(layer) {
      return source.layers.indexOf(layer.id) >= 0;
    });
  }).reduce(function(a, b) {
    return a.concat(b);
  }, []);

  return res.send({
    "attribution": "",
    "bounds": [ -180, -85.0511, 180, 85.0511 ],
    "center": [ 0, 0, 0 ],
    "format": "pbf",
    "id": "custom.custom-vtiles",
    "maskLevel": 8, // TODO
    "maxzoom": 14, // TODO
    "minzoom": 0, // TODO
    "name": "Tiled Vector Remix",
    "scheme": "xyz",
    "tilejson": "2.0.0",
    "tiles": [
      util.format("http://%s:%d/%s/{z}/{x}/{y}.vtile", os.hostname(), 8080, req.params.layers)
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
