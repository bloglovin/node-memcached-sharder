//
// # Memcached Lite
//
// Super simple memcached client tailored to our needs at Bloglovin.
//

var _      = require('lodash');
var mc     = require('memcached-wrapper');
var crc32  = require('buffer-crc32');
var domain = require('domain');

var Memcached = module.exports = function memcached(options) {
  // Set default options
  options = typeof options === 'object' ? options : {};
  options.servers = options.servers || [ { uri: '127.0.0.1:11211', weight: 1 } ];

  // Setup connections and hashrings
  var config     = options.options || {};
  this.sumWeight = _.reduce(options.servers, function (r, i) { return r + i.weight; }, 0);
  this.conns     = this.setupConnections(_.map(options.servers, 'uri'), config);
  this.servers   = options.servers;
};

//
// ## Setup connections
//
// Opens connections to each server.
//
// * **servers**, _an array of IP:ports._
//
// **Returns** an object with connections keyed to ip.
//
Memcached.prototype.setupConnections = function setupConnections(servers, config) {
  var connections = [];
  _.map(servers, function (server) {
    var opts = _.cloneDeep(config);
    connections.push(mc({ servers: server, options: opts }));
  });
  return connections;
};

//
// ## Hash key
//
// Hashes the key and returns the correct server.
//
// * **key**, _a key to hash._
//
// **Returns** a memcached connection object.
//
Memcached.prototype.hashKey = function hashKey(key) {
  var checksum = crc32.unsigned(key);
  var index    = checksum % this.sumWeight;

  for (var i in this.servers) {
    var server = this.servers[i];
    if (index < server.weight) {
      return this.conns[i];
    }
    else {
      index -= server.weight;
    }
  }
};

//
// ## Simulate the memcached module's API
//
// Dynamically create prototype methods for the memcached API.
//
var methods = ['touch', 'get', 'gets', 'getMulti', 'set', 'replace', 'add',
    'cas', 'append', 'prepend', 'incr', 'decr', 'remove'];
_.map(methods, function (method) {
  Memcached.prototype[method] = function (method) {
    return function () {
      var self = this;
      var d = domain.create();
      var args = Array.prototype.slice.call(arguments);
      var server = this.hashKey(args[0]);

      d.on('error', function (err) {
        // Find any callback
        var len = args.length;
        var cb  = null;
        for (len; len >= 0; len--) {
          if (typeof args[len] === 'function') {
            cb = args[leb];
            break;
          }
        }

        if (cb) {
          cb(err);
        }
        else {
          //console.error('Memcached error.\n\tServer: %s\n\tArguments: %s\n\t%s', server.Mc.servers, args, err);
          throw err;
        }
      });

      d.run(function () {
        server[method].apply(server, args);
      });
    };
  }(method);
});

//
// ## Register Plugin
//
// Register plugin with Hapi.
//
// Options is an object that corresponds to the options outlined in the
// [docs](https://npmjs.org/package/memcached) for the memcached module.
//
Memcached.register = function (plugin, options, next) {
  var mc = new Memcached(options);
  plugin.expose('connection', function () {
    return mc;
  });
  next();
};

