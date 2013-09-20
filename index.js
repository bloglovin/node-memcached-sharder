//
// # Memcached Lite
//
// Super simple memcached client tailored to our needs at Bloglovin.
//

var _     = require('lodash');
var mc    = require('memcached-wrapper');
var crc32 = require('buffer-crc32');

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
    'cas', 'append', 'prepend', 'incr', 'decr', 'del'];
_.map(methods, function (method) {
  Memcached.prototype[method] = function (method) {
    return function () {
      var server = this.hashKey(arguments[0]);
      server[method].apply(server, arguments);
    };
  }(method);
});

