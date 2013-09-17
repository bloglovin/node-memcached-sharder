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
  options.servers = options.servers || { '127.0.0.1:11211': 1 };

  // Setup connections and hashrings
  var config     = options.config || {};
  this.sumWeight = 0;
  this.conns     = this.setupConnections(Object.keys(options.servers), config);
  this.servers   = this.serverRing(options.servers);
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
  var connections = {};
  _.map(servers, function (server) {
    connections[server] = new mc({ servers: server, options: config });
  });
  return connections;
};

//
// ## Generate hash ring
//
// "Custom" hashring algorithm that creates an array with servers for sharding.
//
// Given the following server object:
//
//   {
//     '1.1.1.1': 3,
//     '1.1.1.2': 3,
//     '1.1.1.3': 2,
//     '1.1.1.4': 1
//   }
//
// The following hash ring is created:
//
//   [
//     '1.1.1.1',
//     '1.1.1.2',
//     '1.1.1.3',
//     '1.1.1.4',
//     '1.1.1.1',
//     '1.1.1.2',
//     '1.1.1.3',
//     '1.1.1.1',
//     '1.1.1.2',
//     '1.1.1.1',
//     '1.1.1.2',
//   ]
//
// * **servers**, _an object containing servers and their respective weight._
//
// **Returns** an array of servers.
//
Memcached.prototype.serverRing = function serverRing(servers) {
  var hashRing    = [];
  var serverCount = _.reduce(servers, function (t, w) { return t + w; }, 0);
  this.sumWeight  = serverCount;
  function addToRing(weight, ip) {
    hashRing.push(ip);
    if (--servers[ip] < 1) {
      delete(servers[ip]);
    }
  }
  while (hashRing.length < serverCount) {
    _.forEach(servers, addToRing);
  }

  return hashRing;
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
  var server   = this.servers[index];
  return this.conns[server];
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

