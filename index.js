//
// # Memcached Lite
//
// Super simple memcached client tailored to our needs at Bloglovin.
//

var net = require('net');
var _   = require('lodash');

var Memcached = module.exports = function memcached(options) {
  options = typeof options === 'object' ? options : {};
  this.servers = this.serverRing(options.servers || { '127.0.0.1:11211': 1 });
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

