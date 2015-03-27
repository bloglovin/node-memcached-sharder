/* jshint node: true */
'use strict';

//
// # Memcached Lite
//
// Super simple memcached client tailored to our needs at Bloglovin.
//

var assert = require('assert');
var util = require('util');
var events = require('events');
var async = require('async');
var EdenMemcached = require('memcached');
var crc32 = require('buffer-crc32');
var domain = require('domain');
var _ = require('lodash');

function Memcached(options) {
  // Set default options
  options = typeof options === 'object' ? options : {};
  options.options = options.options || {};

  // Normalise our incoming server list
  options.servers = options.servers || [{ host: '127.0.0.1:11211', weight: 1 }];
  this.servers = Memcached.normaliseServers(options.servers);

  // Setup connections and hashrings
  this.connectionFactory = options.connectionFactory || Memcached.defaultConnectionFactory;
  this.connections = this.setupConnections(this.servers, options.options);
  this.sumWeight   = _.reduce(this.servers, function addWeight(r, server) {
      return r + server.weight;
    }, 0);
}
util.inherits(Memcached, events.EventEmitter);

module.exports = Memcached;

//
// ## Register Plugin
//
// Register plugin with Hapi.
//
// Options should have two keys: "servers" and "options".
//
// "servers" can be a host string: "127.0.0.1:11211"; a server spec object:
// `{"host":"127.0.0.1:11211", weight: 100}`; or an array of either.
//
// "options" is an object that corresponds to the options outlined in the
// [docs](https://npmjs.org/package/memcached) for the memcached module.
//
Memcached.register = function (server, options, next) {
  var mc = new Memcached(options);
  server.expose('connection', function returnConnection() {
    return mc;
  });
  next();
};
Memcached.register.attributes = {
  pkg: require('./package.json'),
};

//
// ## Default connection factory
//
// Creates a connection using memcached.
//
Memcached.defaultConnectionFactory = function (servers, options) {
  return new EdenMemcached(servers, options);
};

//
// ## Normalise server list
//
// Turns the permitted server option formats into the full [{host:"",weight:0}]
// format.
//
Memcached.normaliseServers = function (servers) {
  if (!Array.isArray(servers)) {
    servers = [servers];
  }

  servers = _.map(servers, function normalise(server) {
    if (typeof server === 'string') {
      server = { host: server, weight: 1 };
    } else {
      if (server.weight === undefined) {
        server.weight = 1;
      }
      if (server.uri) {
        server.host = server.uri;
        delete server.uri;
      }
    }

    return server;
  });

  return servers;
};

//
// ## Setup connections
//
// Opens connections to each server.
//
// * **servers**, _an array of server objects._
//
// **Returns** an object with connections keyed to host.
//
Memcached.prototype.setupConnections = function setupConnections(servers, config) {
  var connect = this.connectionFactory;
  var connections = {};

  servers.forEach(function createConnection(server) {
    var opts = _.cloneDeep(config);
    connections[server.host] = connect(server.host, opts);
  });

  return connections;
};

//
// ## Hash key
//
// Hashes the key and returns the correct server host.
//
// * **key**, _a key to hash._
//
// **Returns** the host for the selected shard.
//
Memcached.prototype.hostForKey = function (key) {
  var checksum = crc32.unsigned(key);
  var index    = checksum % this.sumWeight;
  var selected;

  for (var idx in this.servers) {
    var server = this.servers[idx];
    if (index < server.weight) {
      selected = server.host;
      break;
    } else {
      index -= server.weight;
    }
  }

  return selected;
};

Memcached.prototype.delMulti = function (keys, callback) {
  var self = this;
  var buckets = {};

  // Put the keys in per-host buckets.
  keys.map(this.hostForKey, this).forEach(function addToBucket(host, index) {
    buckets[host] = buckets[host] || [];
    buckets[host].push(keys[index]);
  });

  // Run one series of delete ops per server.
  async.each(Object.keys(buckets), function deleteKeysOnHost(host, hostDone) {
    var keys = buckets[host];
    var encounteredErrors = [];

    // Run the individual delete ops and store failed deletes for logging
    async.eachSeries(keys, function deleteKey(key, deleteDone) {
      self.connections[host].del(key, function delResult(error) {
        if (error) {
          encounteredErrors.push(key);
        }
        deleteDone();
      });
    }, function hostResult() {
      // Just log delete failures
      if (encounteredErrors.length) {
        self.emit('error', new Error(
          'Failed to delete keys ' +
          JSON.stringify(encounteredErrors) +
          ' from memcache server ' + host));
      }

      hostDone();
    });
  }, callback);
};

Memcached.prototype.setMulti = function (values, ttl, callback) {
  var self = this;
  var buckets = {};
  var keys = Object.keys(values);

  // Put the keys in per-host buckets.
  keys.map(this.hostForKey, this).forEach(function addToBucket(host, index) {
    buckets[host] = buckets[host] || [];
    buckets[host].push(keys[index]);
  });

  // Run one series of delete ops per server.
  async.each(Object.keys(buckets), function deleteKeysOnHost(host, hostDone) {
    var keys = buckets[host];
    var encounteredErrors = [];

    // Run the individual set ops and store failed sets for logging
    async.eachSeries(keys, function deleteKey(key, deleteDone) {
      self.connections[host].set(key, values[key], ttl, function delResult(error) {
        if (error) {
          encounteredErrors.push(key);
        }
        deleteDone();
      });
    }, function hostResult() {
      // Just log delete failures
      if (encounteredErrors.length) {
        self.emit('error', new Error(
          'Failed to set keys ' +
          JSON.stringify(encounteredErrors) +
          ' on memcache server ' + host));
      }

      hostDone();
    });
  }, callback);
};

Memcached.prototype.getMulti = function (keys, callback) {
  var self = this;
  var buckets = {};
  var result = {};
  var failed = false;
  var done = false;

  // Put the keys in per-host buckets.
  keys.map(this.hostForKey, this).forEach(function addToBucket(host, index) {
    buckets[host] = buckets[host] || [];
    buckets[host].push(keys[index]);
  });

  // Send off a getMulti for each host.
  var jobs = [];
  _.forOwn(buckets, function getData(bucket, host) {
    jobs.push(host);
    if (bucket.length > 1) {
      this.connections[host].getMulti(bucket, addResult.bind(this, host));
    } else {
      this.connections[host].get(bucket[0], function singleResult(error, result) {
        var results;

        if (!error) {
          results = {};
          results[bucket[0]] = result;
        }

        addResult(host, error, results);
      });
    }
  }, this);

  // Handler function for result that is used with the host bound to the
  // first argument.
  function addResult(host, error, results) {
    // Guard against failed or finished sessions
    if (failed || done) { return; }

    // Remove our job, as it's completed
    var jobIndex = jobs.indexOf(host);
    if (jobIndex >= 0) {
      jobs.splice(jobIndex, 1);

      if (error) {
        // We want to allow partial success without going bat shit.
        self.emit('error', new Error(
          'Failed to fetch batch of keys ' +
          JSON.stringify(buckets[host]) +
          ' from memcache server ' + host));
      } else {
        // Merge in our result.
        for (var key in results) {
          result[key] = results[key];
        }
      }

      // Return the result if this was the last job.
      if (!jobs.length) {
        done = true;
        callback(null, result);
      }
    } else {
      // Fail hard if we're getting nonsensical data.
      failed = true;
      callback(new Error('Got faulty or duplicate job result for memcache server ' + host));
    }
  }
};

//
// ## Simulate the memcached module's API
//
// Dynamically create prototype methods for the memcached API.
//
var methods = ['touch', 'get', 'gets', 'set', 'replace', 'add',
    'cas', 'append', 'prepend', 'incr', 'decr', 'del'];
_.map(methods, function (method) {
  Memcached.prototype[method] = (function createWrapperFunction(method) {
    return function shardingWrapper(key) {
      assert.equal(typeof key, 'string', 'The key argument must be a string');

      var self = this;
      var d = domain.create();
      var args = Array.prototype.slice.call(arguments);
      var host = this.hostForKey(key);
      var connection = this.connections[host];

      d.on('error', function onDomainError(err) {
        // Check for a callback
        var lastArgument = args[args.length - 1];
        var callback  = typeof lastArgument === 'function' ? lastArgument : null;

        if (callback) {
          callback(err);
        } else {
          self.emit('error', err);
        }
      });

      d.run(function runMemcachedCommand() {
        connection[method].apply(connection, args);
      });
    };
  })(method);
});
