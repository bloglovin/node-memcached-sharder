/* jshint node: true */
'use strict';

//
// # Memcached Lite
//
// Super simple memcached client tailored to our needs at Bloglovin.
//

var lib = {
  lodash: require('lodash'),
  memcached: require('memcached'),
  crc32: require('buffer-crc32'),
  domain: require('domain')
};
var _ = lib.lodash;

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
Memcached.register = function (plugin, options, next) {
  var mc = new Memcached(options);
  plugin.expose('connection', function returnConnection() {
    return mc;
  });
  next();
};

//
// ## Default connection factory
//
// Creates a connection using memcached.
//
Memcached.defaultConnectionFactory = function (servers, options) {
  return new lib.memcached(servers, options);
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
    if (typeof server == 'string') {
      server = { host: server, weight: 1 };
    }
    else {
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
  var checksum = lib.crc32.unsigned(key);
  var index    = checksum % this.sumWeight;
  var selected;

  for (var idx in this.servers) {
    var server = this.servers[idx];
    if (index < server.weight) {
      selected = server.host;
      break;
    }
    else {
      index -= server.weight;
    }
  }

  return selected;
};

Memcached.prototype.getMulti = function (keys, callback) {
  var buckets = {};
  var result = {};

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
    }
    else {
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
    // Remove our job, as it's completed
    var jobIndex = jobs.indexOf(host);
    if (jobIndex >= 0) {
      jobs.splice(jobIndex, 1);
    }
    else {
      console.error("Got faulty or duplicate job result for memcache server " + host);
      return;
    }

    if (error) {
      console.error("Failed to fetch batch of keys from memcache server " + host);
    }
    else {
      // Merge in our result.
      for (var key in results) {
        result[key] = results[key];
      }

      // Return the result if this was the last job.
      if (!jobs.length) {
        callback(null, result);
      }
    }
  }
};

//
// ## Simulate the memcached module's API
//
// Dynamically create prototype methods for the memcached API.
//
var methods = ['touch', 'get', 'gets', 'set', 'replace', 'add',
    'cas', 'append', 'prepend', 'incr', 'decr', 'remove'];
_.map(methods, function (method) {
  Memcached.prototype[method] = function createWrapperFunction(method) {
    return function shardingWrapper(key) {
      var d = lib.domain.create();
      var args = Array.prototype.slice.call(arguments);
      var host = this.hostForKey(key);
      var connection = this.connections[host];

      d.on('error', function onDomainError(err) {
        // Check for a callback
        var lastArgument = args[args.length-1];
        var callback  = typeof lastArgument === 'function' ? lastArgument : null;

        if (callback) {
          callback(err);
        }
        else {
          //console.error('Memcached error.\n\tServer: %s\n\tArguments: %s\n\t%s', server.Mc.servers, args, err);
          throw err;
        }
      });

      d.run(function runMemcachedCommand() {
        connection[method].apply(connection, args);
      });
    };
  }(method);
});
