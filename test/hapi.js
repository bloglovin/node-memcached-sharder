/* jshint node: true */
/* globals suite, test */
'use strict';

//
// # Test MC lite
//

var assert = require('assert');
var hapi = require('hapi');
var sharder = require('../');

suite('Hapi plugin', function () {

  test('Can get connection', function () {
    // Create dummy connections
    function connFactory() {
      return {};
    }

    var server = new hapi.Server();
    var plugin = {
      register: sharder.register,
      options: {
        connectionFactory: connFactory,
      },
    };

    server.register(plugin, function registerResult(error) {
      assert.equal(error, null, error ? error.message : undefined);

      var plug = server.plugins['bloglovin-memcached-sharder'];
      assert.equal(typeof plug, 'object',
        'The plugin wasn\'t properly registered');
      assert.equal(typeof plug.connection, 'function',
        'The connection function wasn\'t exposed');
      assert(plug.connection() instanceof sharder,
        'The connection function doesn\'t return a sharder instance');
    });
  });
});
