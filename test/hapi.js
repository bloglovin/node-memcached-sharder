//
// # Test MC lite
//

var lib = {
  assert: require('assert'),
  hapi: require('hapi'),
  sharder: require('../'),
  packageInfo: require('../package.json')
};
var assert = lib.assert;

suite('Hapi plugin', function () {

  test('Can get connection', function () {
    // Create dummy connections
    function connFactory(options) {
      return {};
    }

    var server = new lib.hapi.Server();
    var plugin = {
      name: lib.packageInfo.name,
      version: lib.packageInfo.version,
      path: '../',
      register: lib.sharder.register
    };

    server.pack.register(plugin, {}, function registerResult(error) {
      assert.equal(error, null, error ? error.message : undefined);

      var plug = server.plugins['bloglovin-memcached-sharder'];
      assert.equal(typeof plug, 'object',
        'The plugin wasn\'t properly registered');
      assert.equal(typeof plug.connection, 'function',
        'The connection function wasn\'t exposed');
      assert(plug.connection() instanceof lib.sharder,
        'The connection function doesn\'t return a sharder instance');
    });
  });
});
