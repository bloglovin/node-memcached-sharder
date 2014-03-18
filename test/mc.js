//
// # Test MC lite
//

var assert = require('assert');
var mc     = require('../');

suite('BL Memcached Sharder', function () {

  test('Correctly creates server connections', function () {
    var m = new mc({
      servers: [
        { uri: '1.1.1.1:11211', weight: 1 },
        { uri: '1.1.1.2:11211', weight: 1 }
      ]
    });

    assert.equal(m.conns[0].servers[0], '1.1.1.1:11211');
    assert.equal(m.conns[1].servers[0], '1.1.1.2:11211');
  });

  test('Dynamic API wrappers correctly setup', function () {
    var methods = ['touch', 'get', 'gets', 'set', 'replace', 'add',
        'cas', 'append', 'prepend', 'incr', 'decr', 'del'];
    var m = new mc();
    methods.map(function (method) {
      assert.equal(typeof m[method], 'function');
    });
  });

  test('Matches output form PHP implementation', function () {
    // Create dummy connections
    function connFactory(options) {
      return {};
    }

    // Servers
    var m = new mc({
      servers: [
        { uri: '192.168.1.140:11211', weight: 100 },
        { uri: '192.168.1.141:11211', weight: 100 },
        { uri: '192.168.1.142:11211', weight: 100 },
        { uri: '192.168.1.143:11211', weight: 100 }
      ],
      connectionFactory: connFactory
    });

    // Test keys returning the same values as the PHP app.
    var a = m.hostForKey('lol');
    assert.equal(a, '192.168.1.142:11211');
    var b = m.hostForKey('hejsan_varlden%$%#');
    assert.equal(b, '192.168.1.141:11211');
  });
});
