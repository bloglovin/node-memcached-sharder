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

    assert.equal(m.conns[0].Mc.servers[0], '1.1.1.1:11211');
    assert.equal(m.conns[1].Mc.servers[0], '1.1.1.2:11211');
  });

  test('Hashing key works', function () {
    var m = new mc({
      servers: [
        { uri: '1.1.1.1:11211', weight: 1 },
        { uri: '1.1.1.2:11211', weight: 1 }
      ]
    });
    var server = m.hashKey('foobar');
    assert.equal(typeof server, 'object');
  });

  test('Dynamic API wrappers correctly setup', function () {
    var methods = ['touch', 'get', 'gets', 'getMulti', 'set', 'replace', 'add',
        'cas', 'append', 'prepend', 'incr', 'decr', 'del'];
    var m = new mc();
    methods.map(function (method) {
      assert.equal(typeof m[method], 'function');
    });
  });
});

