//
// # Test MC lite
//

var assert = require('assert');
var mc     = require('../');

suite('BL Memcached Sharder', function () {

  test('Correctly generates hashring', function () {
    var m = new mc();

    var a = m.serverRing({
      '1.1.1.1': 3,
      '1.1.1.2': 3,
      '1.1.1.3': 2,
      '1.1.1.4': 1
    });

    var a_expected = [
      '1.1.1.1',
      '1.1.1.2',
      '1.1.1.3',
      '1.1.1.4',
      '1.1.1.1',
      '1.1.1.2',
      '1.1.1.3',
      '1.1.1.1',
      '1.1.1.2',
    ];
    assert.deepEqual(a, a_expected);
  });

  test('Correctly creates server connections', function () {
    var m = new mc({
      servers: {
        '1.1.1.1:11211': 1,
        '1.1.1.2:11211': 1
      }
    });

    assert.equal(m.conns['1.1.1.1:11211'].servers[0], '1.1.1.1:11211');
    assert.equal(m.conns['1.1.1.2:11211'].servers[0], '1.1.1.2:11211');
  });

  test('Hashing key works', function () {
    var m = new mc();
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

