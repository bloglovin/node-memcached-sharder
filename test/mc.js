//
// # Test MC lite
//

var assert = require('assert');
var mc     = require('../');

suite('Memcached Lite', function () {

  test ('Correctly generates hashring', function () {
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
});

