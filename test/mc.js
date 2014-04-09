//
// # Test MC lite
//

var lib = {
  assert: require('assert'),
  memcached: require('memcached'),
  async: require('async'),
  sharder: require('../')
};
var assert = lib.assert;

suite('Sharding', function () {

  test('Create a default instance', function createInstance() {
    var m = new lib.sharder();
    assert.equal(m.servers[0].host, '127.0.0.1:11211', 'The default server wasn\'t set to localhost');
  });

  test('Matches output form PHP implementation', function checkSharder() {
    // Create dummy connections
    function connFactory(options) {
      return {};
    }

    // Servers
    var m = new lib.sharder({
      servers: [
        { uri: '192.168.1.140:11211', weight: 16384 },
        { uri: '192.168.1.141:11211', weight: 16384 },
        { uri: '192.168.1.142:11211', weight: 16384 },
        { uri: '192.168.1.143:11211', weight: 16384 }
      ],
      connectionFactory: connFactory
    });

    var shardCases = {
      '192.168.1.140:11211': [
        'areally long key with a few _spaces and åäö',
        'bl_json2::sess_2348234i2uy4i2u3kl23jl4b2j3h4gk23f4j2t3f4j2h3g4v2j',
        '',
        'from the hudson river out to the nile',
        'you\'ve got gall, you\'ve got guile',
        'cast you off into EXILE!'
      ],
      '192.168.1.141:11211': [
        'foobars',
        'you\'ll stick around I\'ll make it worth your while',
        'I ran the marathon to the very last mile',
        'well if you battle me I will revile',
        'hejsan_varlden%$%#'
      ],
      '192.168.1.142:11211': [
        'lol',
        'a',
        'got numbers beyond what you can dial',
        'maybe because I\'m so versitile',
        'to step to me - I\'m a rapophile',
        'coming from Uranus to check my style',
        'go ahead put my rhymes on trial',
        'intergalactic. planetary. planetary. intergalactic.'
      ],
      '192.168.1.143:11211': [
        'jkvbryu43fbdfdsfsdfdsfuvib37vbvr1232',
        '!"#€%&/()="',
        'well now don\'t you tell me to smile',
        'style, profile - I said it always brings you back when you hear "oh child"',
        'people always say that my style is wild',
        'weeell, if you wanna battle you\'re in denial',
        '#yoloswag'
      ]
    };

    // Test keys returning the same values as the PHP app.
    function testCase(host, str) {
      var failMessage = "Shard mismatch for the string " + JSON.stringify(str);
      assert.equal(m.hostForKey(str), host, failMessage);
    }
    for (var host in shardCases) {
      shardCases[host].forEach(testCase.bind(this, host));
    }
  });
});

suite('Basic commands', function basicCommandsTest() {
  var m = new lib.sharder({
    servers: '127.0.0.1:11211',
    options: {
      namespace: 'mc:sharder-test:'
    }
  });

  test('Set and get', function runCommands(done) {
    lib.async.auto({
      setItem: function setItem(callback) {
        m.set('set-test', 'foo', 2, callback);
      },
      getItem: ['setItem', function getItem(callback) {
        m.get('set-test', function getResult(error, value) {
          if (error) return callback(error);
          assert.equal(value, 'foo');
          callback();
        });
      }],
    }, done);
  });

  test('Set and wait for expire', function runCommands(done) {
    this.timeout(3000);

    lib.async.series([
      function setItem(callback) {
        m.set('expire-test', 'foo', 1, callback);
      },
      function waitABit(callback) {
        setTimeout(callback, 2000);
      },
      function getItem(callback) {
        m.get('expire-test', function getResult(error, value) {
          if (error) return callback(error);
          assert.equal(value, false);
          callback();
        });
      },
    ], done);
  });
});

suite('Multi operations', function batchFetchTests(done) {
  var m = new lib.sharder({
    servers: [
      '127.0.0.1:11211',
      '127.0.0.1:11311',
      '127.0.0.1:11411'
    ],
    options: {
      namespace: 'mc:sharder-test:'
    },
    // We want to test the sharding in combination with multi-fetch, but not
    // deal with setting up multiple memcache instances for it, so change all
    // hosts to localhost:11211.
    connectionFactory: function useLocalhost(servers, options) {
      return lib.sharder.defaultConnectionFactory('127.0.0.1:11211', options);
    }
  });

  test('Set and fetch multiple values', function runCommands(done) {
    var values = {
      'multi-a': ['a'],
      'multi-b': ['b'],
      'multi-c': ['c'],
      'multi-d': ['d'],
      'multi-e': ['e'],
      'multi-f': ['f']
    };
    var keys = Object.keys(values);

    m.setMulti(values, 5, fetchItems);

    function fetchItems(error) {
      if (error) return done(error);

      lib.async.series([
        function fetchBatch(callback) {
          m.getMulti(keys, function multiResult(error, result) {
            if (error) return callback(error);
            assert.deepEqual(values, result, 'Get multi values didn\'t add up');
            callback();
          });
        },
        function fetchSingle(callback) {
          m.getMulti(['multi-a'], function multiResult(error, result) {
            if (error) return callback(error);
            assert.equal(typeof result, 'object', 'Get multi didn\'t return an object');
            assert.deepEqual(result['multi-a'], values['multi-a'], 'Get multi didn\'t return the correct value when fetching a single object');
            callback();
          });
        }
      ], done);
    }
  });

  test('Set and delete multiple values', function runCommands(done) {
    var values = {
      'multi-del-a': ['a'],
      'multi-del-b': ['b'],
      'multi-del-c': ['c'],
      'multi-del-d': ['d'],
      'multi-del-e': ['e'],
      'multi-del-f': ['f']
    };
    var keys = Object.keys(values);

    m.setMulti(values, 5, deleteItems);

    function deleteItems(error) {
      if (error) return done(error);

      m.delMulti(keys, fetchItems);
    }

    function fetchItems(error) {
      if (error) return done(error);

      m.getMulti(keys, function multiResult(error, result) {
        if (error) return done(error);

        assert.equal(Object.keys(result).length, 0, 'All items were not deleted');

        done();
      });
    }
  });
});

suite('Error handling', function errorHandlingTest() {
  var runtimeError = true;
  var m = new lib.sharder({
    servers: [
      {host:'127.0.0.1:11211'},
      {host:'127.0.0.1:11311'},
      {host:'127.0.0.1:11411'}
    ],
    connectionFactory: function getFailer() {
      return {
        get: function(key, callback) {
          callback(new Error('Simulated error'));
        },
        del: function(key, callback) {
          callback(new Error('Simulated error'));
        },
        set: function(key, value, ttl, callback) {
          if (runtimeError) {
            setTimeout(function triggerRuntimeError() {
              var object;
              callback(null, object.result);
            }, 1);
          }
          else {
            callback(new Error('Simulated error'));
          }
        },
        getMulti: function(keys, callback) {
          setTimeout(function triggerFaultyBehaviour() {
            if (keys[0] == 'error') {
              callback(new Error('Simulated error'));
            }
            else if (keys[0] == 'duplicate') {
              callback(null, {});
              callback(null, {});
              callback(null, {});
            }
            else {
              callback(null, {});
            }
          }, 1);
        }
      };
    }
  });

  test('Errors get propagated', function runCommands() {
    m.get('foobar-key', function handleError(error) {
      assert(error, 'The get command didn\'t return an error as it should');
    });
  });

  test('Exception get caught and propagated', function runCommands() {
    m.set('foobar-key', 'bux', 10, function handleError(error) {
      assert(error, 'The set command didn\'t return an error as it should');
    });
  });

  test('Check get multi errors', function runCommands(done) {
    var gotError = false;

    function registerError(error) {
      gotError = true;
    }

    m.on('error', registerError);

    m.getMulti(
      ['error', 'signifies', 'that an error ', 'should be triggered'],
      function handleError(error) {
        assert.equal(error, null, 'Multi shouldn\'t return an error just because one shard fails');
        assert(gotError, 'The failed batch didn\'t trigger an error on the connection as it should have');

        m.removeListener('error', registerError);
        done();
      });
  });

  test('Check get multi processing errors', function runCommands(done) {
    m.getMulti(
      ['duplicate', 'signifies that', 'the batch processing', 'should be sabotaged'],
      function handleError(error) {
        assert(error, 'The get multi didn\'t return an error as it should');
        done();
      });
  });

  test('Check delete multi errors', function runCommands(done) {
    var gotError = false;

    function registerError(error) {
      gotError = true;
    }

    m.on('error', registerError);

    m.delMulti(
      ['error', 'signifies', 'that an error ', 'should be triggered'],
      function handleError(error) {
        assert.equal(error, null, 'Multi shouldn\'t return an error just because one shard fails');
        assert(gotError, 'The failed batch didn\'t trigger an error on the connection as it should have');

        m.removeListener('error', registerError);
        done();
      });
  });

  test('Check set multi errors', function runCommands(done) {
    var gotError = false;
    runtimeError = false;

    function registerError(error) {
      gotError = true;
    }

    m.on('error', registerError);

    m.setMulti(
      {
        'error': 1,
        'signifies': 2,
        'that an error ': 3,
        'should be triggered': 4
      }, 5,
      function handleError(error) {
        assert.equal(error, null, 'Multi shouldn\'t return an error just because one shard fails');
        assert(gotError, 'The failed batch didn\'t trigger an error on the connection as it should have');

        m.removeListener('error', registerError);
        done();
      });
  });

  test('Exceptions will be emitted when we don\'t have a callback', function runCommands(done) {
    runtimeError = true;
    var gotException = false;

    m.set('foobar-key', 52, 2);

    m.once('error', function handleException(error) {
      gotException = true;
    });

    setTimeout(function () {
      assert(gotException, 'The exception didn\'t get emitted as expected');
      done();
    }, 10);
  });
});
