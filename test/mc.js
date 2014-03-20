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

  test('Matches output form PHP implementation', function () {
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

suite('Basic commands', function() {
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
    lib.async.auto({
      setItem: function setItem(callback) {
        m.set('expire-test', 'foo', 1, callback);
      },
      getItem: ['setItem', function getItem(callback) {
        setTimeout(function checkExpired() {
          m.get('expire-test', function getResult(error, value) {
            if (error) return callback(error);
            assert.equal(value, false);
            done();
          });
        }, 1100);
      }],
    }, done);
  });
});

suite('Batch fetch', function runCommands(done) {
  var m = new lib.sharder({
    servers: '127.0.0.1:11211',
    options: {
      namespace: 'mc:sharder-test:'
    }
  });

  test('Fetch multiple values', function runCommands(done) {
    var values = {
      'multi-a': 'a',
      'multi-b': 'b',
      'multi-c': 'c'
    };
    var keys = Object.getOwnPropertyNames(values);

    var queue = lib.async.queue(function setKey(key, callback) {
      m.set(key, values[key], 5, callback);
    }, 1);

    queue.push(keys);
    queue.drain = function fetchItems(error) {
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
            assert.equal(result['multi-a'], values['multi-a'], 'Get multi didn\'t return the correct value when fetching a single object');
            callback();
          });
        }
      ], done);
    };
  });
});
