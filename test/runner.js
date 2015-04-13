var Acl = require('../'), tests = require('./tests');

var levelup = require('levelup');
var memdown = require('memdown');
var db = levelup({ db: memdown });

describe('Levelup', function () {
  before(function () {
    this.backend = new Acl.levelupBackend(db, 'acl');
  });

  run();
});

function run() {
  Object.keys(tests).forEach(function (test) {
    tests[test]()
  })
}
