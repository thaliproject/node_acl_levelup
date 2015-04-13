var Rx = require('rx');
var _ = require('lodash');

function noop() { }

function makeArray(item) {
  return Array.isArray(item) ? item : [item];
}

var LevelUpAdapter = {
  get: function (db, bucketKey) {
    return Rx.Observable.create(function (o) {
      db.get(bucketKey, function (err, results) {
        if (err && err.notFound) {
          o.onNext([]);
          o.onCompleted();
        } else if (err) {
          o.onError();
        } else {
          o.onNext(_.compact(results.split(',')));
          o.onCompleted();
        }
      });
    });
  },
  put: function (db, bucketKey, values) {
    return Rx.Observable.create(function (o) {
      db.put(bucketKey, values, function (err) {
        if (err) {
          o.onError(err);
        } else {
          o.onNext(null);
          o.onCompleted();
        }
      })
    });
  },
  del: function (db, bucketKey) {
    return Rx.Observable.create(function (o) {
      db.del(bucketKey, function (err) {
        if (err) {
          o.onError(err);
        } else {
          o.onNext(null);
          o.onCompleted();
        }
      })
    });
  }
}

function LevelUpBackend(db, prefix) {
  this.db = db;
  this.prefix = prefix || 'acl';
}

LevelUpBackend.prototype = {
  /**
   Begins a transaction
  */
  begin : function() {
    return [];
  },
  /**
     Ends a transaction (and executes it)
  */
  end : function(transaction, cb){
    Rx.Observable.concat(transaction).subscribe(
      noop,
      cb,
      function () {
        cb(null);
      }
    );
  },
  /**
    Cleans the whole storage.
  */
  clean : function(cb) {
    cb(null);
  },
  /**
     Gets the contents at the bucket's key.
  */
  get : function(bucket, key, cb) {
    LevelUpAdapter.get(this.db, this.bucketKey(bucket, key)).subscribe(
      function (x) {
        cb(null, x)
      },
      cb
    );
  },
  /**
  * Returns the union of the values in the given keys.
  */
  union : function(bucket, keys, cb) {
    var db = this.db;

    var source = Rx.Observable
      .fromArray(this.bucketKey(bucket, keys))
      .concatMap(function (bk) { return LevelUpAdapter.get(db, bk); })
      .toArray()
      .subscribe(
        function (results) {
          cb(null, _.uniq(_.compact(_.flatten(results))));
        },
        cb
      );
  },
  /**
   * Adds values to a given key inside a bucket.
   */
  add : function(transaction, bucket, key, values) {
    var bk = this.bucketKey(bucket, key), db = this.db;

    transaction.push(LevelUpAdapter.get(db, bk)
      .concatMap(function (results) {
        results.push.apply(results, makeArray(values));

        var uniqueResults = _.uniq(_.compact(results));
        return LevelUpAdapter.put(db, bk, uniqueResults);
      }));
  },
  /**
   * Delete the given key(s) at the bucket
  */
  del : function(transaction, bucket, keys) {
    var keyArray = makeArray(keys), bks = this.bucketKey(bucket, keyArray), db = this.db;

    transaction.push(
      Rx.Observable
        .fromArray(bks)
        .concatMap(function (bk) {
          return LevelUpAdapter.del(db, bk);
        })
    );
  },
  /**
   * Removes values from a given key inside a bucket.
   */
  remove : function(transaction, bucket, key, values){
    var db = this.db, valueArray = makeArray(values), bk = this.bucketKey(bucket, key);

    transaction.push(
      LevelUpAdapter.get(db, bk)
        .concatMap(function (results) {
          return LevelUpAdapter.put(db, bk, _.difference(results, valueArray));
        })
    );
  },
  //
  // Private methods
  //

  bucketKey : function(bucket, keys){
    if (Array.isArray(keys)) {
      return keys.map(function(key){
        return this.prefix+'_'+bucket+'@'+key;
      }, this);
    } else {
      return this.prefix+'_'+bucket+'@'+keys;
    }
  }
};

module.exports = LevelUpBackend;
