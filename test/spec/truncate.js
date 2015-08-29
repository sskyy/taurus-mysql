var assert = require('assert')
var co = require('co')
var mongodb = require('mongodb')
var MongoClient = mongodb.MongoClient
var ObjectID = mongodb.ObjectID
var _ = require('lodash')
var util = require('../../lib/util')
var pull = require('../../lib/collection').pull

var log = require('pretty-log-2').pp
var printTrackerMap = require('../common/printTrackerMap')


describe('truncate', function () {
  var database = 'mongodb://localhost:27017/test'

  it('empty', function (done) {

    util.connect(database, function (db) {

      var TodoCollection = db.collection('Todo')
      var UserCollection = db.collection('User')
      TodoCollection.remove({})
      UserCollection.remove({})

      co(function *() {
        var todos = yield (TodoCollection.find({}).toArray())
        var users = yield (UserCollection.find({}).toArray())

        assert.equal(todos.length, 0)
        assert.equal(users.length, 0)
        done()

      }).catch(done)

    }).catch(done)

  })

})