var assert = require('assert')
var co = require('co')
var MongoClient = require('mongodb').MongoClient
var _ = require('lodash')
var util = require('../../lib/util')

function random(arr) {
  return arr[_.random(0, arr.length - 1)]
}


describe('ensureCollection', function () {
  var database = 'mongodb://localhost:27017/test'
  var todos = ['swim', 'sing', 'dance', 'shot']


  it('query with relation', function (done) {


    util.connect(database, function (db) {
      return co(function*() {

        var TodoCollection = db.collection('Todo')

        var batchResult = yield TodoCollection.find({}).toArray()

        console.log(batchResult)

        done()
      })
    }).catch(done)
  })
})