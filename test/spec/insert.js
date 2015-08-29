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


  it('insert raw data', function (done) {

    util.connect(database, function (db) {
      return co(function*() {

        var TodoCollection = db.collection('Todo')

        var batchResult = yield TodoCollection.insert({content: random(todos)})
        var oneResult = yield TodoCollection.insertOne({content: random(todos)})

        console.log(batchResult, oneResult)

        done()
      })
    }).catch(done)
  })


})