var assert = require('assert')
var co = require('co')
var mongodb = require('mongodb')
var MongoClient = mongodb.MongoClient
var ObjectID = mongodb.ObjectID
var _ = require('lodash')
var util = require('../../lib/util')
var destroy = require('../../lib/collection').destroy


function random(arr) {
  return arr[_.random(0, arr.length - 1)]
}

function fill( keys, value){
  return _.zipObject( keys, keys.map(function(){return value}))
}


describe('ensureCollection', function () {
  var database = 'mongodb://localhost:27017/test'
  var todosToInsert = ['swim', 'sing', 'dance', 'shot']
  var usersToInsert = [{
    name: 'john',
    age: 20,
    gender: 'male'
  }, {
    name: 'jason',
    age: 21,
    gender: 'male'
  }, {
    name: 'trinity',
    age: 22,
    gender: 'female'
  }]



  it('insert related data', function (done) {

    util.connect(database, function (db) {
      return co(function*() {


        var TodoCollection = db.collection('Todo')
        var UserCollection = db.collection('User')

        var todoResult = yield TodoCollection.insert({content: random(todosToInsert)})
        var userResult = yield UserCollection.insert([random(usersToInsert), random(usersToInsert)])

        //建立双向联系

        var insertedTodoIds = todoResult.ops.map(function( todo ){
          return  ObjectID(todo._id)
        })

        var insertedUserIds = userResult.ops.map(function( user){
          return  ObjectID( user._id )
        })

        yield Promise.all(insertedTodoIds.map(function (insertedTodoId) {
          return TodoCollection.update({_id: insertedTodoId}, {
            $set: {
              _R_mentioned_User: fill(insertedUserIds,{type:'User'})
            }
          })
        }))

        yield Promise.all(insertedUserIds.map(function (insertedUserId) {
          return UserCollection.update({_id: insertedUserId}, {
            $set: {
              _R_Todo_mentioned: fill(insertedTodoIds,{type:'Todo'})
            }
          })
        }))


        //尝试 query

        console.log( yield (TodoCollection.find({
            _id: {
              $in: insertedTodoIds
            }
          }).toArray()) )


        console.log( yield (UserCollection.find({
          _id: {
            $in: insertedUserIds
          }
        }).toArray()) )


        //尝试删除

        yield Promise.all(insertedTodoIds.map( function( insertedTodoId){

          return co(function *(){
            yield destroy( 'Todo', insertedTodoId)
          })

        })).then(function(){
          console.log("========")
          return co(function *(){
            console.log( yield (TodoCollection.find({
              _id: {
                $in: insertedTodoIds
              }
            }).toArray()) )


            console.log( yield (UserCollection.find({
              _id: {
                $in: insertedUserIds
              }
            }).toArray()) )
          })



        })


        done()

      })
    }).catch(done)


  })
})