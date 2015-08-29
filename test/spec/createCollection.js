var assert = require('assert')
var co = require('co')
var _ = require('lodash')
var util = require('../../lib/util')




describe('ensureCollection', function(){
  var database = 'mongodb://localhost:27017/test'
  it('checkCollection', function(done){
    var collectionsToEnsure = ['Todo','User']

      util.connect(database, function( db ){
        return  co(function*(){

          var collections = util.collectionName(yield db.collections())

          console.log('collections', collections)
          var collectionsToCreate = util.without( collectionsToEnsure, collections)

          console.log( collectionsToCreate)

          for( var i in collectionsToCreate){
            yield db.createCollection( collectionsToCreate[i])
          }

          var collectionsToCheck = util.collectionName(yield db.collections())
          assert.equal(util.without( collectionsToEnsure, collectionsToCheck).length, 0 )

          done()
        })
      }).catch( done )
  })
})