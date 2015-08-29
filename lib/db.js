var util = require('../util')


function getCollectionNames( db ){
  return db.collections().then( function( collections){
    return collections.map(function(c){
      return c.s.name
    })
  })
}


function createCollections( db, collectionsToCreate ){
  return Promise.all( collectionsToCreate.map(function(collectionToCreate){
    return db.createCollection( collectionToCreate)
  }))
}



module.exports = {
  getCollectionNames,
  createCollections
}