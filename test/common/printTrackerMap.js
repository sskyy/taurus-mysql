var Table = require('cli-table');

var stringify = function( obj ){
  return JSON.stringify(obj, undefined,2)
}


var mapValue = function( obj, handler ){
  var output = {}
  Object.keys(obj).forEach(function(key){
    output[key] = handler(obj[key], key)
  })
  return output
}


module.exports = function( trackerMap ){
  var trackers = Object.keys( trackerMap)


  var table = new Table({head:trackers});


  var data  = Object.keys( trackerMap).map(function( tracker, i){
      return stringify( trackerMap[tracker])
  })

  table.push(data)

  console.log( table.toString())
}