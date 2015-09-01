var mongodb = require('mongodb')
var MongoClient = mongodb.MongoClient
var ObjectID = mongodb.ObjectID
var _ = require('lodash')
var co = require('co')

function connect(db, handler) {
  return MongoClient.connect(db).then(function (db) {

    return co(function *(){
      try{
        var result = yield handler(db)
        return result

      }catch(e){
        console.log('connect hanlder erro')
        console.error(e)
        db.close()
        throw e
      }
    })
  })
}

function without(arrA, arrB) {
  return _.without.apply(_, [arrA].concat(arrB))
}

function collectionNames(collections) {
  return collections.map(function (c) {
    return c.s.name
  })
}


function walkAst(ast, handler, context) {
  if (context === undefined) context = {}

  handler(ast, context)

  var dive = true
  _.forEach(ast.relations, function (relation, relationStrKey) {
    //必须生成一个新的 context, 这样 relation 才不会互相干扰
    var childContext = _.extend({}, context, {
      relationStrKey: relationStrKey,
      relation: {
        name: relation.name,
        reverse: relation.reverse,
        to: relation.to.type,
      },
      parent: {
        type: ast.type,
        fields: ast.fields,
        tracker: ast.tracker
      }
    })

    //保证只有第一个dive为true
    dive = false

    walkAst(relation.to, handler, childContext)
  })
}


function walkAstAsync(ast, handler, context) {
  if (context === undefined) context = {}

  return co(function *(){
    yield handler(ast, context)

    var dive = true
    for( var relationStrKey  in ast.relations ){
      //必须生成一个新的 context, 这样 relation 才不会互相干扰
      var relation = ast.relations[relationStrKey]
      var childContext = _.extend({}, context, {
        relationStrKey: relationStrKey,
        relation:{
          from : ast.type,
          name: relation.name,
          reverse: !!relation.reverse,
          to: relation.to.type,
        },
        parent: {
          type: ast.type,
          fields: ast.fields,
          tracker: ast.tracker
        },
        dive : dive
      })

      //保证只有第一个dive为true
      dive = false

      yield walkAstAsync(relation.to, handler, childContext)
    }
  })

}


function getRootCursor(db, astNode ){
  var parsedAttrs = parseAttrs(astNode.attrs)
  var criteria = parsedAttrs.criteria
  var options = parsedAttrs.options


  console.log('get root cursors', astNode.type, criteria, options)

  var collection =  db.collection( astNode.type )

  return collection.find( criteria ).skip( defaultUndefined(options.skip,0)).limit( defaultUndefined(options.limit ,25))

}


//这里和根节点不同，因为要再次匹配，所以它直接获取到了数据
function getCurrentCursor(db, astNode, nodeIds ) {

  var parsedAttrs = parseAttrs(astNode.attrs)
  var criteria = parsedAttrs.criteria
  var options = parsedAttrs.options

  criteria._id = {
    $in: nodeIds.map(function (id) {
      return ObjectID(id)
    })
  }
  //console.log('get normal cursor',typ
  // e, nodeIds)
  var collection = db.collection(astNode.type)

  //限制子元素
  return collection.find(criteria).skip(defaultUndefined(options.skip, 0)).limit(defaultUndefined(options.limit, 25))

}

function defaultUndefined( o , v){
  return o=== undefined ? v : o
}


function zipObject( keys, values ){
  var result = {}
  keys.forEach(function( key, i ){
    if( typeof values=== 'function'){
      result[key] = values(key)
    }else if( typeof values !== 'object' || values.length === undefined){
      result[key] = values
    }else{
      result[key] = values[i]
    }
  })
  return result
}

function map( obj, handler ){
  return Object.keys(obj).map(function(key){
    return handler( obj[key], key)
  })
}

function exist( obj , keys ){
  var cursor = obj
  return keys.every(function(key){
    if( cursor[key] === undefined ) return false
    cursor = cursor[key]
    return true
  })
}


//TODO 用 UniversalObject 去掉

function parseAttrs( attrs ){
  var result = { criteria : {}, options : {}}

  _.forEach( attrs.data, function( attrValue, attrKey ){
    if( /^_/.test( attrKey )){
      // 拿到 limit 等选项，这里结构比较复杂，要分别处理
      result.options[attrKey.slice(1)] = attrValue
    }else{
      result.criteria[attrKey] = attrValue
    }
  })

  return result
}


module.exports = {
   connect,
   without,
  collectionNames,
  walkAst,
  walkAstAsync,
  getRootCursor,
  getCurrentCursor,
  defaultUndefined,
  zipObject,
  map,
  exist
}