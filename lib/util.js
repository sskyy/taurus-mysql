var _ = require('lodash')
var co = require('co')

function promisify( obj, fnName ){
  var _fn = obj[fnName]

  obj[fnName] = function(){
    var args = Array.prototype.slice.call(arguments, 0)
    return new Promise(function(resolve, reject){
      _fn.apply(obj,args.concat(function(err, result){
        console.log('execute', fnName)
        if( err) return reject( err)
        return resolve(result)
      }))
  })}
}

function without(arrA, arrB) {
  return _.without.apply(_, [arrA].concat(arrB))
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
        to: relation.to.type
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


  return co(function *() {

    yield handler(ast, context )

    var dive = true
    for (var relationStrKey  in ast.relations) {
      //必须生成一个新的 context, 这样 relation 才不会互相干扰
      var relation = ast.relations[relationStrKey]
      var childContext = _.extend({}, context, {
        relationStrKey: relationStrKey,
        relation: {
          from: ast.type,
          name: relation.name,
          reverse: !!relation.reverse,
          to: relation.to.type
        },
        parent: ast,
        dive: dive
      })

      //保证只有第一个dive为true
      dive = false
      yield walkAstAsync(relation.to, handler, childContext)
    }
  })

}




function defaultUndefined(o, v) {
  return o === undefined ? v : o
}


function zipObject(keys, values) {
  var result = {}
  keys.forEach(function (key, i) {
    if (typeof values === 'function') {
      result[key] = values(key)
    } else if (typeof values !== 'object' || values.length === undefined) {
      result[key] = values
    } else {
      result[key] = values[i]
    }
  })
  return result
}

function map(obj, handler) {
  return Object.keys(obj).map(function (key) {
    return handler(obj[key], key)
  })
}

function exist(obj, keys) {
  var cursor = obj
  return keys.every(function (key) {
    if (cursor[key] === undefined) return false
    cursor = cursor[key]
    return true
  })
}


//TODO 用 UniversalObject 去掉

function parseAttrs(attrs) {
  var result = {criteria: {}, options: {}}

  _.forEach(attrs.data, function (attrValue, attrKey) {
    if (/^_/.test(attrKey)) {
      // 拿到 limit 等选项，这里结构比较复杂，要分别处理
      result.options[attrKey.slice(1)] = attrValue
    } else {
      result.criteria[attrKey] = attrValue
    }
  })

  return result
}

function ensure( obj, key, defaultValue){
  if( obj[key] === undefined ) obj[key] = defaultValue
}

module.exports = {
  without,
  walkAst,
  walkAstAsync,
  defaultUndefined,
  zipObject,
  map,
  exist,
  promisify,
  ensure
}