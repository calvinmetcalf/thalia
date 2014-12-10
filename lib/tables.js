var fs = require('fs');
var ldj = require('ldjson-stream');
var StringMap = require("stringmap");
var through2 = require('through2');
function entry(key, value) {
  key = toBuffer(key);
  value = toBuffer(value);
  var klen = key.length;
  var vlen = value.length;
  var headerLen = 6;
  var header = new Buffer(6);
  header.writeUInt32LE(klen + vlen, 0);
  header.writeUInt16LE(klen, 4);
  return Buffer.concat([header, key, value], klen + vlen + headerLen);
}


function toBuffer(something) {
  if (something === false) {
    return new Buffer('');
  }
  if (Buffer.isBuffer(something)) {
    return something;
  }
  if (typeof something !== 'string') {
    something = JSON.stringify(something);
  }
  return new Buffer(something);
}
function toString(something) {
  if (typeof something === 'string') {
    return something;
  }
  if (Buffer.isBuffer(something)) {
    return something.toString();
  }
  return JSON.stringify(something);
}
function logToTableEntry(log) {
  var key = toString(log.key);
  var entry = entry(key, log.value);
  return {
    key: key,
    entry: entry
  };
}
function parseTable() {
  var cache = new Buffer('');
  var chunkSize, keySize, key, value;
  return through2(function (chunk, _, next) {
    cache = Buffer.concat([cache, chunk]);
    while (true) {
      if (typeof chunkSize === 'undefined') {
        if (cache.length < 6) {
          return next();
        }
        chunkSize = cache.readUInt32LE(0);
        keySize = header.readUInt16LE(4);
        cache = cache.slice(6);
      }
      if (cache.length < chunkSize) {
        return next();
      }
      this.push({
        key: cache.slice(0, keySize).toString(),
        value: cache.slice(0, chunkSize)
      });
      cache = cache.slice(chunkSize);
    }
  });
}
function mergeLogs(left, right) {
  var done = {};
  var leftData, leftCB, rightData, rightCB;
  var out = through.obj();
  var index = {};
  var ret = {
    stream: out,
    index: index
  };
  var len = 0;
  left.pipe(through2.obj(function (chunk, _, next) {
    leftData = chunk;
    leftCB = next;
    checkData();
  }, function (next) {
    leftData = done;
    checkData();
    next();
  }));
  right.pipe(through2.obj(function (chunk, _, next) {
    rightData = chunk;
    rightCB = next;
    checkData();
  }, function (next) {
    rightData = done;
    checkData();
    next();
  }));
  function write(data) {
    out.write(data);
    index[data.key] = len;
    len += data.entry.length;
  }
  function checkData() {
    if (!leftData || !rightData) {
      return;
    }
    if (leftData === done) {
      if (rightData === done) {
        return out.end();
      }
      write(rightData);
      return rightCB();
    }
    if (rightData === done) {
      write(leftData);
      return leftCB();
    }
    if (leftData.key < rightData.key) {
      write(leftData);
      return leftCB();
    } else {
      write(rightData);
      return rightCB();
    }
  }
  return ret;
}
exports.logToStream = function logToStream(path) {
  var things = new StringMap();
  return fs.createReadStream(path).pipe(ldj.parse()).pipe(through2.obj(function (log, _, next){
    var entry = logToTableEntry(log);
    things.set(entry.key, entry);
    next();
  }, function (next) {
    var out = sm1.values();
    out.sort(function (a, b){
      return a.key < b.key ? -1: 1;
    });
    out.forEach(function (item) {
      this.push(item);
    }, this);
    next();
  }));
};