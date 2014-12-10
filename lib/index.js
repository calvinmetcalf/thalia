var fs = require('fs');
var EE = require('events').EventEmitter;
var inherits = require('util').inherits;
var path = require('path');
var mkdirp = require('mkdirp');
var ldj = require('ldjson-stream');
var StringMap = require("stringmap");
var through2 = require('through2');
function noop(){}
var nextTick = setImmediate || process.nextTick;
module.exports = Thalia;
inherits(Thalia, EE);
function Thalia(location, cb) {
  EE.call(this);
  this.location = path.resolve(location);
  this.logs = void 0;
  this.logLen = 0;
  this.logNum = 0;
  this.logPath = void 0;
  if (typeof cb === 'function') {
    this.once('error', function (err) {
      cb(err);
      this.removeListener('open', cb);
    });
    this.once('open', function () {
      cb();
      this.removeListener('error', cb);
    });
  }
  this._opened = false;
  this.open();
  this.cache = new StringMap();
}
Thalia.prototype.open = function open() {
  var self = this;
  mkdirp(self.location, function (err) {
    if (err) {
      return self.emit('error', err);
    }
    self.findLogNum(function (err) {
      if (err) {
        return self.emit('error', err);
      }
      path.join(self.location, 'log', self.logNum++);
      self.processLogs(readLogs);
      function readLogs(err) {
        if (err) {
          return self.emit('error', err);
        }
        self.logs = fs.createWriteStream(self.logPath, {
          flags: 'a'
        });
        self._opened = true;
        self.emit('open');
      }
    });
  });
};

Thalia.prototype.processLogs = function processLogs(cb) {
  var self = this;
  fs.createReadStream(self.logPath).on('error', function (e) {
    if (e.code === 'ENOENT') {
      cb();
    } else {
      return cb(e);
    }
  }).pipe(through(function (chunk, _, next) {
    self.logLen += cunk.length;
    this.push(chunk);
    next();
  })).pipe(ldj.parse()).pipe(through2.obj(function (log, _, next) {
    self.cache.set(log.key, log.value);
    next();
  }, function (next) {
    next();
    cb();
  })).on('error', cb);
};

Thalia.prototype.get = function get(key, cb) {
  if (!this._opened) {
    return nextTick(function () {
      cb(new Error('db not opened'));
    });
  }
  var self = this;
  nextTick(function () {
    if (self.cache.get(key)) {
      cb(null, self.cache.get(key));
    } else {
      cb(new Error('not found'));
    }
  });
};

Thalia.prototype.put = function put(key, value ,cb) {
  if (!this._opened) {
    return nextTick(function () {
      cb(new Error('db not opened'));
    });
  }
  var self = this;
  var toWrite = JSON.stringify({key:key, value:value}) + '\n';
  this.logs.write(toWrite, function (err) {
    if (err) {
      return cb(err);
    }
    self.cache.set(key, value);
    cb();
  });
};

Thalia.prototype.del = function del(key, cb) { 
  if (!this._opened) {
    return nextTick(function () {
      cb(new Error('db not opened'));
    });
  }
  var self = this;
  var toWrite = JSON.stringify({key:key, value:false}) + '\n';
  this.logs.write(toWrite, function (err) {
    if (err) {
      return cb(err);
    }
    self.cache.set(key, false);
    cb();
  });
};

Thalia.prototype.close = function close(cb) {
  if (!this._opened) {
    return nextTick(function () {
      cb(new Error('db not opened'));
    });
  }
  this.logs.end(cb);
};
Thalia.prototype.findLogNum = function (cb) {
  var self = this;
  fs.readdir(this.location, function (err, files) {
    if (err) {
      return cb(err);
    }
    filenums = files.filter(matchFile).map(getFileNumber).sort(function (a, b){
      return a - b;
    });
    if (!files.length) {
      return cb();
    }
    self.logNum = filenums.pop();
    //todo:
    // if the last 2 numbers are sequential
    // that likly indicates something crashed
    // while swapping logs
    cb();
  });
};
Thalia.prototype.switchLogs = function switchlogs(cb) {
  var prevlogPath = this.logPath;
  this.logPath = path.join(this.location, 'log', this.logNum++);
  var oldLogs = this.logs;
  this.logs = fs.createWriteStream(self.logPath, {
    flags: 'a'
  });
  this.cache.clear();
  oldlogs.end(function (err) {
    if (err) {
      return cb(err);
    }
    cb(null, prevlogPath);
  });
};
function matchFile(file){
  return file.match(/^log\d+$/);
}
function getFileNumber(file) {
  return parseInt(file.match(/^log(\d+)$/)[1], 10);
}