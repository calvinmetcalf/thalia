var fs = require('fs');
var EE = require('events').EventEmitter;
var inherits = requrie('utils').inherits;
var path = require('path');
var mkdirp = require('mkdirp');
function noop(){}
var nextTick = setImmediate || process.nextTick;
module.exports = Thalia;
inherits(Thalia, EE);
function Thalia(location, cb) {
  EE.call(this);
  this.location = path.resolve(location);
  this.logs = void 0;
  this.logPath = path.join(this.location, 'log');
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
  this.cache = {};
}
Thalia.prototype.open = function open() {
  var self = this;
  mkdirp(self.location, function (err) {
    if (err) {
      self.emit('error', err);
    }
    fs.readFile(self.logPath, function (err, logs) {
      if (logs) {
        self.processLogs(logs);
      }
      self.logs = fs.createWriteStream(self.logPath, {
        flag: 'a'
      });
      self._opened = true;
      self.emit('open');
    });
  });
}
Thalia.prototype.processLogs = function processLogs(logs) {
  var self = this;
  logs.toString().split('\n').forEach(function (line) {
    try {
      var log = JSON.parse(line);
      var key = '$' + log.key;
      var value = log.value;
      self.cache[key] = value;
    } catch(e){}
  });
};

Thalia.prototype.get = function get(key, cb) {
  if (!this._opened) {
    return nextTick(function () {
      cb(new Error('db not opened'));
    });
  }
  key = '$' + key;
  var self = this;
  nextTick(function () {
    if (self.cache[key]) {
      cb(null, self.cache[key]);
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
  var $key = '$' + key;
  var self = this;
  var toWrite = JSON.stringify({key:key, value:value}) + '\n';
  this.logs.write(toWrite, function (err) {
    if (err) {
      return cb(err);
    }
    self.cache[$key] = value;
    cb();
  });
};
Thalia.prototype.del = function del(key, cb) { 
  if (!this._opened) {
    return nextTick(function () {
      cb(new Error('db not opened'));
    });
  }
  var $key = '$' + key;
  var self = this;
  var toWrite = JSON.stringify({key:key, value:false}) + '\n';
  this.logs.write(toWrite, function (err) {
    if (err) {
      return cb(err);
    }
    self.cache[$key] = false;
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