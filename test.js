var Thalia = require('./');

var loc = './tdb';

var db = new Thalia(loc, function (err) {
  if (err) {
    console.log("err", err);
    return;
  }
  console.log('opened', db.location, db.logPath);
  db.put('food', {bar: 'bit'}, function (err) {
    console.log('2', err);
    db.get('foo', function (err, resp) {
      console.log('3', err, resp);
      db.del('food', function (err, resp) {
        console.log('4', err, resp);
        db.close(function (err, resp) {
          console.log('5', err, resp);
        });
      });
    });
  });
});