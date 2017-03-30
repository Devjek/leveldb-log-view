var request = require('request');
var through2 = require('through2');
var JSONStream = require('JSONStream');
var bytewise = require('bytewise');
var diff = require('changeset');
var config = require('./config');
var db_util = require('./leveldb_util');

//#region utility function                                               //
var checksynctime = function(db, cb){
  request({
    method: 'GET',
    uri: config.domain + 'api/dbs/synctime/' + db,
    json: true,
  }, function (err, httpResponse, body) {
    if (err) {
      cb(err, null);
    } else {
      if(body.ok == false) {
        body = {'lastsynctime' : 0 };
      }else{
        body.lastsynctime += 1;
      }

      cb(null, body);
    }
  });
};

var login = function (db, cb) {
  request({
    method: 'POST',
    body: config.login_obj,
    uri: config.source_domain +'/login',
    json: true,
  }, function (err, httpResponse, data) {
    checksynctime(db, cb);
  });
}
//#endregion                                                             //

var sync_attendance = function(onFinish)
{
  var startkey = '';

  var sub_key = function(obj) {
    var value = [];
    var keys = ['hostid','year',
    'semester','class','room',
    'subject'];

    keys.forEach(function(key) {
      value.push(obj[key]);
    });

    return bytewise.encode(value).toString('hex');
  }

  var get_attenddata = function(err, resdata){
    if(err){
      onFinish(err, 'Error!');
    }else{
      startkey = resdata.lastsynctime;

      request({
        url: config.source_domain+ '/api/log/attendance',
        qs: {'apikey': config.apikey,
        'start': startkey
      }})
      .pipe(JSONStream.parse('*'))
      .pipe(through2.obj(function(chunk, enc, cb) {
        var _value = diff.apply(chunk.value.changes, {});
        var obj  = {'key': chunk.value.key, 'value': _value};
        var key = sub_key(_value);
        var self = this;
        startkey = chunk.key;
        if(_value.data){
          if(_value.data.length > 0)
          {
            request({
              method: 'GET',
              uri: config.domain + 'api/dbs/attendance/'+ chunk.value.key,
              json: true
            },function(err, response, body) {
              var subject_desc = String(_value.hostid+ ':'+ _value.year+ ':'+ _value.semester+ ':'+ _value.class+
              ':'+ _value.room+ ':'+ _value.subject);

              if(body.ok != false) {
                self.push({'method': 'dec', 'ts': chunk.key, 'key': key, 'value': {'total': body.data.length, 'subject': subject_desc}});

                for(let i = 0; i < body.data.length; i++){
                  for(let j = 0; j < body.data[i].student.length; j++){
                    var attend_desc = String(body.data[i].student[j].desc);

                    self.push({'method': 'attendance_individual', 'key': bytewise.encode([body.data[i].student[j].cid]).toString('hex'),
                    'ts': chunk.key, 'value': {'desc': attend_desc, 'amount': -1, 'subject': subject_desc, 'cid': _value.data[i].student[j].cid}});
                  }
                }
              }

              for(let i = 0; i < _value.data.length; i++){
                for(let j = 0; j < _value.data[i].student.length; j++){
                  var attend_desc = String(_value.data[i].student[j].desc);

                  self.push({'method': 'attendance_individual', 'key': bytewise.encode([_value.data[i].student[j].cid]).toString('hex'),
                  'ts': chunk.key, 'value': {'desc': attend_desc, 'amount': 1, 'subject': subject_desc, 'cid': _value.data[i].student[j].cid}});
                }
              }

              self.push({'method': 'inc', 'key': key, 'ts': chunk.key, 'value': {'total': _value.data.length, 'subject': subject_desc}});

              request({
                method: 'POST',
                uri: config.domain + 'api/dbs/attendance/'+ chunk.value.key,
                json: true,
                body: _value
              },function(err, response, body) {
                cb();
              });
            });
          }else{
            cb();
          }
        }else{
          cb();
        }
      }))
      .pipe(through2.obj(function(chunk, enc, cb) {
        if(chunk.method === 'dec' || chunk.method === 'inc'){
          request({
            method: 'GET',
            uri: config.domain + 'api/dbs/attendance_summary/'+ chunk.key,
            json: true
          },function(err, response, body) {
            var obj = body;
            if(!body.total) {
              obj = {'total': 0};
            }

            if(chunk.method == 'dec') {
              obj.total -= chunk.value.total;
            } else {
              obj.total += chunk.value.total;
            }

            // view of database does not sync
            if(obj.total < 0) obj.total = 0;
            request({
              method: 'POST',
              uri: config.domain + 'api/dbs/attendance_summary/'+ chunk.key,
              json: true,
              body: obj
            },function(err, response, body) {
              if(body.ok) {
                var tmpArr = chunk.value.subject.split(':');
                request({
                  method: 'GET',
                  uri: config.domain + 'api/dbs/attendance_host_summary/'+ bytewise.encode([tmpArr[0], parseInt(tmpArr[1]), parseInt(tmpArr[2])]).toString('hex'),
                  json: true
                },function(err, response, body){
                  if(body.ok == false ) body = {};
                  if(body[String(tmpArr[3] + ':' + tmpArr[4])]){
                    if(body[String(tmpArr[3] + ':' + tmpArr[4])][String(tmpArr[5])]){
                      body[String(tmpArr[3] + ':' + tmpArr[4])][String(tmpArr[5])]['total'] = obj.total;
                    }else{
                      body[String(tmpArr[3] + ':' + tmpArr[4])][String(tmpArr[5])] = {};
                      body[String(tmpArr[3] + ':' + tmpArr[4])][String(tmpArr[5])]['total'] = obj.total;
                    }
                  }else{
                    body[String(tmpArr[3] + ':' + tmpArr[4])] = {};
                    body[String(tmpArr[3] + ':' + tmpArr[4])][String(tmpArr[5])] = {};
                    body[String(tmpArr[3] + ':' + tmpArr[4])][String(tmpArr[5])]['total'] = obj.total;
                  }

                  request({
                    method: 'POST',
                    uri: config.domain + 'api/dbs/attendance_host_summary/'+ bytewise.encode([tmpArr[0], parseInt(tmpArr[1]), parseInt(tmpArr[2])]).toString('hex'),
                    json: true,
                    body: body
                  },function(err, response, body) {
                    if(body.ok) {
                      request({
                        method: 'POST',
                        uri: config.domain + 'api/dbs/synctime/attendance',
                        json: true,
                        body: {'lastsynctime': chunk.ts}
                      },function(err,response,body) {
                        if(body.ok){
                          cb();
                        }else{
                          onFinish(err, body);
                        }
                      });
                    }
                  });
                });
              }
            });
          });
        }else if(chunk.method === 'attendance_individual'){
          request({
            method: 'GET',
            uri: config.domain + 'api/dbs/attendance_individual/'+ chunk.key,
            json: true
          },function(err, response, body){
            var obj = body;
            if(body.ok == false) {
              obj = {};
            }

            if(!body[chunk.value.subject]){
              var tmp = {};
              tmp[chunk.value.desc] = chunk.value.amount;
              obj[chunk.value.subject] = tmp;
            }else{
              if(!obj[chunk.value.subject][chunk.value.desc]){
                obj[chunk.value.subject][chunk.value.desc] = chunk.value.amount;
              }else{
                obj[chunk.value.subject][chunk.value.desc] += chunk.value.amount;
                if(obj[chunk.value.subject][chunk.value.desc] === 0){
                  delete obj[chunk.value.subject][chunk.value.desc];
                }
              }
            }

            request({
              method: 'POST',
              uri: config.domain + 'api/dbs/attendance_individual/'+ chunk.key,
              json: true,
              body: obj
            },function(err, response, body) {
              if(body.ok) {
                var tmpArr = chunk.value.subject.split(':');
                request({
                  method: 'GET',
                  uri: config.domain + 'api/dbs/attendance_host_summary/'+ bytewise.encode([tmpArr[0], parseInt(tmpArr[1]), parseInt(tmpArr[2])]).toString('hex'),
                  json: true
                },function(err, response, body){
                  if(body.ok == false ) body = {};
                  if(body[String(tmpArr[3] + ':' + tmpArr[4])]){
                    if(body[String(tmpArr[3] + ':' + tmpArr[4])][String(tmpArr[5])]){
                      if(!body[String(tmpArr[3] + ':' + tmpArr[4])][String(tmpArr[5])]['student']){
                        body[String(tmpArr[3] + ':' + tmpArr[4])][String(tmpArr[5])]['student'] = {};
                      }

                      if(!body[String(tmpArr[3] + ':' + tmpArr[4])][String(tmpArr[5])]['student'][chunk.value.cid]){
                        var tmp = {};
                        tmp[chunk.value.desc] = chunk.value.amount;
                        body[String(tmpArr[3] + ':' + tmpArr[4])][String(tmpArr[5])]['student'] = {};
                        body[String(tmpArr[3] + ':' + tmpArr[4])][String(tmpArr[5])]['student'][chunk.value.cid] = tmp;
                      }else{
                        if(!body[String(tmpArr[3] + ':' + tmpArr[4])][String(tmpArr[5])]['student'][chunk.value.cid][chunk.value.desc]){
                          body[String(tmpArr[3] + ':' + tmpArr[4])][String(tmpArr[5])]['student'][chunk.value.cid][chunk.value.desc] = chunk.value.amount;
                        }else{
                          body[String(tmpArr[3] + ':' + tmpArr[4])][String(tmpArr[5])]['student'][chunk.value.cid][chunk.value.desc] += chunk.value.amount;
                          if(body[String(tmpArr[3] + ':' + tmpArr[4])][String(tmpArr[5])]['student'][chunk.value.cid][chunk.value.desc] === 0){
                            delete body[String(tmpArr[3] + ':' + tmpArr[4])][String(tmpArr[5])]['student'][chunk.value.cid][chunk.value.desc];
                          }
                        }
                      }
                    }else{
                      body[String(tmpArr[3] + ':' + tmpArr[4])][String(tmpArr[5])] = {};
                      var tmp = {};
                      tmp[chunk.value.desc] = chunk.value.amount;
                      body[String(tmpArr[3] + ':' + tmpArr[4])][String(tmpArr[5])]['student'] = {};
                      body[String(tmpArr[3] + ':' + tmpArr[4])][String(tmpArr[5])]['student'][chunk.value.cid] = tmp;
                    }
                  }else{
                    body[String(tmpArr[3] + ':' + tmpArr[4])] = {};
                    body[String(tmpArr[3] + ':' + tmpArr[4])][String(tmpArr[5])] = {};
                    var tmp = {};
                    tmp[chunk.value.desc] = chunk.value.amount;
                    body[String(tmpArr[3] + ':' + tmpArr[4])][String(tmpArr[5])]['student'] = {};
                    body[String(tmpArr[3] + ':' + tmpArr[4])][String(tmpArr[5])]['student'][chunk.value.cid] = tmp;
                  }

                  request({
                    method: 'POST',
                    uri: config.domain + 'api/dbs/attendance_host_summary/'+ bytewise.encode([tmpArr[0], parseInt(tmpArr[1]), parseInt(tmpArr[2])]).toString('hex'),
                    json: true,
                    body: body
                  },function(err, response, body) {
                    if(body.ok) {
                      request({
                        method: 'POST',
                        uri: config.domain + 'api/dbs/synctime/attendance',
                        json: true,
                        body: {'lastsynctime': chunk.ts}
                      },function(err,response,body) {
                        if(body.ok){
                          cb();
                        }else{
                          onFinish(err, body);
                        }
                      });
                    }
                  });
                });
              }else{
                onFinish(err, body);
              }
            });
          });
        }
      }))
      .on('finish',function() {
        onFinish(null, 'Finish!');
      });
    }
  }

  login('attendance', get_attenddata);
};

var sync_newindicator = function(onFinish)
{
  var startkey = '';

  var cal_GPAX = function(obj, attr){
    var elementary_list = ['ประถมศึกษาปีที่ 1', 'ประถมศึกษาปีที่ 2', 'ประถมศึกษาปีที่ 3',
    'ประถมศึกษาปีที่ 4', 'ประถมศึกษาปีที่ 5', 'ประถมศึกษาปีที่ 6'];
    var secondary_list = ['มัธยมศึกษาปีที่ 1', 'มัธยมศึกษาปีที่ 2', 'มัธยมศึกษาปีที่ 3'];
    var highschool_list = ['มัธยมศึกษาปีที่ 4', 'มัธยมศึกษาปีที่ 5', 'มัธยมศึกษาปีที่ 6'];
    var gp = 0;
    var ca = 0;
    var new_attr = '';
    var attr_arr = attr.split(':');

    if(elementary_list.indexOf(attr_arr[3]) !== -1){
      let index = elementary_list.indexOf(attr_arr[3]);
      gp += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 0 - index) + ':2:' + elementary_list[0]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 0 - index) + ':2:' + elementary_list[0]].gp : 0);
      gp += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 1 - index) + ':2:' + elementary_list[1]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 1 - index) + ':2:' + elementary_list[1]].gp : 0);
      gp += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 2 - index) + ':2:' + elementary_list[2]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 2 - index) + ':2:' + elementary_list[2]].gp : 0);
      gp += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 3 - index) + ':2:' + elementary_list[3]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 3 - index) + ':2:' + elementary_list[3]].gp : 0);
      gp += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 4 - index) + ':2:' + elementary_list[4]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 4 - index) + ':2:' + elementary_list[4]].gp : 0);
      gp += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 5 - index) + ':2:' + elementary_list[5]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 5 - index) + ':2:' + elementary_list[5]].gp : 0);

      ca += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 0 - index) + ':2:' + elementary_list[0]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 0 - index) + ':2:' + elementary_list[0]].ca : 0);
      ca += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 1 - index) + ':2:' + elementary_list[1]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 1 - index) + ':2:' + elementary_list[1]].ca : 0);
      ca += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 2 - index) + ':2:' + elementary_list[2]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 2 - index) + ':2:' + elementary_list[2]].ca : 0);
      ca += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 3 - index) + ':2:' + elementary_list[3]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 3 - index) + ':2:' + elementary_list[3]].ca : 0);
      ca += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 4 - index) + ':2:' + elementary_list[4]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 4 - index) + ':2:' + elementary_list[4]].ca : 0);
      ca += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 5 - index) + ':2:' + elementary_list[5]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 5 - index) + ':2:' + elementary_list[5]].ca : 0);

      new_attr = attr_arr[0] + ':ประถมศึกษา';
    }else if(secondary_list.indexOf(attr_arr[3]) !== -1){
      let index = secondary_list.indexOf(attr_arr[3]);
      gp += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 0 - index) + ':1:' + secondary_list[0]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 0 - index) + ':1:' + secondary_list[0]].gp : 0);
      gp += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 0 - index) + ':2:' + secondary_list[0]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 0 - index) + ':2:' + secondary_list[0]].gp : 0);
      gp += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 1 - index) + ':1:' + secondary_list[1]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 1 - index) + ':1:' + secondary_list[1]].gp : 0);
      gp += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 1 - index) + ':2:' + secondary_list[1]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 1 - index) + ':2:' + secondary_list[1]].gp : 0);
      gp += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 2 - index) + ':1:' + secondary_list[2]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 2 - index) + ':1:' + secondary_list[2]].gp : 0);
      gp += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 2 - index) + ':2:' + secondary_list[2]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 2 - index) + ':2:' + secondary_list[2]].gp : 0);

      ca += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 0 - index) + ':1:' + secondary_list[0]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 0 - index) + ':1:' + secondary_list[0]].ca : 0);
      ca += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 0 - index) + ':2:' + secondary_list[0]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 0 - index) + ':2:' + secondary_list[0]].ca : 0);
      ca += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 1 - index) + ':1:' + secondary_list[1]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 1 - index) + ':1:' + secondary_list[1]].ca : 0);
      ca += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 1 - index) + ':2:' + secondary_list[1]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 1 - index) + ':2:' + secondary_list[1]].ca : 0);
      ca += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 2 - index) + ':1:' + secondary_list[2]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 2 - index) + ':1:' + secondary_list[2]].ca : 0);
      ca += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 2 - index) + ':2:' + secondary_list[2]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 2 - index) + ':2:' + secondary_list[2]].ca : 0);

      new_attr = attr_arr[0] + ':มัธยมศึกษาตอนต้น';
    }else{
      let index = highschool_list.indexOf(attr_arr[3]);
      gp += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 0 - index) + ':1:' + highschool_list[0]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 0 - index) + ':1:' + highschool_list[0]].gp : 0);
      gp += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 0 - index) + ':2:' + highschool_list[0]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 0 - index) + ':2:' + highschool_list[0]].gp : 0);
      gp += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 1 - index) + ':1:' + highschool_list[1]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 1 - index) + ':1:' + highschool_list[1]].gp : 0);
      gp += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 1 - index) + ':2:' + highschool_list[1]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 1 - index) + ':2:' + highschool_list[1]].gp : 0);
      gp += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 2 - index) + ':1:' + highschool_list[2]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 2 - index) + ':1:' + highschool_list[2]].gp : 0);
      gp += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 2 - index) + ':2:' + highschool_list[2]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 2 - index) + ':2:' + highschool_list[2]].gp : 0);

      ca += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 0 - index) + ':1:' + highschool_list[0]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 0 - index) + ':1:' + highschool_list[0]].ca : 0);
      ca += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 0 - index) + ':2:' + highschool_list[0]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 0 - index) + ':2:' + highschool_list[0]].ca : 0);
      ca += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 1 - index) + ':1:' + highschool_list[1]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 1 - index) + ':1:' + highschool_list[1]].ca : 0);
      ca += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 1 - index) + ':2:' + highschool_list[1]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 1 - index) + ':2:' + highschool_list[1]].ca : 0);
      ca += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 2 - index) + ':1:' + highschool_list[2]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 2 - index) + ':1:' + highschool_list[2]].ca : 0);
      ca += (obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 2 - index) + ':2:' + highschool_list[2]] ?
      obj[attr_arr[0] + ':' + String(parseInt(attr_arr[1]) + 2 - index) + ':2:' + highschool_list[2]].ca : 0);

      new_attr = attr_arr[0] + ':มัธยมศึกษาตอนปลาย';
    }

    var tmpGPA = (gp / ca ).toFixed(4).toString().split('.');
    var f_tmpGPA = parseFloat(parseInt(tmpGPA[0]) + '.' + String(tmpGPA[1]).substring(0, 2));
    f_tmpGPA = isNaN(f_tmpGPA) ? null : f_tmpGPA;

    return {'obj': new_attr, 'gpax': f_tmpGPA};
  };

  var convert_subjectweight = function(educlass, ca){
    if(educlass.includes('ประถมศึกษาปีที่')){
      return parseFloat(parseFloat(ca) * 2);
    }else{
      return parseFloat(ca);
    }
  };

  var get_newindicatordata = function(err, resdata){
    if(err){
      onFinish(err, 'Error!');
    }else{
      startkey = resdata.lastsynctime;
      var _value = {};
      var _key = '';

      request({
        url: config.source_domain+ '/api/log/newindicator',
        qs: {'apikey': config.apikey,
        'start': startkey
      }})
      .pipe(JSONStream.parse('*'))
      .pipe(through2.obj(function(chunk, enc, cb) {
        _key = chunk.value.key;
        _value = diff.apply(chunk.value.changes, {});
        var self = this;
        startkey = chunk.key;
        if(!_value.data) {
          request({
            method: 'POST',
            uri: config.domain + 'api/dbs/newindicator/' + _key,
            json: true,
            body: _value
          },function(err,response,body) {
            if(body.ok){
              request({
                method: 'POST',
                uri: config.domain + 'api/dbs/synctime/newindicator' ,
                json: true,
                body:  {'lastsynctime': startkey}
              },function(err,response,body) {
                if(body.ok){
                  cb();
                }else{
                  onFinish(err, body);
                }
              });
            }else{
              onFinish(err, body);
            }
          });
        }else{
          if(_value.data.length > 0) {
            request({
              method: 'GET',
              uri: 'http://newtestnew.azurewebsites.net/ServiceControl/GetEduService.svc/GetSubject?schooltimeId=' + _value.schooltimeid,
              json: true
            },function(err, response, body){
              for(let i = 0; i < _value.data.length; i++){
                var gp = _value.data[i].gradefix ? _value.data[i].gradefix : _value.data[i].grade;

                if( body.subjectweight != null){
                  self.push({'method': 'GPA', 'ts': chunk.key, 'key': _value.data[i].cid, 'value': {'academicyear': body.academicyear,
                  'semester': body.semester, 'educlass': body.educlass, 'CA': body.subjectweight, 'GP': gp, 'document_key': chunk.value.key,
                  'hostid': body.hostid}});
                }
              }

              cb();
            });
          } else{
            request({
              method: 'POST',
              uri: config.domain + 'api/dbs/newindicator/' + _key,
              json: true,
              body: _value
            },function(err,response,body) {
              if(body.ok){
                request({
                  method: 'POST',
                  uri: config.domain + 'api/dbs/synctime/newindicator' ,
                  json: true,
                  body:  {'lastsynctime': startkey}
                },function(err,response,body) {
                  if(body.ok){
                    cb();
                  }else{
                    onFinish(err, body);
                  }
                });
              }else{
                onFinish(err, body);
              }
            });
          }
        }
      }))
      .pipe(through2.obj(function(chunk, enc, cb) {
        if(chunk.method === 'GPA'){
          request({
            method: 'GET',
            uri: config.domain + 'api/dbs/newindicator/'+ chunk.value.document_key,
            json: true
          },function(err, response, body) {
            var obj = body;
            var gp = 0;
            var ca = 0;
            var oldGotZero = false;
            var newGotZero = false;
            var olddata = false;

            if(body.ok != false) {
              for(let i = 0; i < obj.data.length; i++){
                if(obj.data[i].cid === chunk.key){
                  oldGotZero = parseFloat(obj.data[i].grade) == 0 ? true : false;
                  olddata = true;
                  if(obj.data[i].grade != 'ร' && obj.data[i].grade != 'มส' && (obj.data[i].grade  != null || obj.data[i].gradefix != null)){
                    gp -= (parseFloat(obj.data[i].gradefix ? obj.data[i].gradefix : obj.data[i].grade) * convert_subjectweight(chunk.value.educlass, chunk.value.CA));
                  }else{
                    ca = convert_subjectweight(chunk.value.educlass, chunk.value.CA);
                  }

                  break;
                }
              }
            }else{
              if(chunk.value.GP != 'ร' && chunk.value.GP != 'มส' && chunk.value.GP != null){
                ca = convert_subjectweight(chunk.value.educlass, chunk.value.CA);
              }
            }

            newGotZero = parseFloat(chunk.value.GP ) == 0 ? true : false;
            if(chunk.value.GP != 'ร' && chunk.value.GP != 'มส' && chunk.value.GP != null){
              gp += (parseFloat(chunk.value.GP) * convert_subjectweight(chunk.value.educlass, chunk.value.CA));
            }

            var tmpObj = chunk.value.hostid + ':' + chunk.value.academicyear + ':' + chunk.value.semester + ':' + chunk.value.educlass;

            if(oldGotZero || newGotZero){
              var numberFail = 0;

              if(olddata){
                if(oldGotZero !== newGotZero){
                  if(newGotZero){
                    numberFail = 1;
                  }else{
                    numberFail = -1;
                  }
                }
              }else{
                numberFail = 1;
              }

              request({
                method: 'GET',
                uri: config.domain + 'api/dbs/student_fail/'+ bytewise.encode([chunk.key]).toString('hex'),
                json: true
              },function(err, response, body){
                var fail_obj = body;

                if(body.ok == false) {
                  fail_obj = {};
                  fail_obj[tmpObj] = numberFail;
                }else{
                  if(!fail_obj[tmpObj]) fail_obj[tmpObj] = 0; // create new object
                  fail_obj[tmpObj] += numberFail;
                }

                if(fail_obj[tmpObj]  === 0) delete fail_obj[tmpObj] ;

                request({
                  method: 'POST',
                  uri: config.domain + 'api/dbs/student_fail/' + bytewise.encode([chunk.key]).toString('hex'),
                  json: true,
                  body: fail_obj
                },function(err,response,body) {
                  request({
                    method: 'GET',
                    uri: config.domain + 'api/dbs/student_GPA/'+ bytewise.encode([chunk.key]).toString('hex'),
                    json: true
                  },function(err, response, body){
                    var GPA_obj = body;

                    if(body.ok == false) {
                      var tmpGPA = (gp / ca ).toFixed(4).toString().split('.');
                      var f_tmpGPA = parseFloat(parseInt(tmpGPA[0]) + '.' + String(tmpGPA[1]).substring(0, 2));
                      GPA_obj = {};
                      GPA_obj[tmpObj] = {'ca': ca, 'gp': gp, 'gpa': f_tmpGPA};
                    }else{
                      if(!GPA_obj[tmpObj]) GPA_obj[tmpObj] = {}; // create new object
                      GPA_obj[tmpObj].ca = (GPA_obj[tmpObj].ca ? GPA_obj[tmpObj].ca : 0) + ca;
                      GPA_obj[tmpObj].gp = (GPA_obj[tmpObj].gp ? GPA_obj[tmpObj].gp : 0) + gp;
                      var tmpGPA = (GPA_obj[tmpObj].gp / GPA_obj[tmpObj].ca ).toFixed(4).toString().split('.');
                      var f_tmpGPA = parseFloat(parseInt(tmpGPA[0]) + '.' + String(tmpGPA[1]).substring(0, 2));
                      GPA_obj[tmpObj].gpa = f_tmpGPA;
                    }

                    request({
                      method: 'POST',
                      uri: config.domain + 'api/dbs/student_GPA/' + bytewise.encode([chunk.key]).toString('hex'),
                      json: true,
                      body: GPA_obj
                    },function(err,response,body) {
                      if(body.ok){
                        var tmpGPAX = cal_GPAX(GPA_obj, tmpObj);

                        request({
                          method: 'GET',
                          uri: config.domain + 'api/dbs/student_GPAX/'+ bytewise.encode([chunk.key]).toString('hex'),
                          json: true
                        },function(err, response, body){
                          var GPAX_obj = body;

                          if(body.ok == false) {
                            GPAX_obj = {};
                            GPAX_obj[tmpGPAX.obj] = {'gpax': tmpGPAX.gpax};
                          }else{
                            if(!GPAX_obj[tmpGPAX.obj]) GPAX_obj[tmpGPAX.obj] = {}; // create new object
                            GPAX_obj[tmpGPAX.obj].gpax = tmpGPAX.gpax;
                          }

                          request({
                            method: 'POST',
                            uri: config.domain + 'api/dbs/student_GPAX/' + bytewise.encode([chunk.key]).toString('hex'),
                            json: true,
                            body: GPAX_obj
                          },function(err,response,body) {
                            if(body.ok){
                              request({
                                method: 'POST',
                                uri: config.domain + 'api/dbs/newindicator/' + _key,
                                json: true,
                                body: _value
                              },function(err,response,body) {
                                if(body.ok){
                                  request({
                                    method: 'POST',
                                    uri: config.domain + 'api/dbs/synctime/newindicator' ,
                                    json: true,
                                    body:  {'lastsynctime': startkey}
                                  },function(err,response,body) {
                                    if(body.ok){
                                      cb();
                                    }else{
                                      onFinish(err, body);
                                    }
                                  });
                                }else{
                                  onFinish(err, body);
                                }
                              });
                            }else{
                              onFinish(err, body);
                            }
                          });
                        });
                      }else{
                        onFinish(err, body);
                      }
                    });
                  });
                });
              });
            }else{
              request({
                method: 'GET',
                uri: config.domain + 'api/dbs/student_GPA/'+ bytewise.encode([chunk.key]).toString('hex'),
                json: true
              },function(err, response, body){
                var GPA_obj = body;

                if(body.ok == false) {
                  var tmpGPA = (gp / ca ).toFixed(4).toString().split('.');
                  var f_tmpGPA = parseFloat(parseInt(tmpGPA[0]) + '.' + String(tmpGPA[1]).substring(0, 2));
                  GPA_obj = {};
                  GPA_obj[tmpObj] = {'ca': ca, 'gp': gp, 'gpa': f_tmpGPA};
                }else{
                  if(!GPA_obj[tmpObj]) GPA_obj[tmpObj] = {}; // create new object
                  GPA_obj[tmpObj].ca = (GPA_obj[tmpObj].ca ? GPA_obj[tmpObj].ca : 0) + ca;
                  GPA_obj[tmpObj].gp = (GPA_obj[tmpObj].gp ? GPA_obj[tmpObj].gp : 0) + gp;
                  var tmpGPA = (GPA_obj[tmpObj].gp / GPA_obj[tmpObj].ca ).toFixed(4).toString().split('.');
                  var f_tmpGPA = parseFloat(parseInt(tmpGPA[0]) + '.' + String(tmpGPA[1]).substring(0, 2));
                  GPA_obj[tmpObj].gpa = f_tmpGPA;
                }

                request({
                  method: 'POST',
                  uri: config.domain + 'api/dbs/student_GPA/' + bytewise.encode([chunk.key]).toString('hex'),
                  json: true,
                  body: GPA_obj
                },function(err,response,body) {
                  if(body.ok){
                    var tmpGPAX = cal_GPAX(GPA_obj, tmpObj);

                    request({
                      method: 'GET',
                      uri: config.domain + 'api/dbs/student_GPAX/'+ bytewise.encode([chunk.key]).toString('hex'),
                      json: true
                    },function(err, response, body){
                      var GPAX_obj = body;

                      if(body.ok == false) {
                        GPAX_obj = {};
                        GPAX_obj[tmpGPAX.obj] = {'gpax': tmpGPAX.gpax};
                      }else{
                        if(!GPAX_obj[tmpGPAX.obj]) GPAX_obj[tmpGPAX.obj] = {}; // create new object
                        GPAX_obj[tmpGPAX.obj].gpax = tmpGPAX.gpax;
                      }

                      request({
                        method: 'POST',
                        uri: config.domain + 'api/dbs/student_GPAX/' + bytewise.encode([chunk.key]).toString('hex'),
                        json: true,
                        body: GPAX_obj
                      },function(err,response,body) {
                        if(body.ok){
                          request({
                            method: 'POST',
                            uri: config.domain + 'api/dbs/newindicator/' + _key,
                            json: true,
                            body: _value
                          },function(err,response,body) {
                            if(body.ok){
                              request({
                                method: 'POST',
                                uri: config.domain + 'api/dbs/synctime/newindicator' ,
                                json: true,
                                body:  {'lastsynctime': startkey}
                              },function(err,response,body) {
                                if(body.ok){
                                  cb();
                                }else{
                                  onFinish(err, body);
                                }
                              });
                            }else{
                              onFinish(err, body);
                            }
                          });
                        }else{
                          onFinish(err, body);
                        }
                      });
                    });
                  }else{
                    onFinish(err, body);
                  }
                });
              });
            }
          });
        }
      }))
      .on('finish',function() {
        onFinish(null, 'Finish!');
      });
    }
  }

  login('newindicator', get_newindicatordata);
};

var choose_sync = function(data, cb){
  if(data === 'attendance_individual' || data === 'attendance_summary' || data === 'attendance_host_summary'){
    sync_attendance(cb);
  }else if( data === 'student_GPA' || data === 'student_GPAX' || data === 'student_fail'){
    sync_newindicator(cb);
  }
};

module.exports.views = ['attendance_individual', 'attendance_summary', 'attendance_host_summary', 'student_GPA', 'student_GPAX', 'student_fail'];
module.exports.choose_sync = choose_sync;
