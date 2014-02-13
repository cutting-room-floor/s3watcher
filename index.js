var path = require('path');
var util = require('util');
var AWS = require ('aws-sdk');
var Readable = require('stream').Readable;
var debug = require('debug')('s3watcher');

module.exports = watch = new Readable();
var config, s3;

watch.config = function(c) {
    var namespace = c.namespace || 'default';
    config = c;
    config.timeout = config.timeout || 3e5;
    config.watchkey = path.join(c.prefix, util.format('.%s.s3watcher', namespace));
    AWS.config.update({accessKeyId: c.awsKey, secretAccessKey: c.awsSecret});
    s3 = new AWS.S3();
};

// Files from cloudfront look like this:
// {bucket-name}.s3.amazonaws.com/{optional-prefix/}{distribution-ID}.{YYYY}-{MM}-{DD}-{HH}.{unique-ID}.gz

function checkForNew(lastDate, cb) {
    debug('scanning last %s hour prefixes for keys added after %s', config.hoursBack || 24, lastDate);
    var hours = watch.calcHours(config.hoursBack);
    (function load(items, callback) {
        var loaded = new Array(items.length);
        var error;
        items.forEach(function(item, i) {
            checkForNewByTime(item, lastDate, function(err, obj) {
                error = error || err;
                loaded[i] = err || obj;
                if (loaded.filter(function(n) { return n; }).length === loaded.length) {
                    callback(error, loaded);
                }
            });
        });
    })(hours, function(err, results) {
        if (err) return cb(err);
        var resultDate = results.reduce(function(a, b) {
            return (a > b ? a : b);
        });
        if(resultDate > lastDate) {
            lastDate = resultDate;
            watch.setLastDate(lastDate, function(e){
                cb(err || e);
            });
        } else {
            cb(err);
        }

    });
}

function checkForNewByTime(hour, lastDate, cb) {

    var opts = {
        Marker: config.prefix + hour,
        Prefix: config.prefix + hour,
        Bucket: config.bucket
    };

    var newLastDate = lastDate;

    var fetch = function(opts){

        s3.listObjects(opts, function(err, data){
            if(err) return cb(err);

            var i = 0;
            data.Contents.forEach(function(c) {
                if (c.LastModified > lastDate) {
                    watch.push(c.Key + '\n');
                    i++;
                }

                if (c.LastModified > newLastDate) {
                    newLastDate = c.LastModified;
                }
            });

            debug('%d of %d keys with prefix %s were previously unseen', i, data.Contents.length, opts.Prefix);

            if (data.IsTruncated) {
                if(!data.NextMarker) {
                    opts.Marker = data.Contents[data.Contents.length-1].Key;
                } else {
                    opts.Marker = data.NextMarker;
                }
                fetch(opts);
            } else {
                cb(err, newLastDate);
            }

        });
    };
    fetch(opts);
}

watch.setLastDate = function(date, cb){
    debug('saving mtime %s to s3://%s/%s', date, config.bucket, config.watchkey);
    var opts = {
        Bucket: config.bucket,
        Key: config.watchkey,
        Body: (+date).toString()
    };
    s3.putObject(opts, cb);
};

watch.getLastDate = function(cb) {
    debug('loading mtime from s3://%s/%s', config.bucket, config.watchkey);
    var opts = {
        Bucket: config.bucket,
        Key: config.watchkey
    };

    s3.getObject(opts, function(err, resp) {
        if (err) {
            if (err.code === 'NoSuchKey') {
                var date = new Date();
                debug('state file does not exist; creating it');
                return watch.setLastDate(date, function(err) {
                    if (err) return cb(err);
                    return cb(null, date);
                });
            }
            return cb(err);
        }

        try {
            var lastDate = new Date(parseInt(resp.Body.toString()));
            debug('parsed date %s', lastDate);
            cb(null, lastDate);
        } catch(e) {
            cb(e);
        }
    });
};



var started = false;
watch._read = function(){
    debug('read');
    if(!started) start();

    started = true;
    return true;
};

function start() {
    debug('starting');
    (function check() {
        debug('checking');

        watch.getLastDate(function(err, date){
            if (err) return watch.emit('error', err);

            checkForNew(date, function(err) {
                if (err) return watch.emit('error', err);
                debug('waiting for %d ms', config.timeout);
                setTimeout(check, config.timeout);
            });
        });
    })();
}

watch.calcHours = function(count){
    var months = ['01','02','03','04','05','06','07','08','09','10','11','12'];
    count = count || 24;
    var hours = Array.apply(null, Array(count));
    var now = new Date();
    hours = hours.map(function(_, i){
        var d = new Date(now-36e5*(i+1));

        var dayOfMonth = d.getUTCDate()+'';
        if(dayOfMonth.length < 2)
            dayOfMonth = '0'+dayOfMonth;

        var hour = d.getUTCHours()+'';
        if(hour.length < 2)
            hour = '0'+hour;

        return d.getUTCFullYear()+'-'+months[d.getUTCMonth()]+'-'+dayOfMonth+'-'+hour;
    });

    return hours;
};
