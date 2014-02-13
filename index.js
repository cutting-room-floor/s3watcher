var path = require('path');
var util = require('util');
var AWS = require ('aws-sdk');
var Readable = require('stream').Readable;
var debug = require('debug')('s3watcher');
var queue = require('queue-async');

var watch = module.exports = new Readable();
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

function checkForNew(marker, mtime, cb) {
    debug('scanning last %s hour prefixes for keys added after %s', config.hoursBack || 24, mtime);
    var hours = watch.calcHours(config.hoursBack, keyDate(marker));
    var q = queue();
    hours.forEach(function(hour) {
        q.defer(checkForNewByTime, hour, mtime);
    });
    q.awaitAll(function(err, results) {
        if (err) return cb(err);
        var state = results.reduce(function(a, b) {
            return (a.mtime > b.mtime ? a : b);
        });
        if (state.mtime > mtime) {
            watch.saveState(state.marker, state.mtime, function(e) {
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
    var marker;

    (function fetch(opts){
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
                    marker = c.Key;
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
                // Take newest marker and newest mtime. Don't need to be a pair.
                cb(err, {mtime: newLastDate, marker: marker});
            }

        });
    })(opts);
}

watch.saveState = function(marker, mtime, cb){
    debug('saving marker %s and mtime %d to s3://%s/%s', marker, mtime, config.bucket, config.watchkey);
    var opts = {
        Bucket: config.bucket,
        Key: config.watchkey,
        Body: [marker, (+mtime).toString()].join('\n')
    };
    s3.putObject(opts, cb);
};

watch.loadState = function(cb) {
    debug('loading state from s3://%s/%s', config.bucket, config.watchkey);
    var opts = {
        Bucket: config.bucket,
        Key: config.watchkey
    };

    s3.getObject(opts, function(err, resp) {
        if (err) {
            if (err.code === 'NoSuchKey') {
                debug('state file does not exist; starting at the top');

                return s3.listObjects({
                    Prefix: config.prefix,
                    Bucket: config.bucket
                }, function(err, data) {
                    if (err) return cb(err);
                    var marker = data.Contents[0].Key;
                    var mtime = 0;
                    watch.saveState(marker, 0, function(err) {
                        if (err) return cb(err);
                        return cb(null, marker, mtime);
                    });
                });
            }
            return cb(err);
        }

        try {
            var state = resp.Body.toString().split('\n');
            var marker = state[0];
            var mtime = new Date(parseInt(state[1]));
            debug('parsed marker %s and mtime %d', marker, mtime);
            cb(null, marker, mtime);
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

        watch.loadState(function(err, marker, mtime) {
            if (err) return watch.emit('error', err);

            checkForNew(marker, mtime, function(err) {
                if (err) return watch.emit('error', err);
                debug('waiting for %d ms', config.timeout);
                setTimeout(check, config.timeout);
            });
        });
    })();
}

watch.calcHours = function(count, date) {
    var months = ['01','02','03','04','05','06','07','08','09','10','11','12'];
    count = count || 24;
    var hours = Array.apply(null, Array(count));
    hours = hours.map(function(_, i){
        var d = new Date(date - 36e5 * (i + -1));

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

function keyDate(key) {
    var datestr = key.split('.')[1].split('-');
    return new Date(Date.UTC(datestr[0], datestr[1] - 1, datestr[2], datestr[3]));
}
