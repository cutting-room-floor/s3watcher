var _ = require('underscore');
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

            scan(marker, mtime, function(err) {
                if (err) return watch.emit('error', err);
                debug('waiting for %d ms', config.timeout);
                setTimeout(check, config.timeout);
            });
        });
    })();
}

function scan(marker, mtime, callback) {
    debug('scanning starting at %s for keys created after %s', marker, mtime);

    (function fetch(opts){
        debug('list objects starting with marker %s ', opts.Marker);
        s3.listObjects(opts, function(err, data){
            if (err) return callback(err);
            if (!data.Contents.length) return callback();

            _(data.Contents).each(function(obj) {
                watch.push(obj.Key + '\n');
            });

            if (+keyToDate(marker) > Date.now() - 864e5) {
                var oldmarker = marker;
                marker = dateToKey(new Date(Date.now() - 864e5), config.prefix);
                debug('marker %s less than 24 hour old; using %s instead', oldmarker, marker);
            } else {
                marker = _(data.Contents).last().Key;
            }

            mtime = new Date(_(data.Contents).max(function(obj) {
                return +new Date(obj.LastModified);
            }).LastModified);
            watch.saveState(marker, mtime, function(err) {
                if (data.IsTruncated) {
                    opts.Marker = _(data.Contents).last().Key;
                    fetch(opts);
                } else {
                    callback();
                }
            });
        });
    })({
        Marker: marker,
        Prefix: config.prefix,
        Bucket: config.bucket
    });
}

function keyToDate(key) {
    var datestr = key.split('.')[1].split('-');
    return new Date(Date.UTC(datestr[0], datestr[1] - 1, datestr[2], datestr[3]));
}

function dateToKey(d, prefix) {
    prefix = prefix || '';
    var months = ['01','02','03','04','05','06','07','08','09','10','11','12'];
    var dayOfMonth = d.getUTCDate()+'';
    if(dayOfMonth.length < 2) {
        dayOfMonth = '0' + dayOfMonth;
    }

    var hour = d.getUTCHours()+'';
    if(hour.length < 2) {
        hour = '0' + hour;
    }

    return util.format('%s%s-%s-%s-%s', prefix, d.getUTCFullYear(), months[d.getUTCMonth()], dayOfMonth, hour);
}
