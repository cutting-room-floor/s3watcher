var _ = require('underscore');
var path = require('path');
var util = require('util');
var AWS = require ('aws-sdk');
var Readable = require('stream').Readable;
var debug = require('debug')('s3watcher');
var queue = require('queue-async');
var LRU = require('lru-cache');

var watch = module.exports = new Readable();
var config, s3;

watch.config = function(c) {
    var namespace = c.namespace || 'default';
    config = c;
    config.timeout = config.timeout || 3e5;
    config.state = path.join(c.prefix, util.format('.%s.s3watcher', namespace));
    config.watchkey = path.join(config.state, 'state');
    config.processed = path.join(config.state, 'processed');
    AWS.config.update({accessKeyId: c.awsKey, secretAccessKey: c.awsSecret});
    s3 = new AWS.S3();
};

watch.saveState = function(marker, callback){
    debug('saving marker %s to s3://%s/%s', marker, config.bucket, config.watchkey);
    s3.putObject({
        Bucket: config.bucket,
        Key: config.watchkey,
        Body: marker
    }, callback);
};

watch.loadState = function(callback) {
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
                    Bucket: config.bucket,
                    Delimiter: '/'
                }, function(err, data) {
                    if (err) return callback(err);
                    var marker = data.Contents[0].Key;
                    watch.saveState(marker, 0, function(err) {
                        if (err) return callback(err);
                        return callback(null, marker);
                    });
                });
            }
            return callback(err);
        }

        try {
            var marker = resp.Body.toString();
            debug('parsed marker %s', marker);
            callback(null, marker);
        } catch(e) {
            callback(e);
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

        watch.loadState(function(err, marker) {
            if (err) return watch.emit('error', err);

            scan(marker, function(err) {
                if (err) return watch.emit('error', err);
                debug('waiting for %d ms', config.timeout);
                setTimeout(check, config.timeout);
            });
        });
    })();
}

function scan(marker, callback) {
    debug('scanning at %s for keys', marker);

    (function fetch(opts){
        debug('list objects starting with marker %s', opts.Marker);
        s3.listObjects(opts, function(err, data){
            if (err) return callback(err);
            if (!data.Contents.length) return callback();

            var q = queue(10);
            _(data.Contents).each(function(obj) {
                if (obj.Key.indexOf('.s3watcher') !== -1) return
                q.defer(emit, obj.Key);
            });
            q.awaitAll(function(err) {
                if (err) return callback(err);

                if (+keyToDate(marker) > Date.now() - 864e5) {
                    var oldmarker = marker;
                    marker = dateToKey(new Date(Date.now() - 864e5), config.prefix);
                    debug('marker %s less than 24 hour old; using %s instead', oldmarker, marker);
                } else {
                    marker = _(data.Contents).last().Key;
                }

                watch.saveState(marker, function(err) {
                    if (data.IsTruncated) {
                        opts.Marker = _(data.Contents).last().Key;
                        fetch(opts);
                    } else {
                        callback();
                    }
                });
            });
        });
    })({
        Marker: marker,
        Prefix: config.prefix,
        Bucket: config.bucket
    });
}

var cache = LRU({
    max: 24000,
    dispose: function() {
        debug('disposed cache object');
    }
});

function emit(key, callback) {
    var processed = cache.get(key);
    if (processed) return callback();

    var opts = {
        Bucket: config.bucket,
        Key: path.join(config.processed, key)
    };
    s3.headObject(opts, function(err, data) {
        if (err && err.code !== 'NotFound') return callback(err);
        if (!err) {
            cache.set(key, true);
            return callback();
        }
        s3.putObject(opts, function(err) {
            if (err) return callback(err);
            watch.push(key + '\n');
            cache.set(key, true);
            callback();
        });
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
