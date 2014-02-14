var _ = require('underscore');
var path = require('path');
var util = require('util');
var AWS = require ('aws-sdk');
var Readable = require('stream').Readable;
var debug = require('debug');
var log = debug('s3watcher');
var verbose = debug('s3watcher:verbose');
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
    config.processed = path.join(config.state, 'emitted');
    AWS.config.update({accessKeyId: c.awsKey, secretAccessKey: c.awsSecret});
    s3 = new AWS.S3();

    log('saving marker at %s', config.watchkey);
    log('saving processed at %s', config.processed);
};

watch.saveState = function(marker, callback){
    log('saving marker to S3');
    s3.putObject({
        Bucket: config.bucket,
        Key: config.watchkey,
        Body: marker
    }, callback);
};

watch.loadState = function(callback) {
    log('loading marker from S3');
    var opts = {
        Bucket: config.bucket,
        Key: config.watchkey
    };

    s3.getObject(opts, function(err, resp) {
        if (err) {
            if (err.code === 'NoSuchKey') {
                log('state file does not exist; starting at the top');

                return s3.listObjects({
                    Prefix: config.prefix,
                    Bucket: config.bucket,
                    Delimiter: '/'
                }, function(err, data) {
                    if (err) return callback(err);
                    var marker = data.Contents[0].Key;
                    watch.saveState(marker, function(err) {
                        if (err) return callback(err);
                        return callback(null, marker);
                    });
                });
            }
            return callback(err);
        }

        try {
            var marker = resp.Body.toString().trim();
            log('parsed marker %s', marker);
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
    log('starting');
    (function check() {
        log('checking');

        watch.loadState(function(err, marker) {
            if (err) return watch.emit('error', err);

            marker = dateToKey(new Date(keyToDate(marker) - 864e5), config.prefix);

            scan(marker, function(err) {
                if (err) return watch.emit('error', err);
                log('waiting for %d ms', config.timeout);
                setTimeout(check, config.timeout);
            });
        });
    })();
}

// Scan a bucket starting at the given marker and call emit() on each key.
function scan(marker, callback) {
    (function fetch(opts){
        log('list objects starting with marker %s', opts.Marker);
        s3.listObjects(opts, function(err, data){
            if (err) return callback(err);
            if (!data.Contents.length) return callback();

            var q = queue(10);
            var emitted = 0;
            _(data.Contents).each(function(obj) {
                if (obj.Key.indexOf('.s3watcher') !== -1) return
                q.defer(function(callback) {
                    emit(obj.Key, function(err, e) {
                        if (e) emitted++;
                        callback(err, e);
                    });
                });
            });
            q.awaitAll(function(err) {
                if (err) return callback(err);

                log('emitted %d of %d keys', emitted, data.Contents.length);

                watch.saveState(_(data.Contents).last().Key, function(err) {
                    if (err) return callback(err);
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

var emitted = LRU({
    max: 25000,
    dispose: function() {
        verbose('disposed cache object');
    }
});

// Emit a key if it hasn't been emitted before.
function emit(key, callback) {

    // Check for key in local cache before checking S3. Don't emit if it's there.
    if (emitted.get(key)) return callback();

    var opts = {
        Bucket: config.bucket,
        Key: path.join(config.processed, key)
    };

    s3.headObject(opts, function(err, data) {
        if (err && err.code !== 'NotFound') return callback(err);

        // Don't emit key if is has been emitted before. Set key in local cache
        // so we don't need to HEAD S3 next time.
        if (!err) {
            emitted.set(key, true);
            return callback();
        }

        // Emit the key and record it was emitted in S3 and the local cache.
        s3.putObject(opts, function(err) {
            if (err) return callback(err);
            emitted.set(key, true);
            watch.push(key + '\n');
            callback(null, true);
        });
    });
}

// Convert a key into a JavaScript Date object.
function keyToDate(key) {
    var datestr = key.split('.')[1].split('-');
    return new Date(Date.UTC(datestr[0], datestr[1] - 1, datestr[2], datestr[3]));
}

// Convert a JavaScript Date object and S3 prefix into a key suitable for use
// as a marker.
function dateToKey(d, prefix) {
    prefix = prefix || '';

    var dayOfMonth = d.getUTCDate().toString();
    if (dayOfMonth.length < 2) {
        dayOfMonth = '0' + dayOfMonth;
    }

    var hour = d.getUTCHours().toString()+'';
    if (hour.length < 2) {
        hour = '0' + hour;
    }

    var months = ['01','02','03','04','05','06','07','08','09','10','11','12'];
    return util.format('%s%s-%s-%s-%s',
        prefix,
        d.getUTCFullYear(),
        months[d.getUTCMonth()],
        dayOfMonth,
        hour);
}
