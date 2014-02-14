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

var s3watcher = module.exports = function(options) {
    options.namespace = options.namespace || 'default';
    options.timeout = options.timeout || 3e5;
    var statepath = path.join(options.prefix, util.format('.%s.s3watcher', options.namespace));
    var markerpath = path.join(statepath, 'marker');
    var emittedpath = path.join(statepath, 'emitted');

    log('marker persisted at %s', markerpath);
    log('list of emitted keys persisted at %s', emittedpath);

    var started = false;
    var watcher = new Readable();
    watcher._read = function() {
        if (!started) start();
        started = true;
        return true;
    };

    AWS.config.update({
        accessKeyId: options.awsKey,
        secretAccessKey: options.awsSecret
    });
    var s3 = new AWS.S3();

    function saveState(marker, callback){
        log('saving marker %s to S3', marker);
        s3.putObject({
            Bucket: options.bucket,
            Key: markerpath,
            Body: marker
        }, callback);
    }

    function loadState(callback) {
        var opts = {
            Bucket: options.bucket,
            Key: markerpath
        };

        s3.getObject(opts, function(err, resp) {
            if (err) {
                if (err.code === 'NoSuchKey') {
                    log('state file does not exist; starting at the top');

                    return s3.listObjects({
                        Prefix: options.prefix,
                        Bucket: options.bucket,
                        Delimiter: '/'
                    }, function(err, data) {
                        if (err) return callback(err);
                        var marker = data.Contents[0]? data.Contents[0].Key : '0';
                        saveState(marker, function(err) {
                            if (err) return callback(err);
                            return callback(null, marker);
                        });
                    });
                }
                return callback(err);
            }

            var marker = resp.Body.toString().trim();
            log('loaded marker %s from S3', marker);
            callback(null, marker);
        });
    }

    function start() {
        log('starting');
        (function check() {
            log('checking');

            loadState(function(err, marker) {
                if (err) return watcher.emit('error', err);

                if (marker !== '0') {
                    marker = s3watcher.dateToKey(new Date(s3watcher.keyToDate(marker) - 864e5), options.prefix);
                }

                scan(marker, function(err) {
                    if (err) return watcher.emit('error', err);
                    log('waiting for %d ms', options.timeout);
                    setTimeout(check, options.timeout);
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

                    saveState(_(data.Contents).last().Key, function(err) {
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
            Prefix: options.prefix,
            Bucket: options.bucket,
            Delimiter: '/'
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
            Bucket: options.bucket,
            Key: path.join(emittedpath, key)
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
                watcher.push(key + '\n');
                callback(null, true);
            });
        });
    }

    return watcher;
};

// Convert a key into a JavaScript Date object.
s3watcher.keyToDate = function(key) {
    var datestr = key.split('.')[1].split('-');
    return new Date(Date.UTC(datestr[0], datestr[1] - 1, datestr[2], datestr[3]));
};

// Convert a JavaScript Date object and S3 prefix into a key suitable for use
// as a marker.
s3watcher.dateToKey = function(d, prefix) {
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
};
