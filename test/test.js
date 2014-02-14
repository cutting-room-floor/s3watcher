var assert = require("assert");
var AWS = require('aws-sdk');
var s3watcher = require('../index.js')
var env = require('superenv')('s3watcher');

AWS.config.update({
    accessKeyId: env.AWS_KEY,
    secretAccessKey: env.AWS_SECRET
});

var s3 = new AWS.S3();

describe('s3watcher', function() {
    describe('keyToDate', function() {
        it('should convert a key to a Date object', function() {
            assert.equal(+s3watcher.keyToDate('test/EJ123456.2014-02-14-22'), 1392415200000);
        });
    });
    describe('dateToKey', function() {
        it('should convert a Date object to a key', function() {
            assert.equal(s3watcher.dateToKey(new Date(1392415207053)), '2014-02-14-22');
        });
        it('should convert a Date object to a key with a prefix', function() {
            assert.equal(s3watcher.dateToKey(new Date(1392415207053), 'test/EJ123456.'), 'test/EJ123456.2014-02-14-22');
        });
    });

    describe('', function() {
        this.timeout(10000);
        it('should start on a clean bucket and detect new keys', function(done) {
            var options = {
                namespace: 'test',
                awsKey: env.AWS_KEY,
                awsSecret: env.AWS_SECRET,
                bucket: env.BUCKET,
                prefix: 'tiles/asdfasdf' + Date.now() + '.',
                timeout: 1000
            };
            var watcher = s3watcher(options);
            watcher.once('data', function(chunk) {
                assert.equal(chunk.toString(), s3watcher.dateToKey(new Date(1392415207053), options.prefix) + '\n');

                watcher.once('data', function(chunk) {
                    assert.equal(chunk.toString(), s3watcher.dateToKey(new Date(1392501607053), options.prefix) + '\n');
                    done();
                });

                s3.putObject({
                    Bucket: options.bucket,
                    Key: s3watcher.dateToKey(new Date(1392501607053), options.prefix)
                }, assert.ifError);
            });
            // Delay first put to make sure start up is clean with a blank bucket.
            setTimeout(function() {
                s3.putObject({
                    Bucket: options.bucket,
                    Key: s3watcher.dateToKey(new Date(1392415207053), options.prefix)
                }, assert.ifError);
            }, 2000);
        });
    });
});
