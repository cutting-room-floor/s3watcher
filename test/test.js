var assert = require("assert");
var AWS = require('aws-sdk');
var s3watcher = require('../index.js')
var env = require('superenv')('s3watcher');

AWS.config.update({
    accessKeyId: env.AWS_KEY,
    secretAccessKey: env.AWS_SECRET
});

var s3 = new AWS.S3();

s3watcher.config({
    namespace: 'test',
    awsKey: env.AWS_KEY,
    awsSecret: env.AWS_SECRET,
    markerPrefix: 'tiles/asdfasdf.',
    bucket: env.BUCKET,
    hoursBack: 36,
    timeout: 1
});

describe('s3watcher module', function() {
    var keys = [];
    var twoHoursAgo = (+new Date) - 2 * 36e5;
    var hours = s3watcher.calcHours(3);
    var newKey = "";
    this.timeout(15000);
    before(function(done){
        var count = 0;

        var putOpts = {
            Key: 'tiles/asdfasdf./.test.s3watcher',
            Bucket: env.BUCKET,
            Body: twoHoursAgo.toString()
        };
        keys.push(putOpts.Key);
        s3.putObject(putOpts, areWeDone);

        //.s3.amazonaws.com/{optional-prefix/}{distribution-ID}.{YYYY}-{MM}-{DD}-{HH}.{unique-ID}.gz

        hours.forEach(function(h){
            putOpts = {
                Key: 'tiles/asdfasdf.' + h + '.12345.gz',
                Bucket: env.BUCKET
            };
            keys.push(putOpts.Key);
            s3.putObject(putOpts, areWeDone);
        });

        function areWeDone(err){
            assert.ifError(err);
            count +=1;
            if(count === keys.length)
                done();
        }
    });


    after(function(done) {
        s3.deleteObjects({
            Bucket: env.BUCKET,
            Delete: {
                Objects: keys.map(function(k){
                    return {Key:k};
                })
            }
        }, done);
    });

    describe('load s3 bucket', function() {

        it("should load the last read time", function(done){
            //load the last read timestamp from .s3watcher file
            s3watcher.getLastDate(function(err, date){
                assert.ifError(err);
                assert.deepEqual(date, new Date(parseInt(twoHoursAgo)));
                done();
            });
        });

        it("should stream newer keys", function(done){
            // get keys that were already in the bucket
            var streamKeys = []
            s3watcher.on('data', function(d){

                streamKeys.push(d.toString('utf8').split("\n")[0]);
                if(streamKeys.length === 3){
                    assert(streamKeys.indexOf(keys[1]) !== -1);
                    assert(streamKeys.indexOf(keys[2]) !== -1);
                    assert(streamKeys.indexOf(keys[3]) !== -1);


                    // this is a quite a hack, but I only want this to happen after this test..
                    putOpts = {Key: 'tiles/asdfasdf.' + hours[0] + '.12346.gz', Bucket: process.env.BUCKET};
                    newKey = putOpts.Key;
                    keys.push(putOpts.Key);
                    s3.putObject(putOpts, function(){});
                    done();
                }
            });
        });
    });
    describe("watch for new keys", function(){
        it("should stream new keys when they are added", function(done){
            // add some keys

            s3watcher.on('data', function(d){

                assert.equal(newKey, d.toString('utf8').split("\n")[0]);
                done();
            });
        });

        it("should save the last modified time", function(done){
            // check to make sure .s3watcher file is up to date

            var getOpts = {
                Key: 'tiles/asdfasdf./.test.s3watcher',
                Bucket: process.env.BUCKET
            };

            s3.getObject(getOpts, function(err, resp){
                assert.ifError(err);
                assert(parseInt(resp.Body) > twoHoursAgo);
                done();
            });
        });
    });
});
