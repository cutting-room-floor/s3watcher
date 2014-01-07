var assert = require("assert");
var AWS = require ('aws-sdk');
var s3watcher = require('../index.js')

AWS.config.update({accessKeyId: process.env.AWS_KEY, secretAccessKey: process.env.AWS_SECRET});
s3 = new AWS.S3();

s3watcher.config({awsKey:process.env.AWS_KEY,
                  awsSecret:process.env.AWS_SECRET,
                  markerPrefix: process.env.MARKER_PREFIX,
                  bucket:process.env.BUCKET,
                  hoursBack: 36,
                  timeout: 1});


describe('s3watcher module', function() {
    var keys = [];
    var twoHoursAgo = (+new Date) - 2 * 36e5;
    var hours = s3watcher.calcHours(3);
    var newKey = "";
    this.timeout(15000);
    before(function(done){
        var count = 0;

        var putOpts = {Key: 'tiles/.s3watcher', Bucket: process.env.BUCKET, Body:""+twoHoursAgo};
        keys.push(putOpts.Key);
        s3.putObject(putOpts, areWeDone);

        //.s3.amazonaws.com/{optional-prefix/}{distribution-ID}.{YYYY}-{MM}-{DD}-{HH}.{unique-ID}.gz

        hours.forEach(function(h){
            putOpts = {Key: 'tiles/asdfasdf.' + h + '.12345.gz', Bucket: process.env.BUCKET};
            keys.push(putOpts.Key);
            s3.putObject(putOpts, areWeDone);
        });

        function areWeDone(err){
            count +=1;
            if(count === keys.length)
                done();
        }
    });


    after(function(done){

        var delKeys = keys.map(function(k){
            return {Key:k};
        })

        s3.deleteObjects({Bucket:process.env.BUCKET, Delete:{Objects:delKeys}}, done);
    });

    describe('load s3 bucket', function() {

        it("should load the last read time", function(done){
            //load the last read timestamp from .s3watcher file
            s3watcher.getLastDate(function(err, date){
                assert(!err);
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

            var getOpts = {Key: 'tiles/.s3watcher', Bucket:process.env.BUCKET};

            s3.getObject(getOpts, function(err, resp){
                assert(!err);
                assert(parseInt(resp.Body) > twoHoursAgo);
                done();
            });
        });
    });
});


//s3watcher.pipe(process.stdout);


//s3watcher.on('data', function(d){
//    console.log(d.toString('utf8').split("\n"));
//});
