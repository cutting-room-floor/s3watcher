var AWS = require ('aws-sdk');
var Readable = require('stream').Readable;

module.exports = watch = new Readable();
var config, s3;

watch.config = function(c){
    config = c;
    config.prefexDir = getPrefixDir(config.markerPrefix);
    AWS.config.update({accessKeyId: c.awsKey, secretAccessKey: c.awsSecret});
    s3 = new AWS.S3();
};

// Files from cloudfront look like this:
// {bucket-name}.s3.amazonaws.com/{optional-prefix/}{distribution-ID}.{YYYY}-{MM}-{DD}-{HH}.{unique-ID}.gz

var lastDate = null;

function checkForNew(initial, cb){

    var hours = calcHours(config.hoursBack);
    (function load(items, callback) {
        var loaded = new Array(items.length);
        var error;
        items.forEach(function(item, i) {
            checkForNewByTime(item, initial, function(err, obj) {
                error = error || err;
                loaded[i] = err || obj;
                if (loaded.filter(function(n) { return n; }).length === loaded.length) {
                    callback(error, loaded);
                }
            });
        });
    })(hours, function(err, results) {
        resultDate = results.reduce(function(a, b){
            return (a > b ? a : b);
        });

        if(resultDate > lastDate){
            lastDate = resultDate;
            console.log("new date:", lastDate);
            //save date
        }
        cb(err);
    });
}

var months = ['01','02','03','04','05','06','07','08','09','10','11','12'];

function calcHours(count){
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
}

function checkForNewByTime(date, initial, cb){

    var opts = {Marker:config.markerPrefix+date,
                Prefix:config.markerPrefix+date,
                Bucket:config.bucket};

    var newLastDate = lastDate;

    s3.listObjects(opts, function(err, data){
        if(err) return cb(err);

        console.log('in ', date, 'we have: ', data.Contents.length);

        data.Contents.forEach(function(c){
            if(!initial && lastDate){
                if(c.LastModified > lastDate)
                    watch.push(c.Key+'\n');
            }else{
                watch.push(c.Key+'\n');
            }

            if(c.LastModified > newLastDate)
                newLastDate = c.LastModified;
        });

        cb(err, newLastDate);

    });
}

function getPrefixDir(markerPrefix){
    var prefixDir = markerPrefix;
    if(markerPrefix[markerPrefix.length-1] !== '/'){
        var segs = markerPrefix.split('/');
        segs.pop();
        prefixDir = segs.join('/') + "/";
    }
    return prefixDir;
}

function setLastDate(date){
    var opts = {Bucket:config.bucket,
                Key:config.prefixDir+'.s3watcher',
                Body:date.toUTCString()};

  //  s3.putObject(opts, function(err, resp){
   //     if(err) console.log(err);
    //});
}

function getLastDate(cb){
    var opts = {Bucket:config.bucket,
                Key:config.prefixDir+'.s3watcher'};

    s3.getObject(opts, function(err, resp){
        if(err) return cb(err);

        console.log(resp);

        try{
            var lastDate = new Date(resp.Body);
            cb(err, lastDate);
        }catch(e){
            cb(e);
        }
    });
}



var started = false;
watch._read = function(){
    if(!started) start();

    started = true;
    return true;
};

function start(){
    var init = true;

    var check = function(){

        console.log('checking');

        checkForNew(init, function(err){
            setTimeout(check, 10000);
        });
        init = false;

    };
    check();
}
