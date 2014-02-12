var AWS = require ('aws-sdk');
var Readable = require('stream').Readable;

module.exports = watch = new Readable();
var config, s3;

watch.config = function(c){
    config = c;
    config.timeout = config.timeout || 3e5;
    config.prefixDir = watch.getPrefixDir(c.markerPrefix);
    AWS.config.update({accessKeyId: c.awsKey, secretAccessKey: c.awsSecret});
    s3 = new AWS.S3();
};

// Files from cloudfront look like this:
// {bucket-name}.s3.amazonaws.com/{optional-prefix/}{distribution-ID}.{YYYY}-{MM}-{DD}-{HH}.{unique-ID}.gz

var lastDate = null;

watch.checkForNew = function(cb){

    var hours = watch.calcHours(config.hoursBack);
    (function load(items, callback) {
        var loaded = new Array(items.length);
        var error;
        items.forEach(function(item, i) {
            checkForNewByTime(item, function(err, obj) {
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
        console.log(resultDate, lastDate);
        if(resultDate > lastDate){
            lastDate = resultDate;
            watch.setLastDate(lastDate, function(e){
                cb(err || e );
            });
        }else{
            cb(err);
        }

    });
};

var months = ['01','02','03','04','05','06','07','08','09','10','11','12'];

watch.calcHours = function(count){
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

function checkForNewByTime(date,  cb){

    var opts = {Marker:config.markerPrefix+date,
                Prefix:config.markerPrefix+date,
                Bucket:config.bucket};

    var newLastDate = lastDate;

    var fetch = function(opts){

        s3.listObjects(opts, function(err, data){
            if(err) return cb(err);

            console.log("in ", opts.Prefix, " there are ", data.Contents.length);

            data.Contents.forEach(function(c){
                if(c.LastModified > lastDate){
                    watch.push(c.Key+'\n');
                }

                if(c.LastModified > newLastDate)
                    newLastDate = c.LastModified;
            });


            if(data.IsTruncated){
                if(!data.NextMarker)
                    opts.Marker = data.Contents[data.Contents.length-1].Key;
                else
                    opts.Marker = data.NextMarker;
                fetch(opts);
            }else{
                cb(err, newLastDate);
            }

        });
    };
    fetch(opts);
}

watch.getPrefixDir = function(markerPrefix){
    var prefixDir = markerPrefix;
    if(markerPrefix[markerPrefix.length-1] !== '/'){
        var segs = markerPrefix.split('/');
        segs.pop();
        prefixDir = segs.join('/') + "/";
    }
    return prefixDir;
};

watch.setLastDate = function(date, cb){
    var opts = {Bucket:config.bucket,
                Key:config.prefixDir+'.s3watcher',
                Body: (+date).toString()};
    s3.putObject(opts, function(err, resp){
        cb(err);
    });
};

watch.getLastDate = function(cb){
    var opts = {Bucket:config.bucket,
                Key:config.prefixDir+'.s3watcher'};

    s3.getObject(opts, function(err, resp){
        if(err) return cb(err);

        try{
            var lastDate = new Date(parseInt(resp.Body.toString()));
            console.log("got date: ", lastDate);
            cb(err, lastDate);
        }catch(e){
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

function start(){
    var check = function(){
        watch.getLastDate(function(err, date){

            console.log("last Date:", date)

            if(date === undefined){
                date = (new Date());
                watch.setLastDate(date, function(){});
            }

            lastDate = date;
            watch.checkForNew(function(err){
                setTimeout(check, config.timeout);
            });
        });
    };
    check();
}
