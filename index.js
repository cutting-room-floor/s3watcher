var AWS = require ('aws-sdk');
var Readable = require('stream').Readable;

module.exports = watch = new Readable();
var config, s3;


watch.config = function(c){
    config = c;
    AWS.config.update({accessKeyId: c.awsKey, secretAccessKey: c.awsSecret});
    s3 = new AWS.S3();
};

// Files from cloudfront look like this:
// {bucket-name}.s3.amazonaws.com/{optional-prefix/}{distribution-ID}.{YYYY}-{MM}-{DD}-{HH}.{unique-ID}.gz

var baseMarker = process.env.BASE_MARKER;
var seenKeys = [];

function checkForNew(){

    var hours = last24hours();
    hours.forEach(function(h){
        checkForNewByTime(h);
    });
}

var months = ['01','02','03','04','05','06','07','08','09','10','11','12'];
var days = ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16',
            '17','18','19','20','21','22','23','24','25','26','27','28','29','30','31'];

function last24hours(){
    var hours = Array.apply(null, Array(24));
    var now = new Date();
    hours = hours.map(function(_, i){
        var d = new Date(now-36e5*(i+1));
        return d.getFullYear()+"-"+months[d.getMonth()]+"-"+days[d.getDay()]+"-"+d.getHours();
    });

    return hours;
}

function checkForNewByTime(date){
    
    var opts = {"Marker":baseMarker+date,
                "Prefix":baseMarker+date,
                "Bucket":config.bucket};

    s3.listObjects(opts, function(err, data){
        if(err) console.log("Error", err);

        data.Contents.forEach(function(c){

            if(seenKeys.indexOf(c.Key) === -1){  
                seenKeys.push(c.Key);
                watch.push(c.Key+"\n");
            }
        });
    });
}

var started = false;
watch._read = function(){
    if(!started) start();

    started = true;
    return true;
};

function start(){
    var check = function(){
        checkForNew();
        setTimeout(check, 5000);
    };
    check();
}
