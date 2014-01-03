var AWS = require ('aws-sdk');
var Readable = require('stream').Readable;

module.exports = watch = new Readable();
var config, s3;


watch.config = function(c){
  config = c;
  AWS.config.update({accessKeyId: c.awsKey, secretAccessKey: c.awsSecret});
  s3 = new AWS.S3();
}
// {bucket-name}.s3.amazonaws.com/{optional-prefix/}{distribution-ID}.{YYYY}-{MM}-{DD}-{HH}.{unique-ID}.gz

// E1MSP5URRDWXN6.2013-04-14-00.62RFS2Yg.gz

var baseMarker = process.env.BASE_MARKER;


var seenKeys = [];

function checkForNew(){

  var hours = last24hours();
  hours.forEach(function(h){
    checkForNewByTime(h);
  });
}

function last24hours(){
  
  var hours = Array.apply(null, Array(24));
  var now = new Date();

  var months = Array.apply(null, Array(12)).map(function(_, i){
    if((i+1+"").length === 1) return "0"+(i+1);
    else return i+1+"";
  })

  var days = Array.apply(null, Array(31)).map(function(_, i){
    if((i+1+"").length === 1) return "0"+(i+1);
    else return i+1+"";
  })


  hours = hours.map(function(_, i){
    var d = new Date(now-36e5*(i+1));
    return d.getFullYear()+"-"+months[d.getMonth()]+"-"+days[d.getDay()]+"-"+d.getHours();
  });

  return hours;
}

function checkForNewByTime(date){
 
  var opts = {"Marker":baseMarker+date,
              "Prefix":baseMarker+date,
              "Bucket":config.bucket}

  //console.log(opts);

  s3.listObjects(opts, function(err, data){
    if(err) console.log("Error", err);
    //console.log(data);

    data.Contents.forEach(function(c){

      if(seenKeys.indexOf(c.Key) === -1){  
        seenKeys.push(c.Key);
        watch.push(c.Key+"\n");
      }
    });
  });
}


/*setTimeout(function(){
  watch.push('beep 3 ', 'utf8');
  watch.push('boop\n', 'utf8');

  watch.push(null);

}, 2000)
*/


var started = false;


watch._read = function(){


  if(!started) start();

  started = true;
  return true;
};


function start(){

  var check = function(){
    checkForNew();
    setTimeout(check, 5000)
  }

  check();

}
