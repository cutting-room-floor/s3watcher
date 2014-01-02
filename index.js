var AWS = require ('aws-sdk');


module.exports = watch = {};
var config, s3;


watch.config = function(c){
    
    s3 = AWS.S3();
}


