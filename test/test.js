var assert = require("assert");

var s3watcher = require('../index.js')


s3watcher.config({awsKey:process.env.AWS_KEY, 
                  awsSecret:process.env.AWS_SECRET, 
                  markerPrefix: process.env.MARKER_PREFIX,
                  bucket:process.env.BUCKET,
                  hoursBack: 36});


s3watcher.pipe(process.stdout);


//s3watcher.on('data', function(d){

//    console.log(d.toString('utf8').split("\n"));
//});



