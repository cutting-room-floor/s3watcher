var assert = require("assert");
var s3watcher = require('../index.js');


s3watcher.config({awsKey:process.env.AWS_KEY, 
                  awsSecret:process.env.AWS_SECRET, 
                  bucket:process.env.BUCKET});


s3watcher.pipe(process.stdout);

