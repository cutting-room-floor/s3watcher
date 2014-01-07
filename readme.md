S3 Watcher
==========


This watches an an s3 bucket for new files to be added to it.  Its useful if you need to know when log files from cloudfront get added to an s3 bucket.


### Install

It's a node module, install with npm.

     npm install s3watcher


### Usage


    var s3watcher = require('s3watcher')

    s3watcher.config({awsKey:process.env.AWS_KEY,
                      awsSecret:process.env.AWS_SECRET,
                      makerPrefix:process.env.MARKER_PREFIX,
                      bucket:process.env.BUCKET});

    s3watcher.pipe(process.stdout);
