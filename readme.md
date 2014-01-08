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


### How it works:


It's designed to be watching a bucket where AWS is putting cloudfront logs.


Cloudfront logs might come from anytime the last 24hrs. The filenames have the
year, month, day and hour in them. So s3watcher divides up the last 24hrs of
logs by hour. S3Watcher then makes 24 requests to listObjects on S3 with each
hour key prefix. As those respond, it checks for any keys that have a
LastModified timestamp that are newer then the last most recently seen, those
keys it push into the stream. Once all of the 24 list requests respond, then we
find the most recent modified date in the bucket, and save that to `.s3watcher`
in the same bucket. On subsequent checks it first loads the last modified
timestamp from `.s3watcher`
