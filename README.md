S3 Watcher
==========

Watches an S3 bucket for CloudFront logs and an emits keys as logs are
delivered.

### Install

It's a node module so install with npm:

     npm install s3watcher

### Usage

    var s3watcher = require('s3watcher')

    var watcher = s3watcher({
        awsKey: 'xxxx',
        awsSecret: 'xxxx',
        bucket: 'bucketname',
        prefix: 'foobar/baz',
        namespace: 'foobar',
    });

    watcher.pipe(process.stdout);

- `awsKey` and `awsSecret` (both required) are obviously your AWS credentials
- `bucket` (required) the name of the S3 bucket to watch
- `prefix` (defaults to '') is the prefix to watch
- `namespace` (defaults to 'default') is a unique string that allows you to run
  multiple watcher instances against the same bucket and prefix pair.

### Tests

Copy `.s3watcherrc.example` to `.s3watcherrc` and provide actual values.

### How it works

Cloudfront logs might come from anytime the last 24hrs. The filenames have the
year, month, day and hour in them. So s3watcher divides up the last 24hrs of
logs by hour. S3Watcher then makes 24 requests to listObjects on S3 with each
hour key prefix. As those respond, it checks for any keys that have a
LastModified timestamp that are newer then the last most recently seen, those
keys it push into the stream. Once all of the 24 list requests respond, then we
find the most recent modified date in the bucket, and save that to `.s3watcher`
in the same bucket. On subsequent checks it first loads the last modified
timestamp from `.s3watcher`
