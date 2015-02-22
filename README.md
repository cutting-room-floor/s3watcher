DEPRECATED: Use [S3 Event Notifications](http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html) instead.

S3 Watcher
==========

Watches an S3 bucket for CloudFront logs and an emits keys as logs are
delivered.

It tracks its own state, which consists of a marker and a list of all keys that
have been seen already. CloudFront logs are not delivered in order, so the
watcher will scan 24 hours back from the current marker.

### Install

It's a node module so install with npm:

```
npm install s3watcher
```

### Usage

```
var s3watcher = require('s3watcher')

var watcher = s3watcher({
    awsKey: 'xxxx',
    awsSecret: 'xxxx',
    bucket: 'bucketname',
    prefix: 'foobar/baz',
    namespace: 'foobar',
});

watcher.pipe(process.stdout);
```

- `awsKey` and `awsSecret` (both required) are obviously your AWS credentials
- `bucket` (required) the name of the S3 bucket to watch
- `prefix` (defaults to '') is the prefix to watch
- `namespace` (defaults to 'default') is a unique string that allows you to run
  multiple watcher instances against the same bucket and prefix pair.

### Tests

Copy `.s3watcherrc.example` to `.s3watcherrc` and provide actual values.
Then run:

```
npm test
```

For debug output set the `DEBUG` environment variable:

```
DEBUG=s3watcher npm test
```
