# Configuring an AWS CLI Profile for LocalStack

## Overview

LocalStack emulates AWS services locally, allowing you to test without
real AWS credentials. This guide describes how to configure an AWS CLI
profile named `cloud_cache` that connects to `LocalStack`.

## Steps

1. Create the profile

Open your terminal and run the following command to create a profile
named `cloud_cache`:

```bash
aws configure --profile cloud_cache
```

2. Enter placeholder credentials

When prompted, enter dummy values since `LocalStack` does not require real AWS credentials:

```bash
AWS Access Key ID [None]: test
AWS Secret Access Key [None]: test
Default region name [None]: us-west-1
Default output format [None]: json
```

The AWS CLI will then create entries in two configuration files:

- `~/.aws/credentials` - Stores the actual access keys (`aws_access_key_id` and `aws_secret_access_key`)
for each named profile. Each section (e.g. `[cloud_cache]`) contains the authentication values
used by the AWS CLI and SDKs when making requests.

- `~/.aws/config` - Stores general configuration for each profile, such as the default region, output
format, and optional service endpoints. It complements the credentials file by defining how and where
the profile should connect.

The CLI interactively asks for credentials and settings, then automatically writes them to both files:

- The keys go into `~/.aws/credentials`
- The region and output format go into `~/.aws/config`

3. Edit the configuration files

Open the AWS configuration file located at `~/.aws/config` and ensure it
contains the following:

```ini
[profile cloud_cache]
region = us-west-1
output = json
endpoint_url = http://s3.localhost.localstack.cloud:4566
```

4. Open the credentials file located at `~/.aws/credentials` and verify:

```ini
[cloud_cache]
aws_access_key_id = test
aws_secret_access_key = test
```

5. Test the connection

You can verify that the profile connects correctly by listing S3 buckets:

```bash
aws --profile cloud_cache --endpoint-url http://s3.localhost.localstack.cloud:4566 s3 ls
```

If `LocalStack` is running, you should receive a valid (possibly empty)
response instead of a connection error.
