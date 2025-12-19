# Setting Up and Running LocalStack

## Overview

LocalStack provides a fully functional local AWS cloud stack that allows you to develop and test cloud applications offline.
It runs inside a Docker container and emulates a wide range of AWS services, including S3, DynamoDB, Lambda, and others.
This guide describes how to install, configure, and start LocalStack using the LocalStack CLI.

## Prerequisites

Before proceeding, ensure that your system has the following installed:

- Docker (required to run LocalStack containers). For instructions on how to install Docker, see [Docker's official documentation](https://docs.docker.com/get-docker/).
- A working internet connection for the initial image pull

## Installation

### Brew (macOS or Linux with Homebrew)

Install the LocalStack CLI through the official LocalStack Homebrew tap:

```bash
brew install localstack/tap/localstack-cli
```

## Starting LocalStack

Once installed, you can start LocalStack in detached mode by running:

```bash
localstack start -d
```

You should see output similar to the following:

```bash
     __                     _______ __             __
    / /   ____  _________ _/ / ___// /_____ ______/ /__
   / /   / __ \/ ___/ __ `/ /\__ \/ __/ __ `/ ___/ //_/
  / /___/ /_/ / /__/ /_/ / /___/ / /_/ /_/ / /__/ ,<
 /_____/\____/\___/\__,_/_//____/\__/\__,_/\___/_/|_|

- LocalStack CLI: 4.9.0
- Profile: default
- App: https://app.localstack.cloud

[17:00:15] starting LocalStack in Docker mode               localstack.py:512
           preparing environment                            bootstrap.py:1322
           configuring container                            bootstrap.py:1330
           starting container                               bootstrap.py:1340
[17:00:16] detaching                                        bootstrap.py:1344
```

This command downloads and launches the LocalStack Docker image, setting up the emulated AWS services locally.

## Checking Service Status

After LocalStack starts, you can view the status of available services by running:

```bash
localstack status services
```

You should see a table listing all active services:

```bash
┏━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┓
┃ Service                  ┃ Status      ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━┩
│ acm                      │ ✔ available │
│ apigateway               │ ✔ available │
│ cloudformation           │ ✔ available │
│ cloudwatch               │ ✔ available │
│ config                   │ ✔ available │
│ dynamodb                 │ ✔ available │
│ s3                       │ ✔ available │
...
```

## Integration with AWS CLI

Once LocalStack is running, you can use the AWS CLI to interact with the emulated services by specifying the LocalStack endpoint.

For example, to list S3 buckets:

```bash
aws --profile cloud_cache --endpoint-url http://s3.localhost.localstack.cloud:4566 s3 ls
```

This command connects to the LocalStack S3 service instead of AWS.

For details on setting up AWS credentials, see [Configuring an AWS CLI Profile for LocalStack](./Configuring%20an%20AWS%20CLI%20Profile%20for%20LocalStack.md).

## Stopping LocalStack

To stop LocalStack, you can use the following command:

```bash
localstack stop
```
