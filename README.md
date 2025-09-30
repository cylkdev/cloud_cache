# CloudCache

**TODO: Add description**

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `cloud_cache` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:cloud_cache, "~> 0.1.0"}
  ]
end
```

## Testing with LocalStack

`LocalStack` provides a fully functional local AWS cloud stack
that you can use to test your application without connecting
to real AWS services.

### 1. Quick Start with Environment Variables

If you just want to run a single test command against `LocalStack`,
you can export temporary AWS credentials and override the endpoint URL:

```sh
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-west-1
aws --endpoint-url=http://localhost:4566 s3 ls
```

* `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` must be set to any non-empty value.
`LocalStack` recommends `test`.
* `AWS_DEFAULT_REGION` can be any AWS region (e.g., `us-west-1` or `us-east-1`).
* `--endpoint-url` tells the AWS Command to talk to `LocalStack` instead of AWS.

> Note: `LocalStack` requires the access key/secret to be set (even if they aren’t real),
> because its pre-signed URL signature validation checks for their presence.

### 2. Using a Custom AWS Profile for LocalStack

For a more seamless setup, you can configure a dedicated AWS Command profile for LocalStack.

Add this profile to `~/.aws/config`:

```sh
[profile localstack]
endpoint_url = http://localhost:4566
region = us-west-1
output = json
```

Add matching credentials in `~/.aws/credentials`:

```sh
[localstack]
aws_access_key_id = test
aws_secret_access_key = test
```

Now you can run commands without repeating environment variables or `--endpoint-url`.

For example:

```sh
aws --profile=localstack s3api create-bucket --bucket test --region us-west-1 --create-bucket-configuration LocationConstraint=us-west-1
aws s3 mb s3://test --profile localstack
aws s3 ls --profile localstack
```

Alternatively, you can also set the `AWS_PROFILE=localstack` environment variable,
in which case the `--profile localstack` parameter can be omitted in the commands above.

For example:

```sh
export AWS_PROFILE=localstack
aws s3api create-bucket --bucket test --region us-west-1 --create-bucket-configuration LocationConstraint=us-west-1
aws s3 mb s3://test
aws s3 ls
```

### 3. Using `awslocal`

`awslocal` serves as a thin wrapper and a substitute for the standard aws command, enabling you to run AWS Command commands within the LocalStack environment without specifying the --endpoint-url parameter or a profile.

Install the awslocal command using the following command:

```sh
pip install awscli-local
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/cloud_cache>.

