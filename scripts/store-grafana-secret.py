"""Store the Grafana Cloud token from a local terminal in the Academy secret."""
import getpass
import json
import os
import sys
import warnings

import boto3
from botocore.exceptions import BotoCoreError, ClientError


def main() -> int:
    account = "307845861255"
    region = "us-east-1"
    name = "lab/grafana"
    os.environ["AWS_IGNORE_CONFIGURED_ENDPOINT_URLS"] = "true"
    try:
        session = boto3.Session(profile_name="academy", region_name=region)
        identity = session.client("sts").get_caller_identity()
        if identity["Account"] != account or not identity["Arn"].startswith(
            f"arn:aws:sts::{account}:assumed-role/voclabs/"
        ):
            print("Expected the Academy voclabs role. Stopping.", file=sys.stderr)
            return 1

        manager = session.client("secretsmanager")
        manager.describe_secret(SecretId=name)
        with open("/dev/tty", "r+") as terminal, warnings.catch_warnings():
            warnings.simplefilter("error", getpass.GetPassWarning)
            token = getpass.getpass("Grafana Cloud access-policy token: ", stream=terminal)
        if not token or any(character.isspace() for character in token):
            print("Enter a nonempty token without whitespace.", file=sys.stderr)
            return 1
        payload = {
            "url": "https://prometheus-prod-43-prod-ap-south-1.grafana.net/api/prom/push",
            "user": "3794044",
            "token": token,
        }
        manager.put_secret_value(SecretId=name, SecretString=json.dumps(payload))
        del payload, token
        versions = manager.describe_secret(SecretId=name).get("VersionIdsToStages", {})
        if not any("AWSCURRENT" in stages for stages in versions.values()):
            print("AWS did not report an AWSCURRENT version.", file=sys.stderr)
            return 1
    except ClientError as error:
        print(f"AWS request failed ({error.response['Error']['Code']}).", file=sys.stderr)
        return 1
    except (BotoCoreError, OSError, EOFError, KeyboardInterrupt, getpass.GetPassWarning) as error:
        print(f"Credential entry stopped ({type(error).__name__}). Use a local terminal.", file=sys.stderr)
        return 1

    print("Verified lab/grafana has an AWSCURRENT version in Academy us-east-1.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
