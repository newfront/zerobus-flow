"""CLI and main entry logic for zerobus-flow."""

import argparse


def parse_args():
    parser = argparse.ArgumentParser(description="zerobus-flow")
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help="Environment: dev (uses .env) or prod (uses .env-prod)",
    )
    return parser.parse_args()


def main(env: str = "dev") -> None:
    print(f"zerobus-flow running in {env!r} environment.")
