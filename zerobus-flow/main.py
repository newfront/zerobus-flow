"""Entry point script for zerobus-flow."""

from zerobus_flow.main import main, parse_args

if __name__ == "__main__":
    args = parse_args()
    main(env=args.env)
