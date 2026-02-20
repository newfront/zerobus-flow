.PHONY: build install-dist package publish release test

# Format and lint Python source.
build:
	uv run ruff format src tests
	uv run ruff check src tests --fix

# Run tests with pytest.
test:
	uv run pytest

# Package the application: build wheel and sdist into dist/.
# See: https://docs.astral.sh/uv/guides/package/
package:
	uv build

# Bump the project version in pyproject.toml.
# Usage: make release [bump=minor]
#   bump: major | minor | patch | stable | alpha | beta | rc | post | dev (default: minor)
# Examples:
#   make release           # 0.1.0 -> 0.2.0 (minor)
#   make release bump=patch # 0.1.0 -> 0.1.1
#   make release bump=major # 0.1.0 -> 1.0.0
release:
	uv version --bump $(or $(bump),minor)

# Install from dist/ (wheel or sdist).
# Run 'make package' first.
install-dist:
	@if [ -n "$$(find dist -maxdepth 1 -name '*.whl' -print -quit)" ]; then \
		uv pip install dist/*.whl; \
	elif [ -n "$$(find dist -maxdepth 1 -name '*.tar.gz' -print -quit)" ]; then \
		uv pip install dist/*.tar.gz; \
	else \
		echo "No wheel or sdist in dist/. Run 'make package' first."; exit 1; \
	fi

# Publish the package to PyPI (or UV_PUBLISH_INDEX).
# Requires UV_PUBLISH_TOKEN (PyPI API token) or UV_PUBLISH_USERNAME + UV_PUBLISH_PASSWORD.
# Example: UV_PUBLISH_TOKEN=pypi-xxx make publish
publish: package
	uv publish
