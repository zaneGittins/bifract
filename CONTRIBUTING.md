# Contributing to Bifract

Bifract is maintained by a single developer. Contributions are welcome and appreciated.

## How to Contribute

1. **Bug reports and feature requests** - Open a [GitHub Issue](https://github.com/zaneGittins/bifract/issues).
2. **Code contributions** - Fork the repo, create a branch, and open a pull request.
3. **Documentation** - Improvements to docs are always welcome.

## Development Setup

```bash
# Clone the repo
git clone https://github.com/zaneGittins/bifract.git

# Build and run locally
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d

# Run tests
go test ./pkg/parser/...
```

## Pull Request Guidelines

- Keep PRs focused on a single change.
- Include a clear description of what and why.
- Ensure `go vet` and existing tests pass.
- Sign the [CLA](CLA.md) by commenting on your PR: *"I have read the CLA Document and I hereby sign the CLA"*

## Code Style

- Go standard formatting (`gofmt`).
- Concise comments.

## License

By contributing, you agree that your contributions will be licensed under the [AGPL-3.0](LICENSE).
