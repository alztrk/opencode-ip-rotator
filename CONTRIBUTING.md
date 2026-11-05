# Contributing Guidelines

Thank you for considering contributing to `opencode-ip-rotator`. Please read through these guidelines to maintain project quality.

## Code Style & Standards

- **Python**: Follow PEP 8 guidelines. Write clean, readable code with type hints where appropriate.
- **Microservices Integrity**: Maintain clear separation of concerns between `proxy-server` (request handling & metrics) and `warp-rotator` (WARP daemon & networking).
- **No Direct Exception Swallowing**: Always log exceptions with context (`log.error`) instead of passing silently.

## Submitting Pull Requests

1. Fork the repository and create a new feature branch from `master`.
2. Make your changes and test using `pytest`, `python test.py`, and `docker compose up -d --build`.
3. Commit with concise, descriptive commit messages (e.g. `feat: add retry backoff logic`, `fix: resolve SQLite lock issue`).
4. Submit a Pull Request targeting the `master` branch with a summary of changes.

## Security & Confidentiality

- Do not commit local environment files, SQLite databases (`.db`), or private API keys.
- Ensure `.gitignore` rules are respected before pushing.
