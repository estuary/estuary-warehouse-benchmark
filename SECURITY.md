# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please **report it responsibly**.

- Contact: Estuary Support  
- We will acknowledge your report within 3 business days and provide a status update within 7 days.
- Please **do not** open public GitHub issues for security-related topics.

##  Scope and Assurance

This project:

- **Does not process or store any sensitive or personal data**.
- **Does not include production credentials, secrets, or proprietary workloads**.
- Is intended for **benchmarking and testing** against open datasets.

## Dependencies

We recommend users:
- Keep dependencies (e.g., Python packages) up to date using `requirements.txt` or `pip-tools`.
- Use isolated environments (like `venv` or Docker) to minimize system-wide risks.

##  Safe Contribution Guidelines

When contributing, please:
- Avoid adding real credentials or sensitive configurations to the `.env` or `main.py` file.
- Use mock or sample datasets only that are in line with TPCH schema.
- Do not include connections to live or production systems as it will create memory errors on your system.

---

Let us know if you see something concerning.
