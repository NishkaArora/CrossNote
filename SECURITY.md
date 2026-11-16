# Security policy

## Reporting a vulnerability

Please report security issues privately through GitHub: **Security → Report a vulnerability** on this repository. Don't open a public issue.

We especially want to hear about anything that could:

- let someone emit or remove labels as crossnote (the label signing key, the `/admin/label` endpoint, the label stream);
- let someone post as the crossnote account;
- expose API keys or the Fly deployment;
- make the matcher follow instructions embedded in posts or notes (prompt injection) so that it labels posts it shouldn't.

We'll acknowledge a report within a week and keep you updated on the fix.

## Supported versions

Only the code on `main`, which is what the live labeler runs.
