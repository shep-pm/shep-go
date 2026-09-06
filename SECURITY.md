# Security Policy

## Disclaimer

shep is a community project, maintained on a reasonable-effort basis. It
cannot provide legally-binding guarantees of security.

## Security premise

This module opens no socket, binds no port, runs no server and holds no
secret. It takes a file descriptor named by the environment and writes
JSON to it: the shepherd names a descriptor it opened for this process,
and the module refuses anything below 3 rather than adopting the app's
own standard streams.

## Reporting a vulnerability

Report security issues privately through
[GitHub Security Advisories](https://github.com/shep-pm/shep/security/advisories/new)
for this repository. Do not open a public issue for a suspected
vulnerability.

This project is maintained on a reasonable-effort basis. Please allow at
least 90 days to investigate and prepare a fix before any public disclosure.
