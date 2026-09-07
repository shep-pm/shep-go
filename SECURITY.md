# Security Policy

## Disclaimer

shep is a community project, maintained on a reasonable-effort basis. It
cannot provide legally-binding guarantees of security.

## Security premise

This module listens for nothing. It binds no port, accepts no
connection, runs no server and holds no secret. What it opens is the one
channel the shepherd already opened for this process and named in the
environment: an inherited descriptor on unix, or a named pipe path on
Windows. It refuses a descriptor below 3 rather than adopting the app's
own standard streams.

## Reporting a vulnerability

Report security issues privately through
[GitHub Security Advisories](https://github.com/shep-pm/shep/security/advisories/new)
on the main shep repository, which takes reports for this module too. Do
not open a public issue for a suspected vulnerability.

This project is maintained on a reasonable-effort basis. Please allow at
least 90 days to investigate and prepare a fix before any public disclosure.
