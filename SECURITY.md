# Security Policy

## Supported versions

Security fixes are applied to the latest release and to the `main` branch.

## Reporting a vulnerability

Please do not open a public issue for security problems. Report them privately
through GitHub's security advisory feature on this repository ("Report a
vulnerability" under the Security tab). You will receive an acknowledgement
within a few days, and a fix or mitigation plan once the report is confirmed.

## Scope notes

kvorum's Raft and client APIs are unauthenticated and unencrypted by design in
this version; they are meant to run on a trusted network. Exposing them to
untrusted networks is out of scope for vulnerability reports until transport
security is added.
