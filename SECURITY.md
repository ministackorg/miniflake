# Security Policy

## ⚠️ Important: Local Development Only

MiniFlake is designed **exclusively for local development and CI/CD
testing** against the Snowflake protocol.

**Do not expose MiniFlake to the internet or any untrusted network.**

- It has no authentication — any request is accepted. The Snowflake
  drivers' `user` / `password` fields are required to construct a
  connection but the values are ignored by the server.
- All data is stored on local disk (DuckDB) and in-memory state; nothing
  is encrypted at rest.
- The `PUT` / `GET` server-side handlers read from and write to the host
  filesystem. A misconfigured `file://` URL could expose any path the
  miniflake process can read.
- The `EXECUTE TASK` and `EXECUTE IMMEDIATE` flows run arbitrary SQL
  against the DuckDB engine in-process — same trust boundary as the
  miniflake binary itself.

## Reporting a Vulnerability

If you find a security issue that could affect users running MiniFlake
in a way that exposes their host system or data, please open a GitHub
issue tagged `security`.

Since this is a local dev tool with no authentication by design, most
"vulnerabilities" are intentional trade-offs for simplicity. But if you
find something that could cause unintended host compromise — for example
path traversal in stage file access, command injection in a UDF
execution path, SSRF via a misparsed `file://` URL in `PUT`/`GET` — please
report it.

## Recommended Usage

```yaml
# docker-compose.yml — bind to localhost only, never 0.0.0.0 on a shared machine.
services:
  miniflake:
    image: ghcr.io/ministackorg/miniflake:latest
    ports:
      - "127.0.0.1:8084:8084"
    volumes:
      - ./data:/data
      - ./stages:/stages
```

For container-only deployments, prefer the distroless image (default):
no shell, no package manager, no busybox — the typical CVE-bearing
surface is gone. The trade-off is no `docker exec sh` for debugging.

Never run MiniFlake with the stage directory pointing at a sensitive
host path. The `PUT`/`GET` handlers do their best to constrain access to
the configured `--stage-dir`, but treat the process boundary the same as
you would treat any local-only test daemon.
