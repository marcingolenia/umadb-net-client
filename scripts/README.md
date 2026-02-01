# TLS certificates for tests

Test certs are **generated**, not committed. Generate them with the script below.

## Generate certs

From the repo root:

```bash
./scripts/gen-tls-certs.sh
```

Creates **Tests/certs/** with:

| File         | Use |
|--------------|-----|
| **ca.pem**   | CA certificate — set `UMADB_TLS_CA_CERT` to this path for the TLS test (client trusts server). |
| **server.pem**   | Server certificate — use with your TLS-enabled UmaDB server. |
| **server-key.pem** | Server private key — use with your TLS-enabled UmaDB server. |
| ca-key.pem   | CA private key (used only to sign server.pem; keep private). |

**Tests/certs/** is in `.gitignore` — do not commit these files.

## Run the TLS test locally

1. Generate certs: `./scripts/gen-tls-certs.sh`
2. Start a TLS UmaDB server that uses **Tests/certs/server.pem** and **server-key.pem** (e.g. on port 50451).
3. Run the test with the CA cert:

   ```bash
   export UMADB_TLS_CA_CERT="$(pwd)/Tests/certs/ca.pem"
   dotnet test Tests/Tests.fsproj --filter "TLS with CA cert"
   ```

Optional: `UMADB_TLS_PORT`, `UMADB_TLS_HOST`, `UMADB_TLS_API_KEY`.

## CI

- Generate certs in CI (e.g. in a job step), then set `UMADB_TLS_CA_CERT` and start your TLS server with `server.pem` + `server-key.pem`.
- If `UMADB_TLS_CA_CERT` is not set, the TLS test is skipped and the run still passes.
