# TLS certificates for tests

Test certs are **generated**.

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
