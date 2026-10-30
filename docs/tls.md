# TLS

Every cluster needs TLS. The nodes connect to each other over HTTPS and always check each other's
certificate. One keystore is used for all ports.

You can bring your own certificate, or let cert-manager issue it.

## Use your own certificate

Create a Secret with a PKCS#12 keystore **without a password**, and the CA that signed it:

```bash
kubectl create secret generic camusdb-tls \
  --from-file=certificate.pfx=/path/to/certificate.pfx \
  --from-file=ca.crt=/path/to/ca.crt
```

```yaml
spec:
  tls:
    secretRef: {name: camusdb-tls}
    keystoreKey: certificate.pfx   # default
    caKey: ca.crt                  # default
```

- For a self-signed certificate, `ca.crt` is the certificate itself.
- With [authentication](authentication.md), the nodes also use this certificate as a client
  certificate. It must allow both server and client authentication.

## Use cert-manager

```yaml
spec:
  tls:
    certManager:
      issuerRef: {name: camusdb-ca, kind: Issuer}
```

The operator creates the Certificate, then builds a keystore without a password in the Secret
`<name>-keystore`.

- **Use a CA issuer.** With a self-signed issuer, every renewal makes a new CA, and old and new
  nodes stop trusting each other during the rollout.
- The issuer must write `ca.crt`. ACME issuers do not.
- The certificate uses a wildcard name for the pods, so scaling does not issue a new certificate.

## Renewals

CamusDB reads its certificate only at startup. When the certificate changes, the operator restarts
the nodes one at a time. With your own Secret, the change is picked up at the next check, within
about a minute.
