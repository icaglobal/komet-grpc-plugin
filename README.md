# komet-grpc-plugin

A Komet datastore and search provider backed by a remote
[tinkar-service](https://github.com/icaglobal/tinkar-service) over gRPC, instead of a local
data store. Selected in Komet's startup dialog as **Connect to gRPC service**.

The plugin supplies two lifecycle services:

| Service | Phase | Role |
|---------|-------|------|
| `GrpcPrimitiveDataService.Controller` | `DATA_STORAGE` | the datastore (`DATA_PROVIDER` group) |
| `GrpcSearchService.Controller` | `INDEXING` | remote search (`SEARCH_ENGINE` group) |

Selecting this datastore also excludes the local Lucene indexer, so no index is built for a
store that is never queried locally.

---

## Connecting

Set the **Service URL** field in Komet's startup dialog. It accepts `host:port`, defaulting to
`localhost:9095`:

```
localhost:9095
svc.example.com:443
```

An optional scheme selects the transport and is authoritative when present, so the dialog alone
can move between plaintext and TLS:

| Service URL | Transport |
|-------------|-----------|
| `localhost:9095` | whatever the `komet.grpc.tls*` properties say (plaintext by default) |
| `grpc://localhost:9095` or `http://…` | plaintext |
| `grpcs://svc.example.com:443` or `https://…` | TLS |

`komet.grpc.host` and `komet.grpc.port` override the dialog entirely, which is useful when
auto-selecting this controller by name without a UI round trip.

---

## Transport Security (TLS)

Plaintext is the default. TLS is enabled per launch through system properties:

| Property | Effect |
|----------|--------|
| `komet.grpc.tls` | `true` to use TLS, validating against the JDK default trust store |
| `komet.grpc.tls.ca` | path to a PEM certificate to trust — **implies TLS** |
| `komet.grpc.tls.authority` | override the name used for hostname verification |

Use `komet.grpc.tls` alone for a certificate issued by a public CA: the JDK trust store already
contains the issuer, and there is nothing to distribute. Use `komet.grpc.tls.ca` for a
self-signed or privately-issued certificate — a self-signed certificate is its own trust
anchor, so the file to trust is the one the server presents.

Setting a CA path implies TLS deliberately: a caller who names trust material has already said
which transport they mean, and requiring a second flag alongside it only creates a way to
configure a CA that is silently ignored.

### Passing the properties

**Use the `JAVA_OPTS` environment variable.** The `launchKomet` script appends command-line
arguments *after* `-m module/MainClass`, so a `-D` typed on the command line becomes an
application argument and is **silently ignored** as a system property:

```bash
# ✗ silently does nothing
./komet-desktop/target/kometRuntimeImage/bin/launchKomet -Dkomet.grpc.tls.ca=/path/ca.pem

# ✓ reaches the JVM
JAVA_OPTS="-Dkomet.grpc.tls.ca=/abs/path/server-cert.pem" \
  ./komet-desktop/target/kometRuntimeImage/bin/launchKomet
```

`JAVA_OPTS` must be set on or before the launch — the script reads it while building the java
command. Quote the value if you keep it in a variable: an unset variable expands to nothing and
shifts the remaining arguments.

There is currently **no way to set this in an installed application**, which has no launcher
script to wrap. Configuring the certificate from the startup dialog is the intended fix and is
not yet implemented; see *Known limitations*.

### Local development against a TLS service

tinkar-service ships an idempotent certificate generator. From the tinkar-service checkout:

```bash
./scripts/generate-dev-cert.sh          # writes certs/server-cert.pem + certs/server-key.pem

export GRPC_TLS_ENABLED=true
export GRPC_TLS_CERT_CHAIN=$PWD/certs/server-cert.pem
export GRPC_TLS_PRIVATE_KEY=$PWD/certs/server-key.pem
./mvnw spring-boot:run -Dspring-boot.run.arguments="--dataset.name=gudidsubset"
```

Then launch Komet trusting that certificate, and pick **Connect to gRPC service**:

```bash
JAVA_OPTS="-Dkomet.grpc.tls.ca=/abs/path/to/tinkar-service/certs/server-cert.pem" \
  ./komet-desktop/target/kometRuntimeImage/bin/launchKomet
```

Confirm the transport in the log — the bracket always reports what was chosen:

```
Initializing gRPC connection to localhost:9095 [TLS (CA: /…/server-cert.pem)]
gRPC client initialised → localhost:9095 [TLS (CA: /…/server-cert.pem)]
```

`[plaintext]` there means no TLS property arrived.

---

## Troubleshooting

**`UNAVAILABLE: Network closed for unknown reason`, and every bootstrap concept fails.**
A plaintext client reached a TLS port. The server accepted the connection and dropped it. Check
for `[plaintext]` in the log — usually the properties did not arrive, or the runtime image
contains an older plugin build than the one you compiled.

**`certificate signed by unknown authority`.** TLS is working, but the certificate is not
trusted. Expected for a self-signed certificate with no `komet.grpc.tls.ca` set.

**Handshake fails with nothing naming the cause.** The certificate probably lacks a
`subjectAltName` covering the host being dialled. Java matches SANs only and ignores CN
entirely. Inspect it with:

```bash
openssl x509 -in certs/server-cert.pem -noout -text | grep -A1 'Alternative Name'
```

macOS ships LibreSSL, whose `x509` has no `-ext` option — the `-text | grep` form above is the
portable check. If the host genuinely cannot be covered by a SAN (a port-forward, an
IP-addressed service), `komet.grpc.tls.authority` overrides the verified name. It weakens the
guarantee that the peer is who the address says, so treat it as a development convenience.

**`gRPC TLS trust material not found`.** `komet.grpc.tls.ca` points at a missing or unreadable
file. The message names the absolute path it tried.

---

## Known limitations

- **No UI configuration for the certificate.** TLS is launch-time only, so an installed
  application cannot be pointed at a TLS service. Adding a field to the startup dialog is
  blocked by a rendering bug in komet: a provider declaring a *second*
  `DataServiceProperty` makes the datasource dialog render blank, with CSS failures on
  `.text-input` and scroll-pane rules because modena lookups such as `-fx-box-border` do not
  resolve. Every provider currently shipped declares exactly one property, so the case has
  never been exercised.
- **`komet.grpc.tls.authority` is untested.** Implemented, but no scenario has exercised it.
- **Public-CA TLS is untested.** The JDK-trust-store path is exercised only negatively, by
  confirming it rejects a self-signed certificate.

---

## Notes on the implementation

Two details are load-bearing and easy to undo by accident:

- The channel is built with `NettyChannelBuilder` **named directly**, not through
  `ManagedChannelBuilder.forAddress()`. Transport discovery cannot work from this jar:
  `ServiceLoader` is inert in a named module, and gRPC's hard-coded fallback looks for the
  *unshaded* `io.grpc.netty.NettyChannelProvider` while the shaded artifact supplies
  `io.grpc.netty.shaded.io.grpc.netty.NettyChannelProvider`.
- `GrpcSslContexts` is used from the **shaded** package, matching the shaded builder it is
  handed to. Mixing shaded and unshaded contexts fails at runtime on classes that only share a
  simple name.

For the same reason, `GrpcSearchClient` registers the DNS name resolver and pick-first load
balancer explicitly rather than relying on discovery.

<!-- BEGIN ike-managed: developer-setup -->

## Developer Setup

New to IKE development? The
[Developer Environment guide](https://ike.network/ike-tooling/ike-build-standards/developer-environment.html)
covers IDE configuration, JDK 25 setup, and the tooling conventions
every IKE workspace expects — start there before your first build.
<!-- END ike-managed: developer-setup -->
