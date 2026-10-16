# Optional authenticated peer transport

The C++ TCP runtime supports opt-in mutual TLS for peer RPCs. Without any
`peer_tls_*` settings, the existing plaintext mode remains enabled and peer IDs
are claims, not cryptographic identities. Enable TLS on every member together;
mixed secure/plaintext peer clusters cannot communicate.

## Configuration and certificates

Each node needs its own PEM certificate chain and unencrypted PEM private key,
plus an explicit PEM CA trust bundle. Protect private keys with host filesystem
permissions; never commit keys or production certificates. OpenSSL 3 is a build
dependency, supplied by the pinned vcpkg manifest.

The leaf must not be a CA. It must explicitly allow both `serverAuth` and
`clientAuth` extended key usages, and contain exactly one node URI SAN:
`urn:choreoos:node:<id>`. The ID must match the configured node ID. Common names
and DNS SANs do not supply node identity. A typical leaf extension set is:

```ini
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature
extendedKeyUsage = serverAuth,clientAuth
subjectAltName = URI:urn:choreoos:node:node-a
```

For node-a in a three-member cluster, add these settings to its normal config:

```yaml
id: node-a
listen: 127.0.0.1:7101
peer: node-b@127.0.0.1:7102
peer: node-c@127.0.0.1:7103
peer_tls_listen: 0.0.0.0:8101
peer_tls_ca: /secure/choreoos/ca-bundle.pem
peer_tls_certificate: /secure/choreoos/node-a-chain.pem
peer_tls_key: /secure/choreoos/node-a.key
peer_tls_peer: node-b@10.0.0.12:8102
peer_tls_peer: node-c@10.0.0.13:8103
```

The ordinary `peer` directory remains the client redirect/contact directory;
configure addresses reachable by your clients or local gateways. The separate
`peer_tls_peer` directory carries replication traffic and must list every other
member exactly once. Addresses must be numeric IPs. Relative certificate paths
resolve against the process working directory. The existing scalar/repeated-key
config parser is not a general YAML parser.

Secure mode requires the plaintext client/management listener to bind to a
numeric loopback address and to use a different port from the TLS listener.
That listener rejects every peer frame, including peer handshakes. The TLS
listener rejects client and management frames. There is no plaintext peer
fallback. Partial TLS configuration, unknown `peer_tls_*` keys, invalid routes,
invalid local certificate identity/chain/validity and mismatched keys fail
startup before the replica opens its storage.

## Authentication boundary

Connections require TLS 1.3 and a trusted, currently valid certificate with the
required usages. Incoming identities must be configured members. Outgoing
connections additionally require the certificate ID to equal the member being
dialed, even if a different valid member answers. The protocol handshake must
agree with this certificate identity; existing RPC sender checks still apply.

Session tickets, session caching and early data are disabled. Handshakes have a
five-second deadline, a verification depth limit of four, and a 64 KiB peer
certificate-list limit. Secure mode admits at most 128 simultaneous sessions
across both listeners; the existing outbound queue limit is 64 frames per
session. These bounds do not establish denial-of-service resistance.

## Rotation and remaining limits

There is no automated certificate issuance or live reload. For CA rotation:

1. Distribute an old-plus-new CA bundle to every node and roll restart all nodes.
2. Issue replacement node certificates with the same URI identities under the
   new CA, replace each node's certificate/key, and roll restart one node at a
   time while retaining quorum. Check replication and state hashes after each.
3. Remove the old CA from every bundle and roll restart again. Restarting closes
   existing authenticated sessions as well as reloading trust.

Certificate validity and trust are checked at startup/handshake, not continuously
on established channels. There is no CRL/OCSP checking, immediate in-session
revocation, encrypted-key provider or remote client authentication here. A
compromised authorized node remains an authorized peer until its credentials
are removed and connections are closed. Loopback management assumes trusted
local processes; a remote gateway needs its own authentication and policy.
Optional gRPC management security is outside this transport change. Mutual TLS
does not establish Byzantine tolerance, full Raft correctness, or a formal
refinement proof of the production implementation.

## Verification

`PeerTlsTest` generates temporary synthetic PKI and exercises valid peer RPCs,
missing/untrusted/expired/wrong-purpose certificates, ambiguous or unknown URI
identities, protocol identity forgery, TLS 1.2 rejection, plaintext bypass,
client/peer plane separation, startup validation, wrong-member dial targets,
leader failure, disk restart, and leaf rotation with overlapping CA trust.
No test keys or production credentials are committed.

## Transition backend

`consensus_engine: transition` uses the same certificate validation and identity
binding. Its plaintext endpoints are restricted to numeric loopback addresses;
remote peers require this TLS configuration. Transition-specific integration
tests cover certificate rejection, claimed-identity mismatch, plaintext bypass
and authenticated leader failover/recovery. See [runtime integration](transition-runtime.md).
