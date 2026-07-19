# Runbook: internal CA rotation

Leaf certificates rotate automatically (`flintctl rotate-certs`; every
component hot-reloads within ~2s — see ADR references in flint-tls). The
CA itself rotates rarely and DELIBERATELY as a supervised runbook
(ADR-0006 scope decision): it is a trust-root change, and v1 favors an
operator walking five verifiable steps over push-button automation.

Mechanics this leans on: every component's trust store loads EVERY
certificate in `ca.crt` (a bundle is multiple trusted roots), and configs
are rebuilt — re-reading the bundle — whenever the leaf files change, so
`rotate-certs` doubles as the reload trigger. `rotate-certs` signs with
the FIRST certificate in `ca.crt` plus `ca.key`.

Throughout: `$C` = the fleet's cert dir (`<statedir>/certs`).

## 1. Mint the new CA (nothing changes yet)

    openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
      -keyout $C/ca-new.key -out $C/ca-new.crt \
      -subj /CN=flint-ca -addext basicConstraints=critical,CA:TRUE

## 2. Expand trust: old + new CA in the bundle (old still signs)

    cat $C/ca.crt $C/ca-new.crt > $C/ca.bundle && mv $C/ca.bundle $C/ca.crt
    flintctl -f <inventory> rotate-certs    # re-sign (old key) + reload all

Verify every component still serves, now trusting both roots:

    openssl s_client -connect <node>:<port> -CAfile $C/ca.crt </dev/null

Run a live-traffic check (the cert_reload_fleet drill pattern): writes
must flow with zero errors.

## 3. Swap the signing root to the NEW CA

    cat $C/ca-new.crt $C/ca.crt.old-only > $C/ca.crt   # NEW first: it signs
    cp $C/ca-new.key $C/ca.key
    flintctl -f <inventory> rotate-certs    # leaves now signed by the new CA

(Keep the old cert in the bundle: components that have not yet reloaded
still present old-signed leaves, and both must verify during the roll.)

Verify each leaf now chains to the new root ONLY:

    openssl verify -CAfile $C/ca-new.crt $C/int.crt

## 4. Soak

Let the fleet run one full rotation interval. Watch
`flint_*_cert_days_remaining` (all leaves fresh) and error logs (no
handshake failures).

## 5. Contract trust: remove the old root

    cp $C/ca-new.crt $C/ca.crt
    flintctl -f <inventory> rotate-certs    # rebuild trust stores everywhere

Anything still presenting an old-signed cert is now refused — which is
the point. Verify live traffic once more.

## Rollback

Before step 5 the old root is still trusted: restore the previous
`ca.crt`/`ca.key` and run `rotate-certs`. After step 5, rollback is a
re-expansion: repeat step 2 with the roles swapped.
