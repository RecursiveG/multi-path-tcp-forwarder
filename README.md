## r05-multi-path-tcp-forwarder

```
# build
cargo build --release

# on server
RUST_LOG=info r05-multi-path-tcp-forwarder \
    --is-server \
    --target=x.x.x.x:yy \
    --listen-on="[::]:5678"

# on local
RUST_LOG=info r05-multi-path-tcp-forwarder \
    --target="server.ip.address:5678" \
    --listen-on="[::1]:1234"
```

TCP traffics to `[::1]:1234` will then be forwarded to `x.x.x.x:yy`.