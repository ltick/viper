

See this more of breaking change wishlists.


## v4.0.0 (TBD)

See [code changes](https://github.com/coreos/etcd/compare/v3.3.0...v4.0.0) and [v4.0 upgrade guide](https://github.com/coreos/etcd/blob/master/Documentation/upgrades/upgrade_4_0.md) for any breaking changes.

### Breaking Changes

- [Secure etcd by default](https://github.com/coreos/etcd/issues/9475).
- Change `/health` endpoint output.
  - Previously, `{"health":"true"}`.
  - Now, `{"health":true}`.
  - Breaks [Kubernetes `kubectl get componentstatuses` command](https://github.com/kubernetes/kubernetes/issues/58240).
- Deprecate [v2 storage backend](https://github.com/coreos/etcd/issues/9232).
  - v2 API is still supported via [v2 emulation]().
- `clientv3.Client.KeepAlive(ctx context.Context, id LeaseID) (<-chan *LeaseKeepAliveResponse, error)` is now [`clientv4.Client.KeepAlive(ctx context.Context, id LeaseID) <-chan *LeaseKeepAliveResponse`](TODO).
  - Similar to `Watch`, [`KeepAlive` does not return errors](https://github.com/coreos/etcd/issues/7488).
  - If there's an unknown server error, kill all open channels and create a new stream on the next `KeepAlive` call.
- Rename `github.com/coreos/client` to `github.com/coreos/clientv2`.

