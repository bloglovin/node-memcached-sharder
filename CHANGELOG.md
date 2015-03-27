## 3.0.0 (2015-03-27)


#### Features

* **deps:** upgrade to hapi@8.x and memcached@2.x ([3a25d723](https://github.com/bloglovin/bloglovin-memcached-sharder/commit/3a25d723be3c20f03b3f02e296385759e2ecec0c))


#### Breaking Changes

* Updated the underlying memcached library. The one detected difference is that `undefined` now is returned instead of `false` when a key doesn’t exist.
 ([3a25d723](https://github.com/bloglovin/bloglovin-memcached-sharder/commit/3a25d723be3c20f03b3f02e296385759e2ecec0c))


### 2.1.1 (2014-10-21)


#### Bug Fixes

* **plugin:** properly register hapi plugin metadata ([c2be91ba](https://github.com/bloglovin/bloglovin-memcached-sharder/commit/c2be91ba7eb9a068f984e06671f73e310d514876))

