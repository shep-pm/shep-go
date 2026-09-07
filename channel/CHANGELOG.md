# Changelog

## 1.0.0 (2026-09-07)


### Features

* **channel:** add the wire types and the module manifest ([5e791a3](https://github.com/shep-pm/shep-go/commit/5e791a35bf86bee4034e5ff000e0eee13d0205c4))
* **channel:** answer every action, including the ones nobody registered ([4398136](https://github.com/shep-pm/shep-go/commit/4398136fdc3031b6228a5eeffb7f2818bbef8e58))
* **channel:** find the channel and open it on both platforms ([9963c5b](https://github.com/shep-pm/shep-go/commit/9963c5bf08dc723837e4b6e492556e4be6b158f5))
* **channel:** frame one message in each direction ([fa55e30](https://github.com/shep-pm/shep-go/commit/fa55e3073deca95f19a9e2166dc2d404e6a33cbe))
* **channel:** open the channel without owning the loop ([756337b](https://github.com/shep-pm/shep-go/commit/756337b3fe9a1c180169a2027ef3a8c188f401c9))
* **channel:** queue what must be sent and drop what may be ([3f5a12c](https://github.com/shep-pm/shep-go/commit/3f5a12cd323f523eb4d58c70c340a8a3e700e18a))
* **channel:** serve the channel, and do nothing well without one ([2b2a376](https://github.com/shep-pm/shep-go/commit/2b2a37665ea1ebbbc9f034a7ebdfb0b102ac5f65))
* **channel:** the shepherd-channel client for Go ([364df68](https://github.com/shep-pm/shep-go/commit/364df680ad9a8c25b5740881f1615d34c1315338))
* **channel:** warn when the read loop stops before the shepherd does ([c55c040](https://github.com/shep-pm/shep-go/commit/c55c04053718c6d0beed96ae11fc995eaa0d4e0c))


### Bug Fixes

* **channel:** close the duplicated descriptor when the handoff close fails ([3e4fe1f](https://github.com/shep-pm/shep-go/commit/3e4fe1fe1d968d96c0abd8f102af197af08d36e4))
* **channel:** keep a handler that ends its goroutine out of the read loop ([f9bce12](https://github.com/shep-pm/shep-go/commit/f9bce1260a811314c1d5b3c97f26bd81ee27642f))
* **channel:** mark socketpair descriptors close-on-exec in the harness ([6197ac2](https://github.com/shep-pm/shep-go/commit/6197ac2eb02dd21bc6090ae47b7bb6523e0930cd))
* **channel:** never report a stranded message as sent ([cf21d72](https://github.com/shep-pm/shep-go/commit/cf21d7228cd1c97ff8207b2b3453d5342873681a))
* **channel:** serialize pushLossy's closed check against close ([bd1306e](https://github.com/shep-pm/shep-go/commit/bd1306e95714c219d3c87722869e94ec613830c6))
* **channel:** split the Params doc comment under IR-47's cap ([7c73703](https://github.com/shep-pm/shep-go/commit/7c73703a4402d596ab7327d71d83d26a18c8a7a7))
* **channel:** take the pipe handle once instead of on every peek ([f65a67b](https://github.com/shep-pm/shep-go/commit/f65a67b53797583845d972589f9a29ddd4fac9fe))
