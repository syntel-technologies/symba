# Changelog

## 0.1.0 (2026-09-13)


### Features

* expose worker task registrations and improve scheduling fairness ([06cd4ac](https://github.com/syntel-technologies/symba/commit/06cd4ac851d2fdb952ebab72098ea706cec4fc86))


### Bug Fixes

* **ci:** align protocol and performance checks with current contracts ([7d6e25b](https://github.com/syntel-technologies/symba/commit/7d6e25b4308eb87741c9a175dbe22a9652738844))
* **ci:** isolate database checks and cover worker capabilities ([f6836cb](https://github.com/syntel-technologies/symba/commit/f6836cbc2e0fc92bf97a0480db638018936bc600))
* **ci:** measure sustained throughput with a working echo worker ([3b5ac36](https://github.com/syntel-technologies/symba/commit/3b5ac3684af724f04fded70dede5db5d966f3380))
* **ci:** validate the engine in locked project environments ([c532c8b](https://github.com/syntel-technologies/symba/commit/c532c8b6d497d0e2ca89969bfad88a00fb6f7647))
* restore Symba CI and consolidate development under Syntel ([4ceee6d](https://github.com/syntel-technologies/symba/commit/4ceee6d3dc8bf0dc79840a48a2b83bb4dda83e47))


### Performance Improvements

* coalesce worker status writes during capacity bursts ([e7ccb89](https://github.com/syntel-technologies/symba/commit/e7ccb890a879a432b01d15a4a01ec08471afa10b))
* index archive lookups used by terminal audit writes ([5debb30](https://github.com/syntel-technologies/symba/commit/5debb304b7827996a7ee54f84f27d9caf26b0cee))
* remove HTTP middleware overhead and coalesce worker capacity ([49dda1d](https://github.com/syntel-technologies/symba/commit/49dda1d6e8f68cb73ba73788b7c17d846643d596))
* reuse engine connections through the console proxy ([5c5a38f](https://github.com/syntel-technologies/symba/commit/5c5a38f8cdd9da2362c210af5480253ec9843388))
* use native event loop and HTTP parser for engine traffic ([90bd3a3](https://github.com/syntel-technologies/symba/commit/90bd3a395b34a7ee690eeba92c063e56e44a3e65))
