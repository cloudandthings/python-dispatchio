# Changelog

## [0.3.0](https://github.com/cloudandthings/python-dispatchio/compare/v0.2.0...v0.3.0) (2026-04-29)


### Features

* CLI sandbox ([c89198d](https://github.com/cloudandthings/python-dispatchio/commit/c89198d0aabe7ee81af68bd10ece7c953c206398))
* Improve AWS coverage, add reference architecture ([60b772e](https://github.com/cloudandthings/python-dispatchio/commit/60b772efa42983afc10923449793e0702cfac8c8))
* Introduce [@job](https://github.com/job) decorator for job metadata and enhance run-file command ([00d9b33](https://github.com/cloudandthings/python-dispatchio/commit/00d9b335260dc3182c2a160ff76529df99c83e46))
* Parameterised runs and dates ([58d7248](https://github.com/cloudandthings/python-dispatchio/commit/58d7248df4ec709fa33fc87f7c13860560156865))
* Renames of jobs/namespaces/events ([c2fd1b9](https://github.com/cloudandthings/python-dispatchio/commit/c2fd1b9621825784cd4233131a792cb7e30a8971))


### Bug Fixes

* Clear error when CLI not installed ([34b920f](https://github.com/cloudandthings/python-dispatchio/commit/34b920f9ad64449610e8ea4f37430e671cf14692))
* Config caching ([2967516](https://github.com/cloudandthings/python-dispatchio/commit/2967516c7ee592c662a1ce423df3b351a7d2d39c))
* Namespaced events ([247d129](https://github.com/cloudandthings/python-dispatchio/commit/247d129406ed67b9c4c4266f3778cc9f40313442))
* Namespaces now work as intended ([00a74f1](https://github.com/cloudandthings/python-dispatchio/commit/00a74f18b844cfceb50ba1174384a6d9fbce73ba))
* Optimise imports ([e434b0a](https://github.com/cloudandthings/python-dispatchio/commit/e434b0a1cd210b2c17907c524bb41a32062b8f04))


### Documentation

* Fix README links for Pypi ([6654c5f](https://github.com/cloudandthings/python-dispatchio/commit/6654c5fc66c5f53e5ea2490b1989bc3a82800c5e))

## [0.2.0](https://github.com/cloudandthings/python-dispatchio/compare/v0.1.0...v0.2.0) (2026-04-23)


### Features

* Attempts/reruns ([ec534c7](https://github.com/cloudandthings/python-dispatchio/commit/ec534c7649089e34a6d9cd304d9d9faa5cd96b5e))
* CLI update from click to typer ([d84d979](https://github.com/cloudandthings/python-dispatchio/commit/d84d9791e23edf36d5c9d8c6edc3d212e145db18))
* DataStore ([b2d5651](https://github.com/cloudandthings/python-dispatchio/commit/b2d5651f432f9d9edf2b0bf9969dd0f4d9b99384))
* Enhance context injection and usage examples in job functions ([da4e67a](https://github.com/cloudandthings/python-dispatchio/commit/da4e67abe0f4bf907f97c2ac001d39a661afc09a))
* Event dependencies ([2ecb871](https://github.com/cloudandthings/python-dispatchio/commit/2ecb87160aa3dc83fe1880c5a102499a59d5a53a))
* Implement context management and tick logging for orchestrators ([863e4d7](https://github.com/cloudandthings/python-dispatchio/commit/863e4d7b009ad862dc45daeee988736f6f19820b))
* Initial dispatchio[aws] components ([fac862e](https://github.com/cloudandthings/python-dispatchio/commit/fac862e51f099cdaaaea21eb8f8a191c41a4aaf1))
* Json graph ([c72da9f](https://github.com/cloudandthings/python-dispatchio/commit/c72da9f38fc43f213cf65ffeaae7abdbbedb4ed5))
* Pools ([3729e6b](https://github.com/cloudandthings/python-dispatchio/commit/3729e6b6283ccfd44237c5aa1853ab141761027c))
* Rework CLI to use typer+rich ([3bb7720](https://github.com/cloudandthings/python-dispatchio/commit/3bb77203b81716a5a155266e1356e34601b658b3))


### Bug Fixes

* Config rework ([db308a2](https://github.com/cloudandthings/python-dispatchio/commit/db308a252d6cf6b8ff7b45d0f7cd111c9715e1f7))
* rename simulate to run_loop, added dry-run options ([d8bf16a](https://github.com/cloudandthings/python-dispatchio/commit/d8bf16a83f1d3cc238a17ff3089703a86916d1af))
* Replace run-id with run-key ([4927bc7](https://github.com/cloudandthings/python-dispatchio/commit/4927bc79b6a03097d6099fc571506b98a4f636e4))

## 0.1.0 (2026-04-09)


### Features

* Add basic dynamic job registration ([#1](https://github.com/cloudandthings/python-dispatchio/issues/1)) ([2911fcc](https://github.com/cloudandthings/python-dispatchio/commit/2911fccdfacc21905c74933d823b205221c5e0cf))
