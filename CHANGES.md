# Changelog

## 3.0.0 (upcoming)

https://github.com/flatironinstitute/disBatch/pull/32

### Breaking changes
- The Python package has been renamed `disbatch` from `disbatchc`
- Removed the `disBatch` script from the repo root. Instead, a `disBatch` command will be placed on the `PATH` upon installation, or see the [installation instructions](Readme.md#Installation) for other options.

### Fixes
- disBatch now installs all the necessary helper files so out-of-place installs work
- Bugs (e.g. misspelled variables) in less common code paths fixed

### Enhancements
- PEP518 compliant build system
- More robust discovery of disBatch installation by worker processes
- Initial release on PyPI
- uvx and pipx support
- Set up linting and formatting
- The executable can be invoked as `disbatch` or `disBatch`
- Refreshed the readme
- Added `disbatch --version` and `disbatch.__version__`
- Added MacOS test
- Added `--fail-fast` option [https://github.com/flatironinstitute/disBatch/pull/38]

### Changes
- `kvsstcp` submodule is now vendored
