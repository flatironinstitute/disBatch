# Changelog

## 3.0.0 (upcoming)

https://github.com/flatironinstitute/disBatch/pull/32

### Breaking changes
- The Python package has been renamed `disbatch` from `disbatchc`

### Fixes
- disBatch now installs all the necessary helper files so out-of-place installs work
- Bugs (e.g. misspelled variables) in less common code paths fixed

### Enhancements
- PEP518 compliant build system
- More robust discovery of disBatch installation by worker processes
- Initial release on PyPI
- uvx and pipx support
- Set up linting and formatting

### Changes
- `kvsstcp` submodule is now vendored
