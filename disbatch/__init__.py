__all__ = ['main', '__version__']

from .disBatch import main

try:
    from ._version import __version__
except Exception:
    # TODO: hatch-vcs doesn't seem to work well with editable installs
    # We could switch back to setuptools, but maybe we just wait for the uv build backend...
    __version__ = 'editable'
