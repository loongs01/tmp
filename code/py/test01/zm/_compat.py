# -*- coding: utf-8 -*-
"""
Compatibility helper to re-export modules with relative-first import.
Used by thin wrapper files to avoid duplicated try/except logic.
"""
from __future__ import annotations

import importlib
import warnings
from types import ModuleType
from typing import Dict, Any


def reexport_module(
    target_globals: Dict[str, Any],
    relative_import: str,
    absolute_import: str,
    warn_msg: str,
) -> ModuleType:
    """
    Import a module (relative first, then absolute fallback) and re-export its symbols.

    Args:
        target_globals: globals() of the caller module.
        relative_import: relative import path (e.g., '.lib.config').
        absolute_import: absolute import path (e.g., 'zm.lib.config').
        warn_msg: warning message to emit for deprecation/compat notice.
    """
    try:  # relative import preferred
        mod = importlib.import_module(relative_import, package=target_globals.get('__package__'))
    except Exception:
        mod = importlib.import_module(absolute_import)

    warnings.warn(warn_msg, DeprecationWarning)

    target_globals.update(vars(mod))
    target_globals['__all__'] = getattr(mod, '__all__', [])
    return mod

