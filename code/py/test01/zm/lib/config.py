# -*- coding: utf-8 -*-
"""
Centralized configuration and helpers for the zm project.
- STARROCKS_CONFIG: canonical place for StarRocks connection settings
- get_starrocks_connection(): returns a pymysql connection using the config
- path constants: KB_OUT_DIR, OUT_DIR, DOCUMENT_DIR

Configuration priority:
1. Environment variables (STARROCKS_HOST, STARROCKS_PORT, etc.)
2. Default values (fallback only)
"""
import os
from pathlib import Path
from typing import Dict, Any, Optional
import pymysql

# BASE_DIR currently points to zm/lib; ZM_ROOT is zm, REPO_ROOT is project root
BASE_DIR = Path(__file__).resolve().parent
ZM_ROOT = BASE_DIR.parent
REPO_ROOT = ZM_ROOT.parent
DOCUMENT_DIR = REPO_ROOT / 'document'
KB_OUT_DIR = ZM_ROOT / 'kb_out'
OUT_DIR = ZM_ROOT / 'out'
KB_OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Determine cursorclass safely
_cursorclass: Optional[type] = None
try:
    cursors_mod = getattr(pymysql, 'cursors', None)
    if cursors_mod is not None and hasattr(cursors_mod, 'DictCursor'):
        _cursorclass = getattr(cursors_mod, 'DictCursor')
except Exception:
    _cursorclass = None


def _get_config_from_env() -> Dict[str, Any]:
    """Load configuration from environment variables."""
    config: Dict[str, Any] = {}
    
    # Read from environment variables with fallback to defaults
    config['host'] = os.getenv('STARROCKS_HOST', '10.2.8.36')
    port_str = os.getenv('STARROCKS_PORT', '9030')
    try:
        config['port'] = int(port_str)
    except ValueError:
        config['port'] = 9030
    config['user'] = os.getenv('STARROCKS_USER', 'root')
    config['password'] = os.getenv('STARROCKS_PASSWORD', '')
    config['database'] = os.getenv('STARROCKS_DATABASE', 'test')
    config['charset'] = os.getenv('STARROCKS_CHARSET', 'utf8')
    config['cursorclass'] = _cursorclass
    
    return config


# StarRocks connection configuration
# Priority: environment variables > defaults
STARROCKS_CONFIG: Dict[str, Any] = _get_config_from_env()


def get_starrocks_connection(cfg: Optional[Dict[str, Any]] = None) -> pymysql.Connection:
    """Return a pymysql connection using STARROCKS_CONFIG or provided cfg.
    
    Args:
        cfg: Optional configuration dict. If None, uses STARROCKS_CONFIG.
        
    Returns:
        pymysql.Connection: A configured database connection.
        
    Raises:
        pymysql.Error: If connection fails.
    """
    config = dict(cfg or STARROCKS_CONFIG)
    
    # Ensure port is int
    if isinstance(config.get('port'), str):
        try:
            config['port'] = int(config['port'])
        except (ValueError, TypeError):
            config['port'] = 9030
    
    # Remove None cursorclass for compatibility
    if config.get('cursorclass') is None:
        config.pop('cursorclass', None)
    
    # Validate required fields
    if not config.get('host'):
        raise ValueError("StarRocks host is required")
    if not config.get('password'):
        raise ValueError("StarRocks password is required. Set STARROCKS_PASSWORD environment variable.")
    
    return pymysql.connect(**config)

