KB Agent
========

This folder contains tools to analyze Excel-based knowledge base files under the `document` folder and generate development artifacts (StarRocks DDL, ETL script skeletons, and a JSON plan).

Configuration
-------------

**Important**: Database credentials are now loaded from environment variables for security.

Set the following environment variables before running scripts:

```powershell
# Windows PowerShell
$env:STARROCKS_HOST = "10.2.8.36"
$env:STARROCKS_PORT = "9030"
$env:STARROCKS_USER = "root"
$env:STARROCKS_PASSWORD = "your_password"
$env:STARROCKS_DATABASE = "test"
$env:STARROCKS_CHARSET = "utf8"
```

Or on Linux/Mac:

```bash
export STARROCKS_HOST="10.2.8.36"
export STARROCKS_PORT="9030"
export STARROCKS_USER="root"
export STARROCKS_PASSWORD="your_password"
export STARROCKS_DATABASE="test"
export STARROCKS_CHARSET="utf8"
```

If environment variables are not set, the system will use default values (which may require manual configuration in `lib/config.py`).

Quick start
-----------

Install dependencies using the Python you will run the scripts with (PowerShell example):

```powershell
& 'D:\app\python\python.exe' -m pip install -r d:\note\code\py\test01\zm\requirements.txt
```

Analyze the default Excel and generate artifacts (new entry is in `archive/`):

```powershell
& 'D:\app\python\python.exe' 'd:\note\code\py\test01\zm\archive\kb_agent.py'
```

To execute the generated StarRocks CREATE TABLE statements against the configured StarRocks instance (DANGEROUS: will modify DB), pass `--apply-sr --yes`.

Recent Improvements
-------------------

- **Security**: Removed hardcoded passwords, now uses environment variables
- **Code Quality**: Improved type annotations, error handling, and documentation
- **Configuration**: Centralized configuration management in `lib/config.py`
- **Error Handling**: Better error messages and validation
- **Code Structure**: Reduced code duplication and improved maintainability

