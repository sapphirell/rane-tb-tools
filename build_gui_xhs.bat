@echo off
setlocal

cd /d "%~dp0"

echo [1/4] Checking Python...
python --version >nul 2>&1
if errorlevel 1 (
    echo Python was not found in PATH.
    exit /b 1
)

echo [2/4] Checking PyInstaller...
python -m PyInstaller --version >nul 2>&1
if errorlevel 1 (
    echo PyInstaller not found. Installing...
    python -m pip install pyinstaller
    if errorlevel 1 (
        echo Failed to install PyInstaller.
        exit /b 1
    )
)

echo [3/4] Checking required files...
if not exist "%~dp0gui_xhs.py" (
    echo Missing gui_xhs.py
    exit /b 1
)
if not exist "%~dp0gui_xhs.spec" (
    echo Missing gui_xhs.spec
    exit /b 1
)
if not exist "%~dp0stealth.min.js" (
    echo Missing stealth.min.js
    exit /b 1
)
if not exist "%~dp0bin\chromedriver.exe" (
    echo Missing bin\chromedriver.exe
    exit /b 1
)
if not exist "%~dp0bin\msedgedriver.exe" (
    echo Missing bin\msedgedriver.exe
    exit /b 1
)

echo [4/5] Stopping old gui_xhs processes...
taskkill /F /IM gui_xhs.exe >nul 2>&1
taskkill /F /IM chromedriver.exe >nul 2>&1
taskkill /F /IM msedgedriver.exe >nul 2>&1

if exist "%~dp0dist\gui_xhs" (
    rmdir /S /Q "%~dp0dist\gui_xhs" >nul 2>&1
)

echo [5/5] Building gui_xhs.exe...
python -m PyInstaller "%~dp0gui_xhs.spec" --noconfirm
if errorlevel 1 (
    echo Build failed.
    exit /b 1
)

echo.
echo Build complete:
echo %~dp0dist\gui_xhs
echo.
pause
