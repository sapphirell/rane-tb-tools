# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path


project_dir = Path(SPEC).resolve().parent


a = Analysis(
    ['gui_xhs.py'],
    pathex=[str(project_dir)],
    binaries=[
        (str(project_dir / 'bin' / 'chromedriver.exe'), 'bin'),
        (str(project_dir / 'bin' / 'msedgedriver.exe'), 'bin'),
    ],
    datas=[
        (str(project_dir / 'stealth.min.js'), '.'),
    ],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='gui_xhs',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    name='gui_xhs',
)
