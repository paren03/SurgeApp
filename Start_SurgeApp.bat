:: File: D:\SurgeApp\Start_SurgeApp.bat
:: Purpose: One-click launcher that links everything (hidden windows).
@echo off

set "ROOT=D:\SurgeApp"

if /i not "%~1"=="hidden" (
  powershell -NoProfile -WindowStyle Hidden -Command "Start-Process -FilePath '%~f0' -ArgumentList 'hidden' -WindowStyle Hidden"
  exit /b
)

cd /d "%ROOT%"

if "%LUNA_PROJECT_DIR%"=="" set "LUNA_PROJECT_DIR=%ROOT%"
if "%OLLAMA_API_BASE%"=="" set "OLLAMA_API_BASE=http://127.0.0.1:11434"
if "%LUNA_INSTRUCTOR_MODEL%"=="" set "LUNA_INSTRUCTOR_MODEL=ollama_chat/llama3.1:8b-instruct-q4_K_M"
if "%AIDER_EXE%"=="" set "AIDER_EXE=%ROOT%\.aider_venv\Scripts\aider.exe"

if not exist "%ROOT%\logs"              mkdir "%ROOT%\logs"              >nul 2>nul
if not exist "%ROOT%\memory"            mkdir "%ROOT%\memory"            >nul 2>nul
if not exist "%ROOT%\tasks\active"      mkdir "%ROOT%\tasks\active"      >nul 2>nul
if not exist "%ROOT%\tasks\done"        mkdir "%ROOT%\tasks\done"        >nul 2>nul
if not exist "%ROOT%\tasks\failed"      mkdir "%ROOT%\tasks\failed"      >nul 2>nul
if not exist "%ROOT%\solutions"         mkdir "%ROOT%\solutions"         >nul 2>nul
if not exist "%ROOT%\logic_updates"     mkdir "%ROOT%\logic_updates"     >nul 2>nul
if not exist "%ROOT%\backups"           mkdir "%ROOT%\backups"           >nul 2>nul
if not exist "%ROOT%\aider_jobs\active" mkdir "%ROOT%\aider_jobs\active" >nul 2>nul
if not exist "%ROOT%\aider_jobs\done"   mkdir "%ROOT%\aider_jobs\done"   >nul 2>nul
if not exist "%ROOT%\aider_jobs\failed" mkdir "%ROOT%\aider_jobs\failed" >nul 2>nul

:: -----------------------------------------------------------------------
:: PYEXE  = system Python that has PySide6, Luna modules etc.
::          Order: per-package WindowsApps redirector (0-byte AppX reparse
::          point but works fine) → classic installs → where pythonw
::          (reject bare WindowsApps\pythonw alias that triggers the Store)
::          NOTE: .aider_venv is intentionally SKIPPED here — it has aider
::          but NOT PySide6, so the terminal would crash silently.
:: AIDER_PY = .aider_venv Python, used ONLY for aider_bridge.py
:: -----------------------------------------------------------------------
set "PYEXE="
set "AIDER_PY=%ROOT%\.aider_venv\Scripts\pythonw.exe"

:: per-package redirector (reparse point, 0 bytes but real) — use PowerShell
:: because batch `if exist` resolves the reparse point and may get confused.
:: Wildcard search for any Python 3.11 WindowsApps package (folder name varies by build)
powershell -NoProfile -WindowStyle Hidden -Command ^
  "$found='';" ^
  "try{ $dirs=Get-Item '%LOCALAPPDATA%\Microsoft\WindowsApps\PythonSoftwareFoundation.Python.3.11*' -ErrorAction SilentlyContinue;" ^
  "foreach($d in $dirs){ $p=Join-Path $d.FullName 'pythonw.exe'; if(Test-Path $p){ $found=$p; break } } } catch {};" ^
  "[System.IO.File]::WriteAllText('%ROOT%\logs\_pyexe.tmp', $found)"
set /p PYEXE=<"%ROOT%\logs\_pyexe.tmp"
del /f /q "%ROOT%\logs\_pyexe.tmp" >nul 2>nul

if not defined PYEXE if exist "%LOCALAPPDATA%\Programs\Python\Python311\pythonw.exe" set "PYEXE=%LOCALAPPDATA%\Programs\Python\Python311\pythonw.exe"
if not defined PYEXE if exist "C:\Python311\pythonw.exe" set "PYEXE=C:\Python311\pythonw.exe"

:: fallback: scan running processes to find pythonw used by any SurgeApp process
if not defined PYEXE (
  for /f "tokens=1" %%i in ('wmic process where "CommandLine like '%%SurgeApp%%'" get ExecutablePath /value 2^>nul ^| findstr "pythonw"') do (
    for /f "tokens=2 delims==" %%j in ("%%i") do (
      if not "%%j"=="" set "PYEXE=%%j"
    )
  )
)

:: last resort: bare pythonw from PATH
:gotpy
if not defined PYEXE set "PYEXE=pythonw"

:: ---- clear stale worker lock (only if PID is not alive) ----
powershell -NoProfile -WindowStyle Hidden -Command ^
  "$root='%ROOT%'; $lock=Join-Path $root 'logs\luna_worker.lock.json';" ^
  "if(Test-Path $lock){" ^
  "  $lockPid=0; try{ $j=Get-Content $lock -Raw | ConvertFrom-Json; if($j.pid){$lockPid=[int]$j.pid} } catch {}" ^
  "  if($lockPid -le 0 -or -not (Get-Process -Id $lockPid -ErrorAction SilentlyContinue)){" ^
  "    Remove-Item $lock -Force -ErrorAction SilentlyContinue" ^
  "  }" ^
  "}"

:: ---- ensure Ollama is serving (hidden) ----
powershell -NoProfile -WindowStyle Hidden -Command ^
  "$base=$env:OLLAMA_API_BASE; if(-not $base){$base='http://127.0.0.1:11434'};" ^
  "try{ irm -TimeoutSec 2 ($base + '/api/tags') | Out-Null } catch { Start-Process -WindowStyle Hidden -FilePath 'ollama' -ArgumentList 'serve' -ErrorAction SilentlyContinue };" ^
  "for($i=0;$i -lt 15;$i++){ try{ irm -TimeoutSec 2 ($base + '/api/tags') | Out-Null; break } catch { Start-Sleep -Seconds 1 } }"

:: ---- StartIfMissing: start each service only if not already running ----
:: PYEXE     = system Python (has PySide6, Luna modules) -> guardian, apprentice, worker, UI
:: AIDER_PY  = .aider_venv pythonw (has aider installed)  -> aider_bridge ONLY
powershell -NoProfile -WindowStyle Hidden -Command ^
  "$root='%ROOT%'; $py='%PYEXE%'; $aiderPy='%AIDER_PY%';" ^
  "function StartIfMissing($script, $exe){" ^
  "  $p=(Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -and $_.CommandLine -like ('*' + $script + '*') } | Select-Object -First 1);" ^
  "  if(-not $p){ Start-Process -WindowStyle Hidden -FilePath $exe -ArgumentList ('\"' + (Join-Path $root $script) + '\"') -WorkingDirectory $root }" ^
  "}" ^
  "if(Test-Path (Join-Path $root 'luna_guardian.py')){ StartIfMissing 'luna_guardian.py' $py }" ^
  "if(Test-Path (Join-Path $root 'luna_apprentice.py')){ StartIfMissing 'luna_apprentice.py' $py }" ^
  "if(Test-Path (Join-Path $root 'worker.py')){ StartIfMissing 'worker.py' $py }" ^
  "if((Test-Path (Join-Path $root 'aider_bridge.py')) -and (Test-Path $aiderPy)){ StartIfMissing 'aider_bridge.py' $aiderPy }" ^
  "if(Test-Path (Join-Path $root 'SurgeApp_Claude_Terminal.py')){ Start-Process -WindowStyle Hidden -FilePath $py -ArgumentList ('\"' + (Join-Path $root 'SurgeApp_Claude_Terminal.py') + '\"') -WorkingDirectory $root }"

:: ---- auto-start continues-update loop (skip if one is already alive) ----
:: We match on the unique CLI flag '--continues-update-start' so we don't
:: confuse this with the regular worker.py background process.
if exist "%ROOT%\worker.py" (
  if exist "%ROOT%\memory\continues_update.stop" del /f /q "%ROOT%\memory\continues_update.stop" >nul 2>nul
  powershell -NoProfile -WindowStyle Hidden -Command ^
    "$p=(Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -and $_.CommandLine -like '*--continues-update-start*' } | Select-Object -First 1);" ^
    "if(-not $p){ Start-Process -WindowStyle Hidden -FilePath '%PYEXE%' -ArgumentList ('\"%ROOT%\worker.py\" --continues-update-start') -WorkingDirectory '%ROOT%' }"
)

exit /b
