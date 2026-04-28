@echo off
:: File: D:\SurgeApp\Start_SurgeApp.bat
:: Purpose: One-click launcher that links everything (hidden windows).

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

if not exist "%ROOT%\logs" mkdir "%ROOT%\logs" >nul 2>nul
if not exist "%ROOT%\memory" mkdir "%ROOT%\memory" >nul 2>nul
if not exist "%ROOT%\tasks\active" mkdir "%ROOT%\tasks\active" >nul 2>nul
if not exist "%ROOT%\tasks\done" mkdir "%ROOT%\tasks\done" >nul 2>nul
if not exist "%ROOT%\tasks\failed" mkdir "%ROOT%\tasks\failed" >nul 2>nul
if not exist "%ROOT%\solutions" mkdir "%ROOT%\solutions" >nul 2>nul
if not exist "%ROOT%\logic_updates" mkdir "%ROOT%\logic_updates" >nul 2>nul
if not exist "%ROOT%\backups" mkdir "%ROOT%\backups" >nul 2>nul
if not exist "%ROOT%\aider_jobs\active" mkdir "%ROOT%\aider_jobs\active" >nul 2>nul
if not exist "%ROOT%\aider_jobs\done" mkdir "%ROOT%\aider_jobs\done" >nul 2>nul
if not exist "%ROOT%\aider_jobs\failed" mkdir "%ROOT%\aider_jobs\failed" >nul 2>nul

:: Resolve a real (non-stub) pythonw.exe. Order:
::   1) project's .aider_venv (this is what aider was installed into)
::   2) per-package WindowsApps redirector for Python 3.11
::   3) classic install paths
::   4) `where pythonw` results that are NOT the bare WindowsApps alias stub
set "PYEXE="
if exist "%ROOT%\.aider_venv\Scripts\pythonw.exe" set "PYEXE=%ROOT%\.aider_venv\Scripts\pythonw.exe"
if not defined PYEXE if exist "%LOCALAPPDATA%\Microsoft\WindowsApps\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\pythonw.exe" set "PYEXE=%LOCALAPPDATA%\Microsoft\WindowsApps\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\pythonw.exe"
if not defined PYEXE if exist "%LOCALAPPDATA%\Programs\Python\Python311\pythonw.exe" set "PYEXE=%LOCALAPPDATA%\Programs\Python\Python311\pythonw.exe"
if not defined PYEXE if exist "C:\Python311\pythonw.exe" set "PYEXE=C:\Python311\pythonw.exe"
if not defined PYEXE for /f "delims=" %%i in ('where pythonw 2^>nul') do (
  echo %%i | findstr /i "\\WindowsApps\\pythonw" >nul
  if errorlevel 1 (
    set "PYEXE=%%i"
    goto :gotpy
  )
)
:gotpy
if not defined PYEXE set "PYEXE=pythonw"

powershell -NoProfile -WindowStyle Hidden -Command ^
  "$root='%ROOT%'; $lock=Join-Path $root 'logs\luna_worker.lock.json';" ^
  "if(Test-Path $lock){" ^
  "  $lockPid=0; try{ $j=Get-Content $lock -Raw | ConvertFrom-Json; if($j.pid){$lockPid=[int]$j.pid} } catch {}" ^
  "  if($lockPid -le 0 -or -not (Get-Process -Id $lockPid -ErrorAction SilentlyContinue)){" ^
  "    Remove-Item $lock -Force -ErrorAction SilentlyContinue" ^
  "  }" ^
  "}"

powershell -NoProfile -WindowStyle Hidden -Command ^
  "$base=$env:OLLAMA_API_BASE; if(-not $base){$base='http://127.0.0.1:11434'};" ^
  "try{ irm -TimeoutSec 2 ($base + '/api/tags') | Out-Null } catch { Start-Process -WindowStyle Hidden -FilePath 'ollama' -ArgumentList 'serve' -ErrorAction SilentlyContinue };" ^
  "for($i=0;$i -lt 15;$i++){ try{ irm -TimeoutSec 2 ($base + '/api/tags') | Out-Null; break } catch { Start-Sleep -Seconds 1 } }"

powershell -NoProfile -WindowStyle Hidden -Command ^
  "$root='%ROOT%'; $py='%PYEXE%';" ^
  "function StartIfMissing($script){" ^
  "  $p=(Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -and $_.CommandLine -like ('*' + $script + '*') } | Select-Object -First 1);" ^
  "  if(-not $p){ Start-Process -WindowStyle Hidden -FilePath $py -ArgumentList ('\"' + (Join-Path $root $script) + '\"') -WorkingDirectory $root }" ^
  "}" ^
  "if(Test-Path (Join-Path $root 'luna_guardian.py')){ StartIfMissing 'luna_guardian.py' }" ^
  "if(Test-Path (Join-Path $root 'luna_apprentice.py')){ StartIfMissing 'luna_apprentice.py' }" ^
  "if(Test-Path (Join-Path $root 'aider_bridge.py')){ StartIfMissing 'aider_bridge.py' }" ^
  "if(Test-Path (Join-Path $root 'worker.py')){ StartIfMissing 'worker.py' }" ^
  "if(Test-Path (Join-Path $root 'SurgeApp_Claude_Terminal.py')){ Start-Process -WindowStyle Hidden -FilePath $py -ArgumentList ('\"' + (Join-Path $root 'SurgeApp_Claude_Terminal.py') + '\"') -WorkingDirectory $root }"

exit /b
