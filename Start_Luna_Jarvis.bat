@echo off
REM Start_Luna_Jarvis.bat — launch Luna in Jarvis voice mode.
REM
REM Pipeline: microphone -> "Hey Luna" wake word -> Whisper STT ->
REM           Claude API (brain) -> her XTTS cloned voice -> speakers.
REM
REM This is the CRASH-SAFE Luna: the brain runs in the Claude cloud (no local
REM 8B model on the GPU), only her voice clone uses the GPU (~2 GB).
REM
REM Close this window or press Ctrl+C to stop her listening.

title Luna Jarvis
cd /d D:\SurgeApp
echo ============================================================
echo   LUNA JARVIS — voice assistant
echo ============================================================
echo   Brain:  Claude API (cloud, smart, no GPU crash risk)
echo   Voice:  Luna's XTTS clone (GPU)
echo   Ears:   Whisper + "Hey Luna" wake word
echo.
echo   Say "Hey Luna" to talk to her.
echo   Close this window or press Ctrl+C to stop.
echo ============================================================
echo.
"D:\SurgeApp\.aider_venv\Scripts\python.exe" "D:\SurgeApp\luna_jarvis.py"
echo.
echo Luna Jarvis stopped.
pause
