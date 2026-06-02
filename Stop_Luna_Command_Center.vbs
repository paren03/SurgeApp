' Stop_Luna_Command_Center.vbs
' 2026-06-02 — Silent wrapper for Stop_Luna_Command_Center.ps1.
'
' wscript runs this with no console window. It spawns powershell hidden to
' run the stop script, so closing Luna shows no flashing window.
'
' Desktop shortcut "Stop Luna" -> wscript.exe -> this .vbs -> stop .ps1.

Set sh = CreateObject("WScript.Shell")
cmd = "powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass " & _
      "-WindowStyle Hidden -File ""D:\SurgeApp\Stop_Luna_Command_Center.ps1"""
' 0 = hidden window, False = do not wait.
sh.Run cmd, 0, False
