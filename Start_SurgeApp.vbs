' File: D:\SurgeApp\Start_SurgeApp.vbs
' Purpose: Launcher used by "Luna Command Center.lnk"
' Runs Start_SurgeApp.bat hidden, from D:\SurgeApp.

Option Explicit

Dim shell, root, bat, cmd
Set shell = CreateObject("WScript.Shell")

root = "D:\SurgeApp"
bat = root & "\Start_SurgeApp.bat"

On Error Resume Next
shell.CurrentDirectory = root
On Error GoTo 0

cmd = """" & bat & """"

' 0 = hidden window, False = don't wait
shell.Run cmd, 0, False
