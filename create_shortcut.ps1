$ws = New-Object -ComObject WScript.Shell
$sc = $ws.CreateShortcut("D:\OneDrive\Desktop\Surge.lnk")
$sc.TargetPath = "D:\SurgeApp\dist\Surge.exe"
$sc.WorkingDirectory = "D:\SurgeApp\dist"
$sc.IconLocation = "D:\SurgeApp\dist\Surge.exe,0"
$sc.Description = "Surge - DoD Duplicate Shredder"
$sc.Save()
Write-Output "Shortcut created successfully."
