param([switch]$Uninstall)
# Install_Luna_Always_On.ps1 — wire Luna as an always-on (Jarvis) voice assistant.
#   * Auto-starts at login, hidden (pythonw, no window).
#   * Single instance (named mutex in luna_jarvis.py).
#   * Desktop "Pause Luna" / "Resume Luna" for a graceful off/on.
# Run:  pwsh -File Install_Luna_Always_On.ps1            (install)
#       pwsh -File Install_Luna_Always_On.ps1 -Uninstall (remove auto-start)

$ErrorActionPreference = 'Stop'
$pyw     = 'D:\SurgeApp\.aider_venv\Scripts\pythonw.exe'
$startup = [Environment]::GetFolderPath('Startup')
$desktop = [Environment]::GetFolderPath('Desktop')
$ws      = New-Object -ComObject WScript.Shell

$links = @{
    Startup = Join-Path $startup 'Luna Jarvis.lnk'
    Pause   = Join-Path $desktop 'Pause Luna.lnk'
    Resume  = Join-Path $desktop 'Resume Luna.lnk'
}

function New-Lnk($path, $lnkArgs, $desc) {
    $s = $ws.CreateShortcut($path)
    $s.TargetPath       = $pyw
    $s.Arguments        = $lnkArgs
    $s.WorkingDirectory = 'D:\SurgeApp'
    $s.Description       = $desc
    $s.WindowStyle      = 7        # minimized (pythonw shows no window anyway)
    $s.Save()
}

if ($Uninstall) {
    foreach ($l in $links.Values) {
        if (Test-Path $l) { Remove-Item $l -Force; Write-Output "removed: $l" }
    }
    Write-Output 'Always-on removed. Luna will not auto-start at login.'
    return
}

New-Lnk $links.Startup '"D:\SurgeApp\luna_jarvis.py"'  'Luna Jarvis always-on voice assistant'
New-Lnk $links.Pause   '"D:\SurgeApp\Pause_Luna.pyw"'  'Pause Luna (graceful off)'
New-Lnk $links.Resume  '"D:\SurgeApp\Resume_Luna.pyw"' 'Resume Luna (on + start now)'

Write-Output 'Installed Luna always-on:'
Write-Output "  auto-start : $($links.Startup)"
Write-Output "  pause      : $($links.Pause)"
Write-Output "  resume     : $($links.Resume)"
