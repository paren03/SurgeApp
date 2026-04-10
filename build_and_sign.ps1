# build_and_sign.ps1
# Full pipeline: PyInstaller build -> sign exe -> sign installer
# Run from D:\SurgeApp

$ErrorActionPreference = "Stop"
Set-StrictMode -Off

$Root   = "D:\SurgeApp"
$Venv   = "$Root\.venv\Scripts"
$Dist   = "$Root\dist2"
$Exe    = "$Dist\Surge.exe"
$Pfx    = "$Root\SurgeApp_CodeSign.pfx"
$PfxPwd = ConvertTo-SecureString "SurgeSign2024!" -Force -AsPlainText
$Ts     = "http://timestamp.digicert.com"

function Step($n, $msg) { Write-Host "[$n] $msg" -ForegroundColor Cyan }
function OK($msg)        { Write-Host "  OK: $msg" -ForegroundColor Green }
function Fail($msg)      { Write-Host "  FAIL: $msg" -ForegroundColor Red; exit 1 }

# ── 1. Build with PyInstaller ────────────────────────────────────────────────
Step "1/3" "Building Surge.exe with PyInstaller..."
& "$Venv\pyinstaller.exe" --noconfirm --distpath $Dist "$Root\surge.spec"
if ($LASTEXITCODE -ne 0) { Fail "PyInstaller failed (exit $LASTEXITCODE)" }
if (-not (Test-Path $Exe)) { Fail "Surge.exe not found after build: $Exe" }
OK "Built: $Exe ($([math]::Round((Get-Item $Exe).Length/1MB,1)) MB)"

# ── 2. Sign Surge.exe ────────────────────────────────────────────────────────
Step "2/3" "Signing Surge.exe..."
$cert = Get-PfxCertificate -FilePath $Pfx -Password $PfxPwd
$r = Set-AuthenticodeSignature -FilePath $Exe -Certificate $cert `
     -TimestampServer $Ts -HashAlgorithm SHA256 -Force
if ($r.Status -ne "Valid") { Fail "Signature status: $($r.Status)" }
OK "Signed: $Exe"

# Also sign dist\ copy if it exists
$Exe2 = "$Root\dist\Surge.exe"
if (Test-Path $Exe2) {
    Copy-Item $Exe $Exe2 -Force
    OK "Copied signed exe to dist\"
}

# ── 3. Rebuild installer (if Inno Setup is available) and sign it ────────────
Step "3/3" "Building installer..."
$iscc = Get-Command iscc.exe -ErrorAction SilentlyContinue
if ($iscc) {
    & iscc.exe "$Root\surge_installer.iss"
    if ($LASTEXITCODE -eq 0) {
        $setup = "$Root\installer\SurgeSetup.exe"
        if (Test-Path $setup) {
            $r2 = Set-AuthenticodeSignature -FilePath $setup -Certificate $cert `
                  -TimestampServer $Ts -HashAlgorithm SHA256 -Force
            if ($r2.Status -eq "Valid") { OK "Signed installer: $setup" }
            else { Write-Host "  WARN: Installer signature: $($r2.Status)" -ForegroundColor Yellow }
        }
    } else {
        Write-Host "  WARN: Inno Setup failed — installer not rebuilt." -ForegroundColor Yellow
    }
} else {
    Write-Host "  SKIP: iscc.exe not found. Skipping installer rebuild." -ForegroundColor DarkGray
    Write-Host "        Install Inno Setup from https://jrsoftware.org/isinfo.php" -ForegroundColor DarkGray
}

Write-Host ""
Write-Host "Build complete!" -ForegroundColor Green
Write-Host "  Signed EXE      : $Exe"
Write-Host "  Signed installer: $Root\installer\SurgeSetup.exe"
