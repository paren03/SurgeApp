Set-StrictMode -Off
$ErrorActionPreference = "Stop"

$CertSubject  = "CN=SurgeApp, O=SurgeApp, C=US"
$CertPassword = ConvertTo-SecureString -String "SurgeSign2024!" -Force -AsPlainText
$PfxPath      = "D:\SurgeApp\SurgeApp_CodeSign.pfx"
$CerPath      = "D:\SurgeApp\SurgeApp_CodeSign.cer"
$Targets      = @(
    "D:\SurgeApp\dist\Surge.exe",
    "D:\SurgeApp\dist2\Surge.exe",
    "D:\SurgeApp\installer\SurgeSetup.exe"
)

Write-Host "[1/4] Creating / reusing code-signing certificate..." -ForegroundColor Cyan

$cert = Get-ChildItem Cert:\CurrentUser\My |
        Where-Object { $_.Subject -like "*SurgeApp*" } |
        Sort-Object NotAfter -Descending |
        Select-Object -First 1

if (-not $cert) {
    $cert = New-SelfSignedCertificate `
        -Subject           $CertSubject `
        -CertStoreLocation "Cert:\CurrentUser\My" `
        -KeyUsage          DigitalSignature `
        -Type              CodeSigningCert `
        -KeyAlgorithm      RSA `
        -KeyLength         4096 `
        -HashAlgorithm     SHA256 `
        -NotAfter          (Get-Date).AddYears(10) `
        -FriendlyName      "SurgeApp Code Signing"
    Write-Host "  Created: $($cert.Thumbprint)" -ForegroundColor Green
} else {
    Write-Host "  Reusing: $($cert.Thumbprint)" -ForegroundColor Green
}

Write-Host "[2/4] Exporting PFX and CER..." -ForegroundColor Cyan
Export-PfxCertificate -Cert $cert -FilePath $PfxPath -Password $CertPassword | Out-Null
Export-Certificate    -Cert $cert -FilePath $CerPath -Type CERT | Out-Null
Write-Host "  PFX: $PfxPath"
Write-Host "  CER: $CerPath"

Write-Host "[3/4] Trusting certificate locally..." -ForegroundColor Cyan
foreach ($store in @("Root","TrustedPublisher")) {
    try {
        $s = New-Object System.Security.Cryptography.X509Certificates.X509Store(
            $store,
            [System.Security.Cryptography.X509Certificates.StoreLocation]::CurrentUser
        )
        $s.Open([System.Security.Cryptography.X509Certificates.OpenFlags]::ReadWrite)
        $s.Add($cert)
        $s.Close()
        Write-Host "  Added to CurrentUser\$store" -ForegroundColor Green
    } catch {
        Write-Host "  Warning ($store): $_" -ForegroundColor Yellow
    }
}

Write-Host "[4/4] Signing executables..." -ForegroundColor Cyan
$ts   = "http://timestamp.digicert.com"
$ok   = 0
$skip = 0
$fail = 0

foreach ($f in $Targets) {
    if (-not (Test-Path $f)) {
        Write-Host "  SKIP: $f" -ForegroundColor DarkGray
        $skip++
        continue
    }
    try {
        $r = Set-AuthenticodeSignature -FilePath $f -Certificate $cert `
             -TimestampServer $ts -HashAlgorithm SHA256 -Force
        if ($r.Status -eq "Valid") {
            Write-Host "  OK  : $f" -ForegroundColor Green
            $ok++
        } else {
            Write-Host "  WARN: $f  [$($r.Status)]" -ForegroundColor Yellow
            $fail++
        }
    } catch {
        Write-Host "  FAIL: $f  [$_]" -ForegroundColor Red
        $fail++
    }
}

Write-Host ""
Write-Host "Done -- Signed:$ok  Skipped:$skip  Failed:$fail" -ForegroundColor Cyan
Write-Host ""
Write-Host "NOTE: Self-signed certs only work on THIS PC." -ForegroundColor Yellow
Write-Host "For full AV trust, buy a commercial cert:" -ForegroundColor Yellow
Write-Host "  Sectigo OV  ~100/yr : https://sectigo.com" -ForegroundColor White
Write-Host "  DigiCert EV ~400/yr : https://digicert.com" -ForegroundColor White
Write-Host "  SignPath.io (free)  : https://signpath.io" -ForegroundColor White
