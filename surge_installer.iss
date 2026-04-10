; ============================================================
;  Surge Installer — Inno Setup 6 script
;  Builds:  SurgeSetup.exe  (~36 MB, self-contained)
;  Target:  Windows 10/11 x64
; ============================================================

#define AppName      "Surge"
#define AppVersion   "1.0.0"
#define AppPublisher "SurgeApp"
#define AppExe       "Surge.exe"
#define SrcExe       "D:\SurgeApp\dist2\Surge.exe"
#define SrcIcon      "D:\SurgeApp\surge.ico"
#define SrcReg       "D:\SurgeApp\install_context_menu.reg"
#define SrcUnReg     "D:\SurgeApp\uninstall_context_menu.reg"
#define OutDir       "D:\SurgeApp\installer"

[Setup]
AppId                    = {{A7F3C2E1-84B6-4D1A-9F52-3E7B8C0D4591}
AppName                  = {#AppName}
AppVersion               = {#AppVersion}
AppPublisher             = {#AppPublisher}
AppPublisherURL          = https://github.com
AppSupportURL            = https://github.com
AppUpdatesURL            = https://github.com
DefaultDirName           = {autopf}\{#AppName}
DefaultGroupName         = {#AppName}
AllowNoIcons             = yes
OutputDir                = {#OutDir}
OutputBaseFilename       = SurgeSetup
SetupIconFile            = {#SrcIcon}
Compression              = lzma2/ultra64
SolidCompression         = yes
WizardStyle              = modern
PrivilegesRequired       = lowest
PrivilegesRequiredOverridesAllowed = dialog
ArchitecturesInstallIn64BitMode = x64compatible
MinVersion               = 10.0
UninstallDisplayIcon     = {app}\{#AppExe}
UninstallDisplayName     = {#AppName}

; ── Code signing (uses Windows SDK signtool or PowerShell) ──────────────────
; If you have signtool.exe available, uncomment and set the pfx path:
; SignTool=signtool sign /fd SHA256 /td SHA256 /tr http://timestamp.digicert.com /f D:\SurgeApp\SurgeApp_CodeSign.pfx /p SurgeSign2024! $f

; Installer appearance
WizardImageFile          = compiler:WizClassicImage.bmp
WizardSmallImageFile     = compiler:WizClassicSmallImage.bmp

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon";   Description: "{cm:CreateDesktopIcon}";   GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked
Name: "contextmenu";   Description: "Add 'Secure Shred with Surge' to right-click menu"; GroupDescription: "Windows Integration:"; Flags: unchecked

[Files]
; Main executable — the only file needed (PyInstaller one-file bundle)
Source: "{#SrcExe}";   DestDir: "{app}"; DestName: "{#AppExe}"; Flags: ignoreversion

; Registry scripts (used by the context-menu task below)
Source: "{#SrcReg}";   DestDir: "{app}"; DestName: "install_context_menu.reg";   Flags: ignoreversion
Source: "{#SrcUnReg}"; DestDir: "{app}"; DestName: "uninstall_context_menu.reg"; Flags: ignoreversion

; Application icon
Source: "{#SrcIcon}";  DestDir: "{app}"; DestName: "surge.ico"; Flags: ignoreversion

[Icons]
; Start Menu
Name: "{group}\{#AppName}";                    Filename: "{app}\{#AppExe}"; IconFilename: "{app}\surge.ico"
Name: "{group}\Uninstall {#AppName}";          Filename: "{uninstallexe}"

; Desktop (optional task)
Name: "{autodesktop}\{#AppName}";              Filename: "{app}\{#AppExe}"; IconFilename: "{app}\surge.ico"; Tasks: desktopicon

[Run]
; Launch after install
Filename: "{app}\{#AppExe}"; Description: "{cm:LaunchProgram,{#AppName}}"; Flags: nowait postinstall skipifsilent

; Install right-click context menu (optional task)
Filename: "reg.exe"; Parameters: "import ""{app}\install_context_menu.reg"""; Flags: runhidden; Tasks: contextmenu; StatusMsg: "Registering shell extension..."

[UninstallRun]
; Remove right-click context menu on uninstall
Filename: "reg.exe"; Parameters: "import ""{app}\uninstall_context_menu.reg"""; Flags: runhidden; RunOnceId: "RemoveContextMenu"

