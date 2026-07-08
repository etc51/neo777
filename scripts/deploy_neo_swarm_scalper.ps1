param(
    [Parameter(Mandatory = $true)]
    [string]$HostName,

    [Parameter(Mandatory = $true)]
    [string]$User,

    [string]$RemoteDir = "/opt/neo_trader",
    [string]$ServiceUser = "neo-trader",
    [string]$SshKey = "",
    [string]$EnvFile = "",
    [int]$DashboardPort = 8025
)

$ErrorActionPreference = "Stop"

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FilePath,
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Arguments
    )
    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed: $FilePath $($Arguments -join ' ')"
    }
}

function Invoke-CheckedWithInput {
    param(
        [Parameter(Mandatory = $true)]
        [string]$InputText,
        [Parameter(Mandatory = $true)]
        [string]$FilePath,
        [string[]]$Arguments = @()
    )
    $InputText | & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed: $FilePath $($Arguments -join ' ')"
    }
}

$sshTarget = "$User@$HostName"
$sshArgs = @()
if ($SshKey) {
    $sshArgs += @("-i", $SshKey)
}

$timestamp = Get-Date -Format "yyyyMMddHHmmss"
$archive = Join-Path $env:TEMP "neo_swarm_scalper_$timestamp.tar"

Invoke-Checked git archive "--format=tar" "--output=$archive" "HEAD"
Invoke-Checked scp @sshArgs $archive "${sshTarget}:/tmp/neo_swarm_scalper_deploy.tar"

if ($EnvFile) {
    if (-not (Test-Path -LiteralPath $EnvFile)) {
        throw "EnvFile not found: $EnvFile"
    }
    Invoke-Checked scp @sshArgs $EnvFile "${sshTarget}:/tmp/neo-swarm-scalper.env"
}

$remoteScript = @"
set -euo pipefail
sudo useradd --system --home "$RemoteDir" --shell /usr/sbin/nologin "$ServiceUser" 2>/dev/null || true
sudo mkdir -p "$RemoteDir" /etc/neo-trader
sudo tar -xf /tmp/neo_swarm_scalper_deploy.tar -C "$RemoteDir"
sudo python3 -m venv "$RemoteDir/.venv"
sudo "$RemoteDir/.venv/bin/python" -m pip install -U pip
sudo "$RemoteDir/.venv/bin/python" -m pip install -e "$RemoteDir[dashboard]"
if [ -f /tmp/neo-swarm-scalper.env ]; then
  sudo install -m 600 -o root -g root /tmp/neo-swarm-scalper.env /etc/neo-trader/neo-swarm-scalper.env
elif [ ! -f /etc/neo-trader/neo-swarm-scalper.env ]; then
  sudo install -m 600 -o root -g root "$RemoteDir/deploy/neo-swarm-scalper.env.example" /etc/neo-trader/neo-swarm-scalper.env
fi
sudo sed -i 's/\r$//' /etc/neo-trader/neo-swarm-scalper.env
sudo cp "$RemoteDir/deploy/neo-swarm-scalper.service" /etc/systemd/system/neo-swarm-scalper.service
sudo cp "$RemoteDir/deploy/neo-swarm-scalper-dashboard.service" /etc/systemd/system/neo-swarm-scalper-dashboard.service
sudo sed -i "s#/opt/neo_trader#$RemoteDir#g; s#User=neo-trader#User=$ServiceUser#g; s#Group=neo-trader#Group=$ServiceUser#g" /etc/systemd/system/neo-swarm-scalper.service /etc/systemd/system/neo-swarm-scalper-dashboard.service
sudo sed -i "s#--server.port 8025#--server.port $DashboardPort#g" /etc/systemd/system/neo-swarm-scalper-dashboard.service
sudo chown -R "${ServiceUser}:${ServiceUser}" "$RemoteDir"
sudo systemctl daemon-reload
sudo systemctl enable --now neo-swarm-scalper.service
sudo systemctl enable --now neo-swarm-scalper-dashboard.service
sudo systemctl restart neo-swarm-scalper.service neo-swarm-scalper-dashboard.service
sudo systemctl --no-pager --lines=20 status neo-swarm-scalper.service || true
sudo systemctl --no-pager --lines=20 status neo-swarm-scalper-dashboard.service || true
"@

$remoteArgs = @()
$remoteArgs += $sshArgs
$remoteArgs += @($sshTarget, "bash", "-s")
Invoke-CheckedWithInput -InputText $remoteScript -FilePath "ssh" -Arguments $remoteArgs
Remove-Item -LiteralPath $archive -Force

Write-Output "Dashboard: http://${HostName}:$DashboardPort/"
Write-Output "Services: neo-swarm-scalper.service, neo-swarm-scalper-dashboard.service"
