param(
    [Parameter(Mandatory = $true)]
    [string]$HostName,

    [Parameter(Mandatory = $true)]
    [string]$User,

    [string]$RemoteDir = "/opt/neo_trader",
    [string]$DataDir = "/var/lib/neo-swarm-scalper",
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
sudo install -d -m 0755 "$RemoteDir" /etc/neo-trader /var/backups/neo-swarm-scalper
sudo install -d -o "$ServiceUser" -g "$ServiceUser" -m 0700 "$DataDir" "$DataDir/reports"
sudo systemctl stop neo-swarm-healthcheck.timer neo-swarm-dashboard.service neo-swarm-bot.service || true
if [ -f "$RemoteDir/data/neo_swarm_scalper.sqlite" ] && [ ! -f "$DataDir/neo_swarm_scalper.sqlite" ]; then
  sudo cp -a "$RemoteDir/data/neo_swarm_scalper.sqlite" "$DataDir/neo_swarm_scalper.sqlite"
fi
if [ -f "$DataDir/neo_swarm_scalper.sqlite" ]; then
  sudo cp -a "$DataDir/neo_swarm_scalper.sqlite" "/var/backups/neo-swarm-scalper/neo_swarm_scalper-$timestamp.sqlite"
fi
if [ -d "$RemoteDir/reports/neo_swarm_scalper" ]; then
  sudo cp -an "$RemoteDir/reports/neo_swarm_scalper/." "$DataDir/reports/"
fi
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
sudo sed -i 's#^NEO_TRADER_DATA_DIR=.*#NEO_TRADER_DATA_DIR=$DataDir#' /etc/neo-trader/neo-swarm-scalper.env
sudo cp "$RemoteDir/deploy/neo-swarm-bot.service" /etc/systemd/system/neo-swarm-bot.service
sudo cp "$RemoteDir/deploy/neo-swarm-dashboard.service" /etc/systemd/system/neo-swarm-dashboard.service
sudo cp "$RemoteDir/deploy/neo-swarm-healthcheck.service" /etc/systemd/system/neo-swarm-healthcheck.service
sudo cp "$RemoteDir/deploy/neo-swarm-healthcheck.timer" /etc/systemd/system/neo-swarm-healthcheck.timer
sudo cp "$RemoteDir/deploy/neo-swarm-recover.service" /etc/systemd/system/neo-swarm-recover.service
sudo sed -i "s#/opt/neo_trader#$RemoteDir#g; s#/var/lib/neo-swarm-scalper#$DataDir#g; s#User=neo-trader#User=$ServiceUser#g; s#Group=neo-trader#Group=$ServiceUser#g" /etc/systemd/system/neo-swarm-bot.service /etc/systemd/system/neo-swarm-dashboard.service /etc/systemd/system/neo-swarm-healthcheck.service
sudo sed -i "s#--server.port 8025#--server.port $DashboardPort#g" /etc/systemd/system/neo-swarm-dashboard.service
sudo chown -R root:root "$RemoteDir"
sudo chmod -R a+rX,go-w "$RemoteDir"
sudo chown -R "${ServiceUser}:${ServiceUser}" "$DataDir"
sudo pkill -u "$ServiceUser" -f "neo_swarm_scalper/dashboard.py.*$DashboardPort" || true
sudo systemctl daemon-reload
sudo systemctl reset-failed neo-swarm-bot.service neo-swarm-dashboard.service || true
sudo systemctl enable neo-swarm-bot.service neo-swarm-dashboard.service neo-swarm-healthcheck.timer
sudo systemctl restart neo-swarm-bot.service
sudo systemctl restart neo-swarm-dashboard.service
sudo systemctl restart neo-swarm-healthcheck.timer
sleep 12
sudo systemctl is-active --quiet neo-swarm-bot.service
sudo systemctl is-active --quiet neo-swarm-dashboard.service
sudo systemctl is-active --quiet neo-swarm-healthcheck.timer
sudo systemctl start neo-swarm-healthcheck.service
sudo systemctl --no-pager --lines=20 status neo-swarm-bot.service || true
sudo systemctl --no-pager --lines=20 status neo-swarm-dashboard.service || true
sudo systemctl --no-pager --lines=10 status neo-swarm-healthcheck.timer || true
"@

$remoteArgs = @()
$remoteArgs += $sshArgs
$remoteArgs += @($sshTarget, "tr -d '\r' | bash -s")
Invoke-CheckedWithInput -InputText $remoteScript -FilePath "ssh" -Arguments $remoteArgs
Remove-Item -LiteralPath $archive -Force

Write-Output "Dashboard: http://${HostName}:$DashboardPort/"
Write-Output "Services: neo-swarm-bot.service, neo-swarm-dashboard.service"
