param(
    [Parameter(Mandatory = $true)]
    [string]$HostName,

    [Parameter(Mandatory = $true)]
    [string]$User,

    [string]$RemoteDir = "/opt/neo_trader",
    [string]$ServiceUser = "neo-trader",
    [string]$SshKey = "",
    [string]$EnvFile = "",
    [int]$DashboardPort = 8765
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
$archive = Join-Path $env:TEMP "neo_trader_$timestamp.tar"

Invoke-Checked git archive "--format=tar" "--output=$archive" "HEAD"
Invoke-Checked scp @sshArgs $archive "${sshTarget}:/tmp/neo_trader_deploy.tar"

if ($EnvFile) {
    if (-not (Test-Path -LiteralPath $EnvFile)) {
        throw "EnvFile not found: $EnvFile"
    }
    Invoke-Checked scp @sshArgs $EnvFile "${sshTarget}:/tmp/neo-universal-swarm.env"
}

$remoteScript = @"
set -euo pipefail
sudo useradd --system --home "$RemoteDir" --shell /usr/sbin/nologin "$ServiceUser" 2>/dev/null || true
sudo mkdir -p "$RemoteDir" /etc/neo-trader
sudo tar -xf /tmp/neo_trader_deploy.tar -C "$RemoteDir"
sudo python3 -m venv "$RemoteDir/.venv"
sudo "$RemoteDir/.venv/bin/python" -m pip install -U pip
sudo "$RemoteDir/.venv/bin/python" -m pip install -e "$RemoteDir[dashboard]"
if [ -f /tmp/neo-universal-swarm.env ]; then
  sudo install -m 600 -o root -g root /tmp/neo-universal-swarm.env /etc/neo-trader/neo-universal-swarm.env
elif [ ! -f /etc/neo-trader/neo-universal-swarm.env ]; then
  sudo install -m 600 -o root -g root "$RemoteDir/deploy/neo-universal-swarm.env.example" /etc/neo-trader/neo-universal-swarm.env
fi
sudo sed -i 's/\r$//' /etc/neo-trader/neo-universal-swarm.env
if ! sudo grep -q '^SSL_CERT_FILE=' /etc/neo-trader/neo-universal-swarm.env && [ -f /etc/ssl/certs/ca-certificates.crt ]; then
  echo 'SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt' | sudo tee -a /etc/neo-trader/neo-universal-swarm.env >/dev/null
fi
sudo cp "$RemoteDir/deploy/neo-universal-swarm.service" /etc/systemd/system/neo-universal-swarm.service
sudo cp "$RemoteDir/deploy/neo-universal-swarm-dashboard.service" /etc/systemd/system/neo-universal-swarm-dashboard.service
sudo sed -i "s#/opt/neo_trader#$RemoteDir#g; s#User=neo-trader#User=$ServiceUser#g; s#Group=neo-trader#Group=$ServiceUser#g" /etc/systemd/system/neo-universal-swarm.service /etc/systemd/system/neo-universal-swarm-dashboard.service
sudo sed -i "s#--port 8765#--port $DashboardPort#g" /etc/systemd/system/neo-universal-swarm-dashboard.service
sudo chown -R "${ServiceUser}:${ServiceUser}" "$RemoteDir"
sudo systemctl daemon-reload
sudo systemctl enable --now neo-universal-swarm.service
sudo systemctl enable --now neo-universal-swarm-dashboard.service
sudo systemctl restart neo-universal-swarm.service neo-universal-swarm-dashboard.service
sudo systemctl --no-pager --lines=20 status neo-universal-swarm.service || true
sudo systemctl --no-pager --lines=20 status neo-universal-swarm-dashboard.service || true
"@

$remoteArgs = @()
$remoteArgs += $sshArgs
$remoteArgs += @($sshTarget, "bash", "-s")
Invoke-CheckedWithInput -InputText $remoteScript -FilePath "ssh" -Arguments $remoteArgs
Remove-Item -LiteralPath $archive -Force

Write-Output "Dashboard: http://${HostName}:$DashboardPort/"
Write-Output "Services: neo-universal-swarm.service, neo-universal-swarm-dashboard.service"
