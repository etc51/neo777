param(
    [string]$TaskName = "Neobitcoin Local Research Collector",
    [string]$WatchdogTaskName = "Neobitcoin Local Research Collector Watchdog",
    [switch]$StartNow
)

$ErrorActionPreference = "Stop"
$Repo = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$Python = Join-Path $Repo ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $Python)) {
    $Python = (Get-Command python -ErrorAction Stop).Source
}

$Arguments = '-m neo_trader.neobitcoin_research.local_control _worker'
$Action = New-ScheduledTaskAction -Execute $Python -Argument $Arguments -WorkingDirectory $Repo
$Trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$Settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -RestartCount 999 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable
$Principal = New-ScheduledTaskPrincipal `
    -UserId "$env:USERDOMAIN\$env:USERNAME" `
    -LogonType Interactive `
    -RunLevel Limited

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Principal $Principal `
    -Description "Local read-only T-Bank Neobitcoin research collector" `
    -Force | Out-Null

$WatchdogArguments = '-m neo_trader.neobitcoin_research.local_control start'
$WatchdogAction = New-ScheduledTaskAction `
    -Execute $Python `
    -Argument $WatchdogArguments `
    -WorkingDirectory $Repo
$WatchdogTrigger = New-ScheduledTaskTrigger `
    -Once `
    -At (Get-Date).AddMinutes(1) `
    -RepetitionInterval (New-TimeSpan -Minutes 1) `
    -RepetitionDuration (New-TimeSpan -Days 3650)
$WatchdogSettings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 5) `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable
Register-ScheduledTask `
    -TaskName $WatchdogTaskName `
    -Action $WatchdogAction `
    -Trigger $WatchdogTrigger `
    -Settings $WatchdogSettings `
    -Principal $Principal `
    -Description "Watchdog for the local read-only Neobitcoin collector" `
    -Force | Out-Null

Write-Output "installed=$TaskName"
Write-Output "watchdog_installed=$WatchdogTaskName"
Write-Output "data_root=C:\Users\HONOR\Documents\neobitcoin_research"
if ($StartNow) {
    Start-ScheduledTask -TaskName $TaskName
    Write-Output "started=$TaskName"
}
