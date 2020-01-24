param(
    [Parameter(Position=1)]
    [string]$iniFile = './settings/local.ini'
)

if ($iniFile -eq $null) {
    $iniFile = './settings/local.ini'
}

if ($iniFile -ne '') {
    $localProps = IniProperties $iniFile -IgnoreInexistent
    $JAVA_EXE = $localProps['path.java']
}
$JAVA_EXE = ($JAVA_EXE, 'java' -ne $null)[0]

function CallDistWrapper([string]$direction, [string]$task, [string]$params, [string]$tasksIni, [string]$storePath = '') {
    Remove-Item -Path './settings/distcp.ini' -ErrorAction Ignore

    $pinfo = New-Object System.Diagnostics.ProcessStartInfo
    $pinfo.FileName = $Script:JAVA_EXE
    $pinfo.RedirectStandardError = $true
    $pinfo.RedirectStandardOutput = $true
    $pinfo.UseShellExecute = $false
    $pinfo.WorkingDirectory = (Resolve-Path -Path './').Path
    $pinfo.Arguments = "-jar ./one-ring-dist.jar --config $tasksIni --output ./settings/distcp.ini --direction $direction --task $task -V $params"
    if ('' -ne $storePath) {
        $pinfo.Arguments += " -S $storePath"
    }
    $p = New-Object System.Diagnostics.Process
    $p.StartInfo = $pinfo
    $p.Start() | Out-Null
    $p.WaitForExit()

    if ($p.ExitCode -gt 0) {
        Write-Output 'no'
    } else {
        Write-Output 'yes'
    }
}
