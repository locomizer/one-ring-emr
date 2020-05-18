param(
    [Parameter(Position = 1)]
    [string]$azureIni = './settings/aws.ini',
    [Parameter(Position = 2)]
    [string]$iniFile = './settings/run.ini',

    [Parameter(Mandatory = $true)]
    [string]$clusterId,
    [Parameter(Mandatory = $true)]
    [string]$clusterUri,
    [switch]$waitFor = $false
)

. ./common/functions.ps1 $awsIni $iniFile

. ./common/jobs.ps1

"Spark Jobs:"
$sparkJobs = CallLivy $clusterUri
$sparkJobs | Format-Table

if ($waitFor) {
    WaitForSpark $clusterId $clusterUri 0
}
