param(
    [Parameter(Position = 1)]
    [string]$awsIni = './settings/aws.ini',
    [Parameter(Position = 2)]
    [string]$iniFile = './settings/remove.ini',

    [Parameter(Mandatory = $true)]
    [string]$uniq,
    [switch]$autoConfirm = $false
)

"This script removes an Amazon EMR Cluster"

. ./common/functions.ps1 $awsIni $iniFile $autoConfirm

$yes = ReadProperty 'cluster.remove' -Prompt "Say 'yes' if you really want to remove the Cluster" -Switch
if ($yes -eq 'yes') {
    "Removing the Cluster. Please wait for completion."
    Remove-CFNStack -StackName "cluster-deployment-$uniq" -Force
}

"All done."
