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

$yes = ReadProperty 'emrfs.table.remove' -Prompt "Please say 'yes' if you want to remove DynamoDB table for S3 consistent view" -Switch
if ($yes -eq 'yes') {
    $ddbTableName = ReadProperty 'emrfs.table.name' -Prompt "DynamoDB table for EMR S3 file system connector" -Optional
    if ($null -eq $ddbTableName) {
        $ddbTableName = "EmrfsMetadata$uniq"
    }

    "Removing the DynamoDB table. Please wait for completion."
    Remove-DDBTable -TableName $ddbTableName -Force
}

$yes = ReadProperty 'cluster.remove' -Prompt "Say 'yes' if you really want to remove the Cluster itself" -Switch
if ($yes -eq 'yes') {
    "Removing the Cluster. Please wait for completion."
    Remove-CFNStack -StackName "cluster-deployment-$uniq" -Force
}

"All done."
