param(
    [Parameter(Position = 1)]
    [string]$awsIni = './settings/aws.ini',
    [Parameter(Position = 2)]
    [string]$iniFile = './settings/create.ini',
    [switch]$autoConfirm = $false,

    [string]$coreIni = './settings/core.ini',
    [string]$mapredIni = './settings/mapred.ini',
    [string]$hdfsIni = './settings/hdfs.ini',
    [string]$yarnIni = './settings/yarn.ini',
    [string]$oozieIni = './settings/oozie.ini',
    [string]$sparkIni = './settings/spark.ini',
    [string]$capacityIni = './settings/capacity.ini',
    [string]$livyIni = './settings/livy.ini'
)

"This script creates/checks an AWS EMR Cluster"

. ./common/functions.ps1 $awsIni $iniFile $autoConfirm

$region = Get-AWSRegion
$region | Format-Table Region, Name, IsShellDefault
$location = ReadProperty 'location' -Prompt "Please choose cluster location. It will be used by default for all other entities"

if ($location -notin $region.Region) {
    "Unknown region $location. Exiting"
    exit 1
}

Set-DefaultAWSRegion -Region $location

$clusterName = ReadProperty 'cluster.name' -Prompt "Now enter the name of the EMR cluster"

$workloadType = ReadProperty 'workload.type' -Prompt "If this cluster belongs to a separately billed workload, enter its identifier" -Optional
if ($null -eq $workloadType) {
    $workloadType = $clusterName
}

$keypairs = Get-EC2KeyPair
$keypairs | Format-Table KeyName, KeyFingerprint
$keypair = ReadProperty 'keypair' -Prompt "Please choose EC2 Key Pair name used to log in to cluster nodes"

if ($keypair -notin $keypairs.KeyName) {
    "Unknown Key Pair $keypair. Exiting"
    exit 1
}

$subnets = Get-EC2Subnet
$subnets | Format-Table SubnetId, VpcId, AvailableIpAddressCount, CidrBlock
$subnet = ReadProperty 'subnet' -Prompt "Please choose EC2 Subnet Id to place cluster into"

if ($subnet -notin $subnets.SubnetId) {
    "Unknown Subnet $subnet. Exiting"
    exit 1
}

$sg = Get-EC2SecurityGroup
$sg | Format-Table GroupId, GroupName, VpcId, Description
$masterSG = ReadProperty 'sg.master' -Prompt "Please choose master node Security Group Id"

if ($masterSG -notin $sg.GroupId) {
    "Unknown Security Group $masterSG. Exiting"
    exit 1
}

$slaveSG = ReadProperty 'sg.slave' -Prompt "Please choose slave nodes Security Group Id"

if ($slaveSG -notin $sg.GroupId) {
    "Unknown Security Group $slaveSG. Exiting"
    exit 1
}

$buckets = Get-S3Bucket
$buckets | Format-Table BucketName
$clusterBucket = ReadProperty 'cluster.s3.bucket' -Prompt "Please choose S3 Bucket to place Cluster task files and logs"

if ($clusterBucket -notin $buckets.BucketName) {
    "Unknown S3 Bucket $clusterBucket. Exiting"
    exit 1
}

$parameters = @( `
    @{ Key = "ClusterName"; Value = $clusterName }, `
    @{ Key = "KeyName"; Value = $keypair }, `
    @{ Key = "LogS3Bucket"; Value = $clusterBucket }, `
    @{ Key = "Subnet"; Value = $subnet }, `
    @{ Key = "MasterSG"; Value = $masterSG }, `
    @{ Key = "SlaveSG"; Value = $slaveSG } `
)

"Cluster Nodes configuration"
# Default cluster size (# of worker nodes)
$clusterSizeInNodes = ReadProperty 'cluster.worker.nodes' -Prompt "Specify how many worker nodes are needed"
$clusterCpuCountPerNode = ReadProperty 'cluster.worker.node.vcore.size' -Prompt "Specify the number of vCores on a worker node"
$nodeSizes = ReadProperty 'custom.node.sizes' -Prompt "If you want to customize node sizes, say 'yes' now" -Switch
if ($nodeSizes -eq 'yes') {
    $masterNodeSize = ReadProperty 'node.size.master' -Prompt "Master node size"
    $masterVolumeSize = ReadProperty 'volume.size.master' -Prompt "Master node EBS volume size"
    $coreNodeSize = ReadProperty 'node.size.core' -Prompt "Core nodes sizes"
    $coreVolumeSize = ReadProperty 'volume.size.core' -Prompt "Core nodes EBS volumes sizes"
}

$clusterVersion = ReadProperty 'cluster.version' -Prompt "Please choose EMR release" -Optional
if ($null -eq $clusterVersion) {
    $clusterVersion = "emr-5.23.0"
}

$bidPercentage = ReadProperty 'bid.percentage' -Prompt "Please choose bidding SPOT price as a percentage of ON_DEMAND price (or 0 to use ON_DEMAND instances)" -Optional
if ($null -eq $bidPercentage) {
    $bidPercentage = "20"
}

$bidTimeout = "0"
if ($bidPercentage -ne "0") {
    $bidTimeout = ReadProperty 'bid.timeout' -Prompt "Please choose SPOT bidding timeout in minutes, minimal allowed value is 5 and maximal is 1440, by default 20" -Optional
    if ($null -eq $bidTimeout) {
        $bidTimeout = "20"
    }

    $bidTerminate = ReadProperty 'bid.terminate' -Prompt "Please choose if you wand to terminate cluster if SPOT bidding fails. By default, false which means switch to ON_DEMAND" -Optional
    if ($null -eq $bidTerminate) {
        $bidTerminate = "false"
    }
    elseif ($bidTerminate -ne "false") {
        $bidTerminate = "true"
    }

    $parameters += @{ Key = "BidTimeout"; Value = $bidTimeout }
    $parameters += @{ Key = "BidTerminate"; Value = $bidTerminate }
}

$coresPerCluster = ([convert]::ToInt32($clusterSizeInNodes, 10)) * ([convert]::ToInt32($clusterCpuCountPerNode, 10))

$parameters += @{ Key = "ClusterVersion"; Value = $clusterVersion }
$parameters += @{ Key = "Capacity"; Value = $coresPerCluster }
$parameters += @{ Key = "CoreCpuSize"; Value = $clusterCpuCountPerNode }
$parameters += @{ Key = "MasterSize"; Value = $masterNodeSize }
$parameters += @{ Key = "MasterVolumeSize"; Value = $masterVolumeSize }
$parameters += @{ Key = "CoreSize"; Value = $coreNodeSize }
$parameters += @{ Key = "CoreVolumeSize"; Value = $coreVolumeSize }
$parameters += @{ Key = "BidPercentage"; Value = $bidPercentage }

$config = Get-Content ./cluster.template -Raw | ConvertFrom-Json

function SetConfigProps([string]$ini, [string]$classification) {
    $iniConfig = IniProperties $ini -IgnoreInexistent
    if ($iniConfig.Count -gt 0) {
        $Script:config.Resources.PlatformCalculationCluster.Properties.Configurations += @{
            Classification          = $classification
            ConfigurationProperties = $iniConfig
        }
    }
}

SetConfigProps $coreIni "core-site"
SetConfigProps $mapredIni "mapred-site"
SetConfigProps $hdfsIni "hdfs-site"
SetConfigProps $yarnIni "yarn-site"
SetConfigProps $oozieIni "oozie-site"
SetConfigProps $sparkIni "spark-defaults"
SetConfigProps $capacityIni "capacity-scheduler"
SetConfigProps $livyIni "livy-conf"

$uniq = ReadProperty 'unique.id' -Prompt "Enter an unique id for this deployment" -Optional
if ($null -eq $uniq) {
    $uniq = [DateTimeOffset]::Now.ToUnixTimeSeconds()
}

$parameters += @{ Key = "Uniq"; Value = $uniq }

$ddbTableName = ReadProperty 'emrfs.table.name' -Prompt "DynamoDB table for EMR S3 file system connector" -Optional
if ($null -eq $ddbTableName) {
    $ddbTableName = "EmrfsMetadata$uniq"
}
$parameters += @{ Key = "EmrfsTable"; Value = $ddbTableName }

$parameters += @{ Key = "WorkloadType"; Value = $workloadType }

$parameters

# Create the EMR cluster
"Creating the Cluster. Please wait for completion."
$stackId = New-CFNStack `
    -TemplateBody (ConvertTo-Json $config -Depth 100 -Compress) `
    -StackName "cluster-deployment-$uniq" `
    -ClientRequestToken $uniq `
    -Parameters $parameters `
    -ErrorAction Stop

$tOut = [int]$bidTimeout * 60 + 1800
Wait-CFNStack -StackName $stackId -Timeout $tOut

$masterAddress = 'dummy'
$clusterId = 'dummy'

$exports = Get-CFNExport
foreach ($export in $exports) {
    if ($stackId -eq $export.ExportingStackId) {
        switch -wildcard ($export.Name) {
            'ClusterId-*' {
                $clusterId = $export.Value
                break
            }
            'MasterDNSName-*' {
                $masterFilter = New-Object Amazon.EC2.Model.Filter "dns-name", $export.Value
                $masterAddress = (Get-EC2Instance -Filter $masterFilter).Instances[0].PrivateDnsName
                break
            }
        }
    }
}

"##teamcity[setParameter name='deployment.uniq' value='$uniq']"
"##teamcity[setParameter name='deployment.cluster.id' value='$clusterId']"
"##teamcity[setParameter name='deployment.master.address' value='$masterAddress']"


"Now tagging deployed entities."

. ./common/jobs.ps1

CallCommandRunner $clusterId "emrfs create-metadata -m $ddbTableName" "CreateEMRFSTable"
$ddbTable = Get-DDBTable -TableName $ddbTableName
$ddbTag = New-Object "Amazon.DynamoDBv2.Model.Tag"
$ddbTag.Key = "workload-type"
$ddbTag.Value = $workloadType
Add-DDBResourceTag -ResourceArn $ddbTable.TableArn -Tag $ddbTag


"All done."
