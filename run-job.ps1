param(
    [Parameter(Position = 1)]
    [string]$awsIni = './settings/aws.ini',
    [Parameter(Position = 2)]
    [string]$iniFile = './settings/run.ini',
    [Parameter(Position = 3, Mandatory = $true)][ValidateNotNullOrEmpty()]
    [string]$tasksFile,
    [switch]$autoConfirm = $false,

    [switch]$wrapperStore = $false,
    [string]$clusterUri,
    [string]$clusterId,
    [string]$params = 'YT1h' #a=a
)

"Running job sequence on AWS EMR Cluster"

. ./common/functions.ps1 $awsIni $iniFile $autoConfirm

$tasks = ReadProperty 'tasks' -Prompt "Enter a comma-separated list of task names"
$tasks = $tasks.Split(',').Trim()

$clusterBucket = ReadProperty "cluster.s3.bucket" -Prompt "Enter S3 bucket name where to place cluster files"
$bucket = Get-S3Bucket -BucketName $clusterBucket -ErrorAction SilentlyContinue
if (-not($bucket)) {
    "Inexistent bucket. Exiting"
    exit 1
}

$jobs = @()
foreach ($task in $tasks) {
    "Defining task '$task'"

    $job = @{
        'Name' = $task
    }

    if (-not $clusterUri) {
        "No Cluster master node URI was specified as this script -clusterUri parameter. Exiting"
        exit 1
    }

    $jarFile = ReadProperty "$task.artifact" -Prompt "Enter .jar artifact name for task, or path to .jar"

    if (-not(Test-Path $jarFile)) {
        "Inexistent artifact '$jarFile'. Exiting"
        exit 1
    }
    $job['Artifact'] = $jarFile

    $className = ReadProperty "$task.class.name" -Prompt "Enter main class name for .jar artifact"
    if ($className -eq '') {
        "Jobs with .jar artifact require main class name. Exiting"
        exit 1
    }
    $job['ClassName'] = $className

    $libs = ReadProperty "$task.libs" -Prompt "Enter path to additional libraries" -Optional
    if ($null -ne $libs) {
        $libs = $libs.Split(',').Trim()

        $libJars = @()
        foreach ($libJar in $libs) {
            if (-not(Test-Path $libJar)) {
                "Inexistent library '$libJar'. Exiting"
                exit 1
            }

            $libJars += $libJar
        }

        $job['Libs'] = $libJars
    }

    $arguments = ReadProperty "$task.arguments" -Prompt "Enter task command line arguments, as a comma-separated list" -Optional
    if ($null -ne $arguments) {
        $arguments = $arguments.Split(',').Trim()

        $job['Arguments'] = $arguments
    }
    else {
        $job['Arguments'] = @()
    }

    if ($wrapperStore) {
        $job['WrapperStore'] = "s3://$Script:clusterBucket/artifacts/$Script:clusterId"
    }
    else {
        $job['WrapperStore'] = ''
    }

    $jobs += $job
}

function TransferFile([string]$localFile) {
    $localPath = Resolve-Path $localFile
    $bareName = Split-Path $localPath -Leaf
    Write-S3Object -BucketName $Script:clusterBucket `
        -File $localFile `
        -Key "artifacts/$Script:clusterId/$bareName"

    return "s3://$Script:clusterBucket/artifacts/$Script:clusterId/$bareName"
}

function DownloadFile([string]$bucket, [string]$remotePath, [string]$localPath) {
    Copy-S3Object -BucketName $bucket `
        -Key $remotePath `
        -LocalFile $localPath
}

. ./common/jobs.ps1

. ./common/local.ps1

foreach ($job in $jobs) {
    $name = $job.Name

    "Preparing to run a Job for task $name"

    $splatConfig = @{ }

    "Copying Job files"

    if ($null -ne $job['Artifact']) {
        $splatConfig['JarFile'] = TransferFile $job['Artifact']
    }

    if ($null -ne $job['Libs']) {
        $libJars = @()
        foreach ($lib in $job['Libs']) {
            $libJars += TransferFile $lib
        }

        $splatConfig['LibJars'] = $libJars
    }

    if ($null -ne $job['Arguments']) {
        $splatConfig['Arguments'] = $job['Arguments']. `
            Replace('%params%', $params). `
            Replace('%tasksFile%', $tasksFile). `
            Replace('%task%', $job['Name']). `
            Replace('%wrapperStore%', $job['WrapperStore'])
    }

    $yes = ReadProperty 'yes' -Prompt "Ready to go. Say 'yes' to proceed"
    if ($yes -ne 'yes') {
        "You decided not to proceed. Exiting."
        exit 1
    }

    "Configuring the Job '$($job.Name)'"

    $distTasksFile = $tasksFile
    if ($tasksFile.StartsWith('s3:')) {
        $json = $tasksFile.EndsWith('json')
        if ($json) {
            $distTasksFile = "./settings/tasks.json"
        } else {
            $distTasksFile = "./settings/tasks.ini"
        }

        $tasksFilePath = $tasksFile -split '/+',3
        DownloadFile "$($tasksFilePath[1])" "$($tasksFilePath[2])" "$distTasksFile"
    }

    $distcp = CallDistWrapper 'to' $name $params $distTasksFile
    if ($distcp -eq 'yes') {
        CallDistCp $clusterId
    }
    else {
        "Task configuration validation failed. Exiting."
        exit 1
    }

    "Starting the Job '$name'"

    $def = @{
        'file' = $splatConfig.JarFile
        'name' = $name
    }
    $def['className'] = $job.ClassName
    if ($null -ne $splatConfig.LibJars) {
        $def['jars'] = $splatConfig.LibJars
    }
    $def['args'] = $splatConfig.Arguments

    $body = ($def | ConvertTo-Json)

    Invoke-RestMethod `
        -Method Post `
        -Body "$body" `
        -ContentType 'application/json' `
        -Uri "http://$($clusterUri):8998/batches"

    "Waiting for Job '$name' completion"
    WaitForSpark $clusterId $clusterUri

    if ($wrapperStore) {
        DownloadFile "$Script:clusterBucket" "artifacts/$Script:clusterId/outputs/part-00000" "./settings/outputs"
        $distcp = CallDistWrapper 'from' $name $params $distTasksFile './settings/outputs'
    }
    else {
        $distcp = CallDistWrapper 'from' $name $params $distTasksFile
    }

    if ($distcp -eq 'yes') {
        "Copying task result"
        CallDistCp $clusterId
    }
    else {
        "Task result copying failed. Exiting."
        exit 1
    }
}

"All done."
