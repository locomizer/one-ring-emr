param(
    [Parameter(Position = 1)]
    [string]$awsIni = './settings/aws.ini',
    [Parameter(Position = 2)]
    [string]$iniFile = './settings/run.ini',
    [Parameter(Position = 3, Mandatory = $true)][ValidateNotNullOrEmpty()]
    [string]$tasksFile,
    [string]$tasksPrefix = 'spark.meta',
    [string]$paramsFile,
    [string]$params = 'YT1h', #a=a
    [switch]$autoConfirm = $false,
    [Parameter(Mandatory = $true)][ValidateNotNullOrEmpty()]
    [string]$clusterUri,
    [Parameter(Mandatory = $true)][ValidateNotNullOrEmpty()]
    [string]$clusterId
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

    $job['WrapperStore'] = "s3://$Script:clusterBucket/artifacts/$Script:clusterId"

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


. ./common/jobs.ps1

. ./common/local.ps1


$decodedParams = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($params))
$decodedParams = ConvertFrom-StringData $decodedParams
$decodedParams.Keys | ForEach-Object {
"##teamcity[setParameter name='deployment.params.$_' value='$($decodedParams.Item($_))']"
}


foreach ($job in $jobs) {
    "Preparing to run a Job for task '$($job.Name)'"

    $splatConfig = @{ }

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

    "Configuring the Job"

    if ($null -ne $job['Arguments']) {
        $splatConfig['Arguments'] = $job['Arguments']. `
            Replace('%tasksFile%', $tasksFile). `
            Replace('%prefix%', $tasksPrefix). `
            Replace('%wrapperStore%', $job['WrapperStore'])
        if ($null -ne $params) {
            $splatConfig['Arguments'] = $splatConfig['Arguments']. `
                Replace('%params%', $params)
        }
        elseif ($null -ne $paramsFile) {
            $splatConfig['Arguments'] = $splatConfig['Arguments']. `
                Replace('%paramsFile%', $paramsFile)
        }
    }

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

    $body

    $yes = ReadProperty 'yes' -Prompt "Ready to go. Say 'yes' to proceed"
    if ($yes -ne 'yes') {
        "You decided not to proceed. Exiting."
        exit 1
    }

    "Starting the Job"

    Invoke-RestMethod `
        -Method Post `
        -Body "$body" `
        -ContentType 'application/json' `
        -Uri "http://$($clusterUri):8998/batches"

    "Waiting for Job completion"

    WaitForSpark $clusterId $clusterUri
}

"All done."
