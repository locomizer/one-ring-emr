function CallLivy([string]$clusterUri) {
    return ((Invoke-WebRequest `
        -Uri "http://$($clusterUri):8998/batches" `
        -Method Get `
        -UseBasicParsing `
        -ErrorAction Stop).Content | ConvertFrom-Json).sessions
}

$sparkRunningStates = @( 'not_started', 'starting', 'recovering', 'idle', 'running', 'busy', 'shutting_down' )
$sparkBadStates = @( 'error', 'dead' )

function WaitForSpark([string]$clusterId, [string]$clusterUri, [int]$tOut = 120) {
    $wait = $true
    $waiting = 0

    do {
        Start-Sleep -Seconds 20

        $jobs = CallLivy $clusterUri

        if (($null -eq $jobs) -or (($null -ne $jobs.Count) -and ($jobs.Count -eq 0))) {
            "No jobs yet, wait them to appear"

            if ($tOut -gt 0) {
                $waiting += 20
                if ($waiting -gt $tOut) {
                    "Wait timed out, and we can't continue to next task"

                    exit 1
                }
            }
        } else {
            if ($jobs.getType().Name -eq 'PSCustomObject') {
                $job = $jobs
            } else {
                $job = $jobs.Get($jobs.Count - 1)
            }

            $state = $job.state

            "Task state is '$state'"

            if ($state -in $Script:sparkRunningStates) {
                "Should wait for completion..."
            } elseif ($state -in $Script:sparkBadStates) {
                "This indicates an error, and we can't continue to next task"

                exit 1
            } else {
                "Assuming good, moving on"

                $wait = $false
            }
        }
    } while ($wait)
}

$runningStates = @( 'RUNNING', 'PENDING' )
$badStates = @( 'INTERRUPTED', 'FAILED' )

function WaitForMR([string]$clusterId, [int]$tOut = 120) {
    $wait = $true
    $waiting = 0

    do {
        Start-Sleep -Seconds 20

        $jobs = Get-EMRSteps -ClusterId $clusterId -ErrorAction Stop

        if (($null -eq $jobs) -or (($null -ne $jobs.Count) -and ($jobs.Count -eq 0))) {
            "No jobs yet, wait them to appear"

            if ($tOut -gt 0) {
                $waiting += 20
                if ($waiting -gt $tOut) {
                    "Wait timed out, and we can't continue to next task"

                    exit 1
                }
            }
        } else {
            if ($jobs.GetType().Name -eq 'StepSummary') {
                $job = $jobs
            } else {
                $job = $jobs.Get(0)
            }

            $state = $job.Status.State

            "Task state is '$state'"

            if ($state -in $Script:runningStates) {
                "Should wait for completion..."
            } elseif ($state -in $Script:badStates) {
                "This indicates an error, and we can't continue to next task"

                exit 1
            } else {
                "Assuming good, moving on"

                $wait = $false
            }
        }
    } while ($wait)
}


function CallCommandRunner([string]$clusterId, [string]$commandLine, [string]$name, [int]$tOut = 120) {
    $stepConfig = New-Object `
        -TypeName 'Amazon.ElasticMapReduce.Model.HadoopJarStepConfig' `
        -ArgumentList 'command-runner.jar'
    $stepConfig.Args = $commandLine.Split(' ')

    $jobConfig = New-Object `
        -TypeName 'Amazon.ElasticMapReduce.Model.StepConfig' `
        -ArgumentList $name, $stepConfig
    $jobConfig.ActionOnFailure = New-Object -TypeName 'Amazon.ElasticMapReduce.ActionOnFailure' -ArgumentList 'CANCEL_AND_WAIT'

    Add-EMRJobFlowStep `
        -JobFlowId $clusterId `
        -Step $jobConfig

    WaitForMR $clusterId $tOut
}
