param(
    [Parameter(Position = 1, Mandatory = $true)]
    [string]$iniLocation = './settings',
    [Parameter(Mandatory = $true)]
    [string]$tcBuild,
    [Parameter(Mandatory = $true)]
    [string]$tcAddress,
    [Parameter(Mandatory = $true)]
    [string]$tcUser,
    [Parameter(Mandatory = $true)]
    [string]$tcPwd
)

. ./common/functions.ps1

$inis = Get-ChildItem $iniLocation -Name '*.ini'

$auth = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes(("{0}:{1}" -f $tcUser, $tcPwd)))
$buildParams = (Invoke-RestMethod `
        -Headers @{Authorization = ("Basic {0}" -f $auth) } `
        -Method Get `
        -UseBasicParsing `
        -ContentType 'application/json' `
        -Uri "$tcAddress/httpAuth/app/rest/builds/id:$tcBuild/resulting-properties").properties.property | Where-Object { $_.name -like '*.ini.*' }

foreach ($ini in $inis) {
    $iniContent = IniProperties "$iniLocation/$ini"

    $buildParams | Where-Object { $_.name -like "$ini.*" } | % { $_.name -match "$ini\.(?<param>.+)" ; $iniContent[$Matches['param']] = $_.value }

    $iniContent.Keys | ForEach-Object { "$_=$($iniContent[$_])" } > "$iniLocation/$ini"
}
