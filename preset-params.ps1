param(
    [Parameter(Position = 1, Mandatory = $true)]
    [string]$preset = 'Z',
    [Parameter(Position = 2, Mandatory = $true)]
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

$auth = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes(("{0}:{1}" -f $tcUser, $tcPwd)))

$presetParams = @{}
if ($preset -match '^(S|M|L|XL)$') {
    $presetParams = IniProperties "./presets/$preset.ini"
}

$buildParams = (Invoke-RestMethod `
        -Headers @{Authorization = ("Basic {0}" -f $auth) } `
        -Method Get `
        -UseBasicParsing `
        -ContentType 'application/json' `
        -Uri "$tcAddress/httpAuth/app/rest/builds/id:$tcBuild/resulting-properties").properties.property | Where-Object { $_.name -like '*.ini.*' }

$buildParams | ForEach-Object { $presetParams[$_.name] = $_.value }

$presetParams

$inis = Get-ChildItem $iniLocation -Name '*.ini'

foreach ($ini in $inis) {
    $iniContent = IniProperties "$iniLocation/$ini"

    $presetParams | Where-Object { $_.name -like "$ini.*" } | ForEach-Object { $_.name -match "$ini\.(?<param>.+)" ; $iniContent[$Matches['param']] = $_.value }

    $iniContent.Keys | ForEach-Object { "$_=$($iniContent[$_])" } > "$iniLocation/$ini"
}
