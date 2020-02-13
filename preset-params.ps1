param(
    [Parameter(Position = 1, Mandatory = $true)]
    [string]$preset = 'Z'
)

. ./common/functions.ps1

if ($preset -match '^(S|M|L|XL)$') {
    $presetProperties = IniProperties "./presets/$preset.ini"

    $presetProperties.Keys | % {
        "##teamcity[setParameter name='$_' value='$($presetProperties.Item($_))']"
    }
}
