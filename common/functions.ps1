param(
    [Parameter(Position=1)]
    [string]$awsIni = '',
    [Parameter(Position=2)]
    [string]$iniFile = '',
    [Parameter(Position=3)]
    [switch]$autoConfirm = $false
)

<#
.SYNOPSIS
Reads the .ini file, if specified, and fills array of script properties

.DESCRIPTION
If the script was presented with an argument, it'll be treated as an .ini file with properties
to use as global script properties.
#>
function IniProperties(
    [Parameter(Position=1,Mandatory=$true)]
    [string]$Path = '',
    [switch]$IgnoreInexistent = $false
) {
    if ($Path -ne '') {
        if (Test-Path $Path -PathType Leaf) {
            if ((Get-ItemPropertyValue $Path 'length') -gt 0) {
                return ConvertFrom-StringData (Get-Content $Path -Raw)
            }
        } else {
            if (-not($IgnoreInexistent)) {
                "Can't find specified .ini file '$Path'. Exiting"
                exit 1
            }
        }
    }

    return @{}
}

<#
.SYNOPSIS
Read a value from properties or prompt it from the host

.DESCRIPTION
Checks if there present a property in the global $properties map, and if it doesn't,
invokes a Read-Host with a specified -Prompt.
#>
function ReadProperty(
    [Parameter(Position=1,Mandatory=$true)]
    [string]$Property,
    [Parameter(Position=2,Mandatory=$true)]
    [string]$Prompt,
    [Parameter(Position=3,Mandatory=$false)]
    [switch]$Switch = $false,
    [Parameter(Position=4,Mandatory=$false)]
    [switch]$Optional = $false,
    [switch]$AsSecureString = $false
) {
    if (($Property -eq 'yes') -and $Script:autoConfirm) {
        return 'yes'
    }

    if ($Switch -and -not($Global:properties.ContainsKey($property)) -and $Script:autoConfirm) {
        return 'yes'
    }

    if ($Global:properties.ContainsKey($Property)) {
        if ($AsSecureString) {
            return (ConvertTo-SecureString -AsPlainText -Force -String $Global:properties[$Property])
        }
        return $Global:properties[$Property]
    }

    if (-not($Optional)) {
        return Read-Host -Prompt $Prompt -AsSecureString:$AsSecureString
    }

    return $null
}

$Global:properties = @{}

# Start the magic
if ($awsIni -ne '') {
    $Global:properties += IniProperties $awsIni
}
if ($iniFile -ne '') {
    $Global:properties += IniProperties $iniFile
}

if ($autoConfirm) {
    "AutoConfirm mode is set. This script won't ask your confirmations for either potentially constructive or destructive actions. It will just create and delete anything when needed"
}

Import-Module AWSPowerShell.NetCore
