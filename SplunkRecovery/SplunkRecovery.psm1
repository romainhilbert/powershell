<#
.SYNOPSIS
    This function returns the current Indexes configured in the Splunk Server.
.DESCRIPTION
    This function returns the current Indexes configured in the Splunk Server.

    This function is based on the "splunk list index" command which returns for each index the following
    information:
        mailsecurity * Being deleted *
                PATH_TO\mailsecurity\db
                PATH_TO\mailsecurity\colddb
                PATH_TO\mailsecurity\thaweddb
        main * Default input destination *
                PATH_TO\defaultdb\db
                PATH_TO\defaultdb\colddb
                PATH_TO\defaultdb\thaweddb 

    This function creates a custom object per each Index providing:
    - Index: Index name
    - Deleted: Flag indicating if it is being deleted or not
    - Path: Path to the warm db, cold db and thawed db.

.PARAMETER SplunkBinDir
    Path to the splunk command
.EXAMPLE
    C:\PS>Get-SplunkIndexes -SplunkBinDir 'PATH_TO\SPLUNK_BIN_DIR' -verbose
.LINK
     http://docs.splunk.com/Documentation/Splunk/7.0.0/Admin/CLIadmincommands

.NOTES
    Author: Dario B. (darizotas at gmail dot com)
    Date:   Nov 06, 2017
        
    Copyright 2017 Dario B. darizotas at gmail dot com
    This software is licensed under a new BSD License.
    Unported License. http://opensource.org/licenses/BSD-3-Clause
#>
function Get-SplunkIndexes {
    [CmdletBinding()]
    param(
        # Folder for Splunk binaries directory
        [Parameter(Mandatory=$true)]
        [ValidateScript({Test-Path $_ -PathType Container})]
        [string]
        $SplunkBinDir

    )
    BEGIN {}

    PROCESS {
        $IsNewIndex = $true

        & "$SplunkBinDir\splunk.exe" list index | % {
            # ColdDb and ThawedDb entries are ignored
            $IsIgnored = $_.EndsWith("colddb") -or $_.EndsWith("thaweddb")
            if (-not $IsIgnored) {
                if ($IsNewIndex) {
                    # Index name is followed by "* Being deleted *"
                    $HasComments = $_.indexOf(" ")
                    $Name = $_ 
                    if ($HasComments -gt 0) {
                        $Name = $Name.substring(0, $HasComments)
                    }

                    $o = New-Object –TypeName PSObject -Property @{
                        Index = $Name
                        Deleted = $_.indexOf("* Being deleted *") -gt 0
                    }

                    # Next is the Path to the db, colddb and thaweddb folders
                    $IsNewIndex = $false
                } else {
                    #Remove db from path
                    $o | Add-Member –MemberType NoteProperty –Name Path –Value $_.Trim().TrimEnd("\db") -PassThru -Force

                    $IsNewIndex = $true
                }
            }
        }
    }

    END {}
}

<#
.SYNOPSIS
    This function scans the indexes configured in the Splunk server and returns the list of buckets with their status.
.DESCRIPTION
    This function scans the indexes configured in the Splunk server and returns the list of buckets with their status.

    This function is based on the "fsck scan --all-buckets-one-index --index-name=" command which 
    scan the given index for identifying bucket issues.
    
    The list returned contains custom objects for each bucket providing:
    - Index: index name.
    - Bucket: Path to the bucket.
    - Corrupted: Flag indicating whether it is corrupted (True) or not (False).
    - Message: Output from splunk command.

.PARAMETER SplunkBinDir
    Path to the splunk command
.PARAMETER Index
    Index name(s)
.EXAMPLE
    C:\PS>Get-SplunkIndexesStatus -SplunkBinDir 'PATH_TO\SPLUNK_BIN_DIR' -Index @('main', 'your_index') -verbose
.EXAMPLE
    C:\PS>@('main', 'your_index') | Get-SplunkIndexesStatus -SplunkBinDir 'PATH_TO\SPLUNK_BIN_DIR' -verbose
.EXAMPLE
    Retrieve all indexes and scan them
    C:\PS>Get-SplunkIndexes -SplunkBinDir 'PATH_TO\SPLUNK_BIN_DIR'| select -Expandproperty Index | Get-SplunkIndexesStatus -SplunkBinDir 'PATH_TO\SPLUNK_BIN_DIR' -verbose
.EXAMPLE
    Retrieve only enabled indexes and scan them
    C:\PS>Get-SplunkIndexes -SplunkBinDir 'PATH_TO\SPLUNK_BIN_DIR'| ?{-not $_.Deleted} | select -Expandproperty Index | Get-SplunkIndexesStatus -SplunkBinDir 'PATH_TO\SPLUNK_BIN_DIR' -verbose
.LINK
     http://docs.splunk.com/Documentation/Splunk/7.0.0/Admin/CLIadmincommands
    https://wiki.splunk.com/Community:PostCrashFsckRepair
    Get-SplunkIndexes

.NOTES
    Author: Dario B. (darizotas at gmail dot com)
    Date:   Nov 06, 2017
        
    Copyright 2017 Dario B. darizotas at gmail dot com
    This software is licensed under a new BSD License.
    Unported License. http://opensource.org/licenses/BSD-3-Clause
#>
function Get-SplunkIndexesStatus {
    [CmdletBinding()]
    param(
        # Folder for Splunk binaries directory
        [Parameter(Mandatory=$true)]
        [ValidateScript({Test-Path $_ -PathType Container})]
        [string]
        $SplunkBinDir,

        # Index names
        [Parameter(Mandatory=$true,ValueFromPipeline=$true)]
        [ValidateNotNull()]
        [String[]]
        $Index
 )
    BEGIN {
        $TotalStart = Get-Date
    }

    PROCESS {
        ForEach ($i in $Index) {

            $Start = Get-Date
            write-verbose "Scanning index $i..."

            $Bucket = @()
            $IsNewBucket = $true
            #https://stackoverflow.com/questions/1394084/ignoring-an-errorlevel-0-in-windows-powershell/11826589#11826589
            #https://stackoverflow.com/questions/10666101/lastexitcode-0-but-false-in-powershell-redirecting-stderr-to-stdout-gives-n 
            & "$SplunkBinDir\splunk.exe" fsck scan --all-buckets-one-index --index-name=$i 2>&1 | % { 
                if ($_ -match "idx=(\w+)\s+bucket='(\w:\\[\s\w\\]+)'") {
                    $b = New-Object –TypeName PSObject -Property @{
                        Index = $matches[1]
                        Bucket = $matches[2]
                    }
                    $IsNewBucket = $false
                } elseif (-not $IsNewBucket) {
                    $Corrupted = ($_ -like "Corruption*")
                    $b | Add-Member –MemberType NoteProperty –Name Corrupted –Value $Corrupted -PassThru -Force `
                       | Add-Member –MemberType NoteProperty –Name Message –Value $_ -PassThru -Force `
                       | out-null
                    $Bucket += $b
                    $IsNewBucket = $true
                }
            }
            
            $Bucket
            
            $Elapsedtime = New-Timespan $Start $(Get-Date)
            write-verbose "Analysis done in $ElapsedTime"
        }
    }
    END {
        write-verbose "Total elapsed time: $(New-Timespan $TotalStart $(Get-Date))"
    }
}


<#
.SYNOPSIS
    This function tries to recover those buckets that are in an unrecoverable status.
.DESCRIPTION
    This function tries to recover those buckets that are in an unrecoverable status.

    A bucket is in an unrecoverable status when the Splunk command "splunk rebuild path_to_corrupted_bucket" fails.
    In that case, the script follows these steps (https://answers.splunk.com/answers/174669/what-do-i-do-if-rebuilding-a-bucket-fails.html):
    1. Tries to rebuild the corrupted bucket. Why not to try that first?
       splunk rebuild path_to_corrupted_bucket
       
    2. Export in CSV format the corrupted bucket.
       splunk cmd exporttool path_to_corrupted_bucket path_to_temp_file -csv
    
    3. Move the corrupted bucket to a temp folder.
       move path_to_corrupted_bucket path_to_temp_folder
       
    4. Import the CSV file exported in step 1
       splunk cmd importtool path_to_where_old_corrupted_bucket_was path_to_temp_file
       
    It returns a list of custom objects that provides a log of the performed commands and their exit status:
    - Step: step number.
    - DateTime: timestamp.
    - Index: index name.
    - Bucket: Path to the bucket.
    - Command: Executed command.
    - Output: Output from executed command.
    - Success: Flag indicating whether it was successful (True) or not (False).

.PARAMETER SplunkBinDir
    Path to the splunk command
.PARAMETER Index
    Index name
.PARAMETER Bucket
    Bucket path(s) belonging to the given index.
.PARAMETER PathNotRecovered
    Path where the not recovered bucket will be saved.
.EXAMPLE
    PS C:\>Repair-SplunkBucketsFromIndex -SplunkBinDir 'PATH_TO\SPLUNK_BIN_DIR' -Index 'your_index' -Bucket 'PATH_TO\CORRUPTED_BUCKET' -PathNotRecovered 'PATH_TO_TEMP_FOLDER' -verbose
.EXAMPLE
    PS C:\>@('path_to_corrupted_bucket1', 'path_to_corrupted_bucket2') | Repair-SplunkBucketsFromIndex -SplunkBinDir 'PATH_TO\SPLUNK_BIN_DIR' -Index 'your_index' -PathNotRecovered 'PATH_TO_TEMP_FOLDER' -verbose
.EXAMPLE
    Recover all buckets from an index and generate a CSV log file.
    PS C:\> $log = @('your_index') | Get-SplunkIndexesStatus -SplunkBinDir 'PATH_TO\SPLUNK_BIN_DIR' -verbose `
        | select -ExpandProperty Bucket `
        | Repair-SplunkBucketsFromIndex -SplunkBinDir 'PATH_TO\SPLUNK_BIN_DIR' -Index 'your_index' -PathNotRecovered 'PATH_TO_TEMP_FOLDER' -verbose

    PS C:\>$log | Export-csv -notype -path 'PATH_TO_CSV_LOG_FILE'   
    
.EXAMPLE
    Recover all buckets from all indexes and generate a CSV log file per index.
    PS C:\> Get-SplunkIndexes -SplunkBinDir 'PATH_TO\SPLUNK_BIN_DIR' | % {
                $Index = $_.Index
                $log = Get-SplunkIndexesStatus -SplunkBinDir 'PATH_TO\SPLUNK_BIN_DIR' -Index $Index -verbose `
                    | select -ExpandProperty Bucket `
                    | Repair-SplunkBucketsFromIndex -SplunkBinDir 'PATH_TO\SPLUNK_BIN_DIR' -Index $Index -PathNotRecovered 'PATH_TO_TEMP_FOLDER' -verbose 
                
                $log | Export-csv -notype -path 'PATH_TO\LOG_FILE_$Index.csv'                    
            } 

.LINK
     http://docs.splunk.com/Documentation/Splunk/7.0.0/Admin/CLIadmincommands
    https://wiki.splunk.com/Community:PostCrashFsckRepair
    https://answers.splunk.com/answers/174669/what-do-i-do-if-rebuilding-a-bucket-fails.html
    http://docs.splunk.com/Documentation/Splunk/6.5.0/Indexer/Bucketissues
    http://docs.splunk.com/Documentation/Splunk/6.0/Indexer/HowSplunkstoresindexes
    Get-SplunkIndexesStatus
    Get-SplunkIndexes

.NOTES
    Author: Dario B. (darizotas at gmail dot com)
    Date:   Nov 06, 2017
        
    Copyright 2017 Dario B. darizotas at gmail dot com
    This software is licensed under a new BSD License.
    Unported License. http://opensource.org/licenses/BSD-3-Clause
#>
function Repair-SplunkBucketsFromIndex {
    [CmdletBinding()]
    param(
        # Folder for Splunk binaries directory
        [Parameter(Mandatory=$true)]
        [ValidateScript({Test-Path $_ -PathType Container})]
        [string]
        $SplunkBinDir,

        # Index name
        [Parameter(Mandatory=$true)]
        [ValidateNotNull()]
        [String]
        $Index,

        # Buckets paths
        [Parameter(Mandatory=$true,ValueFromPipeline=$true)]
        [ValidateScript({Test-Path $_ -PathType Container})]
        [String[]]
        $Bucket,

        # Folder to send corrupted buckets
        [Parameter(Mandatory=$true)]
        [ValidateScript({Test-Path $_ -PathType Container})]
        [string]
        $PathNotRecovered
    )
    BEGIN {
        $TotalStart = Get-Date
        $log = @()
    }

    PROCESS {
        ForEach ($b in $Bucket) {
            $Start = Get-Date
            write-verbose "[*] Processing bucket $b on index $Index..."

            # http://docs.splunk.com/Documentation/Splunk/6.0/Indexer/HowSplunkstoresindexes
            # Checks for the folder structure to get the bucket storage to be properly moved.
            if ($b -match "((?:cold|thawed)?db)\\(db(?:_\w+))+$") {
                # Path where to move the corrupted bucket
                $PathCorrupt = Join-Path $PathNotRecovered -ChildPath $Index | Join-Path -ChildPath $matches[1]

                write-verbose "[1] Trying the easy way to rebuild the bucket..."
                # http://docs.splunk.com/Documentation/Splunk/6.5.0/Indexer/Bucketissues
                $Output = & "$SplunkBinDir\splunk.exe" rebuild $b 2>&1 | %{ "$_" }
                $Failed = $Output | ? { $_ -like "*fail*"}

                # Log the output
                $line =  New-Object PSObject -Property @{
                   'Step' = 1
                   'DateTime' = (Get-Date)
                   'Index' = $Index
                   'Bucket' = $b
                   'Command' = "splunk rebuild $b"
                   'Output' = (out-string -InputObject $Output)
                   'Success' = (($Failed -eq $null) -or ($Failed.count -eq 0))
                }
                $log += $line

                if ($Failed) {
                    write-warning "[!] $Failed"
                    write-verbose "[*] Trying the hard way..."
                    
                    $TmpFile = [System.IO.Path]::GetTempFileName()
                    write-verbose "[2] Exporting bucket $b on index $Index to $TmpFile..."
                    $Output = & "$SplunkBinDir\splunk.exe" cmd exporttool $b $TmpFile -csv 2>&1 | %{ "$_" } 
                    $Failed = $Output | ? { $_ -like "*error*"}

                    $line = New-Object PSObject -Property @{
                       'Step' = 2
                       'DateTime' = (Get-Date)
                       'Index' = $Index
                       'Bucket' = $b
                       'Command' = "splunk cmd exporttool $b $TmpFile -csv"
                       'Output' = (out-string -InputObject $Output)
                       'Success' = (($Failed -eq $null) -or ($Failed.count -eq 0))
                    }
                    $log += $line

                    if ($Failed) {
                       write-warning "[!] $Failed"
                    } else {
                       write-verbose "[+] Exported"
                    }


                    # https://blogs.msdn.microsoft.com/kebab/2013/06/09/an-introduction-to-error-handling-in-powershell/
                    # https://blogs.technet.microsoft.com/heyscriptingguy/2014/07/09/handling-errors-the-powershell-way/
                    write-verbose "[3] Moving corrupted bucket $b to $PathCorrupt ..."
                    $MoveError = @()
                    if (!(Test-Path $PathCorrupt -pathType Container)) {
                        $Output = New-Item -Path $PathCorrupt -type directory -ErrorVariable MoveError

                        $Failed = ($MoveError.Count -ne 0)
                        if ($Failed) {
                            $Output = $MoveError[0]
                            write-error "[!] Failed. $Output"
                        }

                        $line = New-Object PSObject -Property @{
                           'Step' = 3
                           'DateTime' = (Get-Date)
                           'Index' = $Index
                           'Bucket' = $b
                           'Command' = "New-Item -Path $PathCorrupt -type directory"
                           'Output' = $Output
                           'Success' = ($Failed -eq $false)
                        }
                        $log += $line
                    }

                    # Target folder is ready, now it is time to move. 
                    if ($MoveError.Count -eq 0) {

                        Move-Item $b $PathCorrupt -Force -ErrorVariable +MoveError

                        if ($MoveError.Count -ne 0) {
                            $Output = $MoveError[0]
                            $Failed = $true
                            write-error "[!] Failed. $Output"
                        } else {
                            $Output = ''
                            $Failed = $false
                            write-verbose "[+] Moved"
                        }
                        $line = New-Object PSObject -Property @{
                           'Step' = 3
                           'DateTime' = (Get-Date)
                           'Index' = $Index
                           'Bucket' = $b
                           'Command' = "Move-Item $b $PathCorrupt -Force"
                           'Output' = $Output
                           'Success' = ($Failed -eq $false)
                        }
                        $log += $line

                        # Import the bucket again
                        write-verbose "[4] Re-importing $b from $TmpFile..."
                        $Output = & "$SplunkBinDir\splunk.exe" cmd importtool $b $TmpFile 2>&1 | %{ "$_" } 
                        $Failed = $Output | ? { $_ -like "*error*"}

                        $line = New-Object PSObject -Property @{
                           'Step' = 4
                            'DateTime' = (Get-Date)
                            'Index' = $Index
                            'Bucket' = $b
                            'Command' = "splunk cmd importtool $b $TmpFile"
                            'Output' = (out-string -InputObject $Output)
                            'Success' = (($Failed -eq $null) -or ($Failed.count -eq 0))
                        }
                        $log += $line

                        if ($Failed) {
                            write-error "[!] $Failed"
                            write-error "[!] The bucket $b cannot be (re-)imported"
                        } else {
                            write-verbose "[+] Imported"
                            write-verbose "[*] Removing temporary file $TmpFile..."
                            
                            Remove-Item -Path $TmpFile -Force
                        }
                    } 
                }
            } else {
                $line = New-Object PSObject -Property @{
                    'Step' = 1
                    'DateTime' = (Get-Date)
                    'Index' = $Index
                    'Bucket' = $b
                    'Command' = "n/a"
                    'Output' = 'The bucket folder does not meet the format (coldb|thawed)?db\db_epoch-time_epoch-time'
                    'Success' = $false
                }
                $log += $line
            }
            
            $Elapsedtime = New-Timespan $Start $(Get-Date)
            write-verbose "Bucket $b processed in $ElapsedTime"
        }
    }
    
    END {
        write-verbose "Total elapsed time: $(New-Timespan $TotalStart $(Get-Date))"
        $log    
    }
}
