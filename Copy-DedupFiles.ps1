param(
    [string]$Source,
    [string]$Destination,
    [string[]]$Extensions = @()
);

# Adjust max threads based on processor
$maxThreads = 0;
Get-WmiObject -Class Win32_Processor | %{
    $maxThreads = $maxThreads + $_.NumberOfLogicalProcessors;
}
$maxThreads = $maxThreads -2; # Leave room for main thread and other processes.

# Validate Max Threads
if ($maxThreads -le 0) {
    Write-Verbose "Setting Max Threads to 1";
    $maxThreads = 1;
}

# Cleanup any jobs
$jobs = @();
$jobs += Get-Job "Hash-*";
$jobs += Get-Job "Copy-*";
$jobs | Stop-Job;
$jobs | Remove-Job;

# Sanatize extensions
$Extensions = $Extensions | %{ $_.ToUpper().Trim().Replace('.', '') };

# Build a hash table of unique files
$files = @{};
$count = 0;

function Handle-Jobs {
    param($job, [bool]$PerformCopy = $true);
 
    # Handle Results
    $result = $job | Receive-Job;
    if (-not $files[$result.Key]) {
        Write-Host $("Adding {0} = {1}" -f $result.Key,$result.Value)
        $files[$result.Key] = $result.Value;

        # Create copy job
        if ($PerformCopy) {
            Write-Host $("Starting Copy Job {0}" -f $("Copy-{0}" -f $result.Key));
            Start-Job -Name $("Copy-{0}" -f $result.Key) -ArgumentList $result.Value, $Destination -ScriptBlock {
                param($source,$destination);

                $file = Get-Item $source;

                $year = $file.LastWriteTime.Year;
                $day = $file.LastWriteTime.ToString("yyyy-MM-dd");

                $resolvedDestination = Join-Path $destination $("{0}\{1}" -f $year,$day);

                if (-not $(Test-Path -ErrorAction SilentlyContinue $resolvedDestination)) {
                    mkdir $resolvedDestination | Out-Null;
                }

                # Check if something of that name already exists
                if ($(Test-Path -ErrorAction SilentlyContinue -Path $(Join-Path $resolvedDestination $file.Name))) {
                    # Get next version number
                    $newPath = "";
                    $version = 0;
                    do {
                        $version++;
                        $newPath = Join-Path $resolvedDestination $("{0}-{1}{2}" -f $file.BaseName, $version, $file.Extension);
                    } while ($(Test-Path -ErrorAction SilentlyContinue $newPath));

                    $resolvedDestination = $newPath;
                }

                Copy-Item $source $resolvedDestination;
            } | Out-Null;
        }
    } else {
        Write-Host -Foreground Green $("Skipping {0}" -f $result.Value);
    }

    # Remove the job
    Remove-Job -Id $job.Id;

    # Cleanup copy jobs
    if ($PerformCopy) {
        $storageJobs = Get-Job "Copy-*" | ?{ $_.State -ne "Running"};
        $storageJobs | Receive-Job;
        $storageJobs | Remove-Job;
    }
}

# Scan Destination and add pre-copied files
$count = 0;
Get-ChildItem -Recurse -File -Path $Destination | Sort-Object LastWriteTime | %{

    # Increment counter
    $count++;

    # Handle threads
     $jobs = Get-job PreHash-*;
     if ($jobs.Count -ge $maxThreads) {
        $job = Wait-Job -Any -Job $jobs;

        Handle-Jobs $job $false;
    }

    # Start new job
    Start-Job -Name $("PreHash-{0}" -f $count) -ScriptBlock {
        param(
            [string] $fullName
        );

        Write-Output $(New-Object -TypeName psobject -Property @{
            Key=$(Get-FileHash $fullName -Algorithm MD5).Hash;
            Value=$fullName;
        });
    } -ArgumentList $_.FullName | Out-Null;
}

# Wait for remaining pre-hash jobs to complete
$job = $null;
while ($job = Wait-Job -Any "PreHash-*") {
    Handle-Jobs $job $false;
}

$count = 0;
$sourceFiles = $(Get-ChildItem -Recurse -File -Path $Source | ?{
    $(-not $("{0}\" -f $_.FullName.ToUpper()).StartsWith($("{0}\" -f $Destination.ToUpper().TrimEnd("\")))) `
    -and $(-not ($Extensions.Count -ne 0 -and -not ($Extensions -contains $_.Extension.ToUpper().Trim().Replace('.', ''))))
    } | Sort-Object LastWriteTime);
foreach ($file in $sourceFiles) {
  
    # Increment counter
    $count++;
    
    # Handle threads
     $hashJobs = Get-job Hash-*;
     if ($hashJobs.Count -ge $maxThreads) {
        $job = Wait-Job -Any -Job $hashJobs;

        Handle-Jobs $job;    
    }

    # Start new job
    Start-Job -Name $("Hash-{0}" -f $count) -ScriptBlock {
        param(
            [string] $fullName
        );

        Write-Output $(New-Object -TypeName psobject -Property @{
            Key=$(Get-FileHash $fullName -Algorithm MD5).Hash;
            Value=$fullName;
        });
    } -ArgumentList $file.FullName | Out-Null;
}

$job = $null;
while ($job = Wait-Job -Any "Hash-*") {
    Handle-Jobs $job;
}

# Wait for the remaining copy jobs to complete
$job = $null;
while ($job = Wait-Job -Any "Copy-*") {
    $job | Receive-Job;
    $job | Remove-Job;
}