param(
    [Parameter(Mandatory = $true)]
    [string]$ProjectId,

    [string]$Region = "us-central1",

    [string]$DatasetLocation = "US",

    [string]$TopicName = "retail-events",

    [string]$Owner = "candidate"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RawBucketName = ("{0}-retailpulse-raw" -f $ProjectId).ToLowerInvariant()
$TempBucketName = ("{0}-retailpulse-temp" -f $ProjectId).ToLowerInvariant()
$DeadLetterTopic = "{0}-dlq" -f $TopicName
$SubscriptionName = "{0}-sub" -f $TopicName
$ServiceAccountName = "retailpulse-dataflow"
$ServiceAccountEmail = "{0}@{1}.iam.gserviceaccount.com" -f $ServiceAccountName, $ProjectId


function Require-Command {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found. Install the Google Cloud SDK and make sure '$Name' is on PATH."
    }
}


function Invoke-LoggedCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FilePath,

        [string[]]$Arguments = @()
    )

    Write-Host ""
    Write-Host ("==> {0} {1}" -f $FilePath, ($Arguments -join " "))
    & $FilePath @Arguments

    if ($LASTEXITCODE -ne 0) {
        throw ("Command failed: {0} {1}" -f $FilePath, ($Arguments -join " "))
    }
}


function Test-CommandSuccess {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FilePath,

        [string[]]$Arguments = @()
    )

    & $FilePath @Arguments 1>$null 2>$null
    return $LASTEXITCODE -eq 0
}


function Ensure-PubSubTopic {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    if (Test-CommandSuccess "gcloud" @("pubsub", "topics", "describe", $Name, "--project", $ProjectId)) {
        Write-Host "Topic '$Name' already exists."
        return
    }

    Invoke-LoggedCommand "gcloud" @("pubsub", "topics", "create", $Name, "--project", $ProjectId)
}


function Ensure-PubSubSubscription {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    if (Test-CommandSuccess "gcloud" @("pubsub", "subscriptions", "describe", $Name, "--project", $ProjectId)) {
        Write-Host "Subscription '$Name' already exists."
        return
    }

    Invoke-LoggedCommand "gcloud" @(
        "pubsub",
        "subscriptions",
        "create",
        $Name,
        "--project", $ProjectId,
        "--topic", $TopicName,
        "--ack-deadline", "60",
        "--dead-letter-topic", $DeadLetterTopic,
        "--max-delivery-attempts", "10",
        "--min-retry-delay", "10s",
        "--max-retry-delay", "300s"
    )
}


function Ensure-StorageBucket {
    param(
        [Parameter(Mandatory = $true)]
        [string]$BucketName
    )

    if (Test-CommandSuccess "gcloud" @("storage", "buckets", "describe", ("gs://{0}" -f $BucketName), "--project", $ProjectId)) {
        Write-Host "Bucket '$BucketName' already exists."
        return
    }

    Invoke-LoggedCommand "gcloud" @(
        "storage",
        "buckets",
        "create",
        ("gs://{0}" -f $BucketName),
        "--project", $ProjectId,
        "--location", $Region,
        "--uniform-bucket-level-access"
    )
}


function Ensure-ServiceAccount {
    if (Test-CommandSuccess "gcloud" @("iam", "service-accounts", "describe", $ServiceAccountEmail, "--project", $ProjectId)) {
        Write-Host "Service account '$ServiceAccountEmail' already exists."
        return
    }

    Invoke-LoggedCommand "gcloud" @(
        "iam",
        "service-accounts",
        "create",
        $ServiceAccountName,
        "--display-name", "RetailPulse Dataflow Runner",
        "--project", $ProjectId
    )
}


function Ensure-BigQueryDataset {
    param(
        [Parameter(Mandatory = $true)]
        [string]$DatasetId,

        [Parameter(Mandatory = $true)]
        [string]$Description
    )

    if (Test-CommandSuccess "bq" @("--location=$DatasetLocation", "show", "--format=none", ("{0}:{1}" -f $ProjectId, $DatasetId))) {
        Write-Host "Dataset '$DatasetId' already exists."
        return
    }

    Invoke-LoggedCommand "bq" @(
        "--location=$DatasetLocation",
        "mk",
        "--dataset",
        "--description", $Description,
        ("{0}:{1}" -f $ProjectId, $DatasetId)
    )
}


Require-Command "gcloud"
Require-Command "bq"

Write-Host "Preparing RetailPulse core resources in project '$ProjectId'."

Invoke-LoggedCommand "gcloud" @(
    "services",
    "enable",
    "bigquery.googleapis.com",
    "composer.googleapis.com",
    "dataflow.googleapis.com",
    "pubsub.googleapis.com",
    "storage.googleapis.com",
    "--project", $ProjectId
)

Ensure-StorageBucket -BucketName $RawBucketName
Ensure-StorageBucket -BucketName $TempBucketName

Ensure-PubSubTopic -Name $TopicName
Ensure-PubSubTopic -Name $DeadLetterTopic

$ProjectNumber = (& gcloud projects describe $ProjectId --format="value(projectNumber)" --project $ProjectId).Trim()
if (-not $ProjectNumber) {
    throw "Could not resolve the project number for '$ProjectId'."
}

Invoke-LoggedCommand "gcloud" @(
    "pubsub",
    "topics",
    "add-iam-policy-binding",
    $DeadLetterTopic,
    "--project", $ProjectId,
    "--member", ("serviceAccount:service-{0}@gcp-sa-pubsub.iam.gserviceaccount.com" -f $ProjectNumber),
    "--role", "roles/pubsub.publisher"
)

Ensure-PubSubSubscription -Name $SubscriptionName
Ensure-ServiceAccount

foreach ($role in @(
    "roles/bigquery.dataEditor",
    "roles/dataflow.worker",
    "roles/pubsub.subscriber",
    "roles/storage.objectAdmin",
    "roles/viewer"
)) {
    Invoke-LoggedCommand "gcloud" @(
        "projects",
        "add-iam-policy-binding",
        $ProjectId,
        "--member", ("serviceAccount:{0}" -f $ServiceAccountEmail),
        "--role", $role,
        "--quiet"
    )
}

Ensure-BigQueryDataset -DatasetId "bronze" -Description "Streaming validated retail events."
Ensure-BigQueryDataset -DatasetId "silver" -Description "Curated order-level retail tables."
Ensure-BigQueryDataset -DatasetId "gold" -Description "Analytics-ready business KPI tables."
Ensure-BigQueryDataset -DatasetId "ops" -Description "Operational metrics and data quality audit tables."

Write-Host ""
Write-Host "RetailPulse core resources are ready."
Write-Host "Next steps:"
Write-Host ("1. Run sql/01_create_objects.sql in BigQuery to create the base tables in project '{0}'." -f $ProjectId)
Write-Host ("2. Start the Dataflow pipeline using service account '{0}' if you want a dedicated runner identity." -f $ServiceAccountEmail)
Write-Host "3. Follow docs/manual-gcp-setup.md for optional Cloud Composer environment creation and DAG upload."

