# event-sync

Event Sync is for syncing events from multiple sources to multiple destinations, targetted for adhoc events, where sources support acknowledgement functionality.

## suported

- input sources:
  - Google Pub/Sub
- destination:
  - Google BigQuery
    - Streaming
    - Load Jobs (uses Google Cloud Storage)
  - Google Cloud Storage


### Synopsis

Built to ease process of syncing data between different storage systems

```
event-sync [flags]
```

### Options

```
  -c, --config string   job configuration file path (default "app.json")
  -h, --help            help for event-sync
  -v, --verbose         verbose mode
```

## Config Example

```json
{
    "jobs": [
        {
            "name": "app-events",
            "suspend": false,
            "source": {
                "type": "google-pubsub",
                "pubsubConfig": {
                    "projectId": "gcp-project",
                    "subscriptionId": "pubsub-sub",
                    "maxOutstandingMessages": 5000,
                    "attributeKeyName": "event_type"
                }
            },
            "filters": [
                {
                    "type": "attribute",
                    "name": "event_name",
                    "action": "ingest",
                    "target": {
                        "table": "bq-table"
                    },
                    "schema": [
                        {
                            "name": "ts",
                            "type": "TIMESTAMP"
                        },
                        {
                            "name": "field1",
                            "type": "STRING"
                        },
                        {
                            "name": "field1",
                            "type": "STRING"
                        },
                        {
                            "name": "field3",
                            "type": "STRING"
                        }
                    ]
                }
            ],
            "destination": {
                "type": "google-storage-load",
                "timestampColumnName": "ts",
                "timestampFormat": "2006-01-02T15:04:05.999999999Z",
                "timePartitioningType": "DAY",
                "expiration": "2160h",
                "clusterBy": [
                    "field1",
                    "field2",
                    "field3"
                ],
                "bigqueryConfig": {
                    "projectId": "gcp-project",
                    "dataset": "bq-dataset"
                },
                "googleStorageConfig": {
                    "projectId": "gcp-project",
                    "bucket": "gcs-bucket",
                    "blobPrefix": "blob-prefix"
                },
                "batchSize": 1000
            }
        }
    ]
}
```
