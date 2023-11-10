import logging, os, time

from prometheus_client import start_http_server, Enum, Gauge

from BucketAPI import BucketManager

# Environment Variables
PORT = int(os.getenv("S3_EXPORTER_PORT", 8080))
POLL_RATE_SECONDS = int(os.getenv("S3_EXPORTER_POLL_RATE_SECONDS", 900))
MAX_PREFIX_DEPTH = int(os.getenv("S3_EXPORTER_MAX_PREFIX_DEPTH", 1))

# Metrics Creation
enum_metric_names = [
    ("available", " availability - bucket can be written to and read from", ['bucket'])
]
gauge_metric_names = [
    ("summarise_duration", "time taken in seconds to gather summary values", ['bucket']),
    ("total_object_count", "count of all objects", ['bucket', 'prefix']),
    ("total_object_size_b", "size of all objects in bytes", ['bucket', 'prefix']),
    ("total_object_size_kb", "size of all objects in kilobytes", ['bucket', 'prefix']),
    ("total_object_size_mb", "size of all objects in megabytes", ['bucket', 'prefix']),
    ("total_object_size_gb", "size of all objects in gigabytes", ['bucket', 'prefix']),
]
## Enum
enum_metrics = {}
for metric_name, metric_description, metric_labels in enum_metric_names:
    enum_metrics[metric_name] = Enum(f'bucket_{metric_name}', metric_description, metric_labels, states=['true', 'false'])
## Gauge
gauge_metrics = {}
for metric_name, metric_description, metric_labels in gauge_metric_names:
    gauge_metrics[metric_name] = Gauge(f'bucket_{metric_name}', metric_description, metric_labels)

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')
    logging.info(f"Starting server on port {PORT}")
    # Start up the server to expose the metrics.
    start_http_server(PORT)

    bucket_manager = BucketManager()
    while True:
        # create session
        bucket_manager.create_bucket_connections()

        # check s3 availability
        bucket_manager.check_bucket_availability()

        # publish enum availability metrics
        for _, bucket in bucket_manager.buckets.items():
            bucket_name = bucket.name
            value = bucket.bucket_availability_test.success
            if value:
                enum_metrics["available"].labels(bucket=bucket_name).state('true')
            else:
                enum_metrics["available"].labels(bucket=bucket_name).state('false')

        # poll s3 bucket
        bucket_manager.summarize_buckets()

        # publish gauge duration metrics
        for _, bucket in bucket_manager.buckets.items():
            bucket_name = bucket.name
            value = bucket.bucket_summary.summarise_duration
            gauge_metrics["summarise_duration"].labels(bucket=bucket_name).set(value)

        # publish gauge count metrics
        for _, bucket in bucket_manager.buckets.items():
            bucket_name = bucket.name
            for data in bucket_manager.buckets[bucket_name].bucket_summary.d_flat:
                if data['prefix_depth'] > MAX_PREFIX_DEPTH:
                    continue
                for metric_name, gauge_metric in gauge_metrics.items():
                    if not metric_name.startswith('total_object_'):
                        continue
                    value = data[metric_name]
                    gauge_metric.labels(bucket=bucket_name, prefix=data["prefix_path"]).set(value)

        logging.info(f"checked buckets")
        time.sleep(POLL_RATE_SECONDS)
