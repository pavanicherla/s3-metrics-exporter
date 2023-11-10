# Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/bucket/index.html

import logging, os, time, json, filecmp

import boto3

class SingletonMeta(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]

class BucketSummary():
    def __init__(self) -> None:
        pass

    def load_bucket_connection(self, bucket): 
        self.bucket = bucket

    def pre_run(self):
        self.start_time = time.time()
        self.summarise_duration = 0
        self.d = {
            "prefixes": {},
            "prefix_path": '/',
            "prefix_depth": 0,
            "total_object_count": 0,
            "total_object_size_b": 0
        }
        self.d_flat = []

    def parse_key(self, key, delimiter='/'):
        tokens = key.split(delimiter)
        # logging.info(tokens)
        if len(tokens) == 1:
            return [], tokens[-1]
        elif len(tokens) > 1 and tokens[-1] != '':
            return tokens[:-1], tokens[-1]
        else:
            return None, None

    def process_object(self, object):
        prefix_list, name = self.parse_key(object.key)
        # logging.info(f'{prefix_list}, {name}, {object.size}')
        if name:        
            data_ref = self.d
            for index, prefix in enumerate(prefix_list):
                # add count
                data_ref["total_object_count"] += 1
                data_ref["total_object_size_b"] += object.size
                # go deeper
                if prefix not in data_ref['prefixes']:
                    prefix_path = '/' + '/'.join(prefix_list[0:index+1]) + '/'
                    prefix_depth = prefix_path.count('/') - 1
                    data_ref['prefixes'][prefix] = {
                        "prefixes": {},
                        "prefix_path": prefix_path,
                        "prefix_depth": prefix_depth,
                        "total_object_count": 0,
                        "total_object_size_b": 0
                    }
                data_ref = data_ref['prefixes'][prefix]
            # add count
            data_ref["total_object_count"] += 1
            data_ref["total_object_size_b"] += object.size

    def enrich_units(self, data_ref):
        size_in_bytes = data_ref["total_object_size_b"]
        size_in_gb = size_in_bytes / (1024 ** 3)
        size_in_mb = size_in_bytes / (1024 ** 2)
        size_in_kb = size_in_bytes / 1024
        data_ref["total_object_size_gb"] = size_in_gb
        data_ref["total_object_size_mb"] = size_in_mb
        data_ref["total_object_size_kb"] = size_in_kb
        for prefix in data_ref['prefixes'].keys():
            self.enrich_units(data_ref['prefixes'][prefix])

    def flatten_data(self, data):
        self.d_flat.append(data)
        for obj in data['prefixes'].values():
            self.flatten_data(obj)
        del data['prefixes']

    def post_run(self):
        self.summarise_duration = time.time() - self.start_time
    
    def run(self):
        self.pre_run()
        for obj in self.bucket.objects.all():
            self.process_object(obj)
        self.enrich_units(self.d)
        self.flatten_data(self.d)
        self.post_run()
    
    @classmethod
    def pretty_str(cls, data):
        json_str = json.dumps(data, indent=2)
        return json_str

class BucketAvailabilityTest():
    upload_payload_filepath = '/opt/src/test.payload'
    download_payload_path = '/tmp'

    def __init__(self) -> None:
        pass

    def load_bucket_connection(self, bucket): 
        self.bucket = bucket

    def pre_run(self):
        self.success = False
        self.upload_key = '{}.payload'.format(time.strftime("%y%m%d-%Hh%Mm%Ss"))

    def upload_object(self):
        with open(BucketAvailabilityTest.upload_payload_filepath, 'rb') as upload_file:
            self.bucket.put_object(
                Key=self.upload_key,
                Body=upload_file
            )
            self.upload_success = True

    def download_object(self):
        self.download_payload_filepath = f'{BucketAvailabilityTest.download_payload_path}/{self.upload_key}'
        self.bucket.download_file(self.upload_key, self.download_payload_filepath)
        self.download_matches = filecmp.cmp(BucketAvailabilityTest.upload_payload_filepath, self.download_payload_filepath)

    def cleanup_object(self):
        file_obj = self.bucket.Object(self.upload_key)
        delete_result = file_obj.delete()
        self.delete_sucess = delete_result['ResponseMetadata']['HTTPStatusCode'] == 204
        os.remove(self.download_payload_filepath)

    def post_run(self):
        self.success = self.upload_success and self.download_matches and self.delete_sucess
        if not self.success:
            raise Exception(f"BucketAvailabilityTest failed. States({vars(self)})")

    def run(self):
        self.pre_run()
        self.upload_object()
        self.download_object()
        self.cleanup_object()
        self.post_run()

class Bucket():
    def __init__(self, name, credentials) -> None:
        self.name = name
        self.credentials = credentials
        self.bucket_availability_test = BucketAvailabilityTest()
        self.bucket_summary = BucketSummary()
        self.test_objects = [
            self.bucket_availability_test
        ]

    def create_bucket_connection(self):
        session = boto3.Session(
            aws_access_key_id=self.credentials['access_key'],
            aws_secret_access_key=self.credentials['secret_key'])
        resource = session.resource(
            's3',
            verify=False,
            endpoint_url=self.credentials['endpoint_url'])
        self.bucket = resource.Bucket(self.name)

    # Test 1
    def check_bucket_availability(self):
        self.bucket_availability_test.load_bucket_connection(self.bucket)
        self.bucket_availability_test.run()

    # Test 2
    def summarize_bucket(self):
        self.bucket_summary.load_bucket_connection(self.bucket)
        self.bucket_summary.run()

    def show(self):
        logging.info(f"---- Summary (bucket: {self.name}) ----")
        logging.info(BucketSummary.pretty_str(self.bucket_summary.d))
        logging.info(f"---- Flatten Summary (bucket: {self.name}) ----")
        logging.info(BucketSummary.pretty_str(self.bucket_summary.d_flat))

class BucketManager(metaclass=SingletonMeta):
    credentials_filepath = '/opt/data/credentials.json'

    def __init__(self) -> None:
        credentials = BucketManager.load_bucket_credentials()
        self.buckets = self.create_buckets(credentials)

    @classmethod
    def load_bucket_credentials(cls):
        with open(BucketManager.credentials_filepath, 'r') as cred_file:
            return json.load(cred_file)

    def create_buckets(self, bucket_credentials):
        buckets = {}
        for bucket_cred in bucket_credentials:
            bucket = Bucket(bucket_cred['name'], bucket_cred)
            buckets[bucket_cred['name']] = bucket
        return buckets

    def create_bucket_connections(self):
        for _, bucket in self.buckets.items():
            try:
                bucket.create_bucket_connection()
            except Exception as e:
                logging.error(f'Exception: {e}')
    # Test 1
    def check_bucket_availability(self):
        for _, bucket in self.buckets.items():
            try:
                bucket.check_bucket_availability()
            except Exception as e:
                logging.error(f'Exception: {e}')
    
    # Test 2
    def summarize_buckets(self):
        for _, bucket in self.buckets.items():
            try:
                bucket.summarize_bucket()
            except Exception as e:
                logging.error(f'Exception: {e}')
    
    def show_bucket_summary(self):
        for _, bucket in self.buckets.items():
            bucket.show()

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')
    bucket_manager = BucketManager()
    bucket_manager.create_bucket_connections()
    bucket_manager.check_bucket_availability()
    bucket_manager.summarize_buckets()
    bucket_manager.show_bucket_summary()
