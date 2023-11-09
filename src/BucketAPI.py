import time, json

import boto3

class BucketResult():
    def __init__(self) -> None:
        pass

    def initialize_data(self):
        self.start_time = time.time()
        self.available = False
        self.summarise_duration = 0
        self.d = {
            "prefixes": {},
            "prefix_path": '/',
            "prefix_depth": 0,
            "total_object_count": 0,
            "total_object_size_b": 0
        }
        self.d_flat = []

    def finalize_data(self):
        self.available = True
        self.summarise_duration = time.time() - self.start_time

    def set_failed(self):
        self.available = False

    def parse_key(self, key, delimiter='/'):
        tokens = key.split(delimiter)
        # print(tokens)
        if len(tokens) == 1:
            return [], tokens[-1]
        elif len(tokens) > 1 and tokens[-1] != '':
            return tokens[:-1], tokens[-1]
        else:
            return None, None

    def process_object(self, object):
        prefix_list, name = self.parse_key(object.key)
        # print(f'{prefix_list}, {name}, {object.size}')
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
    
    def process_objects(self, objects):
        self.initialize_data()
        for obj in objects:
            self.process_object(obj)
        self.enrich_units(self.d)
        self.flatten_data(self.d)
        self.finalize_data()

    def flatten_data(self, data):
        self.d_flat.append(data)
        for obj in data['prefixes'].values():
            self.flatten_data(obj)
        del data['prefixes']
    
    @classmethod
    def pretty_str(cls, data):
        json_str = json.dumps(data, indent=2)
        return json_str

class Bucket():
    def __init__(self, name, credentials) -> None:
        self.results = BucketResult()
        self.name = name
        self.credentials = credentials

    def summarize_bucket(self):
        session = boto3.Session(
            aws_access_key_id=self.credentials['access_key'],
            aws_secret_access_key=self.credentials['secret_key'])
        resource = session.resource(
            's3',
            verify=False,
            endpoint_url=self.credentials['endpoint_url'])
        bucket = resource.Bucket(self.name)
        object_iterator = bucket.objects.all()
        self.results.process_objects(object_iterator)
    
    def set_failed(self):
        self.results.set_failed()
        print(f'Bucket Failed ({self.name})..')

    def print_results(self):
        print(f"---- Summary (bucket: {self.name}) ----")
        print(BucketResult.pretty_str(self.results.d))
        print(f"---- Flatten Summary (bucket: {self.name}) ----")
        print(BucketResult.pretty_str(self.results.d_flat))

class BucketManager():
    def __init__(self) -> None:
        credentials = self.get_bucket_credentials()
        self.buckets = self.initialize_buckets(credentials)

    def get_bucket_credentials(self):
        f = open('/opt/data/credentials.json')
        return json.load(f)

    def initialize_buckets(self, bucket_credentials):
        buckets = {}
        for bucket_cred in bucket_credentials:
            bucket = Bucket(bucket_cred['name'], bucket_cred)
            buckets[bucket_cred['name']] = bucket
        return buckets
    
    def summarize_buckets(self):
        for _, bucket in self.buckets.items():
            try:
                bucket.summarize_bucket()
            except Exception as e:
                bucket.set_failed()
                print(f'Exception: {e}')
    
    def print_results(self):
        for _, bucket in self.buckets.items():
            bucket.print_results()

if __name__ == '__main__':
    bucket_manager = BucketManager()
    bucket_manager.summarize_buckets()
    bucket_manager.print_results()
