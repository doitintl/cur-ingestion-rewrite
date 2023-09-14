import boto3
import json
import botocore

class Aws:
    def __init__(self):
        self.s3_source_client = boto3.client('s3')
        self.s3_destination_client = boto3.client('s3')
    def read_file_from_s3(self, session_client, s3_bucket: str, object_path: str):
        try:
            #print(f'trying to download "{object_path}" from bucket: {s3_bucket}')
            data = session_client.get_object(Bucket=s3_bucket, Key=object_path)
            read_file = data['Body'].read()
        except botocore.exceptions.ClientError as error:
            print(s3_bucket, object_path)
            print(data)
        return read_file

    def write_file_to_s3(self, session_client, s3_bucket: str, object_path: str, file_contents):
        content_type = 'application/octet-stream'
        if object_path.endswith('.gz'):
            content_type = 'application/x-gzip'
        print(f'--- uploading an object to s3 bucket: {s3_bucket}. object name is: {object_path}')
        response = session_client.put_object(
            ACL='private',
            Body=file_contents,
            Bucket=s3_bucket,
            Key=object_path,
            ContentType=content_type)
        return response

    def get_manifest_files(self, source_bucket: str, prefix: str):
        manifest_files_list = []
        paginator = self.s3_source_client.get_paginator('list_objects')
        page_iterator = paginator.paginate(Bucket=source_bucket, Prefix=prefix)
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('-Manifest.json'):
                        manifest_files_list.append(obj['Key'])
            else:
                print('no results for the current s3 prefix')
                exit(0)
        return manifest_files_list

    def extract_cur_files_from_manifest(self, source_bucket, manifest_list: list, source_prefix):
        csv_list = []
        for manifest_file in manifest_list:
            print(f'reviewing {manifest_file}')
            manifest_report_keys = \
                json.loads(self.read_file_from_s3(self.s3_source_client, source_bucket, manifest_file))['reportKeys']
            for report_key in manifest_report_keys:
                csv_list.append(f'{source_prefix}{report_key}')
        return csv_list

    def rewrite_aws_csv_file(self, source_s3_bucket, destination_s3_bucket, source_cur_path_remove,
                             source_cur_path: str,
                             destination_cur_path: str, csv_file_to_rewrite):
        print(f'- processing {csv_file_to_rewrite}')
        source_csv_gzipped = self.read_file_from_s3(self.s3_source_client, source_s3_bucket, csv_file_to_rewrite)
        previous_report_name = source_cur_path.split('/')[-2:][0]
        new_report_name = destination_cur_path.split('/')[-2:][0]
        destination_csv_location = csv_file_to_rewrite.replace(source_cur_path, destination_cur_path)
        destination_csv_location = destination_csv_location.replace(previous_report_name, new_report_name)
        destination_csv_location = destination_csv_location.replace(source_cur_path_remove, '')
        if not previous_report_name:
            destination_csv_location = destination_csv_location.replace(source_cur_path_remove, '')
        self.write_file_to_s3(self.s3_destination_client, destination_s3_bucket, destination_csv_location,
                              source_csv_gzipped)
        return True


    def rewrite_aws_manifest_file(self, manifest_file: str, destination_report_name: str, source_cur_path_remove: str,
                                  source_cur_path: str, destination_cur_path: str, source_bucket: str,
                                  destination_bucket: str):
        print(f'- processing {manifest_file}')
        manifest_json = json.loads(self.read_file_from_s3(self.s3_source_client, source_bucket, manifest_file))
        previous_report_name = manifest_json['reportName']
        manifest_json['reportName'] = destination_report_name
        manifest_json['bucket'] = destination_bucket
        destination_report_name = destination_cur_path.split('/')[-2:][0]
        report_keys = []
        for csv_file in manifest_json['reportKeys']:
            csv_file = csv_file.replace(source_cur_path, destination_cur_path)
            csv_file = csv_file.replace(previous_report_name, destination_report_name)
            csv_file = csv_file.replace(source_cur_path_remove, '')
            report_keys.append(csv_file)

        manifest_json['reportKeys'] = report_keys
        manifest_file_new_path = manifest_file.replace(source_cur_path, destination_cur_path)
        manifest_file_new_path = manifest_file_new_path.replace(previous_report_name, destination_report_name)
        if source_cur_path_remove:
            manifest_file_new_path = manifest_file_new_path.replace(source_cur_path_remove, '')
        self.write_file_to_s3(self.s3_destination_client, destination_bucket, manifest_file_new_path,
                              json.dumps(manifest_json, indent=4, sort_keys=True))
        return True