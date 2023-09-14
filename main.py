from functions import *

if __name__ == '__main__':
    # FILL ME
    source_bucket = input("Enter source bucket name: ")
    destination_bucket = input("Enter destination bucket name: ")
    source_cur_path = input("Enter source CUR path: ")
    source_cur_path_to_exclude = input("Enter source CUR path to exclude (if any): ")
    destination_cur_path = input("Enter destination CUR path: ")
    # STOP FILL ME

    aws = Aws()  # Initialize the AWS class without providing any roles

    source_cur_files_list = aws.get_manifest_files(source_bucket, '')
    csv_from_source_manifest = aws.extract_cur_files_from_manifest(source_bucket, source_cur_files_list, source_cur_path_to_exclude)
    for source_cur_file in source_cur_files_list:
        aws.rewrite_aws_manifest_file(source_cur_file, destination_cur_path, source_cur_path_to_exclude, source_cur_path, destination_cur_path, source_bucket, destination_bucket)
    for csv_file in csv_from_source_manifest:
        aws.rewrite_aws_csv_file(source_bucket, destination_bucket, source_cur_path_to_exclude, source_cur_path, destination_cur_path, csv_file)
    print('Done')