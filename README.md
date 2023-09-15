# cur-ingestion-rewrite
Getting the CUR files ready for CUR data ingestion into DoiT console. 
Make sure you have the following details prior to running this script:

source_bucket = ("Enter source bucket name: ") Bucket where AWS backfilled the CUR data on customer member account.
![image](https://github.com/doitintl/cur-ingestion-rewrite/assets/39531203/caea6bfd-e90d-4106-962c-7bd85a884340)

destination_bucket = ("Enter destination bucket name: ") Temporary bucket on MPA where modified data is copied.
![image](https://github.com/doitintl/cur-ingestion-rewrite/assets/39531203/07a7bfe2-e71a-4513-b4eb-7293e7d72577)

source_cur_path = ("Enter source CUR path: ") S3 bucket path to CUR files eg., /billing/testcustomer1234-aws-billing/
![image](https://github.com/doitintl/cur-ingestion-rewrite/assets/39531203/caea6bfd-e90d-4106-962c-7bd85a884340)

source_cur_path_to_exclude = ("Enter source CUR path to exclude (if any): ") If the CUR path is created differnt folder under S3 root. Mostly empty or Enter.

destination_cur_path = ("Enter destination CUR path: ") Mimic our DoiT bucket format. eg., CUR/doitintl-awsops-<MPID>/ (CUR/doitintl-awsops-1234/)

From the EC2 instance prompt run 

$python3 main.py

##IT WILL PROMPT FOR ABOVE PARAMETERS. PROVIDE THE DETAILS.

Enter source bucket name: cur-awsbackfill-bucket

Enter destination bucket name: doitintl-awsops-1234-edit

Enter source CUR path: billing/test1234-cost-report/

Enter source CUR path to exclude (if any): 

Enter destination CUR path: CUR/doitintl-awsops-1234/


Sample output:

**Changing the reportkeys and manifest file name**

Updated Report Keys: ['CUR/doitintl-awsops-1234/20220801-20220901/20230825T094827Z/doitintl-awsops-1234-00001.csv.gz', 'CUR/doitintl-awsops-1234/20220801-20220901/20230825T094827Z/doitintl-awsops-1234-00002.csv.gz']

Successfully transformed manifest file: CUR/doitintl-awsops-1234/20220801-20220901/20230825T094827Z/doitintl-awsops-1234-Manifest.json

Processing billing/test1234-cost-report/20220801-20220901/test1234-cost-report-Manifest.json

Updated Report Keys: ['CUR/doitintl-awsops-1234/20220801-20220901/20230825T094827Z/doitintl-awsops-1234-00001.csv.gz', 'CUR/doitintl-awsops-1234/20220801-20220901/20230825T094827Z/doitintl-awsops-1234-00002.csv.gz']

Successfully transformed manifest file: CUR/doitintl-awsops-1234/20220801-20220901/doitintl-awsops-1234-Manifest.json


**Changing the csv file names**

Processing billing/test1234-cost-report/20230801-20230901/20230901T104132Z/test1234-cost-report-00002.csv.gz
to: CUR/doitintl-awsops-1234/20230801-20230901/20230901T104132Z/doitintl-awsops-1234-00002.csv.gz

Processing billing/test1234-cost-report/20230801-20230901/20230901T104132Z/test1234-cost-report-00001.csv.gz
to: CUR/doitintl-awsops-1234/20230801-20230901/20230901T104132Z/doitintl-awsops-1234-00001.csv.gz

Processing billing/test1234-cost-report/20230801-20230901/20230901T104132Z/test1234-cost-report-00002.csv.gz
to: CUR/doitintl-awsops-1234/20230801-20230901/20230901T104132Z/doitintl-awsops-1234-00002.csv.gz

Done


Once all the files are transformed and copied to the destination bucket it finished by printing “Done”

