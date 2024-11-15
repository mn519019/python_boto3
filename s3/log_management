import boto3 
import inquirer
import re
import time
import os, gzip, shutil

aws_client_sts = boto3.client('sts')
aws_s3_client = boto3.client('s3')
bucket_name = 'aws-accelerator-central-logs-637423381326-ca-central-1'
cur_year = str(time.strftime("%Y"))
cur_month = str(time.strftime("%m"))
cur_date = str(time.strftime("%d"))

def account_id():
  aws_account_id = aws_client_sts.get_caller_identity().get('Account')
  return aws_account_id

def get_S3_Bucket(prefix):
    current_date = str(time.strftime("%Y-%m-%d"))
    pattern = re.compile(rf'.*{current_date}.*')
    paginator = aws_s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    matched_keys = []
    for page in page_iterator:
        if 'Contents' in page:
            for key in page['Contents']:
                if pattern.match(key['Key']):
                     matched_keys.append(key['Key'])
                    #  print(key['Key'])
        else:
            print("No contents found in the bucket.")
    return matched_keys

def s3_download_objects(obj_prefix):
    keys = get_S3_Bucket(obj_prefix)
    if keys:
        for key in keys:
            # Extract the filename from the key
            filename = os.path.basename(key)
            # Download the file with the original name
            aws_s3_client.download_file(bucket_name, key, filename)
            print("Downloading =>", filename)
    else:
        print("No matching objects found.")

def unzip_gz_files():
    extension = ".gz"
    dir_path = os.path.dirname(os.path.realpath(__file__))

    files = os.listdir(dir_path)
    for file in files:
        if file.endswith(extension):
            gz_name = os.path.abspath(file)
            file_name = (os.path.basename(gz_name)).rsplit('.',1)[0] #get file name for file within
            with gzip.open(gz_name,"rb") as f_in, open(file_name,"wb") as f_out:
              shutil.copyfileobj(f_in, f_out)
              os.remove(gz_name)
        print("Extracting =>",file)



if __name__ == "__main__":
    # account id
    if account_id() != "xxxxxxxxxxxxxx":
       print("You are using a wrong account.\n please try it again.")
    else:
       print("########################################")
       print("You are signed into LogArchive account.#")
       print("########################################\n\n")

    questions = [
        inquirer.List(
            "Bucket",
            message="What bucket do you need to monitor?",
            choices=["openshift-cloudfront-prod","openshift-cloudfront-staging", "waf-logs-prod", "waf-logs-staging"],
        ),
    ]

    answers = inquirer.prompt(questions)

    if re.match(r'^openshift-cloudfront-.*', answers["Bucket"]):
        print(f"Looking for '{answers['Bucket']}'...\n")
        cf_prefix = answers["Bucket"]
        s3_download_objects(cf_prefix)
        unzip_gz_files()
    else: 
        print(f"Looking for '{answers['Bucket']}'...\n")
        waf_prefix = f"{answers['Bucket']}/{cur_year}/{cur_month}/{cur_date}/"
        print(waf_prefix)
        s3_download_objects(waf_prefix)
        unzip_gz_files()
