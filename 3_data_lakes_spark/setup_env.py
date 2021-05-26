import configparser
import boto3
import json
from botocore.exceptions import ClientError
import time

CONFIG_FILENAME = "./emr.cfg"


def get_config(file_path):
    """
    Returns configuration loaded from file_name
    :param file_path: filename for loading configuration
    :return: configuration object
    """
    config = configparser.ConfigParser()
    config.read(file_path)
    return config


def add_processing_steps(config):
    """
    Method added for additional step processing when cluster is already provisioned
    :param config: configuration object
    :return:
    """
    emr_client = create_boto3_client(config, 'emr')
    code_bucket = config['S3']['EMR_CODE_BUCKET_S3_PATH']
    output_bucket = config['S3']['OUTPUT_DATA_BUCKET_S3_PATH']
    input_bucket = config['S3']['INPUT_DATA_BUCKET_S3_PATH']
    try:
        response = emr_client.add_job_flow_steps(
            JobFlowId=config['CLUSTER']['CLUSTER_ID'],
            Steps=[
                {
                    'Name': 'Copy files to HDFS',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp', code_bucket, '/home/hadoop/',
                                 '--recursive']
                    }
                },
                {
                    'Name': 'Run Spark',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit', '/home/hadoop/etl.py',
                                 input_bucket, output_bucket]
                    }
                }
            ]
        )
    except ClientError as e:
        print("Couldn't setup steps.")
        raise e
    print(response)


def create_iam_role(config):
    """
    Method creates necessary IAM role for EMR cluster
    :param config: configuration object
    :return:
    """
    role_name = config.get('IAM_ROLE', 'NAME')
    print("Creating role name:", role_name)
    iam = create_boto3_client(config, 'iam')
    try:
        cr_response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "elasticmapreduce.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }), Description='Role for configuring EMR access to resources', Tags=[
                {
                    'Key': 'Name',
                    'Value': config.get('IAM_ROLE', 'NAME')
                },
            ])
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print("IAM role already exists")
            cr_response = iam.get_role(RoleName=role_name)
            print('IAM Role Arn:', cr_response['Role']['Arn'])
        else:
            print("Unexpected error: %s" % e)

    # Add policy to role
    try:
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn=config.get('IAM_ROLE', 'POLICY_S3_FULL')
        )

        iam.attach_role_policy(
            RoleName=config.get('IAM_ROLE', 'NAME'),
            PolicyArn=config.get('IAM_ROLE', 'POLICY_ELASTIC_MRR')
        )
    except ClientError as e:
        print("Unexpected error: %s" % e)
        raise e
    return cr_response


def create_emr_cluster(config):
    """
    Creating EMR cluster with necessary steps and configuration
    :param config: Configuration object
    :return:
    """
    emr_client = create_boto3_client(config, 'emr')
    code_bucket = config['S3']['EMR_CODE_BUCKET_S3_PATH']
    output_bucket = config['S3']['OUTPUT_DATA_BUCKET_S3_PATH']
    input_bucket = config['S3']['INPUT_DATA_BUCKET_S3_PATH']
    try:
        response = emr_client.run_job_flow(
            Name=config.get('CLUSTER', 'NAME'),
            LogUri=config.get('CLUSTER', 'LOG_URI'),
            ReleaseLabel=config.get('CLUSTER', 'RELEASE_LABEL'),
            Instances={
                'MasterInstanceType': config.get('CLUSTER', 'MASTER_INSTANCE_TYPE'),
                'SlaveInstanceType': config.get('CLUSTER', 'SLAVE_INSTANCE_TYPE'),
                'InstanceCount': int(config.get('CLUSTER', 'INSTANCE_COUNT')),
                'KeepJobFlowAliveWhenNoSteps': bool(config.get('CLUSTER', 'KEEP_ALIVE')),
                'Ec2SubnetId': config.get('CLUSTER', 'SUBNET_ID'),
                'Ec2KeyName': config.get('CLUSTER', 'EC2_KEY_NAME')
            },
            Applications=[
                {
                    'Name': 'spark',
                }, {
                    'Name': 'Zeppelin',
                }
            ],
            Configurations=[
                {
                    "Classification": "spark-env",
                    "Configurations": [
                        {
                            "Classification": "export",
                            "Properties": {
                                "PYSPARK_PYTHON": "/usr/bin/python3"
                            }
                        }
                    ]
                }
            ],
            Steps=[
                {
                    'Name': 'Debugging',
                    'ActionOnFailure': 'TERMINATE_CLUSTER',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['state-pusher-script']
                    }
                },
                {
                    'Name': 'Copy files to HDFS',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp', code_bucket, '/home/hadoop/',
                                 '--recursive']
                    }
                },
                {
                    'Name': 'Run Spark',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit', '/home/hadoop/etl.py',
                                 input_bucket, output_bucket]
                    }
                }
            ],
            JobFlowRole=config.get('CLUSTER', 'JOB_FLOW_ROLE'),
            ServiceRole=config.get('CLUSTER', 'SERVICE_ROLE'),
            EbsRootVolumeSize=10,
            VisibleToAllUsers=True
        )
        cluster_id = response['JobFlowId']
        print("Created cluster %s.", cluster_id)
    except ClientError:
        print("Couldn't create cluster.")
        raise
    print('Sleep 5 seconds')
    time.sleep(5)
    while True:
        print('Fetching status of cluster..')
        try:
            cluster_status = get_cluster_status(emr_client, cluster_id)
            cluster_status_desc = cluster_status['Cluster']['Status']
            if cluster_status_desc['State'] != 'STARTING':
                print('Cluster Status:', cluster_status_desc)
                break
        except ClientError as e:
            print("Unexpected error wile getting cluster status: %s" % e)
            raise e
        print('Cluster Status:', cluster_status)
        print('Sleep 10 seconds')
        time.sleep(10)
    print('Cluster Status:', get_cluster_status(emr_client, cluster_id))


def create_bucket(config, bucket_name, bucket_acl):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """
    # Create bucket
    print("Creating bucket:", bucket_name)
    try:
        s3_client = create_boto3_client(config, 's3')
        location = {'LocationConstraint': config.get('AWS_ACCESS', 'AWS_REGION')}
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location,
                                ACL=bucket_acl)
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyExists':
            print("BucketAlreadyExists: %s" % e)
            return
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print("BucketAlreadyOwnedByYou: %s" % e)
            return
        print("Error while creating S3 bucket: %s" % e)
        raise e


def create_boto3_client(config, service):
    """
    Creates boto client with configuration from ~/.aws/credentials file and AWS config params from dict
    :param config: configuration object with AWS additional params
    :param service: AWS service for which the client should be invoked
    :return:
    """
    session = boto3.Session(profile_name=config.get('AWS_ACCESS', 'AWS_PROFILE'))
    return session.client(service, region_name=config.get('AWS_ACCESS', 'AWS_REGION'))


def get_cluster_status(boto3_client, cluster_identifier):
    """
    Returns cluster status for given cluster identifier
    :param boto3_client: aws boto3 client
    :param cluster_identifier: unique emr cluster identifier
    :return:
    """
    return boto3_client.describe_cluster(
        ClusterId=cluster_identifier
    )


def upload_code(config, file_name, bucket_name, key_name):
    """
    Method uploads files to given bucket and keyname
    :param config: configuration
    :param file_name: name of file to upload
    :param bucket_name: bucket name
    :param key_name: keyname of the bucket
    :return:
    """
    print("Uploading [code] %s [bucket] %s [key] %s" % (file_name, bucket_name, key_name))
    try:
        s3_client = create_boto3_client(config, 's3')
        s3_client.upload_file(file_name, bucket_name, key_name)
    except ClientError as e:
        print("Error while uploading to S3 bucket: %s" % e)
        raise e


def main():
    """
    Creates necessary IAM role and buckets for EMR Cluster.
    Spins up EMR cluster and copies code to the EMR cluster
    :return:
    """
    config = get_config(CONFIG_FILENAME)
    print("Create IAM Role")
    create_iam_role(config)
    print("Create S3 Buckets")
    create_bucket(config, config.get("S3", "EMR_LOG_BUCKET"), config.get("S3", "EMR_LOG_BUCKET_ACL"))
    create_bucket(config, config.get("S3", "EMR_CODE_BUCKET"), config.get("S3", "EMR_CODE_BUCKET_ACL"))
    create_bucket(config, config.get("S3", "OUTPUT_DATA_BUCKET"), config.get("S3", "OUTPUT_DATA_BUCKET_ACL"))
    print("Upload code to s3")
    upload_code(config, './etl.py', config['S3']['EMR_CODE_BUCKET'], 'etl.py')
    upload_code(config, './emr.cfg', config['S3']['EMR_CODE_BUCKET'], 'emr.cfg')
    print("Creating emr cluster")
    create_emr_cluster(config)
    # Enabled if reprocessing is required
    # add_processing_steps(config)


if __name__ == '__main__':
    main()
