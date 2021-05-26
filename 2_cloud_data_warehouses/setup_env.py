import configparser
import boto3
import json
from botocore.exceptions import ClientError
import time

CONFIG_FILENAME = "dwh.cfg"


def get_config(file_path):
    """
    Returns configuration loaded from file_name
    :param file_path: filename for loading configuration
    :return: configuration object
    """
    config = configparser.ConfigParser()
    config.read(file_path)
    return config


def create_iam_role(config):
    """
    Method creates necessary IAM role for the redshift cluster
    :param config: configuration object
    :return:
    """
    iam = create_boto3_client(config, 'iam')

    try:
        cr_response = iam.create_role(
            RoleName=config.get('IAM_ROLE', 'NAME'),
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "redshift.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }), Description='Role for configuring Redshift access', Tags=[
                {
                    'Key': 'Name',
                    'Value': config.get('IAM_ROLE', 'NAME')
                },
            ])
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print("IAM role already exists")
            cr_response = iam.get_role(RoleName=config.get('IAM_ROLE', 'NAME'))
            print('IAM Role Arn:', cr_response['Role']['Arn'])
        else:
            print("Unexpected error: %s" % e)

    # Add policy to role
    try:
        iam.attach_role_policy(
            RoleName=config.get('IAM_ROLE', 'NAME'),
            PolicyArn=config.get('IAM_ROLE', 'POLICY_ARN')
        )
    except ClientError as e:
        print("Unexpected error: %s" % e)
        raise e
    return cr_response


def create_redshift_cluster(config, redshift_role):
    """
    Creates redshift cluster with configuration from config file and given iam-role
    :param config: redshift configuration dict
    :param redshift_role: redshift iam  role
    :return:
    """
    redshift = create_boto3_client(config, 'redshift')
    cluster_identifier = config.get('CLUSTER', 'CLUSTER_IDENTIFIER')
    print("Creating redshift cluster: %s" % cluster_identifier)
    try:
        cc_response = redshift.create_cluster(
            MasterUsername=config.get('CLUSTER', 'DB_USER'),
            MasterUserPassword=config.get('CLUSTER', 'DB_PASSWORD'),
            ClusterIdentifier=cluster_identifier,
            NodeType=config.get('CLUSTER', 'NODE_TYPE'),
            NumberOfNodes=int(config.get('CLUSTER', 'NODE_COUNT')),
            Port=int(config.get('CLUSTER', 'DB_PORT')),
            IamRoles=[
                redshift_role['Role']['Arn']
            ],
            ClusterSubnetGroupName=config.get('CLUSTER', 'SUBNET_GROUP'),
            ClusterSecurityGroups=[config.get('CLUSTER', 'SECURITY_GROUP_ID')]
        )
        print('Creating Cluster:', cc_response)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ClusterAlreadyExists':
            print("Cluster %s already exists" % cluster_identifier)
            return
        else:
            print("Unexpected error wile creating cluster: %s" % e)

    print('Sleep 5 seconds')
    time.sleep(5)
    while True:
        print('Fetching status of cluster..')
        try:
            cluster_status = get_cluster_status(redshift, cluster_identifier)
            if cluster_status['Clusters'][0]['ClusterStatus'] == 'available':
                break
            print('Cluster Status:', cluster_status)
        except ClientError as e:
            print("Unexpected error wile getting cluster status: %s" % e)
            raise e
        print('Sleep 10 seconds')
        time.sleep(10)
    print('Cluster is created and available.')


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
    :param cluster_identifier: unique redshift cluster identifier
    :return:
    """
    return boto3_client.describe_clusters(
        ClusterIdentifier=cluster_identifier
    )


def main():
    """
    Creates IAM role an creates redshift cluster from dwh.cfg configuration object
    :return:
    """
    config = get_config(CONFIG_FILENAME)
    print("Creating IAM role")
    role = create_iam_role(config)
    print("Creating redshift cluster")
    create_redshift_cluster(config, role)


if __name__ == '__main__':
    main()
