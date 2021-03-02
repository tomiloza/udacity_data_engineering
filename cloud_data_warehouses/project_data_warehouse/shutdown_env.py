from botocore.exceptions import ClientError
import time

from setup_env import get_config, CONFIG_FILENAME, create_boto3_client, get_cluster_status


def destroy_cluster(config):
    """
    Method destroys cluster for given redsfhit cluster configuration
    :param config: redshift cluster configuration object
    :return:
    """
    redshift = create_boto3_client(config, 'redshift')
    cluster_identifier = config.get('CLUSTER', 'CLUSTER_IDENTIFIER')
    try:
        redshift.delete_cluster(
            ClusterIdentifier=cluster_identifier,
            SkipFinalClusterSnapshot=True
        )
        print('deleting redshift cluster....')
    except ClientError as e:
        if e.response['Error']['Code'] == 'ClusterNotFound':
            print("Cluster %s already deleted!" % cluster_identifier)
            return
        else:
            print("Unexpected error wile getting cluster status: %s" % e)
            raise e

    while True:
        try:
            cluster_status = get_cluster_status(redshift, cluster_identifier)
            if cluster_status is None:
                print('Cluster is finally deleted.')
                break

        except ClientError as e:
            if e.response['Error']['Code'] == 'ClusterNotFound':
                print("Cluster %s is deleted" % cluster_identifier)
                break
            else:
                print("Unexpected error wile getting cluster status: %s" % e)
                raise e
        print('Cluster Status:', cluster_status)
        print('Sleep 10 seconds')
        time.sleep(10)


def main():
    """
    Class is used for destroying redshift cluster for given redshift configuration
    :return:
    """
    config = get_config(CONFIG_FILENAME)
    print('Deleting redshift cluster:' + config.get('CLUSTER', 'CLUSTER_IDENTIFIER'))
    destroy_cluster(config)


if __name__ == '__main__':
    main()
