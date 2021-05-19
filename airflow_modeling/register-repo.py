import boto3
import requests
from requests_aws4auth import AWS4Auth

host = 'https://search-netscaler-wjqgq7w5ndl5svi5ma573dkw4u.ap-northeast-2.es.amazonaws.com/'
region = 'ap-northeast-2'  # For example, us-west-1
service = 'es'
credentials = boto3.Session(profile_name='netscaler-e2e').get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

# Register repository
path = '_snapshot/my-snapshot-repo'  # the Elasticsearch API endpoint
url = host + path

payload = {
    "type": "s3",
    "settings": {
        "bucket": "elastic-upgrade-backup-kr",
        "region": "ap-notheast-2",
        "role_arn": "arn:aws:iam::YOUR-ACCOUNT-ID:role/my-manual-es-snap-creator-role"
    }
}

headers = {"Content-Type": "application/json"}

r = requests.put(url, auth=awsauth, json=payload, headers=headers)

print(r.status_code)
print(r.text)
