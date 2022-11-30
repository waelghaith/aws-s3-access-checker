import boto3
import time
import logging
import argparse
import os
import uuid
from slack import WebClient
from slack.errors import SlackApiError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_buckets(excluded_buckets):
    s3 = boto3.client('s3')

    # Retrieve the list of existing buckets
    response = s3.list_buckets()
    buckets = []
    try:
        # Output the bucket names with region
        for bucket in response["Buckets"]:
            bucket_name = bucket["Name"]
            bucket_region = s3.get_bucket_location(Bucket=bucket_name)['LocationConstraint']

            if bucket_region == None:
                bucket_region = "us-east-1"
            
            bucket_info = {'Name': bucket_name, 'Region': bucket_region}
            buckets.append(bucket_info)

        if excluded_buckets:
            final_buckets = [i for i in buckets if not (i['Name'] in excluded_buckets)]
            return final_buckets

    except Exception as e:
        logger.error(f"Exception during listing S3 buckets: {str(e)}")

    return buckets

def get_regions(buckets):
    regions = []
    for bucket in buckets:
        regions.append(bucket["Region"])
    
    regions = sorted(set(regions))  
    return regions

def buckets_access_analyzer(buckets,regions):
    public_buckets = []
    analyzer_arn = ""
    for region in regions:
        try:
            analyzer_client = boto3.client('accessanalyzer',region_name=region)

            # get all active analyzers for the given account,region
            active_analyzers = [a for a in analyzer_client.list_analyzers(type="ACCOUNT").get("analyzers") if a["status"] == "ACTIVE"]
            
            if active_analyzers:
                # take the first active analyzer if there are any active analyzer
                analyzer_arn = active_analyzers[0]["arn"]
            else:
                # try to create a new analyzer if there is no analyzer already created for the account
                analyzer_name = "AccessAnalyzer-" + str(uuid.uuid1())
                analyzer_arn = analyzer_client.create_analyzer(
                    analyzerName=analyzer_name,
                    type="ACCOUNT"
                ).get("arn")

        except Exception as e:
            logger.error(f"Exception during get analyzer: {str(e)}")

        for bucket in buckets:
            if region == bucket["Region"]:
                try:
                    analyzer_client.start_resource_scan(
                            analyzerArn=analyzer_arn,
                            resourceArn=f'arn:aws:s3:::{bucket["Name"]}',
                    )

                    time.sleep(0.5)
                    logger.info(f'Start_resouce_scan for {analyzer_arn}')
                    
                    analyzer_result = analyzer_client.get_analyzed_resource(
                        analyzerArn=analyzer_arn,
                        resourceArn=f'arn:aws:s3:::{bucket["Name"]}'
                    )  

                    if analyzer_result["resource"]["isPublic"] == True:
                        public_buckets.append(bucket["Name"])

                except Exception as e:
                    logger.error(f"Exception during scanning analyzer resources {bucket['Name']}: {str(e)}")

    return public_buckets

def notifySlack(public_buckets):
    client = WebClient(token=os.getenv('SLACK_API_TOKEN'))

    list_public_buckets = " ".join([f"• <https://s3.console.aws.amazon.com/s3/buckets/{k}|{k}> \n" for k in public_buckets])[:-2]
    
    try:
        client.chat_postMessage(
            channel = os.getenv('SLACK_CHANNEL'),
            username = os.getenv('SLACK_USERNAME','AWS Lambda S3 Checker'),
            icon_emoji = os.getenv('SLACK_EMOJI',':amazon_lambda:'),
            blocks  = [
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": "Hi There :wave:"
                            }
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": " :loud_sound: *It looks like you have some public AWS S3 buckets* :loud_sound:"
                            }
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": list_public_buckets
                            }
                        },
                        {
                            "type": "divider"
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": ":pushpin: *Some resources to help you get the task done the right way* :pushpin:"
                            }
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": "*<https://docs.aws.amazon.com/AmazonS3/latest/userguide/privatelink-interface-endpoints.html#types-of-vpc-endpoints-for-s3|AWS PrivateLink>*"
                            }
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": "*<https://aws.amazon.com/premiumsupport/knowledge-center/cloudfront-serve-static-website/|AWS CloudFront with S3>*"
                            }
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": "*<https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-points.html|AWS S3 Access Points>*"
                            }
                        }
                    ]
        )
    except SlackApiError as e:
        logger.error(e.response["error"])

if __name__ == '__main__':
# def lambda_handler(event, context):
    # logger.info("Event: " + str(event))
    excluded_buckets=[]

    parser = argparse.ArgumentParser()
    parser.add_argument("--excluded_buckets","-excluded_buckets",help="Buckets to exclude as comma separated list.",required=False)
    args = parser.parse_args()
    logger.info(f"args {args}")

    if args.excluded_buckets:
        logger.info(f"Excluded buckets by argument are: {args.excluded_buckets}")
        excluded_buckets=args.excluded_buckets.split(",")

    if os.getenv('S3_EXCLUDED_BUCKETS') != None:
        logger.info(f"Excluded buckets by environment variables are: {os.getenv('S3_EXCLUDED_BUCKETS')}")
        excluded_buckets.extend(os.getenv('S3_EXCLUDED_BUCKETS').split(","))

    buckets = get_buckets(excluded_buckets)
    
    regions = get_regions(buckets)

    public_buckets = buckets_access_analyzer(buckets,regions)

    notifySlack(public_buckets)
