from aws_cdk import (
    core,
    aws_opensearchservice as open_search,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_logs as logs,
    aws_secretsmanager as sm
)


class MyCdkAppStack(core.Stack):

    def __init__(self, scope: core.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Retrieve the secret from Secret Manager
        secret = sm.Secret.from_secret_arn(
            self,
            "OpenSearchSecret",
            "arn:aws:secretsmanager:us-east-2:691480250603:secret:MyOpenSearch-z4pZFs",
        )

        # Create IAM role for the Lambda function
        role = iam.Role(
            self,
            'LambdaExecutionRole',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            description='Role for Lambda to access OpenSearch and SecretsManager',
        )

        # Assign AWSLambdaBasicExecutionRole to the Lambda role
        role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'))

        # Assign SecretsManagerReadWrite to the Lambda role
        role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('SecretsManagerReadWrite'))

        # Assign AmazonOpenSearchServiceFullAccess to the Lambda role
        role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('AmazonOpenSearchServiceFullAccess'))

        # Allow this rle to read log events
        # Not sure if I should use AWS managed policy
        role.add_to_policy(iam.PolicyStatement(
            actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
            resources=["*"],
            effect=iam.Effect.ALLOW
        ))

        # optional parameter: engine_version（will use the latest version as default), domain_name
        domain = open_search.CfnDomain(self,
                                       "Domain",
                                       domain_name="my-domain",
                                       node_to_node_encryption_options={
                                           "enabled": True
                                       },
                                       encryption_at_rest_options={
                                           "enabled": True
                                       },
                                       ebs_options={
                                           "ebs_enabled": True,
                                           "volume_size": 10
                                       },
                                       node_group_options={
                                           "node_count": 2,
                                           "instance_type": "r6g.large.search"
                                       },
                                       advanced_security_options={
                                           "enabled": True,
                                           "internal_user_database_enabled": True,
                                           "master_user_options": {
                                               "master_user_name": "master",
                                               # Reference the password from Secrets Manager
                                               "master_user_password": secret.secret_value.to_string()
                                           }
                                       }
                                       )

        endpoint = domain.attr_domain_endpoint
        # Create the first Lambda function with the defined role
        # TODO: Create two lambda function to handle two cw log stream
        lambda_function = _lambda.Function(
            self,
            "push-log",
            role=role,
            code=_lambda.Code.from_asset("lambda/index.py"),
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler="index.lambda_handler",
            environment={
                "SECRET_NAME": secret.secret_name,
                "OPENSEARCH_ENDPOINT": endpoint
            }
        )

        # Connect CloudWatch log group/log stream with the Lambda func
        log_group_name = "/aws/parallelcluster/get-log3-202310032109"
        log_group = logs.LogGroup.from_log_group_name(self, "ExistingLogGroup", log_group_name)
        log_group.add_subscription_filter(
            "LambdaSubscriptionFilter",
            destination=_lambda.LambdaDestination(lambda_function),
            filter_pattern=logs.FilterPattern.all_events()
        )