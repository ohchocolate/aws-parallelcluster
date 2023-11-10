import os
from aws_cdk import core as cdk
from aws_cdk import aws_opensearchservice as open_search
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_iam as iam
from aws_cdk import aws_secretsmanager as sm
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_events as events


# Should use AWS CDK v2
class MyCdkAppStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # SECRETE MANAGER
        # Retrieve the secret from Secret Manager
        secret = sm.Secret.from_secret_arn(
            self,
            "OpenSearchSecret",
            "arn:aws:secretsmanager:us-east-2:691480250603:secret:MyOpenSearch-z4pZFs",
        )

        # IAM ROLE AND POLICY
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

        # Allow this role to read lambda code in a S3 bucket
        s3_bucket_name = "log-lambda-func"
        lambda_code_key = "put_log.zip"
        role.add_to_policy(iam.PolicyStatement(
            actions=["s3:GetObject"],
            resources=[f"arn:aws:s3:::{s3_bucket_name}/{lambda_code_key}"],
            effect=iam.Effect.ALLOW
        ))

        # OPEN SEARCH DOMAIN
        # optional parameter: engine_version（will use the latest version as default), domain_name
        domain = open_search.CfnDomain(
            self,
            f"{construct_id}-Domain",
            domain_name="my-domain",
            node_to_node_encryption_options=open_search.CfnDomain.NodeToNodeEncryptionOptionsProperty(enabled=True),
            encryption_at_rest_options=open_search.CfnDomain.EncryptionAtRestOptionsProperty(enabled=True),
            ebs_options=open_search.CfnDomain.EBSOptionsProperty(
                ebs_enabled=True,
                volume_size=10,
            ),
            advanced_security_options=open_search.CfnDomain.AdvancedSecurityOptionsInputProperty(
                enabled=True,
                internal_user_database_enabled=True,
                master_user_options=open_search.CfnDomain.MasterUserOptionsProperty(
                    master_user_name="master",
                    master_user_password=secret.secret_value.unsafe_unwrap(),
                )
            ),
            domain_endpoint_options=open_search.CfnDomain.DomainEndpointOptionsProperty(
                enforce_https=True,
                tls_security_policy="Policy-Min-TLS-1-2-2019-07",
            )
        )

        endpoint = domain.attr_domain_endpoint

        # LAMBDA CODE
        # Create the first Lambda function with the defined role
        lambda_function = _lambda.Function(
            self,
            "push-log",
            role=role,
            # this method does not work because the lambda code needs a module named request
            # code=_lambda.Code.from_asset(os.path.join(os.path.dirname("index.py"), "lambda")),
            code=_lambda.Code.from_bucket(bucket=s3.Bucket.from_bucket_name(self, "Bucket", "log-lambda-func"),
                                          key="put_log.zip"),
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="index.lambda_handler",
            environment={
                "SECRET_NAME": secret.secret_name,
                "OPENSEARCH_ENDPOINT": endpoint
            }
        )

