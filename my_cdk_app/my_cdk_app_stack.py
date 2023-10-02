from aws_cdk import (
    core,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_secretsmanager as sm
)


class MyCdkAppStack(core.Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Retrieve the secret from Secret Manager
        secret = sm.Secret.from_secret_arn(
            self,
            "OpenSearchSecret",
            "arn:aws:secretsmanager:us-east-2:691480250603:secret:MyOpenSearch-z4pZFs",
        )

        # Create IAM role for the Lambda function
        lambda_role = iam.Role(
            self,
            'LambdaExecutionRole',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            description='Role for Lambda to access OpenSearch and SecretsManager',
        )

        # Allow lambda function to access the secret in Secret Manager
        lambda_role.add_to_policy(iam.PolicyStatement(
            actions=["secretsmanager:GetSecretValue"],
            resources=[secret.secret_arn],
            effect=iam.Effect.ALLOW
        ))

        # Allow lambda function to access OpenSearch domain
        lambda_role.add_to_policy(iam.PolicyStatement(
            actions=["es:ESHttpPost", "es:ESHttpPut"],
            resources=["arn:aws:es:us-east-2:691480250603:domain/mylogs/*"],
            effect=iam.Effect.ALLOW
        ))

        # Assign AWSLambdaBasicExecutionRole to the Lambda role
        lambda_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'))

        # Create the Lambda function with the defined role
        lambda_function = _lambda.Function(
            self,
            "LogsToElasticsearchFunction",
            role=lambda_role,
            # TODO: specify a directory or a .zip file
            code=_lambda.Code.from_asset("lambda/index.py"),
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler="index.handler",
            environment={
                "SECRET_NAME": secret.secret_name
            }
        )



