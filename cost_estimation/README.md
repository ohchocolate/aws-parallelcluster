AWS CDK App: CW Logs to OpenSearch
This CDK application automates the transfer of CloudWatch Logs, specifically those matching the pattern cwl-*, into an 
Amazon OpenSearch domain.

Key Features and Implementations:
IAM Role: Ensures secure access between services.
Lambda Function: Processes and transfers log events. For now, users will need to manually subscribe the Lambda to desired 
CloudWatch Log groups.
Amazon OpenSearch Domain: Creation of a dedicated OpenSearch domain with advanced security options.

Prerequisites:
Install Node.js
Install the AWS CDK CLI: 'npm install -g aws-cdk'
Have an active AWS account and configure your AWS credentials using AWS CLI.

Installation and Running
1. Clone the repository.
2. Install the dependencies with 'pip install -r requirements.txt'.
3. To preview the synthesized CloudFormation template, run 'cdk synth'.
4. Bootstrapping the AWS environment using cdk bootstrap 'aws://ACCOUNT-NUMBER-1/REGION-1'.
5. Deploy the application: 'cdk deploy'