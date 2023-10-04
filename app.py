from aws_cdk import core

from my_cdk_app.my_cdk_app_stack import MyCdkAppStack

app = core.App()
MyCdkAppStack(app, "my_cdk_app_stack")
