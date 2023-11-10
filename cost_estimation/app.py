from aws_cdk import core

from simple_cdk_app_stack import SimpleCdkAppStack
# from my_cdk_app_stack import MyCdkAppStack

app = core.App()
SimpleCdkAppStack(app, "SimpleCdkAppStack")
# MyCdkAppStack(app, "MyCdkAppStack")
app.synth()
