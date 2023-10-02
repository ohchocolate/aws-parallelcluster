from aws_cdk import core, aws_lambda, aws_logs, aws_elasticsearch as aes, aws_lambda_event_sources


class PushToOpenSearchStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        domain = aes.Domain(
            self, "mylogs",
            version=aes.ElasticsearchVersion.V7_4,
            node_count=1,
            removal_policy=core.RemovalPolicy.DESTROY
        )

        log_pusher = aws_lambda.Function(
            self, 'LogPusher',
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            handler='handler.handle',
            code=aws_lambda.Code.from_asset('lambda'),
        )

        domain.grant_write(log_pusher)

        log_group = aws_logs.LogGroup(
            self, "MyLogGroup",
            log_group_name="/aws/lambda/my-function",
            removal_policy=core.RemovalPolicy.DESTROY,
        )

        log_pusher.add_event_source(
            aws_lambda_event_sources.CloudWatchLogEventSource(
                log_group=log_group,
                filter_pattern=aws_logs.FilterPattern.all_events()
            )
        )


app = core.App()
PushToOpenSearchStack(app, "PushToOpenSearchStack")
