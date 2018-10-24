# using aws cron expression,
# run target at 11a.m. (UTC time zone) daily
aws events put-rule \
--schedule-expression "cron(0 11 * * ? *)" \
--name RunDaily

aws events put-targets \
--rule RunDaily \
--targets "Id"="1",
          "arn:aws:lambda:us-east-1:257685430371:function:lambda_check_api_status"
