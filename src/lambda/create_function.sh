# template for create a lambda function
# the deployment package is a zip file containing the actual
# lambda function and corresponding dependencies.
aws lambda create-function \
--region us-east-1 \
--function-name lambda_check_api_status \
--zip-file fileb://deployment-package.zip \
--role arn:aws:iam::account-id:role/lambda_basic_execution  \
--handler checkApi/lambda_function.lambda_handler \
--runtime python3.6 \
--timeout 60 \
--memory-size 512
