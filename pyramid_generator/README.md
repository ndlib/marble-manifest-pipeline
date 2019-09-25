
## Run the container locally.
docker build -t pyramid-generator .

### assume the role you want to run

### run the command with your aws keys and pull the buckets to run.
docker run --env AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  --env AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  --env AWS_SECURITY_TOKEN=$AWS_SECURITY_TOKEN \
  --env AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN \
  --env AWS_REGION=$AWS_REGION \
  --env-file ./dev_env \
  -it pyramid-generator



Use ECS Task as step function.
https://docs.aws.amazon.com/step-functions/latest/dg/connectors-ecs.html
  s
