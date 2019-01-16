
# this is just an example it may or may not work...

# app infrastructure
aws cloudformation deploy \
  --capabilities CAPABILITY_IAM \
  --template-file deploy/cloudformation/app-infrastructure.yml \
  --stack-name mellon-app-infrastructure \
  --tags ProjectName=mellon Name='testaccount-mellonappinfrastructure-dev' Contact='me@myhost.com' Owner='myid'\
  Description='brief-description-of-purpose'\
  --parameter-overrides NetworkStackName='unpeered-network'

# data broker
aws cloudformation deploy \
  --stack-name mellon-data-broker-dev \
  --template-file deploy/cloudformation/data-broker.yml \
  --tags ProjectName=mellon Name='testaccount-mellondatabroker-dev' Contact='me@myhost.com' Owner='myid'\
  Description='brief-description-of-purpose'

# image server dev
aws cloudformation deploy \
  --capabilities CAPABILITY_IAM \
  --stack-name mellon-image-service-dev \
  --template-file deploy/cloudformation/iiif-service.yml \
  --tags ProjectName=mellon NameTag='testaccount-mellonimageservice-dev' \
    ContactTag='me@myhost.com' OwnerTag='myid' \
    Description='brief-description-of-purpose' \
  --parameter-overrides ContainerCpu='1024' ContainerMemory='2048' DesiredCount=1 NetworkStackName='unpeered-network'

# image server test
aws cloudformation deploy \
  --capabilities CAPABILITY_IAM \
  --stack-name mellon-image-service-test \
  --template-file deploy/cloudformation/iiif-service.yml \
  --tags ProjectName=mellon NameTag='testaccount-mellonimageservice-dev' \
    ContactTag='me@myhost.com' OwnerTag='myid' \
    Description='brief-description-of-purpose' \
  --parameter-overrides ContainerCpu='1024' ContainerMemory='2048' DesiredCount=1 NetworkStackName='unpeered-network'


#deploy pipeline
aws cloudformation deploy \
  --capabilities CAPABILITY_IAM \
  --stack-name mellon-image-service-pipeline \
  --template-file deploy/cloudformation/iiif-service-pipeline.yml \
  --tags ProjectName=mellon Name='testaccount-mellonimageservicepipeline' \
    Contact='me@myhost.com' Owner='myid' \
    Description='brief-description-of-purpose' \
--parameter-overrides OAuth='48a56cc7213d65533e90c1ead908e8f0ffb0396a' Approvers=jhartzle@nd.edu IIIFProdServiceStackName='mellon-image-service-dev' SourceRepoName='https://github.com/ndlib/image-server' SourceRepoOwner='ndlib'
