#!/bin/bash 
# 
#  marble_primo_upload.sh
#
#  This script runs regularly to process files in AWS S3 for the Marble application. The goal is to move files
#  from S3 up to the hosted Primo server in a location where they will be processed and added to our Primo data.
#
#  There were a couple of concerns we had when putting this script together:
#    - When processing the files in the bucket, there is the possibility of new files being added while this 
#      script is running. This concern was addressed by first copying current files to a working folder and 
#      only working on those copied files.
#    - In order to make sure we only work with files which have then been copied to the working folder, each
#      file is processed separately.
#
#  This is the workflow of this script:
#
#     - Sync process folder with a working folder
#     - Get list of files from the working folder
#     - Iterate through xml files
#       - mv xml files immediately from process -> finished
#       - Remove same file from working
#     - Iterate through gz files
#       - copy file to Aleph server
#       - sftp file on up to Ex Libris server
#       - remove file from Aleph server
#       - mv file from index/process to index/finished
#       - remove file from index/working
#
#  NOTE: This script should be run as user "marble" on server peter.library.nd.edu. That server and user are set up
#        with AWS and SSH keys and access required to both pull from the S3 bucket and copy files up to the Ex
#        Libris server.
#
#  05/22/19  Create script based on requirements from Steve Mattison    T. Hanstra
#
# VARIABLES
# Set the name of the Marble bucket
#
datestamp=`date +%Y%m%d`
marble_bucket=marble-manifest-prod-processbucket-kskqchthxshg
#
# Sync the folders
#
/usr/local/bin/aws s3 sync s3://$marble_bucket/index/process/ s3://$marble_bucket/index/working/
#
# Create the full file list
#
/usr/local/bin/aws s3 ls s3://$marble_bucket/index/working/ | /bin/cut -c 32-100 | /bin/grep '\.xml$' | /bin/sed '/^$/d' > /tmp/marble_working_xml
#
# Process the .xml files. These are 
#   - moved from index/process to index/finished 
#   - removed from working.
#
for file in `/bin/cat /tmp/marble_working_xml`
  do
    /usr/local/bin/aws s3 mv s3://$marble_bucket/index/process/$file s3://$marble_bucket/index/finished/$file
    /usr/local/bin/aws s3 rm s3://$marble_bucket/index/working/$file
  done
#
# Create the tar.gz list
#
/usr/local/bin/aws s3 ls s3://$marble_bucket/index/working/ | /bin/cut -c 32-100 | /bin/grep '\.tar.gz$' | /bin/sed '/^$/d' > /tmp/marble_working_tar
#
# Process the tar.gz files. These are:
#    - Copied over to aleph_tmp/primodata folder
#    - Copied up to Ex Libris folder
#    - Copied to the index/finished folder
#    - Removed from index/process folder
#    - Removed from the working folder
#
for file in `/bin/cat /tmp/marble_working_tar`
  do
    echo 'Processing: '$file
    /usr/local/bin/aws s3 cp s3://$marble_bucket/index/process/$file /aleph1_tmp/primodata/sandbox/marble
    find /aleph1_tmp/primodata/sandbox/marble -name $file -exec scp -p -P 10022 {} sftp_ndu@ndu-primo.hosted.exlibrisgroup.com:primodata/sandbox/marble \; -exec sleep 3 \;
    /usr/local/bin/aws s3 cp s3://$marble_bucket/index/process/$file s3://$marble_bucket/index/finished/$file
    /usr/local/bin/aws s3 rm s3://$marble_bucket/index/process/$file
    /usr/local/bin/aws s3 rm s3://$marble_bucket/index/working/$file
  done
#
# Check that all files got up to Ex Libris - If any missing, output list
#
/bin/ssh sftp_ndu@ndu-primo.hosted.exlibrisgroup.com -p 10022 ls primodata/sandbox/marble | sed '/^$/d' > /tmp/uploaded
for file in `/bin/cat /tmp/marble_working_tar`
  do
    /bin/grep $file /tmp/uploaded
    check_status=$?
    if [ $check_status -ne 0 ]; then
      echo 'File '$file' has not been uploaded to Ex Libris as expected on '$datestamp >&2
    fi
    /bin/rm /aleph1_tmp/primodata/sandbox/marble/$file
  done
#
# Clean up
#
rm /tmp/marble_working_xml
rm /tmp/marble_working_tar
rm /tmp/uploaded
