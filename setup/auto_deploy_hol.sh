#!/bin/sh

docker_user=$1
cde_user=$2
max_participants=$3
storage_location=$4

echo "CDE BNK HOL DEPLOYMENT INITIATED...."
echo "..."
echo ".."
echo "."
echo "Provided Docker User: "$docker_user
echo "Provided CDE User: "$cde_user

#CREATE DOCKER RUNTIME RESOURCE
echo "Create CDE Credential docker-creds-"$cde_user"-mkt-hol"
cde credential create --name docker-creds-$cde_user"-mkt-hol" --type docker-basic --docker-server hub.docker.com --docker-username $docker_user
echo "Create CDE Docker Runtime dex-spark-runtime-"$cde_user
cde resource create --name dex-spark-runtime-$cde_user --image pauldefusco/dex-spark-runtime-3.2.3-7.2.15.8:1.20.0-b15-great-expectations-data-quality --image-engine spark3 --type custom-runtime-image

# CREATE FILE RESOURCE
echo "Create Resource mkt-hol-setup-"$cde_user
cde resource create --name mkt-hol-setup-$cde_user
echo "Upload utils.py to mkt-hol-setup-"$cde_user
cde resource upload --name mkt-hol-setup-$cde_user --local-path utils.py
echo "Upload setup.py to mkt-hol-setup-"$cde_user
cde resource upload --name mkt-hol-setup-$cde_user --local-path setup.py

# CREATE SETUP JOB
echo "Create job mkt-hol-setup-"$cde_user
cde job create --name mkt-hol-setup-$cde_user --type spark --mount-1-resource mkt-hol-setup-$cde_user --application-file setup.py --runtime-image-resource-name dex-spark-runtime-$cde_user
echo "Run job mkt-hol-setup-"$cde_user
cde job run --name mkt-hol-setup-$cde_user --arg $max_participants --arg $storage_location

echo " "
echo "."
echo ".."
echo "..."
echo ".... CDE MKT HOL DEPLOYMENT IN PROGRESS"
echo ".... CHECK CDE UI TO CONFIRM SUCCESSFUL DEPLOYMENT"
