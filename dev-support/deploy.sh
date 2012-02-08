#!/bin/bash
# Build and deploy an standalone instance of hbase from source
usage()
{
cat << EOF
usage: $0 [options]

Build and package hbase from the current directory. It deploys the built hbase from the child directory in local mode. This script also attempts to shutdown the existing instance, if it is already started.

options:
	-h		Show this message
	-v VERSION	Set the version of hbase being built. REQUIRED
	-r		Use this switch to test a full release. Otherwise, will expect a file built with the name 'hbase-\${VERSION}-SNAPSHOT' 
	-d DIR		Set the deployment directory. Defaults to 'deploy'         
	-s DIR		Set the data storage directory on the local file system, relative to the deployment directory. Defaults to 'data'.
	-x		Expedite process by skipping building of the tarball
	-c		Clean deploy hbase. Removes the previous deploy directory, if it exists
EOF
}

#directoy to deploy hbase from
DEPLOY="deploy"
#data directory under the deploy directory, from which to deploy
DATA="data"
#if the tarball should be built
BUILD=
#if the deployment should be clean
CLEAN=
RELEASE=

#Parse out all the options
while getopts "rchxv:s:d:" OPTION
do
     case $OPTION in
         h)
							usage
							exit 0
							;;
         s)
							DATA=$OPTARG
							;;
         d)
							DEPLOY=$OPTARG
							;;
					x) 
							BUILD=1
							;;
				  v) 
							VERSION=$OPTARG
							;;
					c) 
							CLEAN=1
							;;
					r) 
							RELEASE=1
							;;
					\?) 
						usage
						exit 1
     esac
done

set -e

#Skip building if that is disabled
if [ ! $BUILD  ]; then
	echo "Staring compilation..."
	mvn clean site install assembly:single -DskipTests
fi

if [ $CLEAN ]; then
	echo "Cleaning deploy directory..."
 rm -rf ${DEPLOY}
fi

# make sure the deploy directory exists
if [ ! -d $DEPLOY ]; then
	echo "Deploy directory doesn't exist, creating..."
	mkdir $DEPLOY
fi

echo "Copying over the built tarball"

#set the directory based on if it is a release or snapshot
if [ $RELEASE ]; then
	directory=hbase-${VERSION}
else
	directory=hbase-${VERSION}-SNAPSHOT
fi

tarball=${directory}.tar.gz
cp target/${tarball} deploy/${tarball}

# go into the deployment directory
cd deploy

echo "Untaring hbase into ${DEPLOY}..."
tar xf hbase-*.tar.gz
echo "Done untaring!"

#then go into the exploded directory
cd ${directory}
mkdir $DATA
# update the hbase-site.xml for the data directory
echo "<?xml version=\"1.0\"?>
<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>
<configuration>
  <property>
    <name>hbase.rootdir</name>
    <value>file:///`pwd`/$DATA</value>
  </property>
</configuration>" > conf/hbase-site.xml

echo "Updated hbase-site for local data directory."
HBASE_CLASSPATH=`pwd`/lib
#and then in a subshell, run the startup script
echo "Attempting to disable existing hbase instance..."
set +e
./bin/stop-hbase.sh

set -e
./bin/start-hbase.sh

exit 0
