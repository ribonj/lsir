#bash.sh
# Install Hadoop in Pseudo-Distributed Mode
# Usage: ./hadoop.sh <installation-folder> <username> <version> <config-folder>

if [ $# -eq 4 ]
then

# Path configuration
# Add these paths to your .profile or .bash file
# export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_66.jdk/Contents/Home
# export HADOOP_HOME=$1/hadoop

HADOOP_HOME=$1/hadoop
HADOOP_TEMP=/tmp/hadoop-$2
#HADOOP_CONF=$4
echo $HADOOP_HOME


# Setup directories
# Only create directories if they do not exist
echo "Setup home and temporary directory..."
rm -rf $HADOOP_HOME
rm -rf $HADOOP_TEMP
#[ ! -d $1  ] && mkdir -p $1
[ ! -d $HADOOP_TEMP  ] && mkdir -p $HADOOP_TEMP
sudo chmod -R 777 $1
sudo chmod -R 777 $HADOOP_TEMP

# Download (tar does not work on mac)
echo "Download and decompress Hadoop files..."
#[ ! -d $1/hadoop-$3.tar.gz ] &&  curl -o $1/hadoop-$3.tar.gz http://www-us.apache.org/dist/hadoop/common/hadoop-$3/hadoop-$3.tar.gz
tar -zxvf $1/hadoop-$3.tar -C $1
mv $1/hadoop-$3 $1/hadoop


# Stand-alone Test
#cd $HADOOP_HOME
#mkdir input
#cp etc/hadoop/*.xml input
#bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.2.jar grep input output 'dfs[a-z.]+'
#cat output/*

# Copy configuration files
echo "Copy DFS configuration files..."
cp -fi ./core-site.xml $HADOOP_HOME/etc/hadoop
cp -fi ./hdfs-site.xml $HADOOP_HOME/etc/hadoop

# Setup passphrase-less ssh
# ssh localhost # check if one can ssh localhost without passphrase. 
# If not run the passphrase should be reinitialize.
echo "Setup SSH empty passphrase..."
status=$(ssh -o BatchMode=yes -o ConnectTimeout=5 $2@localhost echo ok 2>&1)
echo $status
if [[ $status == *"ok"* ]]
then
  echo "SSH Passphrase is already empty."
else
  echo "Set SSH Passphrase to empty."
  ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa
  cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys
  chmod 0600 ~/.ssh/authorized_keys
fi

# Install: 
#   - format name and data nodes, 
#   - start DFS and YARN
#   - copy configuration files to DFS input folder,
#   - send a MapReduce job (test)
#   - get result in output folder
#   - stop DFS and YARN
#cd $HADOOP_HOME

# Format DFS (using temporary folder)
echo "Format and start DFS..."

$HADOOP_HOME/bin/hdfs dfsadmin -safemode leave
$HADOOP_HOME/bin/hdfs dfs -rmr /user

$HADOOP_HOME/bin/hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/bin/hdfs dfs -mkdir /user
$HADOOP_HOME/bin/hdfs dfs -mkdir /user/$2

# YARN
#echo "Copy YARN configuration files and start YARN..."
#cp -fi ./mapred-site.xml $HADOOP_HOME/etc/hadoop
#cp -fi ./yarn-site.xml $HADOOP_HOME/etc/hadoop
#$HADOOP_HOME/sbin/start-yarn.sh

# Test
echo "Test: send a simple MapReduce job..."
jps
cd $HADOOP_HOME
#[ ! -d input ] && mkdir input
#[ ! -d output ] && mkdir output
bin/hdfs dfs -put etc/hadoop input
bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-$3.jar grep input output 'dfs[a-z.]+'
bin/hdfs dfs -cat output/*

# Stop YARN and DFS
echo "Stop DFS and YARN..."
#sbin/stop-yarn.sh
#sbin/stop-dfs.sh

echo "Done!"
echo "Start Hadoop using the following commands:"
echo "$HADOOP_HOME/sbin/start-dfs.sh"
echo "$HADOOP_HOME/sbin/start-yarn.sh"

else
  echo "Usage: ./hadoop.sh <installation-folder> <username> <version>"
  echo "Example: ./hadoop.sh /opt jribon 2.6.4"
fi
