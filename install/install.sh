
# Init
sudo mkdir /opt
sudo chmod 777 /opt
cd /opt
curl -O https://bootstrap.pypa.io/get-pip.py
sudo python get-pip.py

# Download App
hg clone https://jribon@bitbucket.org/jribon/lsir

# Download Spark
curl -O http://mirror.switch.ch/mirror/apache/dist/spark/spark-1.6.1/spark-1.6.1-bin-hadoop2.6.tgz
tar zxvf spark-1.6.1-bin-hadoop2.6.tgz
mv spark-1.6.1-bin-hadoop2.6 spark

# Copy configuration files
cp lsir/install/spark/* spark/conf/

# Install App
# install virtual environment and its dependencies
cd lsir/
sudo pip install virtualenv
rm -r venv
virtualenv venv
. venv/bin/activate
pip install Flask
pip install numpy
pip install nltk

# Configure Python Path for PySpark
# Add $SPARK_HOME/python/build to PYTHONPATH
export SPARK_HOME=/opt/spark
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.9-src.zip:$PYTHONPATH

# Run application:
python run.py
