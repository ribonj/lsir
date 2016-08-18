# DaSci #

### Description ###

DaSci is a system for exploring unstructured datasets, which extracts features out of raw text, maps these features into a general-purpose data model, and provides an interface based on a relation-like, tabular level of abstraction to visualize multi-dimensional data. Retrieving data, extracting information and gaining insights is achieved in real time, while visual representations can rapidly be constructed out of raw textual data.


### Requirements ###

The current version of DaSci is implemented using Python 2.7 and is based on Spark 1.6 and Flask 0.10.
Optionally, DaSci can also leverage HDFS with Hadoop 2.7.


### Installation ###

To help you install DaSci, we provided a installation shell script under install/install.sh.
Note that you should have pip installed in order to run the script.

Here are the detailed steps to follow in order to successfully install this software:


```
# Download the App
hg clone https://jribon@bitbucket.org/jribon/lsir

# Install Spark
curl -O http://mirror.switch.ch/mirror/apache/dist/spark/spark-1.6.1/spark-1.6.1-bin-hadoop2.6.tgz
tar zxvf spark-1.6.1-bin-hadoop2.6.tgz
mv spark-1.6.1-bin-hadoop2.6 spark

# Copy the configuration files that are provided under folder install/spark
cp lsir/install/spark/* spark/conf/

# Install the App
# install virtual environment (venv) and its dependencies if not installed yet and activate it.
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


# Run application
python run.py
```

### Running the App ###

To start the application, first, go to the folder where you installed DaSci.
Then load the vistual environment and, finally, run the app using the following commands:

```
. venv/bin/activate
python run.py
```


Enjoy!


### Question? ###

If you have any question regarding DaSci, please, contact the software developer at the following email adresse <julien.ribon@epfl.ch> or the LSIR lab http://lsir.epfl.ch/contact/address/.