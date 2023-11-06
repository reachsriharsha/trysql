import sqlalchemy
import ibis
import configparser
import logging as log
import logging.config
logging.config.fileConfig("/var/lib/vigil/logging/logging.conf")
import psycopg2
import sys
#for built in airflow dbutil, located in util (needed if running from root/shell user (not airflow))
sys.path.append("/opt/airflow/dags/util")


#Pat added for airflow.  The original mint impala database access (db_conn) will only run as root
#use the built in airflow connection
from db_util import createImpalaConnector as af_createImpalaConnector

#May 18, 2023. 
#when called from airflow the file: /ramfs/secrets/analysis/analysispgusr.cert.pem can not be read
#it can only be accessed via root user. And Airflow is not running as root


#this uses sqlalchemy, which is then needed throughout the MINT code (not using cursor to execute (MPC method))
#this is used by both Airflow triggered and root/shell triggered
def createPostgresConnector():
    #Update with try:catch so can see what errors are occuring
    conn = None
    try:
        conn_string = "postgresql://postgres.default.svc.cluster.local:5432/appstore?user=analysispgusr&sslmode=require&sslcert=/ramfs/secrets/analysis/analysispgusr.cert.pem&sslkey=/ramfs/secrets/analysis/analysispgusr.key.pem"
        conn = sqlalchemy.create_engine(conn_string)

        log.info ("Pat: createPostgresConnector the mint sqlalchemy way is done")
        return conn
    except Exception as e:
        log.error('Mint: Exception in createPostgresConnector: %s',e)
        return conn

#May 26: both Airflow and root/shell will call this. The Airflow case will cause Exception and retry 
def createImpalaConnector():
    client=None
    
    #May 30 see if trying the root/shell way from airflow (which causes exception), is causing exception (sometimes) when airflow task is complete
    #this seems to fix it, keep an eye on this over next few days..OK this has fixed it, issue not seen in couple of days
    
    client = af_createImpalaConnector(100000)   #max nbumber of rows (this is low, should revist)
    return client
    
    #this is code plan on using to allow for both root/shell and airflow
    try:
        log.info ('MINT: db_conn: Try the original createImpalaConnector. for root user. ibis version '+str(ibis.__version__))
        HDFS_PORT = 50470
        HDFS_HOST = 'hadoop-master-0.hadoop.default.svc.cluster.local'
        ibis.options.sql.default_limit = 100000  # max number of rows per query
        hdfs_client = ibis.hdfs_connect(host=HDFS_HOST, port=HDFS_PORT, auth_mechanism='GSSAPI', verify=False)
        #Pat:  try this adding .impala to see if in the path for running bash via airflow...OK this worked. Try in at command line..works for analysis user
        #but see issue with root user...try it again..OK  mintTopN_pat.py does not work with root user doing the below..
        #hdfs_client = ibis.impala.hdfs_connect(host=HDFS_HOST, port=HDFS_PORT, auth_mechanism='GSSAPI', verify=False)

        #Exception when run from airflow: ibis Version: 3.0.1 Excpetion: module 'ibis' has no attribute 'hdfs_connect'. I

        client = ibis.impala.connect(
            auth_mechanism='GSSAPI',
            database='sensorstore',
            use_ssl=True,
            ca_cert=None,
            host='hadoop-data-0.default.svc.cluster.local',
            hdfs_client=hdfs_client,
            timeout=360
        )
        client.set_options({'REQUEST_POOL': 'root.recurrent'})  
                  
        return client
    
    except Exception as e:
        
        #When called from BASH, via Airflow, the above generates this exception:
        #<14>[2023-05-25 12:33:19,097][analysis-d55b89dd7-b54r2][INFO][MainProcess][db_conn]: MINT: Exception when creating impala connection. Try it the airflow way. Excpetion: module 'ibis' has no attribute 'hdfs_connect'. If you are trying to access the 'hdfs_connect' backend, try installing it first with `pip install ibis-hdfs_connect`

        #this will be hit if command line way is done when user = analysis (what airflow uses)
        log.info ('MINT: db_conn: Exception when creating impala connection. Try it the airflow way. ibis Version: '+str(ibis.__version__)+' Excpetion: %s',e)
        #the above will fail if called from airflow (not root user), so try the airflow impala access
        client = af_createImpalaConnector(100000)   #max nbumber of rows (this is low, should revist)
        return client


