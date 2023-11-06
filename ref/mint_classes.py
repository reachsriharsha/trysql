
import sqlalchemy
import logging as log
import logging.config
import configparser   #needed in readDBConfig
import psycopg2  #needed in dbConnect

#updated 10/18/2022
#update 10/28/2022 - copied current mint_classes_pat.py as mint_classes.py (in the mintapp folder). 
# It is from mintapp where master copy should be,and where deployment is from. mint_classes_pat.py is used for testing independantly of mintapp

#Change Visual Studo Code to use LF instead of CRLF

#This class will be instantiated when want to read in config type data from Postgres
#the class vars will be set with the values from the various Postgres tables
#for now all config related tables are read

#May 26, 2023
#To do - keep the postgres connection info for reuse - but have to use the sqlalcmeny creation method since MINT app codes needs this

class mint_config(object):

    #the users of this data will access the below fields. These are set in this class

    #this is the PDP to connect to (only one for now)
    pdp_app_name=None
    table_name_for_refd_device=None     #hardcoded in GUI and device creation YAML, stored in database
    pdp_host=None
    pdp_port=0
    pdp_host_port=None  #combine the above with a : in between
    pdp_user_name=None # not used
    pdp_user_password=None # not used

    #this is mostly Network Analytics related config.
    mint_topn_count=None
    mint_topn_period=None

    mint_period=1 # TODO make configurable

    mint_stats_interval=5 # write stats every N mins

    # DCE limits TODO make configurable and per PDP/DCE of course
#    dcepktlim=3507388 # pkts/sec
#    dcebytelim=2540035667 # bytes/sec (not bits)
# test:

    #June 8, 2023: the below 3 should be in config somewhere
    dcepktlim=35073 # pkts/sec
    dcebytelim=25400356 # bytes/sec (not bits)

    # Scan config
    scan_cand_limit=60 # max to scan per period


    scan_period=None # set equal to TopN period later
    tbl_algo2_scan_st='tbl_enb_algo2_scan'
    tbl_enb_scan_state_st='data_mint.tbl_enb_scan_state'

# MPC NB config
    tbl_algo3_mpc_nb_st='tbl_enb_algo3_mpc'
    mpc_cand_limit=1000
    mpc_period=60 # set equal to TopN period later

    CFG_gtpu_flow_tab='data_mint.tbl_gtpu_flows'

    #store full config (may be used in future)
    mint_config_table=None

    #postgres database connection. Will be set up each time a DAG task executes
    conn=None
    cur=None

    # Kill switch. 'filter' = normal operation, 'all' = mirror all S1-U, 'none'=mirror no S1-U.
    kill_switch=None

    '''
    DB Information read from the Postgres ini file
    '''
    def readDBConfig(self,key):

        #copied from connection_dao.py
        dbFileName = '/var/lib/vigil/db.driver.properties'

        cfg = configparser.ConfigParser()
        try:
            cfg.read(dbFileName)
        except IOError:
            return False
        try:
            value = cfg.get('this_app',key)
        except:
            log.error('Unable to read DB Config file.')
            return False
        return value


    def dbConnect(self):
        conn = None
        try:
            #May 19, 2023: Need to use the Airflow connection string, dont have access to the other
            #value = self.readDBConfig('py.pg.dbconn.url')
            value = self.readDBConfig('airflow.pg.dbconn.url')
            #May 22: MINT code assumes sqlalchemy is used...try it out (was using this way for MINT, but now saving and sharing Postgres connection)
            #so can not share this connection for now, only used in mint_config
            conn=psycopg2.connect(value)

            #log.info('MINT: dbConnect.....value...will this get printed')

            return conn

        except Exception as error:
            log.error('Unable to connect to the database.'+str(error))

        return conn


    #close the data base resources
    def close_db_resources(self):
        try:
            if self.conn is not None:
                self.conn.close()
            if self.cur is not None:
                self.cur.close()
        except Exception as e:
            log.error("MINT Classes, Error Closing Database Resources")


    def read_device_config(self):
    #read in the P4 device configuration

        #alldevices is array that will contain p4_device_details (one row of postgres table)
        alldevices = []
        device_to_return=None
        cur=self.cur

        try:

            sql= "select * from config_mint.tbl_p4_devices"
            log.debug(sql)
            cur.execute(sql)

            #for this test just get the first P4 device
            #there will only be one of them for a while

            #fetchall returns a list of all the records. So get each column column data one at a time
            for device_name, ip_address, port, user_name, password, status, sw_version in cur.fetchall():
                log.debug("device_name: {0}, ip_address: {1}, port: {2}, user_name: {3}, password: {4}, sw_version: {5}"\
                        .format(device_name, ip_address, port, user_name, password, password, sw_version))

                #onedevice = P4DeviceDetails(device_name, ip_address, port, user_name, password, status, sw_version)
                onedevice= {'device_name': device_name, 'ip_address': ip_address, 'port': port, 'user_name': user_name, 'password':password,'status': status, 'sw_version': sw_version}

                #add each device to list that will be returned
                alldevices.append(onedevice)

            #for now, we will send back on the first configured device. In future will have more
            #handle case where there are no devices, dont throw exception
            if (len(alldevices)==0):
                return False, device_to_return

            device_to_return=alldevices[0]

        except Exception as e:
            log.debug('Exception: in read_device_config %s',e)
            return False, device_to_return


        #if get here, all is good, have not excepted out. Have set device_to_return
        return True, device_to_return

    def read_na_config_from_json(self):
    #read in the config from Postgres.  It is stored in Postgres as a jsonb type

        config_to_return=None
        cur=self.cur

        try:

            #list all the table rows

            sql= "select * from config_mint.tbl_na_config"
            log.debug(sql)
            cur.execute(sql)

            #fetchall returns a list of all the records. So get each column column data one at a time
            #there should only be one record for now, so use fetchone
            onerecord = cur.fetchone()

            #the above should return a list of one, that has one column (details)

            #we can read this directly, no conversion to str and back is needed (got that working, though)
            #need to import json if start using
            #in rare case they may be no data in table, return False, defaults are set
            if (len(onerecord) == 0):
                config_to_return=""
                return False, config_to_return
            
            config_to_return = onerecord[0]

        except Exception as e:
            log.debug('Exception: read_na_config_from_json %s',e)
            return False, config_to_return

        #if get here, all is good, have not excepted out.
        return True, config_to_return


    def __init__(self):

        #May 22, adding 'where_launched' so can be launched from Airflow and command line
        #this is needed because in some parts of code need to do different things if launched from Airflow
        #If we move to always from Airflow, then will not need this

        #Also save off the Postgres connection info. It is needed / set when we read in config data.
        #no need to recreate for the particular apps (Scan, TopN, etc.)

        #May 26 - removing the where_launched - will handled database connection issues in db_conn 
        #OLD:  def __init__(self,where_launched):


        #these values were hardcoded for testing..
#       self.pdp_app_name='ra-swag-client'
#       self.pdp_host_port='10.228.64.90:8000'

        #put some default values here so when checking there will always be something, not None type
        self.mint_topn_count=2000
        self.mint_topn_period=50
        self.pdp_app_name='ra-swag-client'
        self.table_name_for_refd_device='tbl_gendev_pdprefd'

        self.pdp_host=""
        self.pdp_port=0
        self.pdp_host_port=""
        self.pdp_user_name=""
        self.pdp_user_password=""
        self.using_airflow=False
        self.conn=None
        self.cur=None


        try:

            self.conn = self.dbConnect()
            self.cur = self.conn.cursor()

            #log.info ("Pat: mint_classes...about to call read_na...")
            #read the one off config + network analytics application JSON config data
            result, app_config = self.read_na_config_from_json()

            if (result==True):
                #print(app_config)
                if 'topn' in app_config.keys():
                  self.mint_topn_count=app_config['topn']['sample_count']
                  self.mint_topn_period=app_config['topn']['period']
                  self.scan_period=self.mint_topn_period    # scan window mins
                else:
                  self.mint_topn_count=10
                  self.mint_topn_period=15

                if 'pdp_application_name' in app_config.keys():
                  self.pdp_app_name=app_config['pdp_application_name']
                else:
                  self.pdp_app_name = 'ra-mint-client'

                if 'table_name_for_refd_device' in app_config.keys():
                  self.table_name_for_refd_device=app_config['table_name_for_refd_device']
                else:
                  self.table_name_for_refd_device='tbl_gendev_pdprefd'

                #June 9, 2023: Add in dd in dce_packet_limit, dce_byte_limit, scan_candiate_limit, Set in NodeRED
                if 'dce_packet_limit' in app_config.keys():
                    self.dcepktlim = app_config['dce_packet_limit']
                    log.info ("Pat: dce_packet_limit is read from DB: "+str(self.dcepktlim))
                else:
                    #set to default
                    self.dcepktlim = 35073
                
                if 'dce_byte_limit' in app_config.keys():
                    self.dcebytelim = app_config['dce_byte_limit']
                    log.info ("Pat: dce_byte_limit is read from DB: "+str(self.dcebytelim))
                else:
                    #set to default
                    self.dcebytelim = 25400356       

                if 'scan_candidate_limit' in app_config.keys():
                    self.scan_cand_limit = app_config['scan_candidate_limit']
                    log.info ("Pat: scan candidate_limit is read from DB: "+str(self.scan_cand_limit))
                else:
                    #set to default
                    self.scan_cand_limit = 60      
                       
                if 'mint_enabled' in app_config.keys():
                    if (app_config['mint_enabled'] == True):
                      self.kill_switch='filter'
                    else:
                      self.kill_switch='all'
                    log.info ("Kill switch "+str(self.kill_switch))
                else:
                    self.kill_switch='filter'
                    log.info ("Kill switch default "+str(self.kill_switch))

                if 'stopall' in app_config.keys():
                    if app_config['stopall'] == True:
                      self.kill_switch='stop'
                      log.info ("Stop all set, all traffic mirrors stopped")

            #read in the P4 device configuration details...only 1 allowed for now, enforced by GUI
            result, device_config = self.read_device_config()


            if (result==True):
            #   print(device_config)
                self.pdp_host=device_config['ip_address']
                self.pdp_port=device_config['port']
                self.pdp_host_port=self.pdp_host+":"+str(self.pdp_port)   #combine the above with a : in between - port is an int from DB.
                self.pdp_user_name=device_config['user_name']
                self.pdp_user_password=device_config['password']
                log.info ("MINT: Config successfully read from Postgres and set in vars")

        except Exception as e:
            log.error('MINT Exception: in init config_access: %s',e)

        finally:
            #this gets called even if return is in except block
            #If decide to keep the Postgres connection for later use, dont close it here. But for now close it
            self.close_db_resources()
            #log.info ("Pat: hit finally...closing mint_classes postgres db resources.....")
