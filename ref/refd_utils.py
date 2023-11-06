from partition_utils import get_rounded_epoch, getPartitionKeys
from db_conn import createImpalaConnector,createPostgresConnector
import logging as log
import logging.config
import pandas as pd
import time
logging.config.fileConfig("/var/lib/vigil/logging/logging.conf")

class refd(object):
   impala_conn=None
   refd_table=None

   def __init__(self):
     self.impala_conn = createImpalaConnector()
     self.postgres_conn = createPostgresConnector()
     self.refd_table='vw_gendev_pdprefd_norm'

     try:
       sch=self.impala_conn.get_schema(self.refd_table)
     except:
       log.error("Failed to find REFD table schema - check REFD DIG schema")
       print("Failed to find REFD table schema - check REFD DIG schema ",self.refd_table)
       exit(1)

     return

   def history_total_exist(self,tstart,tend):
     sql=f"select exists (select * from data_mint.tbl_refd_totals where tstart={tstart} and tend={tend})"
     res=self.postgres_conn.execute(sql).fetchone()[0]
     return res

   def update_history(self):
     sql="insert into data_mint.tbl_xnb_mirror_history (ipaddr,starttime,reason) select srcip as ipaddr,cast(extract(epoch from now()) as int) as starttime,source as reason from data_mint.tbl_gtpu_flows where srcip is not null and source in ('static','topn','scan','mpc')"
     self.postgres_conn.execute(sql)
# Add VIP imsi too - ultimately shoudl change schema but equally this won't break anything
     sql="insert into data_mint.tbl_xnb_mirror_history (ipaddr,starttime,reason) select imsi as ipaddr,cast(extract(epoch from now()) as int) as starttime,'vip' as reason from data_mint.tbl_vip_imsi_ip where ip_address is not null"
     self.postgres_conn.execute(sql)

     return
   
   def get_refd_totals(self,tstart,tend):
     sql=f"select * from data_mint.tbl_refd_totals where tstart>={tstart} and tend <={tend}"
     return self.postgres_conn.execute(sql).fetchall()

   def update_refd_totals(self,tstart,tend,mirror_bytes,total_bytes):

     sql=f'UPDATE data_mint.tbl_refd_totals SET total_bytes={total_bytes},mirror_bytes={mirror_bytes} WHERE tstart={tstart} and tend={tend}'
     self.postgres_conn.execute(sql)

     return

   def add_refd_totals(self,tstart,tend,mirror_bytes,total_bytes):

     sql='INSERT INTO data_mint.tbl_refd_totals (tstart,tend,total_bytes,mirror_bytes) VALUES ({0},{1},{2},{3})'.format(tstart,tend,total_bytes,mirror_bytes)
     self.postgres_conn.execute(sql)
     
     return

   def get_top_N_flows(self,start,end,topN):
#     top_order_by='pkt_s_d_sum'
     top_order_by='byte_sum'

     offset = 0
     aggintm=int((end-start)/60)

     pkeys=getPartitionKeys(start,end,'PARTITION_TYPE_HOUR',aggintm)
     refd_query="select srcip,dstip,sum(pkt_s_d_count)+sum(pkt_d_s_count) as pkt_sum,sum(bytes_s_d)+sum(bytes_d_s) as byte_sum from {1} where ((ul is not NULL or dl is not NULL)) and partitionkey in ({0}) and flow_timestamp > {4} and flow_timestamp < {5} group by srcip,dstip ORDER BY {3} DESC LIMIT {2}".format(pkeys,self.refd_table,topN,top_order_by,start,end)
# 2152/2152 Picks up S5-u interface in testing so add test to check dir is enriched, ie is known S1-U GW.

     try:
       res=self.impala_conn.sql(refd_query).execute()
     except Exception as e:
       log.error("Topn query failed: "+str(e))
       print("Topn query failed: "+str(e))
# Query failed, return empty datafram
       return pd.DataFrame([])

     return res

   def last_hr_vol_join(self,candidates_df):
     if len(candidates_df) == 0:
       return pd.DataFrame([])
     top_order_by='byte_sum'
     currtime=time.time()
     currhr=int(currtime-currtime%3600)
     tstart=currhr-3600
     tend=currhr

     offset = 0
     aggintm=int((tend-tstart)/60)

     enblist=', '.join(['\'' + item + '\'' for item in candidates_df['srcip']])

     pkeys=getPartitionKeys(tstart,tend,'PARTITION_TYPE_HOUR',aggintm)
     refd_query="select srcip,sum(pkt_s_d_count)+sum(pkt_d_s_count) as pkt_sum,sum(bytes_s_d)+sum(bytes_d_s) as byte_sum from {1} where ((ul is not NULL or dl is not NULL)) and partitionkey in ({0}) and flow_timestamp > {3} and flow_timestamp < {4} and srcip in ({5}) group by srcip ORDER BY {2} DESC".format(pkeys,self.refd_table,top_order_by,tstart,tend,enblist)
# 2152/2152 Picks up S5-u interface in testing so add test to check dir is enriched, ie is known S1-U GW.

     try:
       res=self.impala_conn.sql(refd_query).execute()
# populate 0s if no result
       for ip in candidates_df['srcip']:
         if ip not in res['srcip'].unique():
           res=res.append({'srcip': ip, 'pkt_sum': 0, 'byte_sum': 0},ignore_index=True)
     except Exception as e:
       log.error("Topn query failed: "+str(e))
       print("Topn query failed: "+str(e))
# Query failed, return empty datafram
       return pd.DataFrame([])

     return res

   def get_flows(self,start,end):

     offset = 0
     aggintm=int((end-start)/60)
     pkeys=getPartitionKeys(start,end,'PARTITION_TYPE_HOUR',aggintm)
     refd_query="select srcip,dstip,sum(pkt_s_d_count)+sum(pkt_d_s_count) as pkt_sum,sum(bytes_s_d)+sum(bytes_d_s) as byte_sum from {1} where ((ul is not NULL or dl is not NULL)) and partitionkey in ({0}) and flow_timestamp > {2} and flow_timestamp < {3} group by srcip,dstip LIMIT 100000".format(pkeys,self.refd_table,start,end)
# could make this just by srcip  - also ultimately can be a KPI set

     try:
       res=self.impala_conn.sql(refd_query).execute()
     except Exception as e:
       log.error("REFD query failed: "+str(e))
       print("REFD query failed: "+str(e))
# Query failed, return empty datafram
       return pd.DataFrame([])

     return res

   def build_mirror_ma(self,refd):
#
# Take a list of dicts of eNB/GW ips and build list of MA ops to mirror eNB IP when src and dst
# [{'srcip': '192.168.10.129', 'dstip': '192.168.10.160', 'pkt_s_d_sum': 9400119, 'byte_s_d_sum': 1996493520, 'pkt_d_s_sum': 0, 'byte_d_s_sum': 0}, .... ]
#
# Assume list has been constructed such that src_ip is always eNB

     enb_ma=[]
     for enb_gw in refd:
       enb_ma.append({'srcip': enb_gw['srcip']})
       enb_ma.append({'dstip': enb_gw['srcip']})

     return enb_ma
