from csv import writer
from venv import create
from pymongo import MongoClient
from bson.objectid import ObjectId
import dns
import pprint
import datetime
import pandas as pd
from enum import Enum
from google.cloud import storage
import numpy as np
import re
import os
from dateutil import parser 
import json
from xlsxwriter.utility import xl_rowcol_to_cell
from datetime import timedelta, datetime, date

import google.auth.credentials
from google.oauth2 import service_account
import base64
import time

class RepEntity:

    def __init__(
        self,
        rep_id: ObjectId,
    ):
        self.__mongo_collection = 'users'
        self.__query = { "_id": rep_id}
        self.__db = self._connect_to_mongo()
        self.__rep_name = self._get_rep(self.__db[self.__mongo_collection],self.__query)

    def _connect_to_mongo(self):
        prod_client = MongoClient("mongodb+srv://meftest:A1exander!@webdirectcluster.m2ccb.gcp.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
        db = prod_client.production
        return db

    def _get_rep(self,mongo_collection, query):
      try:
        x = mongo_collection.find_one(self.__query)
        rep_name = f"{x['data']['info']['firstName']} {x['data']['info']['lastName']}"
      except:
        rep_name = "No Representative"
      return rep_name

    def rep_name(self):
      return self.__rep_name

class DealData:

    def __init__(
        self,
        start_date: datetime=None,
        end_date: datetime=None,
        date_type: str='signed',
        fm_id: str=None,
    ):
        self.__fm_id = fm_id
        self.__start_date = start_date
        self.__end_date = end_date
        self.__df =  pd.DataFrame()
        self.__mongo_collection = 'deals_view'
        self.__mongo_query = None
        self.__db = None
        self.__date_type = date_type

        if start_date == None:
          self.__start_date =  datetime(2021, 7, 1, 12)
        
        if end_date == None:
          self.__end_date = datetime.now()

        pd.set_option('display.float_format', lambda x: '%.2f' % x)  # Not sure if this works if you put in a class since it usually global
        return

    def _connect_to_mongo(self):
        prod_client = MongoClient("mongodb+srv://meftest:A1exander!@webdirectcluster.m2ccb.gcp.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
        db = prod_client.production
        return db


            # self.__mongo_query = { '$or': [
            #                       { "data.info.statusHistory" : { '$elemMatch': { '$or' : 
            #                         [
            #                         {"status": "delivered","date": { '$gte' : self.__start_date.isoformat(), '$lte': self.__end_date.isoformat() } },
            #                         {"status": "signed","date": { '$gte' : self.__start_date.isoformat(), '$lte': self.__end_date.isoformat() } },
            #                         {"status": "canceled","date": { '$gte' : self.__start_date.isoformat(), '$lte': self.__end_date.isoformat() } }
            #                         ] }}
            #                       },
            #                       {'data.info.dealDates.fundedAt' : { '$gte' : self.__start_date.isoformat(), '$lte': self.__end_date.isoformat() }}
            #                     ]}
    @property
    def __query(self):
      if self.__mongo_query == None:
        if self.__fm_id == None:
          if self.__date_type == 'signed':
           self.__mongo_query = { "data.info.statusHistory" : { '$elemMatch': {"status": "signed",
                                  "$or":  [{'date' : { '$gte' : self.__start_date, '$lte': self.__end_date }},
                                            {'date':  { '$gte' : self.__start_date.isoformat(), '$lte': self.__end_date.isoformat() }} ] } }} 

          elif self.__date_type == 'delivered':
            self.__mongo_query = { "data.info.statusHistory" : { '$elemMatch': {"status": "delivered",
                                  "$or":  [{'date' : { '$gte' : self.__start_date, '$lte': self.__end_date }},
                                            {'date':  { '$gte' : self.__start_date.isoformat(), '$lte': self.__end_date.isoformat() }} ] } }} 

           
                          
        # else:
        #   self.__mongo_query = { "$and": [{ 'data.user._id' : ObjectId(self.__fm_id) },
        #                                   { '$or': [
        #                                             { "data.info.statusHistory" : { '$elemMatch': { '$or' : 
        #                                               [
        #                                               {"status": "delivered","date": { '$gte' : self.__start_date.isoformat(), '$lte': self.__end_date.isoformat() } },
        #                                               {"status": "signed","date": { '$gte' : self.__start_date.isoformat(), '$lte': self.__end_date.isoformat() } },
        #                                               {"status": "canceled","date": { '$gte' : self.__start_date.isoformat(), '$lte': self.__end_date.isoformat() } }
        #                                               ] }}
        #                                             },
        #                                             {'data.info.dealDates.fundedAt' : { '$gte' : self.__start_date.isoformat(), '$lte': self.__end_date.isoformat() }}
        #                                           ]}

        #                         ]} 
                                
      return self.__mongo_query

    @property 
    def start_date(self):
      return self.__start_date

    @property 
    def end_date(self):
      return self.__end_date

    @staticmethod
    def first_date(status_history_list,status='signed',just_date=True):

      first_date_in_history = None
      num_times = 0
      date_list = []

      try:
        first_date_in_history = None
        for status_history in status_history_list:
          try:
            if status_history['status'] == status:
              if type(status_history['date']) == str:
                status_history['date'] =  datetime.strptime(status_history['date'],'%Y-%m-%dT%H:%M:%S.%fZ')

              if first_date_in_history == None:
                first_date_in_history = status_history['date']
                num_times += 1
                date_list.append(first_date_in_history.date())
              elif first_date_in_history > status_history['date']:
                first_date_in_history = status_history['date']
                num_times += 1
                date_list.append(first_date_in_history.date())
              else:
                num_times += 1
                date_list.append(first_date_in_history.date())
          except:
            pass
      except:
        pass

      if not first_date_in_history == None:
        try: 
          first_date_in_history = first_date_in_history.date()
        except:
          if type(first_date_in_history) == str:
            print(f'Bad history date, date is str {first_date_in_history}, status looked for is {status}')
            first_date_in_history = datetime.strptime(first_date_in_history,'%Y-%m-%dT%H:%M:%S.%fZ').date()

      if just_date:
        return first_date_in_history
      else:
        return first_date_in_history,num_times,date_list

    def _get_df_from_mongo(self):
      if not self.__db:
        self.__db = self._connect_to_mongo()
        #avgTime: { $avg: ["$maxTime", "$minTime"] } saving for exmple
        pipeline = [
            {'$match' : self.__query},
            {"$unwind": {'path': '$data.data', 'preserveNullAndEmptyArrays': True}},
            { '$project': { '_id"' : 1, 
                            'data.info.status': 1, 
                            'data.info.statusHistory' : 1,
                            'data.info.type': 1,
                            'data.info.dealDates.fundedAt' : 1,
                            'data.dealership.data.info.name': 1, 
                            'data.dealership.data.representativeId': 1, 
                            'data.dealership.data.representative.data.info.firstName': 1, 'data.dealership.data.representative.data.info.lastName': 1,
                            'Representative' : { "$concat" : ['$data.dealership.data.representative.data.info.firstName', ' ', '$data.dealership.data.representative.data.info.lastName']},
                            'data.user._id' : 1,
                            'data.user.data.info.firstName' : 1, 'data.user.data.info.lastName' : 1,  
                            'Finance Manager' : { "$concat" : [ '$data.user.data.info.firstName', ' ', '$data.user.data.info.lastName']},
                            'data.info.vehicle.VIN' : 1, 
                            'data.info.vehicle.year' : 1, 'data.info.vehicle.make' : 1, 'data.info.vehicle.model' : 1,
                            'Vehicle' : { "$concat" : [ { "$toString" : '$data.info.vehicle.year'}, ' ', '$data.info.vehicle.make', ' ', '$data.info.vehicle.model']},
                            'data.info.refNumber' : 1, 
                            "data.applicant.data.info.firstName" : 1, "data.applicant.data.info.middleName" : 1, "data.applicant.data.info.lastName" : 1,
                            "data.lender.data.info.name" : 1, 
                            'data.info.payment.dealTotal' : 1, #amount financed
                            # The following is the deal info from the "blue Box"
                            'data.info.profit.managerProfit.commissionableAmount' : 1,
                            'data.info.profit.managerProfit.commission' : 1,
                            'data.info.profit.wfdProfit.reserveCommission' : 1, 'data.info.profit.wfdProfit.extraReserveProfit' : 1, #extrareserveprofit === additional reserve
                            'data.info.profit.dealershipProfit.reserveCommission' : 1,
                            "blue_totalReserve" : { "$sum" : ['$data.info.profit.wfdProfit.reserveCommission', '$data.info.profit.totalGAPProfit',
                                                         '$data.info.profit.totalServiceWarrantyProfit']},
                            "blue_wfdReserve" : { "$sum" : ['$data.info.profit.wfdProfit.splitFromDeal', '$data.info.profit.wfdProfit.splitTotalFromGap',
                                                         '$data.info.profit.wfdProfit.splitTotalFromServiceWarranty']},    
                            'data.info.profit.wfdProfit.extraReserveProfit' : 1, # additional reserve
                            'data.info.profit.wfdProfit.extraServiceWarrantyProfit' : 1, #Warranty 
                            'data.info.profit.wfdProfit.extraGAPProfit' : 1, #Gap
                            'data.info.profit.wfdProfit.totalProfit' : 1,
                            # The Following is deal info from the green box
                            'data.info.accounting.profit.managerProfit.commissionableAmount' : 1,
                            'data.info.accounting.profit.managerProfit.commission' : 1,
                            'data.info.accounting.profit.wfdProfit.reserveCommission' : 1, 'data.info.accounting.profit.wfdProfit.extraReserveProfit' : 1, #extrareserveprofit === additional reserve
                            'data.info.accounting.profit.dealershipProfit.reserveCommission' : 1,
                            "acct_totalReserve" : { "$sum" : ['$data.info.accounting.profit.wfdProfit.reserveCommission', '$data.info.accounting.profit.totalGAPProfit',
                                                         '$data.info.accounting.profit.totalServiceWarrantyProfit']},
                            "acct_wfdReserve" : { "$sum" : ['$data.info.accounting.profit.wfdProfit.splitFromDeal', '$data.info.accounting.profit.wfdProfit.splitTotalFromGap',
                                                         '$data.info.accounting.profit.wfdProfit.splitTotalFromServiceWarranty']}, 
                            'data.info.accounting.profit.wfdProfit.extraReserveProfit' : 1, # additional reserve
                            'data.info.accounting.profit.wfdProfit.extraServiceWarrantyProfit' : 1, #Warranty 
                            'data.info.accounting.profit.wfdProfit.extraGAPProfit' : 1, #Gap
                            'data.info.accounting.profit.wfdProfit.totalProfit' : 1                           
                            }},
      ]
      query_result = self.__db[self.__mongo_collection].aggregate(pipeline)
      query_result = list(query_result)
      df = pd.json_normalize(query_result)

      return df

    @property
    def df(self):
      try: 
        if self.__df.empty:
          self.__df = self._get_df_from_mongo()
      except:
        pass

      return self.__df


    def get_df(self):
      return self.__df

    def _move_output_to_cloud_storage(self,local_path,remote_bucket_name):

        with open('/etc/secrets/primary/latest') as source:
          json_acct_info = json.load(source)
          print(f'json_acct_info \n{json_acct_info}')
        credentials = service_account.Credentials.from_service_account_info(json_acct_info)

        gcs_client = storage.Client(self.__project_id,credentials)
        bucket = gcs_client.get_bucket(self.__bucket)
        # credentials = gcs_client.from_json_keyfile_dict(create_keyfile_dict())

        blob = bucket.blob(remote_bucket_name)
        blob.upload_from_filename(filename=local_path)
        try: 
          os.remove(local_path)
        except:
          print(f"{local_path} exists but can't delete...this is a problem")


        signed_url = blob.generate_signed_url(
            version="v4",
            # This URL is valid for 7 days
            expiration = timedelta(days=7),
            # Allow GET requests using this URL.
            method="GET",
        )

        return signed_url

    def store_data(self,localfile):
      utc_now = datetime.utcnow().isoformat()
      if self.__fm_id == None:
        rep = 'all'
      else:
        rep = self.__fm_id
      bucket_path = f'{self.__bucket_path}/commission-analysis-{rep}-{utc_now}'
      urls = self._move_output_to_cloud_storage(localfile,f'{self.__bucket_path}/maps-{rep}-{utc_now}.html')

      #link = f"https://storage.cloud.google.com/{HOME_BUCKET}/{filename}"
      resp = { "urls" :  [ urls]}

      return json.dumps(resp), 200, {'Content-Type': 'application/json'}

    @classmethod
    def _get_rename_dict(cls,in_dict):
      return { key: value['name'] for key,value in in_dict.items()}

    @classmethod
    def _get_agg_dict(cls,in_dict):
      return { key: value['agg'] for key,value in in_dict.items()}

    @classmethod
    def _make_data_grouping(cls,df_grouping,cols,agg_dict,rename_dict):

      df = df_grouping[list(cols.keys())].agg(agg_dict)
      # if reset_multiIndex:
      #   if isinstance(df.index, pd.MultiIndex):
      #     df = df.reset_index()

      df.rename(columns=rename_dict,inplace=True)
      df.columns = [ (col if type(col) != tuple else " ".join(col)) for col in df.columns.values]

      df.columns = df.columns.str.replace("mean", "Avg.")
      df.columns = df.columns.str.replace("sum", "")
      df.columns = df.columns.str.replace("list", "")
      df.columns = df.columns.str.replace("count", "")
      df.columns = df.columns.str.replace("first", "")
      df.columns = df.columns.str.replace(" $", "",regex=True)

      return df

    ''' Public class methods '''

    '''Public Instance methods '''
    # this function returns a dataframe 
    # It first does a groupby based on a key and frequency 
    # It then selects the columns for the dataframe based on a dictionary passed in.  that dictionary has a format that
    # indicates how to rename the columns and what aggregation functions should be applied, for example, apply sum and np.mean...
    def get_deals_by_user(self,info_dict,freq='Y',key='createdAt',selector=None):

      if selector == None:  
        df_gp = self.__df.groupby(['data.user._id','data.info.type',pd.Grouper(key=key,freq=freq)])
      else:
        #selector should be a key value concept, such as df['Booked'] == 1 so selector 0 is 'Booked and selector 1 is 1
        df_gp = self.__df[self.__df[selector[0]] == selector[1]].groupby(['data.user._id','data.info.type',pd.Grouper(key=key,freq=freq)])


      #find out what columns we want and how to aggregate them
      rename_dict = DealData._get_rename_dict(info_dict)
      aggregate_dict = DealData._get_agg_dict(info_dict)

      apps = DealData._make_data_grouping(df_gp,info_dict,aggregate_dict,rename_dict)

      apps.index.set_names(['Finance Manager Id','Collateral Type',self._tf_dict[freq]],inplace=True)

      return apps

class ExcelSpreadSheet:

    def __init__(
        self,
    ):
  

        # Add a header format.
      header = {
          'bold': True,
          'text_wrap': True,
          'valign': 'top',
          'bg_color': '#78B0DE',
          'border': 1}

      blue_background = {'bg_color': '#78B0DE'}  # blue cell background color
      white_background = {'bg_color': '#FFFFFF'} # white cell background color

      self._cell_width = 64.0
      self._cell_height = 20.0
      self._bound_width_height = (750, 750)
      self._xls_text_box_rows = 7
      self._text_box_options = {
        'width': 512,
        'height': 100,
      }
      self._text_box_col = 1
      self._xls_report_path = '/tmp/report.xlsx'

      self._writer = pd.ExcelWriter(self._xls_report_path, engine='xlsxwriter',date_format = 'mm/dd/yyyy',datetime_format='mm/dd/yyyy ')
      self._workbook = self._writer.book
      self._header_fmt = self._workbook.add_format({'bold': True, 'text_wrap': True, 'valign': 'top','align': 'middle'})

      # Add a number format for cells with money.
      self._money_fmt = self._workbook.add_format({'num_format': '$#,##0'})
      self._numb_fmt = self._workbook.add_format({'num_format': '#,##0.00','valign': 'top','align': 'middle'})
      #numb_fmt = workbook.add_format({'bold': True})
      self._wrap_fmt = self._workbook.add_format({'text_wrap': True,'valign': 'top','align': 'middle'})
      # Add a percent format with 1 decimal point
      self._percent_fmt = self._workbook.add_format({'num_format': '0.0%', 'bold': False,'valign': 'top','align': 'middle'})
      self._currency_fmt = self._workbook.add_format({'num_format': '$#,##0.00','valign': 'top','align': 'middle'})
      self._blue_background = self._workbook.add_format(blue_background) # blue cell background color
      self._white_background = self._workbook.add_format(white_background) # white cell background color'
      self._workbook.add_format(header)

      return

    # public functions
    @property
    def header_fmt(self):
      return self._header_fmt

    @property
    def money_format(self):
      return self._money_fmt

    @property
    def number_format(self):
      return self._numb_fmt

    @property
    def wrap_format(self):
      return self._wrap_fmt

    @property
    def percent_format(self):
      return self._percent_fmt

    @property
    def currency_format(self):
      return self._currency_fmt

    def write_df_to_excel(self, df, sheet, row,text=None,autoFilter=False,format_columns=None,print_index = False):

      # Example, the xlswriter interface is a challenge, we can't add_worksheet but rather start by writing out the dataframe and then the text box
      if text:
        start_text_box = row
        start_df_row = start_text_box + self._xls_text_box_rows
      else:
        start_df_row = row

      (max_row, max_col) = df.shape
      max_row = max_row + start_df_row

      #Adding +1 for start row because writing header separately
      if autoFilter:
        df.to_excel(self._writer,sheet_name=sheet,startrow=start_df_row+1,float_format="%.2f",merge_cells=True,header=False,index=print_index)
        self._writer.sheets[sheet].autofilter(start_df_row, 0,df.shape[0] , df.shape[1] - 1)
      else:
        df.to_excel(self._writer,sheet_name=sheet,startrow=start_df_row+1,float_format="%.2f",merge_cells=True,header=False,index=print_index)



      col_list = []
      if print_index: 
        col_list = list(df.index.names) + list(df.columns.values)  
      else:
        col_list = list(df.columns.values)  
      col_num=0
      # if isinstance(df.index, pd.MultiIndex):
      #   col_list = list(df.index.names) + list(df.columns.values)
      #   col_num=0
      # else: 
      #   col_list = list(df.columns.values)
      #   col_num=1

      # Write the column headers with the defined format.
      self._writer.sheets[sheet].freeze_panes(start_df_row+1,0)
      for col_num, value in enumerate(col_list):
          self._writer.sheets[sheet].write(start_df_row, col_num, value, self._header_fmt)
          self._writer.sheets[sheet].set_column(start_df_row,col_num,
                                                format_columns[col_num]['col_width'], 
                                                format_columns[col_num]['format'], 
                                                options={'hidden': format_columns[col_num]['hidden']}) 
      if text:
        temp_ws = self._writer.sheets[sheet]
        temp_ws.insert_textbox(start_text_box , self._text_box_col, text,self._text_box_options)

      row += start_df_row + len(df) + 2

      sum_cols = [5,6,7,10,12,13]
      self._writer.sheets[sheet].write(max_row+1, 4, 'Totals', self._header_fmt)  

      # for i in range(start_df_row,max_row): # integer odd-even alternation 
      #     self._writer.sheets[sheet].set_row(i, cell_format=(self._blue_background if i%2==0 else self._white_background))

      #for col in sum_cols:
      for col, value in enumerate(col_list):
        try: 
          if format_columns[col]['total']:
            s_cell = xl_rowcol_to_cell(start_df_row+1,col) 
            e_cell = xl_rowcol_to_cell(max_row,col )  
            formula = f'=SUBTOTAL(109,{s_cell}:{e_cell})'
            self._writer.sheets[sheet].write_formula(max_row+1, col, formula)
        except:
          pass

      return temp_ws,row

    def close(self):
      self._writer.close()
      return

    def _move_output_to_cloud_storage(self,project_id,bucket, remote_path):
        local_path = self._xls_report_path

        with open('/etc/secrets/primary/latest') as source:
          json_acct_info = json.load(source)
          print(f'json_acct_info \n{json_acct_info}')
        credentials = service_account.Credentials.from_service_account_info(json_acct_info)

        gcs_client = storage.Client(project_id,credentials)
        bucket = gcs_client.get_bucket(bucket)
        # credentials = gcs_client.from_json_keyfile_dict(create_keyfile_dict())

        blob = bucket.blob(remote_path)
        blob.upload_from_filename(filename=local_path)
        try: 
          os.remove(local_path)
        except:
          print(f"{local_path} exists but can't delete...this is a problem")


        signed_url = blob.generate_signed_url(
            version="v4",
            # This URL is valid for 7 days
            expiration = timedelta(days=7),
            # Allow GET requests using this URL.
            method="GET",
        )

        return signed_url

    def store_xls(self,project_id,bucket, report_path,rep=None):
      utc_now = datetime.utcnow().isoformat()

      remote_path = f'{report_path}/fm-commission-{rep}-{utc_now}.xls'
      urls = self._move_output_to_cloud_storage(project_id,bucket,remote_path)
      resp = { "urls" :  [ urls]}

      return json.dumps(resp), 200, {'Content-Type': 'application/json'}

def create_commission_df(df):
   
  commission_df =  pd.DataFrame()
  commission_df['Accounting Box'] = df['Accounting Box']

  commission_df['Additional Reserve - Acct Modified'] = np.where(df['Accounting Box']  & 
    df['data.info.accounting.profit.wfdProfit.extraReserveProfit'].notna() &
  (df['data.info.accounting.profit.wfdProfit.extraReserveProfit'] !=  df['data.info.profit.wfdProfit.extraReserveProfit']), True, False)
                                                                          
  #deal_df[deal_df['Accounting Box'] & (deal_df['data.info.accounting.profit.wfdProfit.totalProfit'] != deal_df['data.info.profit.wfdProfit.totalProfit'])]['data.info.refNumber']

  commission_df['GAP - Acct Modified'] = np.where(df['Accounting Box'] & df['data.info.accounting.profit.wfdProfit.extraGAPProfit'].notna() &
    (df['data.info.accounting.profit.wfdProfit.extraGAPProfit'] != df['data.info.profit.wfdProfit.extraGAPProfit']), True, False)
                                                                            

  commission_df['Warranty - Acct Modified'] = np.where(df['Accounting Box'] &
      df['data.info.accounting.profit.wfdProfit.extraServiceWarrantyProfit'].notna() &
    (df['data.info.accounting.profit.wfdProfit.extraServiceWarrantyProfit'] != df['data.info.profit.wfdProfit.extraServiceWarrantyProfit']), True, False)
                                                                            

  commission_df['Total WFD Income - Acct Modified'] = np.where(df['Accounting Box'] &
      df['data.info.accounting.profit.wfdProfit.totalProfit'].notna() &
    (df['data.info.accounting.profit.wfdProfit.totalProfit'] != df['data.info.profit.wfdProfit.totalProfit']), True, False)

  commission_df['Total Reserve'] = np.where(df['Accounting Box'], df['acct_totalReserve'], df['blue_totalReserve'])
  commission_df['WFD Reserve'] = np.where(df['Accounting Box'], df['acct_wfdReserve'], df['blue_wfdReserve'])

  commission_df['Additional Reserve'] = np.where(df['Accounting Box'],df['data.info.accounting.profit.wfdProfit.extraReserveProfit'],
                                                                           df['data.info.profit.wfdProfit.extraReserveProfit'])
                                                                            

  commission_df['GAP'] = np.where(df['Accounting Box'], df['data.info.accounting.profit.wfdProfit.extraGAPProfit'],
                                                             df['data.info.profit.wfdProfit.extraGAPProfit'])
                                                                            

  commission_df['Warranty'] = np.where(df['Accounting Box'], df['data.info.accounting.profit.wfdProfit.extraServiceWarrantyProfit'],
                                                                  df['data.info.profit.wfdProfit.extraServiceWarrantyProfit'])
                                                                            

  commission_df['Total WFD Income'] = np.where(df['Accounting Box'],  df['data.info.accounting.profit.wfdProfit.totalProfit'],
                                                                           df['data.info.profit.wfdProfit.totalProfit'])
                                                                           
  commission_df['Commissionable Amount'] = np.where((df['Accounting Box'] & 
    df['data.info.accounting.profit.managerProfit.commissionableAmount'].notna()), df['data.info.accounting.profit.managerProfit.commissionableAmount'],
                                                             df['data.info.profit.managerProfit.commissionableAmount'])

                     
  commission_df['Commission'] = np.where((df['Accounting Box'] & 
    df['data.info.accounting.profit.managerProfit.commission'].notna()), df['data.info.accounting.profit.managerProfit.commission'],
                                                             df['data.info.profit.managerProfit.commission'])
            
  # commission_df['Commission'] = np.where(df['Accounting Box'] &
  # , df['data.info.accounting.profit.managerProfit.commission'],
  #                                                            df['data.info.profit.managerProfit.commission'])
       

  return commission_df

def get_fm_commission_report(request):

  """Responds to any HTTP request.
  Args:
      request (flask.Request): HTTP request object.
  Returns:
      The response text or any set of values that can be turned into a
      Response object using
      `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.

  """
  # Get Environment Variables
  BUCKET = os.environ.get('REPORT_BUCKET','.')
  REPORT_PATH = os.environ.get('REPORT_DIR','acct-reports')
  PROJECT_ID = os.environ.get('GCP_PROJECT',None)
  if not PROJECT_ID:
    GSPATH=''
    PROJECT_ID='wfd-reporting'
  else:
    GSPATH='gs://'  

  start_date = None
  end_date = None
  fm_requested_id = None

  if request:
    request_json = request.get_json(silent=True)
    request_args = request.args
    dateRange={}

    if request_json and 'dateRange' in request_json:
      dateRange = request_json['dateRange']
    elif request_args and 'dateRange' in request_args:
      dateRange = request_args['dateRange']

    try:
      print(f"Start_date json is  : {dateRange['from']}")
      start_date = dateRange['from']
      start_date = parser.parse(start_date)
      print(f'Start_date : {start_date}')
    except: 
      pass

    try:
      end_date = dateRange['to']
      end_date = parser.parse(end_date)
      print(f'end_date : {end_date}')
    except: 
      pass

    if request_json and 'fmId' in request_json:
      fm_requested_id = request_json['fmId']
    elif request_args and 'fmId' in request_args:
      fm_requested_id = request_args['fmId']

  signed_deal_info = DealData(start_date,end_date,'signed',fm_requested_id)
  signed_deal_df = signed_deal_info.df

  if signed_deal_df.empty:
    return 'No Data Available',404

  s_date = signed_deal_info.start_date.strftime('%m/%d/%Y')
  e_date = signed_deal_info.end_date.strftime('%m/%d/%Y')
  timeframe = f'{s_date} - {e_date}'

  signed_deal_df[['first_signed_at_date','Num Times Status Signed','Status Dates']] = signed_deal_df['data.info.statusHistory'].apply(DealData.first_date,status='signed',just_date=False).to_list()
  signed_deal_df['first_delivered_at_date'] = signed_deal_df['data.info.statusHistory'].apply(DealData.first_date,status='delivered')
  signed_deal_df['first_cancelled_at_date'] = signed_deal_df['data.info.statusHistory'].apply(DealData.first_date,status='canceled')

  def make_applicant_name (row):
    if row["data.applicant.data.info.middleName"] == None:
      return f'{row["data.applicant.data.info.firstName"]} {row["data.applicant.data.info.lastName"]}'
    else:
      return f'{row["data.applicant.data.info.firstName"]} {row["data.applicant.data.info.middleName"]} {row["data.applicant.data.info.lastName"]}'


  signed_deal_df['Customer'] = signed_deal_df.apply(make_applicant_name,axis=1)
  print("Confused Deals")
  print(signed_deal_df[(signed_deal_df['first_signed_at_date'].notna()) & (~(signed_deal_df['data.info.status'] == 'signed'))])
  # if payroll date (to be set automatically by signed and sent to lender) is before cancelled date we need chargeback for info


  xls = ExcelSpreadSheet()

  default_sheet1 = [ 
    {'name' : 'first_signed_at_date', 'col_name' : 'Signed at Date', 'col_width' : 12, 'col_format' : None , 'hidden' : False, 'total' : False, 'format' : xls.wrap_format},
    {'name' : 'Num Times Status Signed', 'col_name' : 'Num Times Status Signed', 'col_width' : 12, 'col_format' : None , 'hidden' : False, 'total' : True, 'format' : xls.number_format},
    {'name' : 'first_cancelled_at_date', 'col_name' : 'Cancelled Date', 'col_width' : 12, 'col_format' : None, 'hidden' : False, 'total' : False, 'format' : xls.wrap_format },
    {'name' : 'data.info.dealDates.fundedAt', 'col_name' : 'Funded Date', 'col_width' : 12, 'col_format' : None, 'hidden' : False, 'total' : False, 'format' : xls.wrap_format },
    {'name' : 'Finance Manager', 'col_name' : 'Finance Manager', 'col_width' : 12, 'col_format' : None , 'hidden' : False, 'total' : False, 'format' : xls.wrap_format},
    {'name' : 'data.info.type', 'col_name' : 'Collateral Type', 'col_width' : 12, 'col_format' : None , 'hidden' : False, 'total' : False, 'format' : xls.wrap_format},
    {'name' : 'Representative', 'col_name' : 'Representative', 'col_width' : 12, 'col_format' : None , 'hidden' : False, 'total' : False, 'format' : xls.wrap_format},
    {'name' : 'data.info.refNumber', 'col_name' : 'Ref Number', 'col_width' : 15, 'col_format' : None, 'hidden' : False, 'total' : False, 'format' : xls.wrap_format },
    {'name' : 'data.dealership.data.info.name', 'col_name' : 'Dealer', 'col_width' : 20, 'col_format' : None, 'hidden' : False, 'total' : False, 'format' : xls.wrap_format },
    {'name' : 'Customer', 'col_name' : 'Customer', 'col_width' : 20, 'col_format' : None, 'hidden' : False, 'total' : False, 'format' : xls.wrap_format},
    {'name' : 'data.info.vehicle.VIN', 'col_name' : 'VIN', 'col_width' : 25, 'col_format' : None, 'hidden' : False, 'total' : False, 'format' : xls.wrap_format},
    {'name' : 'Vehicle', 'col_name' : 'Vehicle', 'col_width' : 30, 'col_format' : None, 'hidden' : False, 'total' : False, 'format' : xls.wrap_format},
    {'name' : 'data.lender.data.info.name', 'col_name' : 'Lender', 'col_width'  : 30, 'col_format' : None, 'hidden' : False, 'total' : False, 'format' : xls.wrap_format },
    {'name' : 'data.info.payment.dealTotal', 'col_name' : 'Amount Financed', 'col_width' : 12, 'col_format' : None, 'hidden' : False, 'total' : True, 'format' : xls.currency_format }
  ]

  conditional_fields = [
    {'name' : 'Accounting Box', 'col_name' : 'Accounting Box', 'col_width' : 10, 'col_format' : None, 'hidden' : False, 'total' : True, 'format' : xls.wrap_format },
    {'name' : 'Additional Reserve - Acct Modified', 'col_name' : 'Additional Reserve - Acct Modified', 'col_width' : 10, 'col_format' : None, 'hidden' : False, 'total' : True, 'format' : xls.wrap_format },
    {'name' : 'GAP - Acct Modified', 'col_name' : 'GAP - Acct Modified', 'col_width' : 10, 'col_format' : None, 'hidden' : False, 'total' : True, 'format' : xls.wrap_format },
    {'name' : 'Warranty - Acct Modified', 'col_name' : 'Warranty - Acct Modified', 'col_width' : 10, 'col_format' : None, 'hidden' : False, 'total' : True, 'format' : xls.wrap_format },   
    {'name' : 'Total WFD Income - Acct Modified', 'col_name' : 'Total WFD Income - Acct Modified', 'col_width' : 10, 'col_format' : None, 'hidden' : False, 'total' : True, 'format' : xls.wrap_format },
    {'name' : 'Total Reserve', 'col_name' : 'Total Reserve', 'col_width' : 12, 'col_format' : None, 'hidden' : False, 'total' : True, 'format' : xls.currency_format },
    {'name' : 'WFD Reserve', 'col_name' : 'WFD Reserve', 'col_width' : 12, 'col_format' : None, 'hidden' : False, 'total' : True, 'format' : xls.currency_format },
    {'name' : 'Additonal Reserve', 'col_name' : 'Additonal Reserve', 'col_width' : 12, 'col_format' : None, 'hidden' : False, 'total' : True, 'format' : xls.currency_format },
    {'name' : 'GAP', 'col_name' : 'GAP', 'col_width' : 12, 'col_format' : None, 'hidden' : False, 'total' : True, 'format' : xls.currency_format },
    {'name' : 'Warranty', 'col_name' : 'Warranty', 'col_width' : 12, 'col_format' : None, 'hidden' : False, 'total' : True, 'format' : xls.currency_format },
    {'name' : 'Total WFD Income', 'col_name' : 'Total WFD Income', 'col_width' : 12, 'col_format' : None, 'hidden' : False, 'total' : True, 'format' : xls.currency_format },
    {'name' : 'Commissionable Amount', 'col_name' : 'Commissionable Amount', 'col_width' : 12, 'col_format' : None, 'hidden' : False, 'total' : True, 'format' : xls.currency_format },
    {'name' : 'Commissionable', 'col_name' : 'Commissionable', 'col_width' : 12, 'col_format' : None, 'hidden' : False, 'total' : True, 'format' : xls.currency_format }

  ]
  #conditional_fields = ['Total Reserve', 'WFD Reserve','Additional Reserve','GAP', 'Warranty','Total WFD Income']

  default_sheet2 = [ 
    {'name' : 'first_signed_at_date', 'col_name' : 'Signed at Date', 'col_width' : 12, 'col_format' : None , 'hidden' : False, 'total' : False, 'format' : xls.wrap_format},
    {'name' : 'first_delivered_at_date', 'col_name' : 'Delivered Date', 'col_width' : 12, 'col_format' : None, 'hidden' : False, 'total' : False, 'format' : xls.wrap_format },
    {'name' : 'Num Times Status Delivered', 'col_name' : 'Num Times Status Delivered', 'col_width' : 12, 'col_format' : None , 'hidden' : False, 'total' : True, 'format' : xls.number_format}, 
    {'name' : 'first_cancelled_at_date', 'col_name' : 'Cancelled Date', 'col_width' : 12, 'col_format' : None, 'hidden' : False, 'total' : False, 'format' : xls.wrap_format },
    {'name' : 'data.info.dealDates.fundedAt', 'col_name' : 'Funded Date', 'col_width' : 12, 'col_format' : None, 'hidden' : False, 'total' : False, 'format' : xls.wrap_format },
    {'name' : 'Finance Manager', 'col_name' : 'Finance Manager', 'col_width' : 12, 'col_format' : None , 'hidden' : False, 'total' : False, 'format' : xls.wrap_format},
    {'name' : 'data.info.type', 'col_name' : 'Collateral Type', 'col_width' : 12, 'col_format' : None , 'hidden' : False, 'total' : False, 'format' : xls.wrap_format},
    {'name' : 'Representative', 'col_name' : 'Representative', 'col_width' : 12, 'col_format' : None , 'hidden' : False, 'total' : False, 'format' : xls.wrap_format},
    {'name' : 'data.info.refNumber', 'col_name' : 'Ref Number', 'col_width' : 15, 'col_format' : None, 'hidden' : False, 'total' : False, 'format' : xls.wrap_format },
    {'name' : 'data.dealership.data.info.name', 'col_name' : 'Dealer', 'col_width' : 20, 'col_format' : None, 'hidden' : False, 'total' : False, 'format' : xls.wrap_format },
    {'name' : 'Customer', 'col_name' : 'Customer', 'col_width' : 20, 'col_format' : None, 'hidden' : False, 'total' : False, 'format' : xls.wrap_format },
    {'name' : 'data.info.vehicle.VIN', 'col_name' : 'VIN', 'col_width' : 20, 'col_format' : None, 'hidden' : False, 'total' : False, 'format' : xls.wrap_format },
    {'name' : 'Vehicle', 'col_name' : 'Vehicle', 'col_width' : 25, 'col_format' : None, 'hidden' : False, 'total' : False, 'format' : xls.wrap_format},
    {'name' : 'data.lender.data.info.name', 'col_name' : 'Lender', 'col_width'  : 25, 'col_format' : None, 'hidden' : False, 'total' : False, 'format' : xls.wrap_format },
    {'name' : 'data.info.payment.dealTotal', 'col_name' : 'Amount Financed', 'col_width' : 12, 'col_format' : None, 'hidden' : False, 'total' : True, 'format' : xls.currency_format },
  ]

  #signed_deal_df['Accounting Box'] = signed_deal_df['data.info.dealDates.fundedAt'].notna()
  # We need to handle those lenders that pay the dealer directly, currently only Royal Credit Union
  signed_deal_df['Accounting Box'] = np.where((signed_deal_df['data.info.dealDates.fundedAt'].notna() & 
      (signed_deal_df['data.lender.data.info.name'] != 'Royal Credit Union')),  True, False)

  signed_deal_df['data.info.dealDates.fundedAt'] = pd.to_datetime(signed_deal_df['data.info.dealDates.fundedAt']).dt.date

  commission_df =  create_commission_df(signed_deal_df)

  report_fields =  [c['name'] for c in default_sheet1]
  report_df = signed_deal_df[report_fields].copy()

  rename_dict = {c['name'] : c['col_name'] for c in default_sheet1}
  report_df = report_df.rename(columns = rename_dict)
  report_df = report_df.join(commission_df)
  report_df = report_df[report_df['Signed at Date'].notna()]
  sheet1 = default_sheet1 + conditional_fields

  xls.write_df_to_excel(report_df,"Signed Deals",row=0,text=f'Deal Information for deals signed  in timeframe {timeframe}',autoFilter=True,format_columns=sheet1)
  
  sheet3_df = report_df.copy()


  delivered_deal_info = DealData(start_date,end_date,'delivered',fm_requested_id)
  delivered_deal_df = delivered_deal_info.df

  if not delivered_deal_df.empty:
    #delivered_deal_df['Accounting Box'] = delivered_deal_df['data.info.dealDates.fundedAt'].notna()
    delivered_deal_df['Accounting Box'] = np.where((delivered_deal_df['data.info.dealDates.fundedAt'].notna() & 
      (delivered_deal_df['data.lender.data.info.name'] != 'Royal Credit Union')),  True, False)
    delivered_deal_df['data.info.dealDates.fundedAt'] = pd.to_datetime(delivered_deal_df['data.info.dealDates.fundedAt']).dt.date
    delivered_deal_df['first_signed_at_date'] = delivered_deal_df['data.info.statusHistory'].apply(DealData.first_date,status='signed')
    delivered_deal_df[['first_delivered_at_date','Num Times Status Delivered','Status Dates']] = delivered_deal_df['data.info.statusHistory'].apply(DealData.first_date,status='delivered',just_date=False).to_list()
    delivered_deal_df['first_cancelled_at_date'] = delivered_deal_df['data.info.statusHistory'].apply(DealData.first_date,status='canceled')
    delivered_deal_df['Customer'] = delivered_deal_df.apply(make_applicant_name,axis=1)
      
    commission_d_df =  create_commission_df(delivered_deal_df)

    report_fields =  [c['name'] for c in default_sheet2]
    report_df = delivered_deal_df[report_fields].copy()

    rename_dict = {c['name'] : c['col_name'] for c in default_sheet2}
    report_df = report_df.rename(columns = rename_dict)
    report_df = report_df.join(commission_d_df)
    #report_df = report_df[report_df['Delivered Date'].notna()]
    sheet2 = default_sheet2 + conditional_fields

  # report_fields =  [c['name'] for c in default_sheet2]
  # report_df = deal_df[report_fields].copy()

  # rename_dict = {c['name'] : c['col_name'] for c in default_sheet2}
  # report_df = report_df.rename(columns = rename_dict)

  # report_df = report_df.join(commission_df)

  # sheet2 = default_sheet2 + conditional_fields

    xls.write_df_to_excel(report_df,"Delivered Deals",row=0,text=f'Deal Information for deals delivered in timeframe {timeframe}',autoFilter=True,
                          format_columns=sheet2)  
  cutoff_date = end_date  +  timedelta(days=10)
  sheet3_df = sheet3_df[sheet3_df['Funded Date'] > cutoff_date.date()]
  xls.write_df_to_excel(sheet3_df,"Funded after 10th",row=0,text=f'Deal Information for deals signed  in timeframe {timeframe} but funded after 10th of the following month',autoFilter=True,format_columns=sheet1)

  xls.close()

  urls = xls.store_xls( PROJECT_ID,BUCKET,REPORT_PATH)
  return  urls
if __name__ == "__main__":
  get_fm_commission_report(None)
