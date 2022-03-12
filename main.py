from csv import writer
from doctest import DocFileSuite
from xml.etree.ElementInclude import include
# from sre_constants import SRE_FLAG_MULTILINE
# from psutil import STATUS_IDLE
from pymongo import MongoClient
from bson.objectid import ObjectId
import dns
import pprint
import datetime
import pandas as pd
from enum import Enum
from datetime import date
import seaborn as sns
from google.cloud import storage
import numpy as np
import re
import os
from dateutil import parser 
import json
import time
import geopy
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import googlemaps
from datetime import timedelta, datetime
import matplotlib.pyplot as plt
import google.auth.credentials
from google.oauth2 import service_account
from matplotlib import gridspec
import googlemaps
import folium
import folium.plugins as plugins
from branca.element import Template, MacroElement
import webbrowser
from folium import IFrame
import base64

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
        #dev_client = MongoClient("mongodb+srv://meftest:A1exander!@cheaptestcluster.m2ccb.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
        #db = dev_client.dev
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

class ApplicantInfo:

    def __init__(
        self,
        gs_path: str,
        bucket: str,
        bucket_path: str,
        project_id:str
    ):
        self.__gs_path = gs_path
        self.__bucket = bucket
        self.__bucket_path = bucket_path
        self.__project_id = project_id
        self.__df =  self._get_existing_df(f'{self.__gs_path}{self.__bucket}/{self.__bucket_path}/applicants_geocode.csv')

    def _get_existing_df(self,bucket_name):
        try:
          df = pd.read_csv(bucket_name, encoding='utf-8')
          # just ensuring there is no ssn
          try:
            df = df.drop('data.applicant.data.info.socialSecurityNumber',axis=1)
          except:
            pass
        except:
          df = pd.DataFrame()


        return(df)


    def feature_engineer_address(self):
      # this is becuause there is junk in the data...maybe just <>
      self.__df = self.__df[(self.__df['data.info.birthDate'] != '1111-11-10T22:26:44.000Z')]
      self.__df = self.__df[(self.__df['data.info.birthDate'] != '1111-01-01T05:50:36.000Z')]
      self.__df = self.__df[(self.__df['data.info.birthDate'] != '0200-05-31T05:50:36.000Z')]
      self.__df = self.__df[(self.__df['data.info.birthDate'] != '1197-07-22T05:50:36.000Z')]
      self.__df = self.__df[(self.__df['data.info.birthDate'].notna())]

      self.__df['_id'] = self.__df['_id'].apply(ObjectId)

      self.__df['birthDate']= pd.to_datetime(self.__df['data.info.birthDate'],errors='coerce')

      self.__df['age'] = (datetime.utcnow() - self.__df['birthDate'].dt.tz_localize(None)  ) // timedelta(days=365.2425)

      self.__df = pd.get_dummies(self.__df,columns=['data.info.maritalStatus'])

      bins= [17,20,30,40,50,60,70,100]
      labels = ['Under 20','21-30','31-40','41-50','51-60','61-70','Greater than 70']
      self.__df['AgeGroup'] = pd.cut(self.__df['age'], bins=bins, labels=labels)
      return self.__df

class GeoCode:

    def __init__(
        self,
    ):
        self.__geolocator = Nominatim(user_agent="wfd-test")
        self.__geocode = RateLimiter(self.__geolocator.geocode, min_delay_seconds=5)
        self.__gmap_client = googlemaps.Client(key='AIzaSyAQQ54vqrH8DexOe7vTv3gONzYRjJo5k5c')

    def geocode_address(self,address):
      return self.__gmap_client.geocode(address)


class DealData:

    def __init__(
        self,
        rep_id: str,
        dealer_id: str,
        start_date: datetime,
        end_date: datetime,
        gs_path: str,
        bucket: str,
        bucket_path: str,
        project_id:str
    ):
        self.__rep_id = rep_id
        self.__dealer_id = dealer_id
        self.__start_date = start_date
        self.__end_date = end_date
        self.__gs_path = gs_path
        self.__bucket = bucket
        self.__bucket_path = bucket_path
        self.__project_id = project_id
        self.__df =  self._get_existing_df(f'{self.__gs_path}{self.__bucket}/{self.__bucket_path}/df_merge.bz2')

    def feature_engineer_dealer(self):
      self.__df['geocode_address'] =  self.__df['data.dealership.data.info.address'] + ' ' + self.__df['data.dealership.data.info.city'] + ' ' + \
                                      self.__df['data.dealership.data.info.state'] +  self.__df['data.dealership.data.info.zipCode']
    def feature_engineer(self):
      bins= [0,10,20,30,40,60,70,80,90,100]
      labels = ['Under 10k','11-20k','21-30k','31-40k','41-50k','51-60k','61-80k','81-90k','91-100k']
      self.__df['AmountFinancedBin'],bin_label = pd.cut(self.__df['data.info.payment.dealTotal'], bins=bins, labels=labels, include_lowest=True, retbins=True)

    def _get_existing_df(self,bucket_name):
        try:
          df = pd.read_pickle(f'{bucket_name}')
          if not self.__rep_id == None:
            if not self.__dealer_id == None:
              df = df.loc[((df['data.dealership._id'] == ObjectId(self.__dealer_id)) & (df['data.dealership.data.representativeId'] == ObjectId(self.__rep_id))),:]
            else: 
              df = df.loc[(df['data.dealership.data.representativeId'] == ObjectId(self.__rep_id)),:]
          elif not self.__dealer_id == None:
            df = df.loc[(df['data.dealership._id'] == ObjectId(self.__dealer_id)),:]
          df = df.loc[((df['createdAt'] >= self.__start_date) & (df['createdAt'] <= self.__end_date)),:]        
          # just ensuring there is no ssn
          try:
            df = df.drop('data.applicant.data.info.socialSecurityNumber',axis=1)
          except:
            pass
        except:
          df = pd.DataFrame()

        return(df)


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
      if self.__rep_id == None:
        rep = 'all'
      else:
        rep = self.__rep_id
      bucket_path = f'{self.__bucket_path}/applicant-analysis-{rep}-{utc_now}'
      urls = self._move_output_to_cloud_storage(localfile,f'{self.__bucket_path}/maps-{rep}-{utc_now}.html')

      #link = f"https://storage.cloud.google.com/{HOME_BUCKET}/{filename}"
      resp = { "urls" :  [ urls]}

      return json.dumps(resp), 200, {'Content-Type': 'application/json'}
# FOLIUM COLORS

marker_info = {'denied' : {'color' : 'red', 'radius' : 3,'icon' : 'thumbs-down' },
              'credit check': {'color' : 'beige', 'radius' : 1.5, 'icon' :'eye-open'},
              'pending' : {'color' : 'white', 'radius' : 2, 'icon' : 'hourglass'},
              'counter' : {'color' : 'gray', 'radius' : 2}, 'icon' : 'hand-left',
              'approved' : {'color' : 'lightblue', 'radius' : 2.5, 'icon' : 'thumbs-up'},
              'delivered' : {'color' : 'blue', 'radius' : 3.0, 'icon' : 'send'},
              'signed': {'color' : 'lightgreen', 'radius' : 4.0 , 'icon' : 'pencil' },
              'booked' : {'color' : 'green', 'radius' : 5.0, 'icon' : 'thumbs-up'},
              'cancelled' : {'color' : 'darkred', 'radius' : 5.0, 'icon' : 'remove'}
            }

template = """
  {% macro html(this, kwargs) %}

  <!doctype html>
  <html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>jQuery UI Draggable - Default functionality</title>
    <link rel="stylesheet" href="//code.jquery.com/ui/1.12.1/themes/base/jquery-ui.css">

    <script src="https://code.jquery.com/jquery-1.12.4.js"></script>
    <script src="https://code.jquery.com/ui/1.12.1/jquery-ui.js"></script>
    
    <script>
    $( function() {
      $( "#maplegend" ).draggable({
                      start: function (event, ui) {
                          $(this).css({
                              right: "auto",
                              top: "auto",
                              bottom: "auto"
                          });
                      }
                  });
  });

    </script>
  </head>
  <body>
  <div id='maplegend' class='maplegend' 
      style='position: absolute; z-index:9999; border:2px solid grey; background-color:rgba(255, 255, 255, 0.8);
      border-radius:6px; padding: 10px; font-size:14px; right: 20px; bottom: 20px;'>

  <div class='legend-title'>Legend</div>
  <div class='legend-scale'>
    <ul class='legend-labels'>
      <li><span style='background:red;opacity:0.7;'></span>Denied</li>
      <li><span style='background:beige;opacity:0.7;'></span>Credit Check</li>
      <li><span style='background:white;opacity:0.7;'></span>Pending</li>
      <li><span style='background:gray;opacity:0.7;'></span>Counter</li>
      <li><span style='background:lightblue;opacity:0.7;'></span>Approved</li>
      <li><span style='background:blue;opacity:0.7;'></span>Delivered</li>
      <li><span style='background:lightgreen;opacity:0.7;'></span>Signed & Sent to Lender</li>
      <li><span style='background:green;opacity:0.7;'></span>Funded</li>
      <li><span style='background:darkred;opacity:0.7;'></span>Cancelled</li>
      <li>Size of circle is proportional to Amount Financed</li>

    </ul>
  </div>
  </div>
  
  </body>
  </html>

  <style type='text/css'>
    .maplegend .legend-title {
      text-align: left;
      margin-bottom: 5px;
      font-weight: bold;
      font-size: 90%;
      }
    .maplegend .legend-scale ul {
      margin: 0;
      margin-bottom: 5px;
      padding: 0;
      float: left;
      list-style: none;
      }
    .maplegend .legend-scale ul li {
      font-size: 80%;
      list-style: none;
      margin-left: 0;
      line-height: 18px;
      margin-bottom: 2px;
      }
      
    .maplegend .legend-scale ul li {
      font-size: 80%;
      list-style: none;
      margin-left: 0;
      line-height: 18px;
      margin-bottom: 2px;
      }
    .maplegend ul.legend-labels li span {
      display: block;
      float: left;
      height: 16px;
      width: 30px;
      margin-right: 5px;
      margin-left: 0;
      border: 1px solid #999;
      }

    .maplegend .legend-source {
      font-size: 80%;
      color: #777;
      clear: both;
      }
    .maplegend a {
      color: #777;
      }
  </style>
{% endmacro %}"""

def mark_it(row,group):
  dt = row['data.info.payment.dealTotal']
  dealer = row['data.dealership.data.info.name']
  pop_string = f"<ul><li>Collateral: {row['data.info.type']}</li><li>Amount Financed: ${dt:.2f}</li><li>Dealer: {dealer}</li></ul>"
  pop = folium.Popup(pop_string, min_width=300, max_width=500)
  radius = dt/10000+2
  try:
    if row['BookedLA'] == 1:
      tip = f"<ul><li>{dealer}'s Customer</li><li>Funded by {row['Lenders']}</li></ul><br>Click for more info"

      #tip = f"{dealer}'s Customer<br>Funded by {row['Lenders']}<br> Click for more info"
      folium.CircleMarker(location=[row["lat"], row["long"]],radius=radius,color=marker_info['booked']['color'],popup=pop,tooltip=tip).add_to(group)
    else:
      status = row['data.info.status']
      tip = f"<ul><li>{dealer}'s Customer</li><li>FStatus: {status}</li></ul><br>Click for more info"
      #tip = f"{dealer}'s Customer<br>Status: {status}<br>Click for more info"
      folium.CircleMarker(location=[row["lat"], row["long"]],radius=radius,color=marker_info[status]['color'],popup=pop,tooltip=tip).add_to(group)  
  except:
    print("Couldn't mark row")
  return

def click_iframe(df):
  fig = plt.subplots(figsize=(10,7)) 
  #gs = gridspec.GridSpec(1, 2, width_ratios=[2, 4],wspace=.25) 
  gs = gridspec.GridSpec(2, 2,width_ratios=[2, 5],wspace=.75,hspace=.75 ) 

  ax0 = plt.subplot(gs[0])
  ax0.set_xticks(ax0.get_xticks()) # this seems to be a hack to surpress warnings
  ax0.set_xticklabels(ax0.get_xticklabels(),rotation = 45)

  ax1 = plt.subplot(gs[1])
  ax1.set_xticks(ax1.get_xticks()) # this seems to be a hack to surpress warnings
  ax1.set_xticklabels(ax1.get_xticklabels(),rotation = 45)

  ax2 = plt.subplot(gs[2])
  # ax2.set_xticks(ax2.get_xticks()) # this seems to be a hack to surpress warnings
  # ax2.set_xticklabels(ax2.get_xticklabels(),rotation = 45)

  ax3 = plt.subplot(gs[3])
  ax3.set_xticks(ax3.get_xticks()) # this seems to be a hack to surpress warnings
  ax3.set_xticklabels(ax3.get_xticklabels(),rotation = 45)

  bins = list(range(400, 900, 50))
  credit_score_range = pd.cut(df['app_credit_score_0'],bins=bins)
  sns.countplot(x=credit_score_range,ax=ax0)

  bins = list(range(10000, 100000, 10000))
  df['price_range'] = pd.cut(df['data.info.price.price'],bins=bins)
  sns.boxplot(x='price_range', y='app_credit_score_0',hue='data.info.type',data=df,ax=ax1);

  sns.histplot(df['age'],kde=False,ax=ax2)
  print(f"histplot ylimit {ax2.get_ylim()}")
  
  #ax2.set_ybound(lower=2)

  sns.countplot(data=df,x='AgeGroup',hue='data.info.maritalStatus_Married',ax=ax3)
  #ax3.set_ybound(lower=ax3.get_ylim()+1)

  png='/tmp/plot.png'
  plt.savefig(png)
  plt.close('all') 
  encoded = base64.b64encode(open(png, 'rb').read()).decode()
  html = '<img src="data:image/png;base64,{}">'.format

  iframe = IFrame(html(encoded), width=1000 ,height=775)
  return(iframe)

def get_dealer_applicant_analysis_report(request):
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
  REPORT_PATH = os.environ.get('REPORT_DIR','rep-reports')
  DATA_PATH = os.environ.get('DATA_PATH','.')  # this will enable reading csv in local directory for local testing
  PROJECT_ID = os.environ.get('GCP_PROJECT',None)
  if not PROJECT_ID:
    GSPATH=''
    PROJECT_ID='wfd-reporting'
  else:
    GSPATH='gs://'  

  start_date = datetime(2021, 7, 1, 12)
  end_date = datetime.utcnow()

  dealer_id = None
  rep_requested_id = None
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

    if request_json and 'representativeId' in request_json:
      rep_requested_id = request_json['representativeId']
    elif request_args and 'representativeId' in request_args:
      rep_requested_id = request_args['representativeId']

    if request_json and 'dealerId' in request_json:
      dealer_id = request_json['dealerId']
    elif request_args and 'dealerId' in request_args:
      dealer_id = request_args['dealerId']
  

  applicant_info = ApplicantInfo(GSPATH,BUCKET,DATA_PATH,PROJECT_ID)
  applicant_df = applicant_info.feature_engineer_address()

  dealer_info = DealData(rep_requested_id,dealer_id,start_date,end_date,GSPATH,BUCKET,DATA_PATH,PROJECT_ID)
  dealer_info.feature_engineer_dealer()
  dealer_info.feature_engineer()

  dealer_df = dealer_info.get_df()

  dealer_df = dealer_df.merge(applicant_df[['_id','age','AgeGroup','location','lat','long','data.info.maritalStatus_Married',
                                            'data.info.maritalStatus_Not married']],left_on='data.applicant._id',right_on='_id',how='left')


  geocoder = GeoCode()
  dealers_map = folium.Map(location=[43.9097, -91.2428],tiles='cartodbpositron',zoom_start=6)
  tooltip = 'Web Finance Direct'
  popup_string = "<i>Web Finance Direct<br>Click For More Info</i>"
  folium.Marker([43.9097, -91.2428], popup=popup_string, tooltip=tooltip).add_to(dealers_map)
  fg = folium.FeatureGroup('WFD Dealers')
  if not rep_requested_id:
    dealers_map.add_child(fg)
    
  marker_colors = [ 'darkpurple', 'blue', 'purple', 'orange','darkblue', 'cadetblue','lightblue','beige', 
                    'pink', 'lightgreen', 'gray', 'black','lightgray','green','darkred','lightred' ,'darkgreen','red','white']



  macro = MacroElement()
  macro._template = Template(template)

  dealers_map.get_root().add_child(macro)
  iframe = click_iframe(dealer_df)
  popup_string = folium.Popup(iframe, max_width=2650)
  folium.Marker([43.9097, -91.2428],icon=folium.Icon(color='red'),popup=popup_string, tooltip=tooltip,color='red').add_to(dealers_map)


  rep_list = dealer_df['data.dealership.data.representativeId'].unique()
  for i,rep in enumerate(rep_list):
    r = RepEntity(rep)
    rep_name = r.rep_name()
    rep_gp = folium.FeatureGroup(f"{rep_name}'s Dealers and Deals",show=False)
    rep_df = dealer_df[dealer_df['data.dealership.data.representativeId'] == rep]

    dealers_map.add_child(rep_gp)
    for dealer_name in sorted(rep_df['data.dealership.data.info.name'].unique()):
      d_df = rep_df[rep_df['data.dealership.data.info.name'] == dealer_name]
      try:
        dealer_addr_location = geocoder.geocode_address(d_df['geocode_address'].values[0])
        dealer_rep_id = d_df['data.dealership.data.representativeId'].values[0]
        lat = dealer_addr_location[0]['geometry']['location']['lat']
        long = dealer_addr_location[0]['geometry']['location']['lng']
        tooltip = f'{dealer_name}'
        popup_string = f'"<i>{dealer_name} </i>"'

        d_df=d_df[d_df['lat'].notna()]
        d_df=d_df.drop_duplicates(subset='data.info.refNumber') #doesn't matter which one we keep

        dealer_iframe = click_iframe(d_df)
        dealer_popup = folium.Popup(dealer_iframe, max_width=2650)

        print(f'Calling folium.Marker for {dealer_name}')
        if rep_requested_id:
          dealer_gp = folium.FeatureGroup(f"{dealer_name}'s Deals",show=False)
          folium.Marker([lat, long], popup=popup_string,icon=folium.Icon(color=marker_colors[rep_list.tolist().index(dealer_rep_id)]), tooltip=tooltip).add_to(dealer_gp)
        else:
          folium.Marker([lat, long], popup=popup_string,icon=folium.Icon(color=marker_colors[rep_list.tolist().index(dealer_rep_id)]), tooltip=tooltip).add_to(fg)

        dealer_sub_gp = plugins.FeatureGroupSubGroup(rep_gp,dealer_name,show=False) 

        folium.Marker([lat, long], popup=dealer_popup,icon=folium.Icon(color=marker_colors[rep_list.tolist().index(dealer_rep_id)]), tooltip=tooltip).add_to(rep_gp)

        if rep_requested_id:
          dealers_map.add_child(dealer_gp)
          d_df.apply(mark_it,group=dealer_gp,axis=1)

        rep_gp.add_child(dealer_sub_gp)
        d_df.apply(mark_it,group=dealer_sub_gp,axis=1)
      except:
        print(f'Dealer {dealer_name} no')
        continue



  folium.LayerControl().add_to(dealers_map)
  output_file = "/tmp/map.html"

  dealers_map.save(output_file)
  urls = dealer_info.store_data(output_file)
  #status = webbrowser.open(f'file://{output_file}', new=2)  # open in new tab
  return  urls
if __name__ == "__main__":
  get_dealer_applicant_analysis_report(None)
