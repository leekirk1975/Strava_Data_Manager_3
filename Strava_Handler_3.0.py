import requests
import pandas as pd
import urllib3 #urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)#this is only used to surpress warnings
from datetime import datetime,timedelta,timezone
from dateutil.parser import parse#much simple than using strptime, examples of both below
from dateutil import tz
import numpy as np
import os


print (str(datetime.today()) + ' starting' )

#surpress warnings only
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)#this is only used to surpress warnings

#set Urls
auth_url = "https://www.strava.com/oauth/token" #the Oauthenticate Url
# Strava API documentation this is list  = list athlete activities
Strava_API_url = "https://www.strava.com/api/v3/"#base strava endpoint for requests

#Set payload variables
client_id, client_secret,refresh_token  = open('client.secret.txt').read().strip().split(',')#set the client secret and refresh token
#the json payload to get the access token
payload = {
    'client_id':  client_id,
    'client_secret':  client_secret,
    'refresh_token':  refresh_token,
    'grant_type': "refresh_token",
    'f': 'json'
}

#set the types to retrieve for activity streams
types = ['time', 'distance', 'latlng', 'altitude', 'velocity_smooth', 'moving', 'grade_smooth', 'temp','heartrate','cadence','watts','act_Segment_Effort_ID','act_Segment_EndLatlng']
effort_types = ['id','name','athlete','elapsed_time','moving_time','start_date_local','distance','average_cadence','device_watts','average_watts','average_heartrate','segment','max_heartrate','pr_rank','kom_rank']
stream_type = ['time','distance','latlng','altitude','velocity_smooth','heartrate','cadence','watts','temp','moving','grade_smooth']


#set testing variables for activity selection
#Set the number of activities to retrieve
number_activities = 200
page_number = 1
date_stamp = datetime.now()
local_zone = tz.tzlocal()
last_ride_date = datetime.strptime("2018-09-28", '%Y-%m-%d')
last_ride_date = last_ride_date.replace(tzinfo=local_zone)
epoch_act_start_date = int((datetime.today() + timedelta(days=1)).strftime('%s'))
act_start_date = datetime.fromtimestamp(epoch_act_start_date).strftime('%Y-%m-%d %H:%M:%S')

#conversions variables
conversions = {
                'msec_to_kph': 3.6,
                'sec_to_mins': 60
}
lst_time= ['moving_time', 'elapsed_time']
lst_speed = ['average_speed','max_speed','velocity_smooth']


Filter_type = ['Ride','VirtualRide'] # set the filter for type of activities
#set segement effort ID for testing
SegmentId = '6162403'#SBV climb
SegmentEffortid = '2686035880283281869'


#Post request to return the user access token, NB the scope for the request needs to be read:all otherwise the get request for data will not work
#for more info watch - 1 - Intro and accessing Strava API with Postman - https://www.youtube.com/watch?v=sgscChKfGyg&t=626s
def Authenticate(Auth_url,Payload ):
    #Post the payload and return the access token for the users account
    print("Requesting Token...\n")
    res = requests.post(Auth_url, data=Payload, verify=False)
    access_token = res.json()['access_token']#  this token is required for the get request to work
    print("Access Token = {}\n".format(access_token))

    #set the header and parameters for the get request
    header = {'Authorization': 'Bearer ' + access_token}
    return header

#API DATA REQUESTS
#https://www.strava.com/api/v3/athlete/activities?access_token=e133077e1b08402ac4458f8c57aac4a86456296f&before=1590380704&page=1&per_page=2

#Get number of requested activities  - pass the  access token, get request for all the users activities
def get_activities(Strava_API_url,header,param):
    #Returns a list of Strava activity objects, up to the number specified by limit
    activites_url = Strava_API_url + 'athlete/activities'
    activities = requests.get(activites_url, headers=header, params=param).json()
    return activities

# - pass the  access token, get request for all the users segment efforts
def get_all_segment_efforts(Strava_API_url,header,param, SegmentID):
    activites_url = Strava_API_url + '/segments/'+ SegmentID + '/all_efforts'
    #Returns a list of all efforts  for a specific segement
    activities = requests.get(activites_url, headers=header, params=param).json()
    return activities

# - pass the  access token, get request for selected streams for the selected activity id
def get_streams(Strava_API_url,header, ID,Url_String, streams_type):
    # stream are comma seperated list
    if streams_type is not None:
        streams_type = ",".join(streams_type)
        activites_url = Strava_API_url + Url_String +'/' + ID + '/streams?keys=' + (streams_type) +'&key_by_type=true'
        #Returns a list of all efforts  for a specific segement
        activities = requests.get(activites_url, headers=header).json()

    return activities


#https://www.strava.com/api/v3/segment_efforts/:id/streams?keys=watts,heartrate,cadence&key_by_type=true


def create_data_frame(alist,types,filter_type, conversions,lst_time,lst_speed):
    #Get nested dict & lists: get the headers from the df for all col that are type Dic or Type List, create new dict of these
    #items to loop through
        outputlist = {}
        for keys in dict(alist[0]).keys():
                if isinstance(alist[0][keys], (dict,list)):
                    outputlist.update({keys:type(alist[0][keys])})

        #Convert the list passed to a dataframe
        if 'type' in dict(alist[0]).keys():  #filter out activity type when using the strava activities data
            df_unfiltered = pd.DataFrame.from_dict(alist)
            filter_mask = df_unfiltered.type.isin(filter_type)
            df = df_unfiltered.loc[filter_mask].copy(deep=True)#filter the DF for the selected activity type.
            #TO DO create deep copy, with practice use locto clean this u

            #two other methods i used caused chained indexing warnings - see pandas  boot camp filtering data frames
        else: #if strava segment efforts data
            df = pd.DataFrame.from_dict(alist)

        #extract the nested dict, list, create a new col + data to dF and then append to the df
        for columns in df:

            #unpack disctionaries and lists
            if columns in outputlist.keys() and outputlist[columns] == dict:
                #for each of the keys in the dictionry add the column to the df, then drop the original col from the DF
                for item in alist[0][columns]:
                    #add a new col to df, original col name + dict key, use list comprehesion to populate the data
                    df[columns+'_'+item] = [x[item] for x in df[columns]]

                df.drop(columns,axis=1, inplace=True)#drop the uneeded col
            if columns in outputlist.keys() and outputlist[columns] == list and type(alist[0][columns][0]) == dict:#convert list of dic to df columns
                dictkeys = alist[0][columns][0].keys()#get keys for the list of dict's
                for key in dictkeys:#loop through each key
                    for k, row in df[[columns]].iterrows():#loop through each row and return the dictionary append to DF
                        if df[columns][k] == []:#if the list is empty put empty string
                            df.loc[k,columns + '_' + key] = ''
                        else:
                            dictval = df[columns][k][0].get(key)#retun the value from the selected dic in the list
                            df.loc[k,columns + '_' + key] =  dictval #add the value at the index in a new col
                df.drop(columns, axis=1, inplace=True)  # drop the uneeded col

            #tidy up virtual ride flags as this does not get maintined correctly in strava
            print('cleaning up ride type.....')
            #creat mask to help identify the type of virtual ride.
            search_for = ['trainerroad','zwift']
            mask1 = df.external_id.str.contains('|'.join(search_for),case=False, regex=True)
            mask2 = df.trainer == True
            #Mask three is use to spot any virtual rides that are not trainer road or strava
            mask3 = df.external_id.str.contains('|'.join(search_for),case=False, regex=True) == False
            df.loc[mask1|mask2,'type'] = "VirtualRide"
            df.loc[mask1|mask2, 'trainer'] = "True"
            mask_TR = df.external_id.str.contains('trainerroad', case=False, regex=True)
            mask_Z = df.external_id.str.contains('zwift', case=False, regex=True)
            #get the ride type into the ride type series
            df.loc[mask_TR,'ride_type'] = "trainerroad"
            df.loc[mask_Z, 'ride_type'] = "zwift"
            mask4 = np.logical_and(mask2 , mask3) #create a new mask - if not trainer road or zwift but using a trainer label inside.
            df.loc[mask4, 'ride_type'] = "inside"
            df.loc[df['ride_type'].isnull(), 'ride_type'] = "outside"

            #apply unit conversions for time and speed
            if columns in lst_time:
                df[columns] = df[columns] / conversions.get('sec_to_mins')  # convert from m per sec to kph
            elif columns in lst_speed:
                df[columns] = df[columns] * conversions.get('msec_to_kph')  # convert from m per sec to kph
        return df

#Combinestreams into a single df, add addtional fields, conver fields
def create_stream_df(dict_streams,id, act_name, act_type, act_start_date,act_trainer,conversions,lst_time,lst_speed):

    df = pd.DataFrame()
#convert dictioanries into datafarmes
    for i,j in dict_streams.items():
            if i in lst_time:
                print('time')
            elif i in lst_speed:
                df[i] = j['data']
                df[i] = df[i] * conversions.get('msec_to_kph')#convert from m per sec to kph
            elif i == 'latlng':#split lat / long into different streams
                df['lat'] = list(map(split_lat, j['data']))
                df['lng'] = list(map(split_long, j['data']))
            else:
                  df[i] = j['data']

#add addtional data for each stream
    df['id'] = id
    df['type'] = act_type
    df['name'] = act_name
    df['start_date']= act_start_date
    df['trainer'] = act_trainer

    return df


#Split latitude and longditude
def split_lat(series):
    if series != '':
        lat = series[0]
        return lat
    else:
        lat = ''
        return lat

def split_long(series):
    if series != '':
        long = series[1]
        return long
    else:
        long = ''
        return long

#Write a DF to CSV
def Write_Data_to_csv(Data, FileName,date_stamp):
    cwd = os.getcwd()#get the current working directory
    # Save all data files to a sub directory to aviod cultering
    filename = cwd + '/data/'+ FileName+ '_' + str(date_stamp.strftime('%Y%m%d%H%M%S')) + '.csv'
    Data.to_csv(filename)


#Authenticate the user and login
header = Authenticate(auth_url,payload)#authnticate the user

#DATA REQUESTS---------------------------------------
#GETACTIVITIES - Get the number of activities request for the user. Pasing parameters created in POSTMAN (https://www.getpostman.com/collections/eecc146847e1afa20364)
print('creating activities '+' '+ str(datetime.today()))
last_date = datetime.today()  # set date to today for the first loop. Then stop when you run out of data or activities go beyond the date set
last_date = last_date.replace(tzinfo=local_zone)
while True:#loop while strava is still returning activities.

    param = {'before': epoch_act_start_date, 'per_page': number_activities, 'page': page_number}#pass the payload for the get request
    lst_activity_dataset = get_activities(Strava_API_url,header,param )#getactivities  list

    # if no results then exit loop or
    #if page_number>3 or last_date <= last_ride_date:
    if not lst_activity_dataset or last_date <= last_ride_date:
         break
    df_temp = create_data_frame(lst_activity_dataset, types, Filter_type,conversions,lst_time,lst_speed)

    if page_number ==1:
        df_activity_dataset = df_temp.copy(deep=True)
    else:
        df_activity_dataset = pd.concat([df_activity_dataset,df_temp])

    last_date = parse(df_activity_dataset['start_date'].iloc[-1])#get the last date from the current page

    page_number +=1 #increment the page

#convert the json data requested to a DF and then export to CSV
Write_Data_to_csv (df_activity_dataset, 'Activities',date_stamp)
print('last activity date ' + str(last_date))
print('finished activities'+' '+ str(datetime.today()))

#GET ACTIVITY STREAMS
print('creating activity streams'+' '+ str(datetime.today()))
#get activity streams for each activity
row_index = 1
for index, row  in df_activity_dataset.iterrows(): #get the activity information to append to each stream df
    #row_index = df_activity_dataset.index.get_loc(index)
    id = str(row['id'])
    act_name = row['name']
    act_type = row['type']
    act_trainer = row['trainer']
    act_start_date = row['start_date']
    print('index ' + str(index) + ' activity id ' + str(id) + ' activity date ' + act_start_date+ ' ' + act_name)
    dict_activity_streams = get_streams(Strava_API_url,header,id,"activities",stream_type)
    df_temp =  create_stream_df(dict_activity_streams,id, act_name, act_type, act_start_date,act_trainer,conversions,lst_time,lst_speed)

    if row_index == 1:
        df_activity_streams = df_temp.copy(deep=True)
    else:
        df_activity_streams = pd.concat([df_activity_streams, df_temp])#append the new stream df to to the exitsing one
    row_index +=1

Write_Data_to_csv (df_activity_streams, 'Activity_streams',date_stamp)
print('finished activity streams'+' '+ str(datetime.today()))

#GET SEGMENT EFFORTS
#Output segment effort list
#lst_all_segment_effort = get_all_segment_efforts(Strava_API_url,header,param,SegmentId )#GetAllSegmentEfforts
#convert the json data requested to a DF and then export to CSV
#Write_Data_to_csv (CreateDataFrame(lst_all_segment_effort,effort_types,None), 'ListSegmentEffort')

#GET SEGMENT EFFORT STREAMS
# 'segment_efforts/'
#output segment effort streams
#df_All_SegmentEffortStreams = GetSegmentEffortStreams(Strava_API_url,header,param,SegmentEffortid,streams)#GetAllSegmentEfforts
#convert the json data requested to a DF and then export to CSV
#Write_Data_to_csv (CreateDataFrame(df_All_SegmentEffortStreams,effort_Types ), 'ListSegmentEffortStreams')

print (str(datetime.today())+' finished' )