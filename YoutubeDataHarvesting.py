# ================================================= / IMPORT LIBRARY / ============================================
# [Youtube API Library]
import googleapiclient.discovery
import googleapiclient.errors

# [pandas]
import pandas as pd

# [File Handling libraries]
import re

# [Dashboard Library]
import streamlit as st
from PIL import Image

# [SQL Library]
import mysql.connector
import pymysql
from sqlalchemy import create_engine

import pymongo
from urllib.parse import quote_plus
from tabulate import tabulate
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import psycopg2

# ======================================  /     DATA COLLECTION ZONE    /   =========================================

# Data collection zone

#connecting to mysql
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="123456"

)
mycursor = mydb.cursor()
Engine = create_engine('mysql+pymysql://root:123456@localhost/youtube_data')

#Function for API connection
def Api_Connect():
    api_service_name = "youtube"
    api_version = "v3"
    api_key = "xxx"
    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
    return youtube
youtube = Api_Connect()


def youtube_data(channel_id):
    ch=Channel_data(youtube,channel_id)
    vi=Video_data(youtube,ch[1])
    pl=Playlist_data(youtube,channel_id)
    co=Comments_data(youtube,vi[1])
    final = {"Channel_id":channel_id,"Channel_data":ch[0],"Video_data":vi[0],"Playlist_data":pl,"Comments_data":co}
    return final

# Function to retrieve channel data
def Channel_data(youtube,channel_id):
    channel_request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    channel_response = channel_request.execute()
    channel_data=[]
    item = channel_response["items"][0]
    channel_details = dict(Channel_name=channel_response["items"][0]['snippet']['title'],
                        Channel_id=channel_id,
                        Subscription_count=item['statistics']['subscriberCount'],
                        Channel_Views=item['statistics']['viewCount'],
                        Video_count=item['statistics']['videoCount'],
                        PublishedAt=item['snippet']['publishedAt'],
                        Channel_description=item['snippet']['description'],
                        Channel_Status=item.get("status",{}).get("privacyStatus","NotAvailable"),
                        Channel_Type=channel_response["kind"],
                        Playlist_id=item['contentDetails']['relatedPlaylists']['uploads'])
    channel_data.append(channel_details)
    Playlist_id = item['contentDetails']['relatedPlaylists']['uploads']
    return channel_data,Playlist_id

# Function to retrieve Video data
def Video_data(youtube,Playlist_id):
    next_page_token = None
    videoid = []
    while 1:
        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=Playlist_id,
            maxResults=50,
            pageToken=next_page_token)
        response = request.execute()
        videoid = videoid + response["items"]
        next_page_token = response.get("nextPageToken")
        if next_page_token is None:
            break
    video_ids = []
    for i in range(len(videoid)):
        video_ids.append(videoid[i]["contentDetails"]["videoId"])

    video_data = []
    for id in video_ids:
        video_request = youtube.videos().list(
            part='statistics,snippet,contentDetails',
            id=id)
        video_response = video_request.execute()
        item = video_response["items"][0]
        video_details = dict(
            Channel_Name=item['snippet']['channelTitle'],
            Channel_id=item['snippet']['channelId'],
            Video_id=item['id'],
            Video_Name=item['snippet']['title'],
            Video_Description=item['snippet'].get('description'),
            Tags=item['snippet'].get('tags', []),
            Published_At=item['snippet']['publishedAt'],
            View_Count=item['statistics'].get('viewCount', 0),
            Like_Count=item['statistics'].get('likeCount', 0),
            Dislike_Count=item['statistics'].get('dislikeCount', 0),
            Favorite_Count=item['statistics'].get('favoriteCount', 0),
            Comment_Count=item['statistics'].get('commentCount', 0),
            Duration=convert_duration(item['contentDetails']['duration']),
            Thumbnail=item['snippet']['thumbnails']['default']['url'],
            Caption_Status=item['contentDetails'].get('caption', "NotAvailable")
        )
        video_data.append(video_details)
    return video_data,video_ids

# Function to convert duartion to specified format
def convert_duration(duration):
    # Define a regular expression pattern to match ISO 8601 duration format
    reg_exp = r"PT(\d+H)?(\d+M)?(\d+S)?"

    # Use re.match to search for a match to the pattern in the duration string
    match = re.match(reg_exp,duration)

    # If no match is found, return '00:00:00'
    if not match:
        return "00:00:00"

    # Convert hours, minutes, and seconds to integers (or 0 if not present)
    hours = int(match.group(1)[:-1]) if match.group(1) else 0
    minutes = int(match.group(2)[:-1]) if match.group(2) else 0
    seconds = int(match.group(3)[:-1]) if match.group(3) else 0

    # Calculate total seconds
    total_seconds = hours*3600 + minutes*60 + seconds

    # Format total_seconds into HH:MM:SS
    return "{:02d}:{:02d}:{:02d}".format(int(total_seconds/3600),int((total_seconds%3600)/60),int(total_seconds%60))

# Function to retrieve playlist data
def Playlist_data(youtube,channel_id):
    next_page_token = None
    data = []
    while True:
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        data = data + response["items"]
        next_page_token = response.get("nextPageToken")
        if next_page_token is None:
            break
    playlist_data = []
    for i in range(len(data)):
        playlist_details = dict(
            Playlist_id=data[i]['id'],
            Playlist_name=data[i]['snippet']['title'],
            Channel_id=data[i]['snippet']['channelId'],
            Channel_Name=data[i]['snippet']['channelTitle'],
            PublishedAt=data[i]['snippet']['publishedAt'],
            Video_Count=data[i]['contentDetails']['itemCount']
        )
        playlist_data.append(playlist_details)
    return playlist_data

# Function to retrieve comments data
def Comments_data(yotube,video_ids):
    comment=[]
    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50,
                textFormat="plainText"
            )
            response = request.execute()
            comment = comment+response["items"]
        except:
            pass
    comment_data = []
    for i in range(len(comment)):
        comment_details = dict(
            Video_id=comment[i]["snippet"]["videoId"],
            Comment_id=comment[i]["snippet"]["topLevelComment"]["id"],
            Comment_Text=comment[i]['snippet']['topLevelComment']['snippet']['textDisplay'],
            Comment_Author=comment[i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
            Comment_PublishedAt=comment[i]['snippet']['topLevelComment']['snippet']['publishedAt']
        )
        comment_data.append(comment_details)
    return comment_data
# create a client instance of MongoDB
# Define your username, password, and cluster address
username = "username"
password = "password"
cluster_address = "ac-z5l2sxl-shard-00-00.a4spb07.mongodb.net"

# Encode the username and password
encoded_username = quote_plus(username)
encoded_password = quote_plus(password)

# Construct the MongoDB connection URI with the encoded username and password
uri = f"mongodb+srv://{encoded_username}:{encoded_password}@{cluster_address}/?retryWrites=true&w=majority"

# Create a MongoClient instance using the connection URI
client = pymongo.MongoClient(uri)
a = pymongo.MongoClient(uri)

# Function to store data in MongoDB
def mongodb(datalake):
    a.projects.youtube_data.insert_one(datalake)
# ========================================= / DATA MIGRATION ZONE / ===================================================
# Function to insert data into SQL
def insert_to_SQL(channel_id):
    try:
        mycursor.execute("use Youtube_Data")
    except:
        mycursor.execute("create database Youtube_Data")
        mycursor.execute("use Youtube_Data")

    # Query MongoDB for channel data
    coll1 = list(a.projects.youtube_data.find({"Channel_id":channel_id},{"_id":False}))
    # Check if the list is empty
    if not coll1:
        print("No data found for channel id:",channel_id)
        return  # or handle the empty result as needed

    # Extract channel data from MongoDB result
    for i in coll1:
        channel_table = i["Channel_data"]
        video_table = i["Video_data"]
        playlist_table = i["Playlist_data"]
        comment_table = i["Comments_data"]

    ch_df = pd.DataFrame(channel_table)   #create dataframe of channel table
    ch_df["Channel_name"]=ch_df["Channel_name"].astype(str)
    ch_df["Channel_id"]=ch_df["Channel_id"].astype(str)
    ch_df["Subscription_count"]= ch_df["Subscription_count"].astype(int)
    ch_df["Channel_Views"]=ch_df["Channel_Views"].astype(int)
    ch_df["Video_count"]=ch_df["Video_count"].astype(int)
    ch_df["PublishedAt"]=ch_df["PublishedAt"].astype(str)
    ch_df["Channel_description"]=ch_df["Channel_description"].astype(str)
    ch_df["Channel_Status"]=ch_df["Channel_Status"].astype(str)
    ch_df["Channel_Type"]=ch_df["Channel_Type"].astype(str)
    ch_df["Playlist_id"]=ch_df["Playlist_id"].astype(str)

    try:
        ch_df.to_sql("channel",Engine,if_exists="replace",index=False)
    except:
        pass

    vi_df = pd.DataFrame(video_table)  #create dataframe of video table
    for i in range(len(vi_df["Comment_Count"])):
        if(vi_df.iloc[i,9])==None:
            vi_df.iloc[i, 9]=0

    vi_df["Channel_Name"]=vi_df["Channel_Name"].astype(str)
    vi_df["Channel_id"]=vi_df["Channel_id"].astype(str)
    vi_df["Video_id"]=vi_df["Video_id"].astype(str)
    vi_df["Video_Name"]=vi_df["Video_Name"].astype(str)
    vi_df["Video_Description"]=vi_df["Video_Description"].astype(str)
    vi_df["Tags"]=vi_df["Tags"].astype(str)
    vi_df["Published_At"]= vi_df["Published_At"].astype(str)
    vi_df["Published_At"] = vi_df["Published_At"].str.replace("Z","")
    vi_df["Published_At"] = vi_df["Published_At"].str.replace("T"," ")
    vi_df["View_Count"]=vi_df["View_Count"].astype(int)
    vi_df["Like_Count"]=vi_df["Like_Count"].astype(int)
    vi_df["Dislike_Count"]=vi_df["Dislike_Count"].astype(int)
    vi_df["Favorite_Count"]=vi_df["Favorite_Count"].astype(int)
    vi_df["Comment_Count"]=vi_df["Comment_Count"].astype(int)
    vi_df["Duration"]=vi_df["Duration"].astype(str)
    vi_df["Thumbnail"]=vi_df["Thumbnail"].astype(str)
    vi_df["Caption_Status"]=vi_df["Caption_Status"].astype(str)

    try:
        vi_df.to_sql("videos",Engine,if_exists="replace",index=False)
    except:
        pass

    pl_df = pd.DataFrame(playlist_table) #create dataframe of playlist table
    try:
        pl_df["Playlist_id"]=pl_df["Playlist_id"].astype(str)
        pl_df["Playlist_name"]= pl_df["Playlist_name"].astype(str)
        pl_df["Channel_id"]=pl_df["Channel_id"].astype(str)
        pl_df["Channel_Name"]=pl_df["Channel_Name"].astype(str)
        pl_df["PublishedAt"]=pl_df["PublishedAt"].astype(str)
        pl_df["Video_Count"]=pl_df["Video_Count"].astype(int)
    except:
        pass

    try:
        pl_df.to_sql("playlist",Engine,if_exists="replace",index=False)
    except:
        pass

    co_df = pd.DataFrame(comment_table) #create dataframe of comment table

    try:
        co_df["Video_id"]=co_df["Video_id"].astype(str)
        co_df["Comment_id"]=co_df["Comment_id"].astype(str)
        co_df["Comment_Text"]=co_df["Comment_Text"].astype(str)
        co_df["Comment_Author"]=co_df["Comment_Author"].astype(str)
        co_df["Comment_PublishedAt"]=co_df["Comment_PublishedAt"].astype(str)
    except:
        pass

    try:
        co_df.to_sql("comments",Engine,if_exists="replace",index=False)
    except:
        pass

def fetch_channel_ids():
    db = client['projects']  # Replace 'your_database_name' with your actual database name
    collection = db['youtube_data']  # Replace 'youtube_data' with your actual collection name
    cursor = collection.find({}, {'Channel_id': 1})  # Fetch only the Channel_id field
    channel_ids = [doc['Channel_id'] for doc in cursor]
    return channel_ids


# Function to create channel,playlist,video,comment table
def databasestructure():
    try:
        mycursor.execute("use Youtube_Data")
        mycursor.execute("""alter table channel modify column channel_id varchar(255) primary key,
                         modify column channel_name varchar(255),modify column channel_type varchar(255),
                         modify column channel_views int,modify column channel_description text,
                         modify column channel_status varchar(255)""")

        mycursor.execute("""alter table playlist modify column playlist_id varchar(255) primary key,
                        modify column channelid varchar(255), 
                        modify column playlist_name varchar(255)""")
        mycursor.execute("alter table playlist add constraint FK_chID foreign key(channelid) references channel(channel_id)")

        mycursor.execute("""alter table video modify column video_id varchar(255) primary key,
                        modify column channelid varchar(255), modify column video_name varchar(255),
                        modify column video_description text,modify column publishedAt Datetime,
                        modify column view_count int, modify column like_count int, modify column dislike_count int,
                        modify column favorite_count int, modify column comment_count int, 
                        modify column duration varchar(255),
                        modify column thumbnail varchar(255), modify column caption_status varchar(255)
                        """)
        mycursor.execute("alter table video add constraint FK_vdID foreign key(channelid) references channel(channel_id) ")

        mycursor.execute("""alter table comment modify column comment_id varchar(255), modify column videoid varchar(255),
                        modify column comment_text text, modify column comment_author varchar(255), 
                        modify column comment_publishedAt Datetime
                        """)
        mycursor.execute("alter table comment add constraint FK_coID foreign key(videoid) refernces video(video_id)")

        mycursor.execute("update video SET duration = TIME_TO_SEC(duration) ")

    except:
        pass

# ====================================================/ DASHBOARD / ===================================================
st.set_page_config(layout="wide")
with st.sidebar:
    select = st.selectbox("",
                          ("Home","Channel Data Scraping")
                          )
if select == "Home":
    st.title(":red[YOUTUBE DATA HARVESTING]")
    st.subheader(":violet[A Simple project helps in retrieving data from the YouTube API, storing it in a MongoDB datalake, migrating data to a SQL data warehouse, querying the data warehouse with SQL, and displaying the data in the Streamlit app.]")
    col1,col2 = st.columns(2)
    with col2:
        st.image(Image.open(r"C:\Users\Dell\OneDrive\Desktop\DS_Course\Projects\Youtube_Data_Harvesting\youtube_api.jpg"),width =500)
    with col1:
        st.header(":blue[Skills Take Away]")
        st.subheader(":gray[Python Scripting]")
        st.subheader(":gray[Data Collection]")
        st.subheader(":gray[MongoDB]")
        st.subheader(":gray[API Integration]")
        st.subheader(":gray[Data Management using MongoDB and SQL]")
        st.subheader(":gray[Streamlit]")
elif select == "Channel Data Scraping":
    st.title(":red[YOUTUBE DATA HARVESTING]")
    tab1,tab2=st.tabs(["Data Collection", "Data Query"])
# ========================================================= / Data Collection / ===============================================================
    with tab1:
        col1,col2 = st.columns(2)

        # Data Collection Zone
        with col1:
            st.header(":blue[Data Collection Zone]")
            st.write(":grey[(Note: This zone collects data by using Channel ID and stores it using **MongoDB**)]")
            channel_id = st.text_input("**Enter Channel ID**")  # input channel_id
            st.write(":grey[To get data and store it, click button below  **Get Data and Store**]")
            if st.button("Get Data and Store",type="primary"):
                x = youtube_data(channel_id)
                mongodb(x)
                st.info("Data stored in MongoDB successfully")

        # Data Migration Zone
        with col2:
            st.header(":blue[Data Migration Zone]")
            st.write(":gray[(Note: Data stored using **MongoDB** will be migrated to **SQL warehouse**)]")
            st.write(":gray[To migrate data to sql, select the **Channel ID** and click button below **Migrate to SQL**]")
            #st.write("Select the channel IDs you want to migrate:")
            channel_ids = fetch_channel_ids()
            selected_channel_id = st.selectbox("Select Channel ID", channel_ids)
            if st.button("Migrate to SQL",type="primary"):
                if selected_channel_id:
                    try:
                        mycursor.execute("use youtube_data")
                        mycursor.execute("SELECT channel_id FROM channel WHERE channel_id = %s", (selected_channel_id,))
                        result = mycursor.fetchone()
                        if result:
                            st.text("Channel Data Already Exists")
                        else:
                            insert_to_SQL(selected_channel_id)
                            databasestructure()
                            st.text("Cool!")

                    except Exception as e:
                        st.error(f"Error: {e}")
                else:
                    st.warning("Please select at least one channel ID to migrate.")
# ================================================== / Data Query / =================================================================

    with tab2:
        selectbox = st.selectbox("",
                                 ("Channel Details", "Queries")
                                 )
# ==========================================/ Channel Details / ========================================================
        if (selectbox=="Channel Details"):
            st.header(":blue[Channel Data]")

            mycursor.execute("use youtube_data")
            mycursor.execute("SELECT channel_name from channel")
            channel_name=mycursor.fetchall()
            selected_channel_name=st.selectbox("**Select Channel Name**",channel_name)
            st.write(":gray[To get Channel Details, select **Channel Name** from the list]")
            if selected_channel_name:
                mycursor.execute("SHOW COLUMNS FROM channel")
                column_names = [column[0] for column in mycursor.fetchall()]
                mycursor.execute("SELECT * from channel where channel_name=%s",(selected_channel_name))
                channel_details=mycursor.fetchone()
                st.write("Channel Details:")
                st.table([column_names,channel_details])


# ============================================ /  QUESTIONS  / ======================================================
        elif (selectbox=="Queries"):
            st.header(":blue[Queries]")
            questions = st.selectbox("Questions: ", ["Please select one",
                                                     "What are the names of all the videos and their Corresponding Channels?",
                                                     "Which channels have the most no of videos and how many videos?",
                                                     "What are the top 10 most viewed videos & the respective channels?",
                                                     "How many comments were made on each video & the respective video names?",
                                                     "Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                     "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                     "What is the total number of views for each channel, and what are their corresponding channel names?",
                                                     "What are the names of all the channels that have published videos in the year 2022?",
                                                     "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                     "Which videos have the highest number of comments, and what are their corresponding channel names?"])

            if (questions=="Please select one"):
                st.text("Please Choose any one Query")
            elif questions=="What are the names of all the videos and their Corresponding Channels?":
                mycursor.execute("use youtube_data")
                mycursor.execute("""select videos.video_name,channel.channel_name from videos 
                                 INNER JOIN channel on videos.channel_id = channel.channel_id """)
                out = mycursor.fetchall()
                a = tabulate(out, headers=[i[0] for i in mycursor.description], tablefmt='psql')
                st.text("{}".format(a))

            elif questions=="Which channels have the most no of videos and how many videos?":
                mycursor.execute("Use youtube_data")
                mycursor.execute("""Select videos.channel_id, channel.channel_name, count(videos.video_id) 
                        from videos INNER JOIN channel on videos.channel_id=channel.channel_id 
                        group by videos.channel_id""")
                out = mycursor.fetchall()
                a = tabulate(out, headers=[i[0] for i in mycursor.description], tablefmt='psql')
                st.text("{}".format(a))

            elif questions=="What are the top 10 most viewed videos & the respective channels?":
                mycursor.execute("Use youtube_data")
                mycursor.execute("""SELECT videos.video_name, channel.channel_name, videos.view_count FROM videos
                                    INNER JOIN channel ON videos.channel_id = channel.channel_id
                                    ORDER BY videos.view_count DESC LIMIT 10""")
                out = mycursor.fetchall()
                a = tabulate(out, headers=[i[0] for i in mycursor.description], tablefmt='psql')
                st.text("{}".format(a))

            elif questions=="How many comments were made on each video & the respective video names?":
                mycursor.execute("Use youtube_data")
                mycursor.execute("""select comment_count,video_name 
                        from videos 
                        order by comment_count desc""")
                out = mycursor.fetchall()
                a = tabulate(out, headers=[i[0] for i in mycursor.description], tablefmt='psql')
                st.text("{}".format(a))

            elif questions=="Which videos have the highest number of likes, and what are their corresponding channel names?":
                mycursor.execute("Use youtube_data")
                mycursor.execute("""select videos.video_name,videos.like_count,channel.channel_name 
                       from videos INNER JOIN channel on videos.channel_id=channel.channel_id 
                       order by like_count desc limit 10""")
                out = mycursor.fetchall()
                a = tabulate(out, headers=[i[0] for i in mycursor.description], tablefmt='psql')
                st.text("{}".format(a))

            elif questions=="What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
                mycursor.execute("Use youtube_data")
                mycursor.execute("select like_Count+comment_count as Total, Video_name from videos")
                out = mycursor.fetchall()
                a = tabulate(out, headers=[i[0] for i in mycursor.description], tablefmt='psql')
                st.text("{}".format(a))

            elif questions=="What is the total number of views for each channel, and what are their corresponding channel names?":
                mycursor.execute("Use youtube_data")
                mycursor.execute("select channel_name,channel_views from channel ")
                out = mycursor.fetchall()
                a = tabulate(out, headers=[i[0] for i in mycursor.description], tablefmt='psql')
                st.text("{}".format(a))

            elif questions=="What are the names of all the channels that have published videos in the year 2022?":
                mycursor.execute("Use youtube_data")
                mycursor.execute("""SELECT DISTINCT channel.channel_name,videos.video_name, videos.published_At FROM videos 
                        INNER JOIN channel ON videos.channel_id = channel.channel_id 
                        WHERE YEAR(videos.published_At) = 2022 """)
                out = mycursor.fetchall()
                a = tabulate(out, headers=[i[0] for i in mycursor.description], tablefmt='psql')
                st.text("{}".format(a))

            elif questions=="What is the average duration of all videos in each channel, and what are their corresponding channel names?":
                mycursor.execute("Use youtube_data")
                mycursor.execute("""select  channel.channel_name, videos.channel_id, avg(videos.duration) as AvgDuration
                        from videos INNER JOIN channel on videos.channel_id=channel.channel_id
                        group by videos.channel_id""")
                out = mycursor.fetchall()
                a = tabulate(out, headers=[i[0] for i in mycursor.description], tablefmt='psql')
                st.text("{}".format(a))

            elif questions=="Which videos have the highest number of comments, and what are their corresponding channel names?":
                mycursor.execute("Use youtube_data")
                mycursor.execute("""select videos.video_name,videos.comment_count,channel.channel_name 
                        from videos INNER JOIN channel on videos.channel_id=channel.channel_id 
                        order by comment_count desc limit 10""")
                out = mycursor.fetchall()
                a = tabulate(out, headers=[i[0] for i in mycursor.description], tablefmt='psql')
                st.text("{}".format(a))

                



