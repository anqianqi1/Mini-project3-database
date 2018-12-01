import tweepy
import os
import sys
import subprocess
import urllib.request

import io
import os
from google.cloud import vision
from google.cloud.vision import types

import mysql.connector
import pymongo

#These are my Twitter API key
consumer_key = ""
consumer_secret = ""
access_key = ""
access_secret = ""


#This is the code which one I want to get all of tweets from my twitter account
def get_all_tweets(screen_name):
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)
    
    
    alltweets = []
    
    
    new_tweets = api.user_timeline(screen_name = screen_name, count = 20, include_rst = False, exclude_replies = True)
    
    
    alltweets.extend(new_tweets)
    
    
    oldest = alltweets[-1].id - 1
    
    
    while len(new_tweets) > 0:
        new_tweets = api.user_timeline(screen_name = screen_name, count = 20, max_id = oldest, include_rst = False, exclude_replies = True)
        
        alltweets.extend(new_tweets)
        
        oldest = alltweets[-1].id - 1
        if(len(alltweets) > 15):
            break
        print("...%s tweets downloaded so far" % (len(alltweets)))
        
    # return alltweets
    image_url = set()
    # print (alltweets)
    for post in alltweets:
        media = post.entities.get('media',[])
        if(len(media) > 0):
            image_url.add(media[0]['media_url'])
    # print(image_url)
            
    for i, url in enumerate(image_url):
        urllib.request.urlretrieve(url,'/home/ece-student/EC 601/image/'+str(i)+'.jpg')
    return(image_url)

def detect_labels(file_name):
    """Detects labels in the file."""
    client = vision.ImageAnnotatorClient()

# 
#    file_name = os.path.join(
 #       os.path.dirname('_file_'),
 #       'img000.jpg')
    # for file in os.listdir():
    #     if file.endswith(".jpg"):
    #         file_name=os.path.join(os.listdir(),file)

    # file_name='./img000.jpg'
    
    with io.open(file_name, 'rb') as image_file:
        content = image_file.read()

    image = vision.types.Image(content=content)

    response = client.label_detection(image=image)
    labels = response.label_annotations
    # print('Labels:')

    for label in labels:
        return(label.description)


if __name__ == '__main__':

    twittername='@anqiguo3'
    get_all_tweets(twittername)

    url=[]
    for i in get_all_tweets("@anqiguo3"):
    #     url=[i]
    # print(url)
        url.append(i)
    # print(url)

 
#     this is step is to convert image urls and labels into a two dimentional array which is the form to store data into mysql
    label=[]
    for i in range(7):
        label.append(detect_labels(str(i)+'.jpg'))
    # print(label)

    database=[]
    database = [list(a) for a in zip(url, label)]
    # print(database)

# mysql database
# to create a mysql database
    mydb= mysql.connector.connect(
    host="localhost",
    user="username",
    passwd="userpassword",
    database="twitterdata"
    )
    print(mydb)

    
# to create database and tables
    mycursor=mydb.cursor()
    mycursor.execute("CREATE DATABASE twitterdata")
    mycursor.execute("SHOW DATABASES")
    for db in mycursor:
        print(db)
    mycursor.execute("CREATE TABLE anqiguo3 (image VARCHAR(225) , Labels VARCHAR(225))")

    mycursor.execute("SHOW TABLES")
    for tb in mycursor:
        print(tb)

# to store data into mysql 
    for i in range(0,len(database)):
        print(database[i][0])
        sqlFormula = "INSERT INTO anqiguo3(image,Labels) VALUES (%s,%s)"
        temp=database[i][0],database[i][1]
        # print(temp)
        mycursor.execute(sqlFormula,temp)
        mydb.commit()

    databasedict=dict(database)
    # for key in databasedict :   
    #     print({key:databasedict[key]})



# mongodb data
# using mongodb to store data, creat database and tables
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["twitterdata"]
    mycol = mydb["anqiguo3"]
    for key in databasedict :   
        # print({key:databasedict[key]})
        x = mycol.insert_one({key:databasedict[key]})
