import subprocess
import pandas as pd
import json
import os
import shutil
import pathlib 

def callScrapy(query): 

    args =  ['scrapy'
            ,'crawl' 
            ,'TweetScraper'
            ,'-a' 
            , query
            ,'-a'
            ,'lang="en"'
            ,'-a' 
            ,'crawl_user="true"'
            ]

    # clean the directory tweet
    if os.path.isdir('Data/tweet'):
        shutil.rmtree('Data/tweet/')
    
    # crawl 
    subprocess.call(args)


# -*- coding: utf-8 -*-
""" on Sat Apr  4 11:01:16 2020
https://medium.com/@kevin.a.crystal/scraping-twitter-with-tweetscraper-and-python-ea783b40443b
"""
def collect (name):
    tweets = []
    for file in os.listdir('Data/tweet/'):
        filename = 'Data/tweet/' + str(file)
        if filename[7:10].isdigit():
            with open(filename, encoding='utf-8') as tweetfile:
                pyresponse = json.loads(tweetfile.read())
                tweets.append(pyresponse)

    df = pd.DataFrame(tweets, columns=['ID','datetime','text','user_id','usernameTweet'])

    df = df.replace({'\n': ' '}, regex=True) # remove linebreaks in the dataframe
    df = df.replace({'\t': ' '}, regex=True) # remove tabs in the dataframe
    df = df.replace({'\r': ' '}, regex=True) # remove carriage return in the dataframe

    # Export to csv
    df.to_csv("/Data/" + name + ".csv") 


def processQuery (query):
    print('start : ',query, sep='\n')
    callScrapy(query);

df = pd.read_excel("Data/data.xlsx")

def processRow (content):
    tag = content['name'] 
    start_date = str(content['start_date']).split(' ')[0]
    end_date = str(content['end_date']).split(' ')[0]
    query = 'query=#'+ tag + ' since:' + start_date + ' until:' + end_date
    return (tag,query);
def batchQueries ():
    return [processRow(content) for _, content  in df.iterrows()]

def main ():
    for tag ,query in batchQueries():
        processQuery(query);
        collect(tag)
    print('finished')

main()