import subprocess
import pandas as pd
import json
import os 
import shutil
import pathlib 

OUTPUT_DIR = 'Data/csv';
SOURCE_DIR = 'Data/tweet'

def clearDir (name):
    print('clear ' + name)
    # clean the directory tweet
    if os.path.isdir(name):
        shutil.rmtree(name)

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

    clearDir(SOURCE_DIR)
    clearDir('Data/user')
    # crawl 
    print ('start Crawler')
    subprocess.call(args)
    print('end Crawler')

def tweetsSources ():
    return [
        os.path.join(SOURCE_DIR,str(file)) 
        for file 
            in os.listdir(SOURCE_DIR) 
            if str(file).isdigit()
    ]

def saveCSV (name, tweets):
    
    print('save ',len(tweets),' tweets as csv');

    df = pd.DataFrame(tweets, columns=['ID','datetime','text','user_id','usernameTweet'])

    df = df.replace({'\n': ' '}, regex=True) # remove linebreaks in the dataframe
    df = df.replace({'\t': ' '}, regex=True) # remove tabs in the dataframe
    df = df.replace({'\r': ' '}, regex=True) # remove carriage return in the dataframe

    savePath = os.path.join(OUTPUT_DIR,name + ".csv")

    # Export to csv
    df.to_csv(savePath) 

def collect (name):
    print('collect Tweets')
    tweets = []
    for src in tweetsSources():
        with open(src, encoding='utf-8') as tweetfile:
            tweets.append(json.loads(tweetfile.read()))

    saveCSV(name,tweets)

def processQuery (query):
    print('start query: ',query, sep='\n')
    callScrapy(query);

df = pd.read_excel("Data/data.xlsx")

def processRow (content):
    tag = content['name'] 
    start_date = str(content['start_date']).split(' ')[0]
    end_date = str(content['end_date']).split(' ')[0]
    query = 'query=#'+ tag + ' since:' + start_date + ' until:' + end_date
    return (tag + ' ' +  start_date + ' ' + end_date ,query);

def batchQueries ():
    return [processRow(content) for _, content  in df.iterrows()]

def main ():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    for tag ,query in batchQueries():
        processQuery(query);
        collect(tag)
    print('finished')

main()