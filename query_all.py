import subprocess
import pandas as pd
 
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

    subprocess.call(args)

def processQuery (query):
    print('start : ',query, sep='\n')
    callScrapy(query);

df = pd.read_excel("Data/data.xlsx")

def processRow (content):
    tag = content['#'] 
    start_date = str(content['start_date']).split(' ')[0]
    end_date = str(content['end_date']).split(' ')[0]
    query = 'query='+ tag + ' since:' + start_date + ' until:' + end_date
    return query;

def batchQueries ():
    return [processRow(content) for _, content  in df.iterrows()]

def main ():
    for query in batchQueries():
        processQuery(query);
    print('finished')

main()