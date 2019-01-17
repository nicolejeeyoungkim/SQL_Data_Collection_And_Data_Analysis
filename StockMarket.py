# -*- coding: utf-8 -*-
"""
Created on Wed Jul  5 11:50:16 2017

@author: DSL
d"""
import requests
import psycopg2
import json



i=0
a=0
b=0
url = 'https://api.nytimes.com/svc/search/v2/articlesearch.json'


conn = psycopg2.connect("host=XXX dbname=data user=XXX password=XXX")
cur = conn.cursor()
cur.execute("SELECT COUNT(NewsID) FROM NewsSearchStockMarket0731;")
z = cur.fetchone()
cur.execute("SELECT COUNT(NewsID) FROM NewsSearchNasdaq0803;")
y = cur.fetchone()
cur.execute("SELECT COUNT(NewsID) FROM NewsSearchNewYorkStockExchange;")
x = cur.fetchone()
cur.execute("SELECT COUNT(NewsID) FROM NewsSearchLondonStockExchange;")
w = cur.fetchone()

# name of first table (made by John) is finance_nlp_dp
# name of second table made by me is People


# ALTER SEQUENCE NewsSearchStockMarket0731_ID_seq RESTART WITH next_number in table;
# change date
# 
print ('done')
    # other API key: e5487d8d8e5b4234a143e2ca6cf0543b
    #ca5c6afbd44d4c09ac938a51c09a6fda

# stopped at 2016/11/29 because of score instead of lead paragraph
for i in range(121):
    print i    
    param = {'fl':'_id,snippet,headline,pub_date', 'api-key':'e5487d8d8e5b4234a143e2ca6cf0543b', 'q': 'stock market','begin_date':'20170812', 'sort': 'oldest', 'page': i}
    r = requests.get('https://api.nytimes.com/svc/search/v2/articlesearch.json', params = param)
    print(r.status_code)
    newsSnippet = r.text
    print(r.url)
    data = json.loads(newsSnippet)
    
    for num in range(10):
                
        _id = data['response']['docs'][num]['_id']
        
        headline = data['response']['docs'][num]['headline']['main']
        pubDate = data['response']['docs'][num]['pub_date']
        
        print ''
        
        print num
        snippet = data['response']['docs'][num]['snippet']      
        only = True
        for a in range(16000, z[0]+1):
            cur.execute("SELECT NewsID FROM NewsSearchStockMarket0731 WHERE ID = %s", [a])
            check = cur.fetchone() 
            
            if _id == check[0]:
                only = False

        # Nasdaq
        for a in range(200, y[0]+1):
            cur.execute("SELECT NewsID From NewsSearchNasdaq0803 WHERE ID = %s", [a])
            compare = cur.fetchone()
            
            if _id == compare[0]:
                only = False        
        # New York Stock Exchange
        for a in range(250, x[0]+1):
            cur.execute("SELECT NewsID From NewsSearchNewYorkStockExchange WHERE ID = %s", [a])
            compare = cur.fetchone()
            
            if _id == compare[0]:
                only = False
        # London Stock Exchange
        for a in range(1000, w[0]+1):
            cur.execute("SELECT NewsID From NewsSearchLondonStockExchange WHERE ID = %s", [a])
            compare = cur.fetchone()
            
            if _id == compare[0]:
                only = False
            
        if only==True:
            cur.execute("INSERT INTO newssearchstockmarket0731 (PubDate, Headline, leadParagraph, NewsID) VALUES (%s,%s,%s,%s);",[pubDate, headline, snippet, _id])
            conn.commit()
            cur.execute("SELECT COUNT(NewsID) FROM NewsSearchStockMarket0731;")            
            z = cur.fetchone()
            
            cur.execute("SELECT COUNT(NewsID) FROM NewsSearchNewYorkStockExchange;")
            x = cur.fetchone()
            cur.execute("SELECT COUNT(NewsID) FROM NewsSearchLondonStockExchange;")
            w = cur.fetchone()

    
    

    



