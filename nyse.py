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


conn = psycopg2.connect("host=192.168.1.145 dbname=data user=data password=data321DATA")
cur = conn.cursor()
cur.execute("SELECT COUNT(NewsID) FROM NewsSearchStockMarket0731;")
z = cur.fetchone()
cur.execute("SELECT COUNT(NewsID) FROM NewsSearchNasdaq0803;")
y = cur.fetchone()
cur.execute("SELECT COUNT(NewsID) FROM NewsSearchNewYorkStockExchange;")
x = cur.fetchone()
cur.execute("SELECT COUNT(NewsID) FROM NewsSearchLondonStockExchange;")
w = cur.fetchone()

cur.execute("ALTER TABLE NewsSearchLondonStockExchange ADD Top1 text;")
conn.commit()
cur.execute("ALTER TABLE NewsSearchLondonStockExchange ADD Top2 text;")
conn.commit()
cur.execute("ALTER TABLE NewsSearchLondonStockExchange ADD Top3 text;")
conn.commit()
# name of first table (made by John) is finance_nlp_dp
# name of second table made by me is People


# ALTER SEQUENCE NewsSearchStockMarket0731_ID_seq RESTART WITH next_number in table;
# change date
# 
print ('done')
    # other API key: e5487d8d8e5b4234a143e2ca6cf0543b
    #ca5c6afbd44d4c09ac938a51c09a6fda

# stopped at 2016/11/29 because of score instead of lead paragraph
for i in range(120):
    print i    
    param = {'fl':'_id,snippet,headline,pub_date', 'api-key':'ca5c6afbd44d4c09ac938a51c09a6fda', 'q': 'New York Stock Exchange','begin_date':'20150604', 'sort': 'oldest', 'page': i}
    r = requests.get('https://api.nytimes.com/svc/search/v2/articlesearch.json', params = param)
    print(r.status_code)
    newsSnippet = r.text
    print(r.url)
    data = json.loads(newsSnippet)
    
    for num in range(10):
                
        _id = data['response']['docs'][num]['_id']
        
        headline = data['response']['docs'][num]['headline']['main']
        pubDate = data['response']['docs'][num]['pub_date']
        print pubDate
        print ''
        
        print num
        snippet = data['response']['docs'][num]['snippet']      
        only = True

        for a in range(3878, z[0]+1):
            cur.execute("SELECT NewsID FROM NewsSearchStockMarket0731 WHERE ID = %s", [a])
            check = cur.fetchone() 
            
            if _id == check[0]:
                only = False
        for b in range(4965, y[0]+1):
            cur.execute("SELECT NewsID From NewsSearchNasdaq0803 WHERE ID = %s", [b])
            compare = cur.fetchone()
            
            if _id == compare[0]:
                only = False        
        
        for c in range(2005, x[0]+1):
            cur.execute("SELECT NewsID From NewsSearchNewYorkStockExchange WHERE ID = %s", [c])
            compare = cur.fetchone()
            
            if _id == compare[0]:
                only = False
        # London Stock Exchange
        for a in range(1, w[0]+1):
            cur.execute("SELECT NewsID From NewsSearchLondonStockExchange WHERE ID = %s", [a])
            compare = cur.fetchone()
            
            if _id == compare[0]:
                only = False
        
            
        if only==True:
            cur.execute("INSERT INTO newssearchnewyorkstockexchange (PubDate, Headline, leadParagraph, NewsID) VALUES (%s,%s,%s,%s);",[pubDate, headline, snippet, _id])
            conn.commit()
           
            cur.execute("SELECT COUNT(NewsID) FROM NewsSearchNewYorkStockExchange;")
            x = cur.fetchone()
            cur.execute("SELECT COUNT(NewsID) FROM NewsSearchLondonStockExchange;")
            w = cur.fetchone()
            

    
    

    



