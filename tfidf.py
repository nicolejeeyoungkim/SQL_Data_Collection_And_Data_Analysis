# -*- coding: utf-8 -*-
"""
Created on Tue Aug 22 10:22:58 2017

@author: nicole
"""
from __future__ import division
import numpy as np

import re
import string
whole = ""
newsIDstring = ""
import psycopg2

conn = psycopg2.connect("host=XXX dbname=data user=XXX password=XXX")
cur = conn.cursor()

cur.execute("SELECT COUNT(NewsID) FROM NewsSearchStockMarket0731;")
z = cur.fetchone()
cur.execute("SELECT COUNT(NewsID) FROM NewsSearchNasdaq0803;")
y = cur.fetchone()
print type(y)
cur.execute("SELECT COUNT(NewsID) FROM NewsSearchNewYorkStockExchange;")
x = cur.fetchone()
cur.execute("SELECT COUNT(NewsID) FROM NewsSearchLondonStockExchange;")
w = cur.fetchone()
tf = 0

for a in range(1, w[0]+1):
    cur.execute("SELECT headline From NewsSearchLondonStockExchange WHERE ID = %s", [a])
    headline = cur.fetchone()
    cur.execute("SELECT leadParagraph From NewsSearchLondonStockExchange WHERE ID = %s", [a])
    leadParagraph = cur.fetchone()
    cur.execute("SELECT newsID From NewsSearchLondonStockExchange WHERE ID = %s", [a])
    newsID = cur.fetchone()    
        
    
    whole += headline[0]
    whole += " "
    if leadParagraph[0] is None:
        whole += "None"
    else:
        whole += leadParagraph[0]
    whole += "*"
    newsIDstring += newsID[0]
    newsIDstring += "*"
    
for a in range(1, x[0]+1):
    cur.execute("SELECT headline From NewsSearchNewYorkStockExchange WHERE ID = %s", [a])
    headline = cur.fetchone()
    cur.execute("SELECT leadParagraph From NewsSearchNewYorkStockExchange WHERE ID = %s", [a])
    leadParagraph = cur.fetchone()
    cur.execute("SELECT newsID From NewsSearchNewYorkStockExchange WHERE ID = %s", [a])
    newsID = cur.fetchone()
    
    whole += headline[0]
    whole += " "
    if leadParagraph[0] is None:
        whole += "None"
    else:
        whole += leadParagraph[0]
    whole += "*"
    newsIDstring += newsID[0]
    newsIDstring += "*"
    
for a in range(1, y[0]+1):
    cur.execute("SELECT headline From NewsSearchNasdaq0803 WHERE ID = %s", [a])
    headline = cur.fetchone()
    cur.execute("SELECT leadParagraph From NewsSearchNasdaq0803 WHERE ID = %s", [a])
    leadParagraph = cur.fetchone()
    cur.execute("SELECT newsID From NewsSearchNasdaq0803 WHERE ID = %s", [a])
    newsID = cur.fetchone()
        
    whole += headline[0]
    whole += " "
    if leadParagraph[0] is None:
        whole += "None"
    else:
        whole += leadParagraph[0]
    whole += "*"
    newsIDstring += newsID[0]
    newsIDstring += "*"
    
for a in range(1, z[0]+1):
    cur.execute("SELECT headline From NewsSearchStockMarket0731 WHERE ID = %s", [a])
    headline = cur.fetchone()
    cur.execute("SELECT leadParagraph From NewsSearchStockMarket0731 WHERE ID = %s", [a])
    leadParagraph = cur.fetchone()
    
    whole += headline[0]
    whole += " "
    if leadParagraph[0] is None:
        whole += "None"
    else:
        whole += leadParagraph[0]
    
    whole += "*"

def extract_words(s):
    return [re.sub('^[{0}]+|[{0}]+$'.format(string.punctuation), '', w) for w in s.split()]      

string1list = whole.split("*")
string2list = newsIDstring.split("*")
whole_data = []
wholenewsIDdata = []
for a in range(len(string1list)):
    whole_data.append(extract_words(string1list[a]))
    #print extract_words(string1list[a])
for a in range(len(string2list)):
    wholenewsIDdata.append(extract_words(string2list[a]))
print len(string1list)
for a in range(len(string1list)):
    # each article
    temp = string1list[a]
    tempList = extract_words(temp)
    tempIDList = string2list[a]
    top1 = 0
    top1word = ""
    top1ID = ""
    top2 = 0
    top2word = ""
    top2ID = ""
    top3 = 0
    top3word = ""
    top3ID = ""
    #print len(tempList)
    for num in range(len(tempList)):
        # each word in the article, taking it to compare        
        current = tempList[num].lower()
        count = 0           
        appearanceCount = 0
        for n in range(len(tempList)):
            #comparing the word to each word in its own article
            if current == tempList[n].lower():
                count+=1
            #print tempList[n]
        for n in range(len(whole_data)):
            appearance = False            
            for i in range(len(whole_data[n])):
                if current == whole_data[n][i].lower():
                    appearance = True
            if appearance == True:
                appearanceCount +=1
        #print tempList
        #print count
        tf = count / len(tempList)
        idf = np.log(len(whole_data) / appearanceCount)
        print tempIDList
        print current
        tf_idf = tf * idf
        if tf_idf > top1:
            top3 = top2
            top3word = top2word
       
            top2 = top1        
            top2word = top1word
      
            top1 = tf_idf
            top1word = current
           
        elif tf_idf > top2 and tf_idf < top1:
            top3 = top2
            top3word = top2word
     
            top2 = tf_idf        
            top2word = current
       
        elif tf_idf > top3 and tf_idf < top2:
            top3 = tf_idf
            top3word = current
  
        print top1word, top2word, top3word
        print tempIDList
        stockMarket = False
        nasdaq = False
        newYork = False
        london = False
        for b in range(1, z[0]+1):
            cur.execute("SELECT NewsID FROM NewsSearchStockMarket0731 WHERE ID = %s", [b])
            check = cur.fetchone() 
            
            if tempIDList == check[0]:
                stockMarket = True

        # Nasdaq
        print type(y)
        print type(b)
        for b in range(1, y[0]+1):
            cur.execute("SELECT NewsID From NewsSearchNasdaq0803 WHERE ID = %s", [b])
            compare = cur.fetchone()
            
            if tempIDList == compare[0]:
                nasdaq = True        
        # New York Stock Exchange
        for b in range(1, x[0]+1):
            cur.execute("SELECT NewsID From NewsSearchNewYorkStockExchange WHERE ID = %s", [b])
            compare = cur.fetchone()
            
            if tempIDList == compare[0]:
                newYork = True
        # London Stock Exchange
        for b in range(1, w[0]+1):
            cur.execute("SELECT NewsID From NewsSearchLondonStockExchange WHERE ID = %s", [b])
            compare = cur.fetchone()
            
            if tempIDList == compare[0]:
                london = True
                
        if stockMarket == True:
            cur.execute("UPDATE newssearchstockmarket0731 SET Top1 = (%s), Top2 = (%s), Top3 = (%s) WHERE NewsID = (%s);",[top1word, top2word, top3word, tempIDList])
            conn.commit()
        elif nasdaq == True:
            cur.execute("UPDATE newssearchnasdaq0803 SET Top1 = (%s), Top2 = (%s), Top3 = (%s) WHERE NewsID = (%s);",[top1word, top2word, top3word, tempIDList])
            conn.commit()
        elif newYork == True:
            cur.execute("UPDATE newssearchnewyorkstockexchange SET Top1 = (%s), Top2 = (%s), Top3 = (%s) WHERE NewsID = (%s);",[top1word, top2word, top3word, tempIDList])
            conn.commit()
        elif london == True:
            cur.execute("UPDATE newssearchlondonstockexchange SET Top1 = (%s), Top2 = (%s), Top3 = (%s) WHERE NewsID = (%s);",[top1word, top2word, top3word, tempIDList])
            conn.commit()
           
