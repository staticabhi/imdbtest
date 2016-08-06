# -*- coding: utf-8 -*-
"""
Created on Sat Jul 23 21:57:31 2016

@author: arorab01
"""

#import pandas as pd # To construct the Data Frame
#import requests # To get the whole source code
#from bs4.element import (CharsetMetaAttributeValue)
#from bs4 import BeautifulSoup # Scrapper

from flask import Flask

app=Flask(__name__)
@app.route('/')
# Set working directory if you want to below:

# Defining a function to extract some predefined characters 
#def right(s, amount):
#    return s[-amount:]

# Important URL which is base for this Scraper
#url="http://www.imdb.com/search/title?sort=num_votes,desc&start=1&title_type=feature&year=1950,2012"

# Defining a functoin to scrape the data:
def imdbdata():
#    def right(s, amount):
#        return s[-amount:]
#    i=0
#    url="http://www.imdb.com/search/title?sort=num_votes,desc&start=1&title_type=feature&year=1950,2012"
#    r=requests.get(url) # Get the whole document
#    soup=BeautifulSoup(r.content) # Beautify/Clean it
#    g_data2=soup.find_all("div",{'class':'lister-item mode-advanced'}) # Finding all "div" tags that have class=lister-item mode-advanced
#    df = pd.DataFrame(index=None,columns=['movie', 'rating', 'year', 'story','metascore','votes','gross','wr_dr_ac0','wr_dr_ac1','wr_dr_ac2','wr_dr_ac3','wr_dr_ac4','wr_dr_ac5','wr_dr_ac6','wr_dr_ac7','wr_dr_ac8','wr_dr_ac9','wr_dr_ac10','wr_dr_ac11','genre','duration','certificate']) # Defining a dataframe, you can also define an empty DF
#    for item in g_data2:
#        i=i+1
#        try:
#            df.loc[i,'movie']=item.contents[5].find_all('a')[0].text #Movie Name
#        except:
#            pass
#        try:
#            df.loc[i,'year']=item.contents[5].find_all('span')[1].text #Year
#        except:
#            pass
#        try:
#            df.loc[i,'rating']=item.contents[5].find_all('span',{"class":"value"})[0].text #Rating
#        except:
#            pass
#        try:
#            df.loc[i,'story']=item.contents[5].find_all('p',{"class":"text-muted"})[1].text.replace('\n','') #Outline 
#        except:
#            pass
#        try:
#            df.loc[i,'metascore']=item.contents[5].find_all('span',{'class':'metascore favorable'})[0].text # Metascore
#        except:
#            pass
#        try:
#            df.loc[i,'duration']=item.contents[5].find_all('span',{'class':'runtime'})[0].text.replace(' min','') # Dureation/Runtime
#        except:
#            pass
#        try:
#            df.loc[i,'certificate']=item.contents[5].find_all('span',{'class':'certificate'})[0].text.replace(' min','') # Certificate
#        except:
#            pass
#        try:
#            df.loc[i,'genre']=item.contents[5].find_all('span',{'class':'genre'})[0].text.replace('\n','') # Genre
#        except:
#            pass
#        try:
#            df.loc[i,'votes']=item.contents[5].find_all('p',{'class':'sort-num_votes-visible '})[0].find_all('span')[1].text #Votes
#        except:
#            pass
#        try:
#            df.loc[i,'gross']=item.contents[5].find_all('p',{'class':'sort-num_votes-visible '})[0].find_all('span')[4].get('data-value') #Gross Income
#        except:
#            pass
#        try:
#             df.loc[i,'wr_dr_ac0']=right(str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[0].get('href')),4)+str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[0].text) #Director, actor mixed
#        except:
#             pass
#        try:
#             df.loc[i,'wr_dr_ac1']=right(str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[1].get('href')),4)+str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[1].text) #Director, actor on Park 
#        except:
#             pass
#        try:
#             df.loc[i,'wr_dr_ac2']=right(str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[2].get('href')),4)+str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[2].text) #Director, actor on Park 
#        except:
#             pass
#        try:
#             df.loc[i,'wr_dr_ac3']=right(str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[3].get('href')),4)+str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[3].text) #Director, actor on Park 
#        except:
#             pass
#        try:
#             df.loc[i,'wr_dr_ac4']=right(str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[4].get('href')),4)+str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[4].text) #Director, actor on Park 
#        except:
#             pass             
#        try:
#             df.loc[i,'wr_dr_ac5']=right(str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[5].get('href')),4)+str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[5].text) #Director, actor on Park 
#        except:
#             pass
#        try:
#             df.loc[i,'wr_dr_ac6']=right(str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[6].get('href')),4)+str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[6].text) #Director, actor on Park 
#        except:
#             pass
#        try:
#             df.loc[i,'wr_dr_ac7']=right(str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[7].get('href')),4)+str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[7].text) #Director, actor on Park 
#        except:
#             pass
#        try:
#             df.loc[i,'wr_dr_ac8']=right(str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[8].get('href')),4)+str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[8].text) #Director, actor on Park 
#        except:
#             pass
#        try:
#             df.loc[i,'wr_dr_ac9']=right(str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[9].get('href')),4)+str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[9].text) #Director, actor on Park 
#        except:
#             pass
#        try:
#             df.loc[i,'wr_dr_ac10']=right(str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[10].get('href')),4)+str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[10].text) #Director, actor on Park 
#        except:
#             pass
#        try:
#             df.loc[i,'wr_dr_ac11']=right(str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[11].get('href')),4)+str(item.contents[5].findAll('p',{'class':''})[1].find_all('a')[11].text) #Director, actor on Park 
#        except:
#             pass
    return("OK!")


#records=input('Please enter number of records(>50) you want to scrape:') # Fetching the number of records
#if int(records)%50<=20:
#    rec=int(records)-int(records)%50
#    frec=rec/50 # Rounding off 
#else:
#    rec=int(records)+50-int(records)%50
#    frec=rec/50 # Rounding off 
    
# Basic URL from where most info. is already available at imdb    



#def imdata(url):
#    imdb=pd.DataFrame()
#    db=imdbdata(url)
#    return(db)

#for x in itera:
#    c=50*x+1
#    url=url1+str(c)+url2
#    db=imdbdata(url)
#    imdb=imdb.append(db)
#    if c==20001:
#        break

#C:/Users/ArorAb01/Documents/My Work/Learning python/

if __name__ == "__main__":
    app.run()