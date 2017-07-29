import selenium
from selenium import webdriver
import urllib.request
from bs4 import BeautifulSoup
import time
import random
from settings import *
from Currency_database import Currency_database


class Currency():
    def __init__(self, settings, source_url):
        
        self.db_host = settings["db_host"]
        self.db_user = settings["db_user"]
        self.db_password = settings["db_password"]
        self.db_port = settings["db_port"]
        self.db_use_unicode = settings["db_use_unicode"]
        self.charset = settings["charset"]
        self.db_name = settings["db_name"]
        self.source_url = source_url
        self.currency_instance = Currency_database(self.db_user,self.db_password,self.db_name, self.db_host,self.db_port,self.db_use_unicode,self.charset)
        self.opener = urllib.request.build_opener()
        self.opener.addheaders = [('User-Agent', 'Mozilla/5.0')]        
        self.Updated_time = self.get_updated_time(self.source_url)
        self.Collection_time = self.get_collection_time()

    def convert_to_number(self, enumString):
        empty = ''
        if 'e-' in enumString:
            return 0
        else:
            for elem in enumString:
                try:
                    int(elem)
                    empty += elem
                except:
                    if elem in ['.', '-']:
                        empty += elem
            if empty == '':
                return None
            else:
                if '.' in empty:
                    return float(empty)
                else:
                    return int(empty)

    def return_content(self, rawStringSet):
        newStringSet = [rawString.get_text().strip() for rawString in rawStringSet]
        return newStringSet

    def get_updated_time(self, webpage):
        response = self.opener.open(webpage)
        ac_page = BeautifulSoup(response, "html.parser")
        rows = ac_page.findAll("div", {"class":"col-xs-12"})
        for row in rows:
            if row.find("p", {"class":"small"}):
                x = row.p.get_text()
        updated_time = x.replace("Last updated: ", "").replace(",", "").replace(" UTC", '')
        time_struct = time.strptime(updated_time, '%b %d %Y  %I:%M %p')
        time_datetime = time.strftime("%Y-%m-%d %H:%M:%S", time_struct)
        return time_datetime

    def get_collection_time(self):
        now = time.localtime(time.time())
        collection_time = time.strftime("%Y-%m-%d %H:%M:%S", now)
        return collection_time

    #store all currencies
    def store_all_currencies(self):
        response = self.opener.open(self.source_url)
        ac_page = BeautifulSoup(response, "html.parser")
        rows = ac_page.find("table", id="currencies-all").tbody.findAll("tr")
        for row in rows:
            tds = row.findAll('td')
            tdsNew = self.return_content(tds)
            Ranking = int(tdsNew[0])
            Name = tdsNew[1]
            Symbol = tdsNew[2]
            Market_cap = self.convert_to_number(tdsNew[3])
            Price = self.convert_to_number(tdsNew[4])
            Circulating_supply = self.convert_to_number(tdsNew[5])
            Volume_24h = self.convert_to_number(tdsNew[6])
            Percent_1h = self.convert_to_number(tdsNew[7])
            Percent_24h = self.convert_to_number(tdsNew[8])
            Percent_7d = self.convert_to_number(tdsNew[9])
            
            currency_info = (Ranking, Name, Symbol, Market_cap, Price, Circulating_supply, Volume_24h, Percent_1h, Percent_24h, Percent_7d, self.Updated_time, self.Collection_time)
            try:
                self.currency_instance.insert_data_into_All_Currencies(currency_info)
                print("Currency information stored successfully.")
            except:
                print("Unable to store currency information.")
            

    #get all volume headers
    def get_all_volume_headers(self):
        response = self.opener.open(self.source_url)
        ac_page = BeautifulSoup(response, "html.parser")

        urls = []
        volume_headers = ac_page.findAll("h3", {"class":"volume-header"})
        for volume_header in volume_headers:
            url = volume_header.a.attrs['href']
            urls += ['https://coinmarketcap.com' + url]
        return urls

    #click "Market"
    def click_market(self, webpage):
        self.dr = webdriver.PhantomJS(executable_path='C:\\Users\\win10\\Downloads\\phantomjs-2.1.1-windows\\phantomjs-2.1.1-windows\\bin\\phantomjs.exe')
        self.dr.get(webpage)
        self.btn = self.dr.find_element_by_css_selector('a[href="#markets"]')
        self.btn.click()
        return self.dr.page_source


    #store 24HVR by exchange
    def store_24HVR_by_exchange(self):

        volume_headers_urls_market = self.get_all_volume_headers()

        for url in volume_headers_urls_market:

            response = self.opener.open(url)
            bx_page = BeautifulSoup(response, "html.parser")
            currencies = bx_page.find("table", {"class":"table no-border table-condensed"}).findAll("tr")
            currencies = currencies[1:len(currencies)]
            for currency in currencies:
                tds = currency.findAll('td')
                tdsNew = self.return_content(tds)
                Market = bx_page.find("div", {"class":"col-xs-4"}).find("h1", {"class":"text-large"}).get_text().strip()
                Market_ranking = volume_headers_urls_market.index(url) + 1
                Ranking = int(tdsNew[0])
                Currency = tdsNew[1]
                Pair = tdsNew[2]
                Volume_24h = self.convert_to_number(tdsNew[3])
                Price = self.convert_to_number(tdsNew[4])
                Volume_percent = self.convert_to_number(tdsNew[5])
                
                exchange_info = (Market, Market_ranking, Ranking, Currency, Pair, Volume_24h, Price, Volume_percent, self.Updated_time, self.Collection_time)
                
                try:
                    self.currency_instance.insert_data_into_24HVR_by_Exchange(exchange_info)
                    print("Currency (by exchange) stored successfully.")
                except:
                    print("Unable to store currency (by exchange).")         


    #store 24HVR by currency    
    def store_24HVR_by_currency(self):
            
        volume_headers_urls_currency = self.get_all_volume_headers()
        
        for url in volume_headers_urls_currency:

            marketPageSource = self.click_market(url)
            bc_page = BeautifulSoup(marketPageSource, "html.parser")
            markets = bc_page.find("table", id="markets-table").findAll("tr")
                
            for market in markets[1:len(markets)]:    

                tds = market.findAll('td')
                tdsNew = self.return_content(tds)
                CurrencyR = bc_page.find("div", {"class":"col-xs-6 col-sm-4 col-md-4"}).get_text()
                Currency_symbol = bc_page.find("div", {"class":"col-xs-6 col-sm-4 col-md-4"}).small.get_text()
                
                Currency = CurrencyR.replace(Currency_symbol, "").strip()
                Currency_ranking = volume_headers_urls_currency.index(url) + 1
                Ranking = int(tdsNew[0])
                Source = tdsNew[1]
                Pair = tdsNew[2]
                Volume_24h = self.convert_to_number(tdsNew[3])
                Price = self.convert_to_number(tdsNew[4])
                Volume_percent = self.convert_to_number(tdsNew[5])
                
                currency_info = (Currency, Currency_ranking, Ranking, Source, Pair, Volume_24h, Price, Volume_percent, self.Updated_time, self.Collection_time)
                
                try:
                    self.currency_instance.insert_data_into_24HVR_by_Currency(currency_info)
                    print("Market (by currency) stored successfully.")
                except:
                    print("Unable to store Market (by currency).")


ac_url = "https://coinmarketcap.com/all/views/all/"
bx_url = "https://coinmarketcap.com/exchanges/volume/24-hour/all/"
bc_url = "https://coinmarketcap.com/currencies/volume/24-hour/"


def execute_for_time_period(seconds):
    while True:
        ac = Currency(settings, ac_url)
        ac.store_all_currencies()

        time.sleep(20)

        bx = Currency(settings, bx_url)
        bx.store_24HVR_by_exchange()

        time.sleep(20)

        bc = Currency(settings, bc_url)
        bc.store_24HVR_by_currency()

        time.sleep(seconds)

#execute_for_time_period()

