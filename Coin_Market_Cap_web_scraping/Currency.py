import urllib.request
from bs4 import BeautifulSoup
import time
import random
from settings import *
from Currency_database import Currency_database


class Currency():
    def __init__(self, settings, ac_url):
        
        self.db_host = settings["db_host"]
        self.db_user = settings["db_user"]
        self.db_password = settings["db_password"]
        self.db_port = settings["db_port"]
        self.db_use_unicode = settings["db_use_unicode"]
        self.charset = settings["charset"]
        self.db_name = settings["db_name"]
        self.ac_url = ac_url
        self.currency_instance = Currency_database(self.db_user,self.db_password,self.db_name, self.db_host,self.db_port,self.db_use_unicode,self.charset)
        self.opener = urllib.request.build_opener()
        self.opener.addheaders = [('User-Agent', 'Mozilla/5.0')]        
        self.Updated_time = self.get_updated_time(self.ac_url)
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

########## store all currencies ##########
    def store_all_currencies(self):
        response = self.opener.open(self.ac_url)
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
            
            Test_unique = Name + Symbol + self.Updated_time
            
            currency_info = (Ranking, Name, Symbol, Market_cap, Price, Circulating_supply, Volume_24h, Percent_1h, Percent_24h, Percent_7d, self.Updated_time, self.Collection_time, Test_unique)
            if not self.currency_instance.currency_exists_in_All_Currencies(Test_unique):
                try:
                    self.currency_instance.insert_data_into_All_Currencies(currency_info)
                    print("Currency information stored successfully.")
                except:
                    print("Unable to store currency information.")
            else:
                print("Currency already exist.")
##########################################

    def get_all_elements(self, table):
        element_list = []
        
        for elem in table:
            try:
                content = elem.td.get_text()
                try:
                    if int(content):
                        element_list += [elem]
                        n += 1    
                except:
                    pass 
            except:
                pass
        return element_list
    
        
    def get_all_titles(self, table):
        title_list = []
        for elem in table:
            try:
                if elem.has_attr("id"):
                    title_list += [elem.a.get_text()]
            except:
                pass
        return title_list  


    def add_title_to_elements(self, element_list, title_list):
        element_title_list = {}
        n = -1
        title = ''
        for element in element_list:
            
            if int(element.td.get_text()) == 1:
                n += 1
                title = title_list[n]
                
            element_title_list[element] = title
        return element_title_list


    def get_view_more_urls(self, smallTagsSet):
        
        urls = []
        for small_tag in smallTagsSet:
            if 'href' in small_tag.a.attrs:
                url = small_tag.a.attrs['href']
                urls += [url]

        complete_urls = []
        for url in urls:
            url = 'https://coinmarketcap.com' + url
            complete_urls += [url]

        return complete_urls
    

########## store 24HVR by exchange ##########
    def store_24HVR_by_exchange(self):
        response = self.opener.open(self.ac_url)
        ac_page = BeautifulSoup(response, "html.parser")
        
        #store currencies on the page
        self.bxrows = ac_page.find("table", {"class":"table table-condensed"}).findAll("tr")
        self.bx_all_currencies = self.get_all_elements(self.bxrows)
        self.bx_all_markets = self.get_all_titles(self.bxrows)
        self.bx_currency_market = self.add_title_to_elements(self.bx_all_currencies, self.bx_all_markets)

        for currency in self.bx_all_currencies:
            tds = currency.findAll('td')
            tdsNew = self.return_content(tds)
            Currency = tdsNew[1]
            Market = self.bx_currency_market[currency]
            Market_ranking = self.bx_all_markets.index(Market) + 1
            Ranking = int(tdsNew[0])
            Pair = tdsNew[2]
            Volume_24h = self.convert_to_number(tdsNew[3])
            Price = self.convert_to_number(tdsNew[4])
            Volume_percent = self.convert_to_number(tdsNew[5])
                            
            Test_unique = Market + Currency + Pair + self.Updated_time
            
            exchange_info = (Market, Market_ranking, Ranking, Currency, Pair, Volume_24h, Price, Volume_percent, self.Updated_time, self.Collection_time, Test_unique)
            
            if not self.currency_instance.currency_exists_in_24HVR_by_Exchange(Test_unique):
                try:
                    self.currency_instance.insert_data_into_24HVR_by_Exchange(exchange_info)
                    print("Currency (by exchange) stored successfully.")
                except:
                    print("Unable to store currency (by exchange).")            
            else:
                print("Currency (by exchange) already exist.")       
        
        #store currencies in each "view more" page
        small_tags = ac_page.find("table", {"class":"table table-condensed"}).findAll("small") 
        view_more_urls = self.get_view_more_urls(small_tags)
        for url in view_more_urls:
            response = self.opener.open(url)
            ac_page = BeautifulSoup(response, "html.parser")
            currencies = ac_page.find("table", {"class":"table no-border table-condensed"}).findAll("tr")
            currencies = currencies[1:len(currencies)]
            for currency in currencies:
                tds = currency.findAll('td')
                tdsNew = self.return_content(tds)
                Market = ac_page.find("div", {"class":"col-xs-4"}).find("h1", {"class":"text-large"}).get_text().strip()
                Market_ranking = self.bx_all_markets.index(Market) + 1
                Ranking1 = int(tdsNew[0])
                Currency = tdsNew[1]
                Pair = tdsNew[2]
                Volume_24h = self.convert_to_number(tdsNew[3])
                Price = self.convert_to_number(tdsNew[4])
                Volume_percent = self.convert_to_number(tdsNew[5])
                Test_unique = Market + Currency + Pair + self.Updated_time
                
                exchange_info = (Market, Market_ranking, Ranking1, Currency, Pair, Volume_24h, Price, Volume_percent, self.Updated_time, self.Collection_time, Test_unique)
                
                if not self.currency_instance.currency_exists_in_24HVR_by_Exchange(Test_unique):   
                    try:
                        self.currency_instance.insert_data_into_24HVR_by_Exchange(exchange_info)
                        print("Currency (by exchange) stored successfully.")
                    except:
                        print("Unable to store currency (by exchange).")
                else:
                    print("Currency (by exchange) already exist.")            
##########################################


########## store 24HVR by currency ##########     
    def store_24HVR_by_currency(self):
            
        response = self.opener.open(self.ac_url)
        ac_page = BeautifulSoup(response, "html.parser")
        
        self.bcrows = ac_page.find("table", {"class":"table no-border table-condensed"}).findAll("tr")
        self.bc_all_markets = self.get_all_elements(self.bcrows)
        self.bc_all_currencies = self.get_all_titles(self.bcrows)
        self.bc_market_currency = self.add_title_to_elements(self.bc_all_markets, self.bc_all_currencies)

        for market in self.bc_all_markets:    

            tds = market.findAll('td')
            tdsNew = self.return_content(tds)
            
            Currency = self.bc_market_currency[market]
            Currency_ranking = self.bc_all_currencies.index(Currency) + 1
            Ranking = int(tdsNew[0])
            Source = tdsNew[1]
            Pair = tdsNew[2]
            Volume_24h = self.convert_to_number(tdsNew[3])
            Price = self.convert_to_number(tdsNew[4])
            Volume_percent = self.convert_to_number(tdsNew[5])
            Test_unique = Currency + Source + Pair + self.Updated_time
            
            exchange_info = (Currency, Currency_ranking, Ranking, Source, Pair, Volume_24h, Price, Volume_percent, self.Updated_time, self.Collection_time, Test_unique)
            
            if not self.currency_instance.currency_exists_in_24HVR_by_Exchange(Test_unique):   
                try:
                    self.currency_instance.insert_data_into_24HVR_by_Exchange(exchange_info)
                    print("Market (by currency) stored successfully.")
                except:
                    print("Unable to store Market (by currency).")
            else:
                print("Market (by currency) already exists.")
                    
        small_tags = ac_page.find("table", {"class":"table no-border table-condensed"}).findAll("small")
        view_more_urls = self.get_view_more_urls(small_tags)
    
        for url in view_more_urls:
            response = self.opener.open(url)
            ac_page = BeautifulSoup(response, "html.parser")
            markets = ac_page.find("table", id="markets-table").findAll("tr")
                
            for market in markets[1:len(markets)]:    

                tds = market.findAll('td')
                tdsNew = self.return_content(tds)
                CurrencyR = ac_page.find("div", {"class":"col-xs-6 col-sm-4 col-md-4"}).get_text()
                Currency_symbol = ac_page.find("div", {"class":"col-xs-6 col-sm-4 col-md-4"}).small.get_text()
                
                Currency = CurrencyR.replace(Currency_symbol, "").strip()
                Currency_ranking = self.bc_all_currencies.index(Currency) + 1
                Ranking = int(tdsNew[0])
                Source = tdsNew[1]
                Pair = tdsNew[2]
                Volume_24h = self.convert_to_number(tdsNew[3])
                Price = self.convert_to_number(tdsNew[4])
                Volume_percent = self.convert_to_number(tdsNew[5])
                Test_unique = Currency + Source + Pair + self.Updated_time
                
                currency_info = (Currency, Currency_ranking, Ranking, Source, Pair, Volume_24h, Price, Volume_percent, self.Updated_time, self.Collection_time, Test_unique)
                
                if not self.currency_instance.currency_exists_in_24HVR_by_Currency(Test_unique):   
                    #try:
                    self.currency_instance.insert_data_into_24HVR_by_Currency(currency_info)
                        #print("Market (by currency) stored successfully.")
                    #except:
                        #print("Unable to store Market (by currency).")
                else:
                    print("Market (by currency) already exists.")
##########################################


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

