# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import logging
from nsepython import *
import pandas as pd
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta, TH
import json
from pynse import *
nse=Nse()

logger = logging.getLogger()
logger.setLevel(logging.INFO)



LOG_FORMAT = "%(Levelname)s %(asctime)s - %(message)s"
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG,format=LOG_FORMAT,mode='a')
# logging.FileHandler('errors_nse.log', mode='a', encoding=None, delay=False,format=LOG_FORMAT)
logger.info("All dependencies are imported")



currentDay = datetime.now().day
currentMonth = datetime.now().month
currentYear = datetime.now().year
currentMonthText = datetime.now().strftime('%h')
date_th=(datetime(currentYear,currentMonth,currentDay) + relativedelta(day=31, weekday=TH(-1))).strftime("%d")
expiry_var_date = date_th
expiry_var = currentMonthText
expiry_var=expiry_var.upper() 
expiry_var_lower = expiry_var.title()
print("Date : " + str(expiry_var_date) + str(expiry_var) + str(currentYear))

logger.debug("looking into nse for list of company name")
list_of_company = fnolist()
logger.debug("imported company list")
list_of_company = list_of_company[3:]
list_of_company = sorted(list_of_company)


def get_lot(company):
    temp = pd.read_csv("D:/NSE/fo_mktlots.csv",sep="\s*,\s*")
    j=0
    for symbol in temp['SYMBOL']:
        if symbol == company:
            # lot = temp['Nov-20'][j]
            lot = temp[expiry_var+'-20'][j]
            break
        j+=1
    return str(lot)

def margin(company_name,strike_price,ce_or_pe,expiry_date):
    # expiry_date = str(expiry_var) + '+' + str(expiry_var_date) + '+' + str(currentYear)
    # 26-Nov-2020
    # NOV+26+2020
    expiry_date = expiry_date.split('-')
    expiry_date = expiry_date[1]+"+"+expiry_date[0]+"+"+expiry_date[2]
    lot = get_lot(company_name)
    url = 'https://dev-in-2.upstox.com/calculators/span/span.php?action=calculateMargin&signature=7ef16bb143db4aaf4465c4046809f495c063f64f&nonce=1956202059&timestamp=1604640863&myProducts=%5B%5B%22NSE%22%2C%22Options%22%2C%22'+ str(company_name) +'%22%2C%22'+str(expiry_date)+'%22%2C%22'+str(ce_or_pe)+'%22%2C%22'+str(strike_price)+'%22%2C-'+ str(lot) +'%2C%22%22%5D%5D'
    r = requests.get(url)
    data = r.json()
    return data['totalAmtRequired'],lot

def get_option_data(company):
    df = nse.option_chain(company)['data']
    df.drop(['CE.strikePrice', 'CE.expiryDate', 'CE.identifier', 'CE.openInterest','CE.changeinOpenInterest', 'CE.pchangeinOpenInterest','CE.totalTradedVolume', 'CE.impliedVolatility','CE.change', 'CE.pChange','CE.totalSellQuantity', 'CE.askQty','CE.totalBuyQuantity','CE.askPrice', 'CE.underlyingValue', 'PE.strikePrice','PE.expiryDate','PE.underlying', 'PE.identifier', 'PE.openInterest','PE.changeinOpenInterest', 'PE.pchangeinOpenInterest','PE.totalTradedVolume', 'PE.impliedVolatility','PE.change', 'PE.pChange', 'PE.totalBuyQuantity','PE.totalSellQuantity','PE.askQty','PE.askPrice', 'PE.underlyingValue'],axis=1,inplace=True)
    return df

remaining_company = []

# %%
logger.debug("Starting the fecthing process")
d = {'company':[],'expiry date':[],'strike price': [], 'stock price': [],'margin':[],'iscall':[],'lot': [], 'call ltp': [], 'call bid price':[], 'call bid quantity':[], 'put ltp': [], 'put bid price':[],'put bid quantity':[],'percentage change in price':[]}
final_dataframe = pd.DataFrame(data=d)
def fetch_main(list_of_company):
    global final_dataframe
    company_number_count = 1
    for company in list_of_company:
        try:
            logger.info(str(company)+" statred.")
            _, ltp, crontime = oi_chain_builder(str(company),expiry=str(expiry_var_date)+"-"+ str(expiry_var_lower) +"-2020")
            df = get_option_data(str(company))
            logger.info(str(company) + ' successful.Ready for the margin.')
        except Exception as error:
            print("********************************************************")
            logger.info("umm..We have a situation here..")
            logger.info("We have the wrong info."+company + " is not valid.")
            logger.info("Resuming the remaining..")
            print("********************************************************")
            remaining_company.insert(0,company)
            continue

        df['percentage change in price'] = ((df['strikePrice']-ltp)/ltp)*100
        df = df[((df['percentage change in price'] > 5) & (df['percentage change in price'] < 20)) | ((df['percentage change in price'] < -5) & (df['percentage change in price'] > -20))]
        df['margin'] = ''
        strike_price_array = df['strikePrice']
        # num = 0
        # i=strike_price_array.index[num]
        logger.info("Margin statred for "+ company)
        df = df.reset_index(drop=True)
        for i,strike in enumerate(strike_price_array):
            # try:
            ce_or_pe = "C" if (df['percentage change in price'][i] > 5) & (df['percentage change in price'][i] < 20) else "P"
            data,lot = margin(company,strike,ce_or_pe,df['expiryDate'][i])
            print("Got margin for "+str(strike))
            df['margin'][i] = data
            df2 = pd.DataFrame([[company,df['expiryDate'][i], strike,ltp,data,ce_or_pe,lot,df['CE.lastPrice'][i],df['CE.bidprice'][i],df['CE.bidQty'][i],df['PE.lastPrice'][i],df['PE.bidprice'][i],df['PE.bidQty'][i],df['percentage change in price'][i]]], columns=['company','expiry date','strike price','stock price','margin','iscall','lot','call ltp','call bid price','call bid quantity','put ltp','put bid price','put bid quantity','percentage change in price'])
            const = final_dataframe.append(df2,ignore_index=True)
            final_dataframe = const
                # try:
                #     num += 1
                #     i = strike_price_array.index[num]
                # except Exception as error:
                #     print(company + " successfully completed." )
                #     pass
            # except Exception as error:
            #     print("********************************************************")
            #     print("strike price could not be fetched")
            #     print(error)
            #     print("not able to fetch " + str(company) + " at "+ str(strike))
            #     print("********************************************************")
            #     remaining_company.insert(0,company)
        else:
            print("remaning company : "+str(len(list_of_company) - company_number_count))
        company_number_count += 1   
fetch_main(list_of_company)


# %%
def adding_percentage_margin():
    final_dataframe['% margin'] = ''
    for i,strike in enumerate(final_dataframe['strike price']):
        ltp = final_dataframe['call ltp'][i] if final_dataframe['iscall'][i] == 'C' else final_dataframe['put ltp'][i]
        try:
            final_dataframe['% margin'][i] = float(ltp) * float(final_dataframe['lot'][i]) * 100 / (final_dataframe['margin'][i])
        except Exception as error:
            print(error)
            print(float(final_dataframe['margin'][i]))
    final_dataframe.sort_values(by=['expiry date'],inplace=True)
adding_percentage_margin()

# fetching for failed companies
print("We failed for follwing companies:")
print(remaining_company)
print("Trying again...")
fetch_main(remaining_company)
remaining_company = []


#ending process
logging.debug("completed fetching process")
print("*********************\nthis are the company that are successfully\n*********************")
print(len(final_dataframe['company'].unique()))


print("*********************\nthis are the total number of company \n*********************")
print(len(list_of_company))


list_of_reaming_company = []


for i in list_of_company:
    if i not in final_dataframe['company'].unique():
        list_of_reaming_company.append(i)


list_of_reaming_company = list_of_reaming_company[3:]
print("*********************\nthis are the companies that are reaming to be fetched\n*********************")
print(list_of_reaming_company)


# %%
final_dataframe.to_csv(str(expiry_var_date) + expiry_var + '_upstox.csv', index=False)
print("*********************\nthe file will in the current dir in csv format and name will be current month and date\nHave a good day..")
# print(final_dataframe.head())