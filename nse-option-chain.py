## -------------------------- Company name function -------------------------- ##

headers = {
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'DNT': '1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36',
    'Sec-Fetch-User': '?1',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-Mode': 'navigate',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9,hi;q=0.8',
}
indices = ['NIFTY','FINNIFTY','BANKNIFTY']

def nsefetch(payload):
    try:
        output = requests.get(payload, headers=headers).json()
        # print(output)
    except ValueError:
        s = requests.Session()
        output = s.get("http://nseindia.com", headers=headers)
        output = s.get(payload, headers=headers).json()
    return output


def fnolist():
    # df = pd.read_csv("https://www1.nseindia.com/content/fo/fo_mktlots.csv")
    # return [x.strip(' ') for x in df.drop(df.index[3]).iloc[:,1].to_list()]
    positions = nsefetch(
        'https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O')
    nselist = ['NIFTY', 'NIFTYIT', 'BANKNIFTY']
    i = 0
    for x in range(i, len(positions['data'])):
        nselist = nselist+[positions['data'][x]['symbol']]
    return nselist


def get_lot(company, expiry_date):
    j = 0
    for symbol in lot_size_df['SYMBOL']:
        if symbol == company:
            # lot = lot_size_df['NOV-20'][j]
            lot = lot_size_df[expiry_date][j]
            break
        j += 1
    return str(lot)

def nse_optionchain_scrapper(symbol):
    symbol = symbol.replace("&",'%26') 
    if any(x in symbol for x in indices):
        payload = nsefetch('https://www.nseindia.com/api/option-chain-indices?symbol='+symbol)
    else:
        payload = nsefetch('https://www.nseindia.com/api/option-chain-equities?symbol='+symbol)
    return payload

def oi_chain_builder (symbol,expiry="latest",oi_mode="full"):

    payload = nse_optionchain_scrapper(symbol)

    if(oi_mode=='compact'):
        col_names = ['CALLS_OI','CALLS_Chng in OI','CALLS_Volume','CALLS_IV','CALLS_LTP','CALLS_Net Chng','Strike Price','PUTS_OI','PUTS_Chng in OI','PUTS_Volume','PUTS_IV','PUTS_LTP','PUTS_Net Chng']
    if(oi_mode=='full'):
        col_names = ['CALLS_Chart','CALLS_OI','CALLS_Chng in OI','CALLS_Volume','CALLS_IV','CALLS_LTP','CALLS_Net Chng','CALLS_Bid Qty','CALLS_Bid Price','CALLS_Ask Price','CALLS_Ask Qty','Strike Price','PUTS_Bid Qty','PUTS_Bid Price','PUTS_Ask Price','PUTS_Ask Qty','PUTS_Net Chng','PUTS_LTP','PUTS_IV','PUTS_Volume','PUTS_Chng in OI','PUTS_OI','PUTS_Chart']
    oi_data = pd.DataFrame(columns = col_names)

    #oi_row = {'CALLS_OI':0, 'CALLS_Chng in OI':0, 'CALLS_Volume':0, 'CALLS_IV':0, 'CALLS_LTP':0, 'CALLS_Net Chng':0, 'Strike Price':0, 'PUTS_OI':0, 'PUTS_Chng in OI':0, 'PUTS_Volume':0, 'PUTS_IV':0, 'PUTS_LTP':0, 'PUTS_Net Chng':0}
    oi_row = {'CALLS_OI':0, 'CALLS_Chng in OI':0, 'CALLS_Volume':0, 'CALLS_IV':0, 'CALLS_LTP':0, 'CALLS_Net Chng':0, 'CALLS_Bid Qty':0,'CALLS_Bid Price':0,'CALLS_Ask Price':0,'CALLS_Ask Qty':0,'Strike Price':0, 'PUTS_OI':0, 'PUTS_Chng in OI':0, 'PUTS_Volume':0, 'PUTS_IV':0, 'PUTS_LTP':0, 'PUTS_Net Chng':0,'PUTS_Bid Qty':0,'PUTS_Bid Price':0,'PUTS_Ask Price':0,'PUTS_Ask Qty':0}
    if(expiry=="latest"):
        expiry = payload['records']['expiryDates'][0]
    m=0
    for m in range(len(payload['records']['data'])):
        if(payload['records']['data'][m]['expiryDate']==expiry):
            if(1>0):
                try:
                    oi_row['CALLS_OI']=payload['records']['data'][m]['CE']['openInterest']
                    oi_row['CALLS_Chng in OI']=payload['records']['data'][m]['CE']['changeinOpenInterest']
                    oi_row['CALLS_Volume']=payload['records']['data'][m]['CE']['totalTradedVolume']
                    oi_row['CALLS_IV']=payload['records']['data'][m]['CE']['impliedVolatility']
                    oi_row['CALLS_LTP']=payload['records']['data'][m]['CE']['lastPrice']
                    oi_row['CALLS_Net Chng']=payload['records']['data'][m]['CE']['change']
                    if(oi_mode=='full'):
                        oi_row['CALLS_Bid Qty']=payload['records']['data'][m]['CE']['bidQty']
                        oi_row['CALLS_Bid Price']=payload['records']['data'][m]['CE']['bidprice']
                        oi_row['CALLS_Ask Price']=payload['records']['data'][m]['CE']['askPrice']
                        oi_row['CALLS_Ask Qty']=payload['records']['data'][m]['CE']['askQty']
                except KeyError:
                    oi_row['CALLS_OI'], oi_row['CALLS_Chng in OI'], oi_row['CALLS_Volume'], oi_row['CALLS_IV'], oi_row['CALLS_LTP'],oi_row['CALLS_Net Chng']=0,0,0,0,0,0
                    if(oi_mode=='full'):
                        oi_row['CALLS_Bid Qty'],oi_row['CALLS_Bid Price'],oi_row['CALLS_Ask Price'],oi_row['CALLS_Ask Qty']=0,0,0,0
                    pass

                oi_row['Strike Price']=payload['records']['data'][m]['strikePrice']

                try:
                    oi_row['PUTS_OI']=payload['records']['data'][m]['PE']['openInterest']
                    oi_row['PUTS_Chng in OI']=payload['records']['data'][m]['PE']['changeinOpenInterest']
                    oi_row['PUTS_Volume']=payload['records']['data'][m]['PE']['totalTradedVolume']
                    oi_row['PUTS_IV']=payload['records']['data'][m]['PE']['impliedVolatility']
                    oi_row['PUTS_LTP']=payload['records']['data'][m]['PE']['lastPrice']
                    oi_row['PUTS_Net Chng']=payload['records']['data'][m]['PE']['change']
                    if(oi_mode=='full'):
                        oi_row['PUTS_Bid Qty']=payload['records']['data'][m]['PE']['bidQty']
                        oi_row['PUTS_Bid Price']=payload['records']['data'][m]['PE']['bidprice']
                        oi_row['PUTS_Ask Price']=payload['records']['data'][m]['PE']['askPrice']
                        oi_row['PUTS_Ask Qty']=payload['records']['data'][m]['PE']['askQty']
                except KeyError:
                    oi_row['PUTS_OI'], oi_row['PUTS_Chng in OI'], oi_row['PUTS_Volume'], oi_row['PUTS_IV'], oi_row['PUTS_LTP'],oi_row['PUTS_Net Chng']=0,0,0,0,0,0
                    if(oi_mode=='full'):
                        oi_row['PUTS_Bid Qty'],oi_row['PUTS_Bid Price'],oi_row['PUTS_Ask Price'],oi_row['PUTS_Ask Qty']=0,0,0,0
            else:
                logging.info(m)

            if(oi_mode=='full'):
                oi_row['CALLS_Chart'],oi_row['PUTS_Chart']=0,0
            oi_data = oi_data.append(oi_row, ignore_index=True)

    return oi_data,float(payload['records']['underlyingValue']),payload['records']['timestamp']


try:
    # from nsepython import *
    import pandas as pd
    from datetime import datetime
    from dateutil.relativedelta import relativedelta, TH
    import requests
    import logging
    logger = logging.getLogger()
    logger.setLevel(logging.ERROR)

    list_of_company = fnolist()
    list_of_company = sorted(list_of_company[3:])

    print("importing lot size for all company plz wait...")
    lot_size_df = pd.read_csv("https://www1.nseindia.com/content/fo/fo_mktlots.csv", sep="\s*,\s*", engine="python")

    def get_lot(company,expiry_date):
        j = 0
        for symbol in lot_size_df['SYMBOL']:
            if symbol == company:
                # lot = lot_size_df['NOV-20'][j]
                lot = lot_size_df[expiry_date][j]
                break
            j += 1
        return str(lot)

    current_month = 2
    next_month = 3

    y = int(lot_size_df.columns.values[current_month].split("-")[1]) + 2000
    m = datetime.strptime(lot_size_df.columns.values[current_month].split("-")[0], "%b").month
    m_text = datetime.strptime(lot_size_df.columns.values[current_month].split("-")[0], "%b").strftime("%b")
    d = 1
    date_th = (datetime(y, m, d) + relativedelta(day=31, weekday=TH(-1))).strftime("%d")

    print("string fetch for expiry date : ",date_th, m, y)


    combined_company_df = pd.DataFrame(columns=['CALLS_OI', 'CALLS_Chng in OI', 'CALLS_Volume', 'CALLS_IV', 'CALLS_LTP',
                                                'CALLS_Net Chng', 'Strike Price', 'PUTS_OI', 'PUTS_Chng in OI',
                                                'PUTS_Volume', 'PUTS_IV', 'PUTS_LTP', 'PUTS_Net Chng',
                                                'CALLS_Ask Price', 'CALLS_Ask Qty', 'CALLS_Bid Price', 'CALLS_Bid Qty',
                                                'PUTS_Ask Price', 'PUTS_Ask Qty', 'PUTS_Bid Price', 'PUTS_Bid Qty'])

    for company in list_of_company:
        try:
            # 27-Jan-2020
            oi_data, ltp, crontime = oi_chain_builder(company, str(date_th)+"-"+m_text+"-"+str(y), "full")
            oi_data['Company_name'] = company
            oi_data['ltp'] = ltp
            oi_data['lot_size'] = get_lot(company, str(
                lot_size_df.columns.values[current_month]))
            combined_company_df = combined_company_df.append(oi_data, ignore_index=True)
            b = "imported data for company : " + company
            print(b, end="\r")
            # print("imported data for company : ", company)

        except Exception as e:
            print("\n\nerror in " + company)
            print("error : ", e)
            pass

    combined_company_df.drop(labels=['CALLS_OI', 'CALLS_Chng in OI', 'CALLS_Net Chng', 'PUTS_OI', 'PUTS_Chng in OI', 'PUTS_Volume',
                             'PUTS_Net Chng', 'CALLS_Ask Qty', 'CALLS_Bid Qty', 'PUTS_Ask Qty', 'PUTS_Bid Qty'], axis=1, inplace=True)
    combined_company_df.to_excel("./"+str(date_th) + "_" + str(m) + '_current_month.xlsx', index=False)












    # # Next month

    y = int(lot_size_df.columns.values[next_month].split("-")[1]) + 2000
    m = datetime.strptime(
        lot_size_df.columns.values[next_month].split("-")[0], "%b").month
    m_text = datetime.strptime(
        lot_size_df.columns.values[next_month].split("-")[0], "%b").strftime("%b")
    d = 1
    date_th = (datetime(y, m, d) + relativedelta(day=31,
               weekday=TH(-1))).strftime("%d")

    print("string fetch for expiry date : ", date_th, m, y)

    next_company_df = pd.DataFrame(columns=['CALLS_OI', 'CALLS_Chng in OI', 'CALLS_Volume', 'CALLS_IV', 'CALLS_LTP',
                                            'CALLS_Net Chng', 'Strike Price', 'PUTS_OI', 'PUTS_Chng in OI',
                                            'PUTS_Volume', 'PUTS_IV', 'PUTS_LTP', 'PUTS_Net Chng',
                                            'CALLS_Ask Price', 'CALLS_Ask Qty', 'CALLS_Bid Price', 'CALLS_Bid Qty',
                                            'PUTS_Ask Price', 'PUTS_Ask Qty', 'PUTS_Bid Price', 'PUTS_Bid Qty'])

    for company in list_of_company:
        try:
            # 27-Jan-2020
            oi_data, ltp, crontime = oi_chain_builder(company, str(date_th)+"-"+m_text+"-"+str(y), "full")
            oi_data['Company_name'] = company
            oi_data['ltp'] = ltp
            oi_data['lot_size'] = get_lot(company,str(lot_size_df.columns.values[next_month]))
            next_company_df = next_company_df.append(oi_data, ignore_index=True)
            b = "imported data for company : " + company
            print(b, end="\r")
        except Exception as e:
            print("error in " + company)
            print("error : " , e)
            pass
    next_company_df.drop(labels=['CALLS_OI', 'CALLS_Chng in OI', 'CALLS_Net Chng', 'PUTS_OI', 'PUTS_Chng in OI','PUTS_Volume', 'PUTS_Net Chng','CALLS_Ask Qty','CALLS_Bid Qty', 'PUTS_Ask Qty', 'PUTS_Bid Qty'],axis=1,inplace=True)
    next_company_df.to_excel("./"+str(date_th) + "_" + str(m) + '_next_month.xlsx', index=False)
    logger.setLevel(logging.DEBUG)

except Exception as e:
    print("fatal error : ", e)
    val = input("press enter to exit")
    logger.setLevel(logging.DEBUG)

