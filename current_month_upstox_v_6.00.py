from distutils.core import setup
import py2exe

try:
    from nsepython import *
    import pandas as pd
    import requests
    from datetime import datetime
    from dateutil.relativedelta import relativedelta, TH
except:
    import pip
    def install(package):
        if hasattr(pip, 'main'):
            pip.main(['install', package])
        else:
            pip._internal.main(['install', package])

    install("nsepython")
    install("pandas")
    install("requests")
    install("datetime")
    from nsepython import *
    import pandas as pd
    import requests
    from datetime import datetime
    from dateutil.relativedelta import relativedelta, TH


print("looking into nse for list of company name")
list_of_company = fnolist()
print("imported company list")
list_of_company = sorted(list_of_company[3:])


# temp = pd.read_csv("fo_mktlots.csv", sep="\s*,\s*", engine="python")
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

# expiry_day_number = (datetime(int(lot_size_df.columns.values[2].split("-")[1]) + 2000, datetime.strptime(lot_size_df.columns.values[2].split("-")[0], "%b").month , nextDay) + relativedelta(day=31, weekday=TH(-1))).strftime("%d")
# expiry_month_number = lot_size_df.columns.values[2].split("-")[0]
# expiry_year_number = int(lot_size_df.columns.values[2].split("-")[1]) + 2000

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
        oi_data['lot_size'] = get_lot(company,str(lot_size_df.columns.values[current_month]))
        combined_company_df = combined_company_df.append(oi_data, ignore_index=True)
    except Exception as e:
        print("error in " + company)
        print("error : " , e)
        pass


combined_company_df.to_csv(str(date_th) + "_" + str(m) + '_current_month.csv', index=False)












# # Next month





y = int(lot_size_df.columns.values[next_month].split("-")[1]) + 2000
m = datetime.strptime(lot_size_df.columns.values[next_month].split("-")[0], "%b").month
m_text = datetime.strptime(lot_size_df.columns.values[next_month].split("-")[0], "%b").strftime("%b")
d = 1
date_th = (datetime(y, m, d) + relativedelta(day=31, weekday=TH(-1))).strftime("%d")

print("string fetch for expiry date : ",date_th, m, y)

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
    except Exception as e:
        print("error in " + company)
        print("error : " , e)
        pass


next_company_df.to_csv(str(date_th) + "_" + str(m) + '_next_month.csv', index=False)