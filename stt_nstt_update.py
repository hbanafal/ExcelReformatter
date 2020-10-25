import pandas as pd
from read_prop import get_prop
import glob
import re
from datetime import datetime

def update_stt_data():
    stt_data = pd.DataFrame()
    nstt_data = pd.DataFrame()
    files = glob.glob(get_prop("excel_path") + "/STTInput/*.csv")
    if len(files) == 0:
        print("There are no STT or NSTT files on the path given in property file.")
        exit(1)
    for f in files:
        if 'NSTT' in f:
            print("NSTT file is present")
            nstt_data = pd.read_csv(f, na_filter=False, skip_blank_lines=True)
            continue
        elif 'STT' in f:
            print("STT file is present")
            stt_data = pd.read_csv(f, na_filter=False, skip_blank_lines=True)
        else:
            print("There is no STT or NSTT files")

    stt_data = update_rates(stt_data)
    print("STT file conversion & reversal rates updated")
    nstt_data = update_rates(nstt_data)
    print("NSTT file conversion & reversal rates updated")
    stt_data.to_csv(path_or_buf=get_prop("excel_path") + "/../STTOutput.csv", index=False, header=True)
    nstt_data.to_csv(path_or_buf=get_prop("excel_path") + "/../NSTTOutput.csv", index=False, header=True)


def update_rates(data):
    current_month = str(datetime.now().strftime('%B')).upper()[:3]
    for index, row in data.iterrows():
        if str(row['Portfolio']) in {"", "nan", "NaN", "null", "NULL"}:
            data.drop(index=index, inplace=True)
            continue
        stock_range = str(row['Portfolio']).split('_')[2]
        stock_range = float(stock_range.split("TO")[0])
        stock_name = str(row['Portfolio']).split('_')[0].split(current_month)[0]
        stock_price = float(str(row['Portfolio']).split('_')[0].split(current_month)[1])
        print("Stock price is ", stock_price)
        rates_df = read_stt_rates()

        max_order_lot = rates_df.at["MAX_ORDER_LOT", "Cutting>10"]

        data.at[index, 'maxorderlot'] = max_order_lot

        if stock_range >= 10:

            if stock_name in rates_df.index:
                cutting_rate = rates_df.at[stock_name, "Cutting>10"]
            else :
                cutting_rate = rates_df.at["Rate", "Cutting>10"]

            if cutting_rate == 'NC':
                continue

            print("Stock name is ", stock_name, "Rate is ", cutting_rate)
            data.at[index, 'Otm'] = 'no'
            if float(data.at[index, 'conversionrate']) > 0:
                data.at[index, 'conversionrate'] = modified_rate(stock_price, cutting_rate)
            else:
                data.at[index, 'reversalrate'] = modified_rate(stock_price, cutting_rate)
        elif stock_range < 10:

            if stock_name in rates_df.index:
                cutting_rate = rates_df.at[stock_name, "Cutting<10"]
                making_rate = rates_df.at[stock_name, "Making<10"]
            else :
                cutting_rate = rates_df.at["Rate", "Cutting<10"]
                making_rate = rates_df.at["Rate", "Making<10"]

            if cutting_rate == 'NC' and making_rate == 'NC':
                continue

            print("Stock name is ", stock_name, "Rates are ", cutting_rate, making_rate)

            if float(data.at[index, 'conversionrate']) == 0:
                data.at[index, 'conversionrate'] = modified_rate(stock_price, making_rate, data.at[index, 'conversionrate'])
                data.at[index, 'reversalrate'] = modified_rate(stock_price, cutting_rate, data.at[index, 'reversalrate'])
            else :
                data.at[index, 'conversionrate'] = modified_rate(stock_price, cutting_rate, data.at[index, 'conversionrate'])
                data.at[index, 'reversalrate'] = modified_rate(stock_price, making_rate, data.at[index, 'reversalrate'])

            data.at[index, 'Otm'] = 'yes'

            if int(data.at[index, 'conversionlot']) == 0 and int(data.at[index, 'conversionrate']) > 0:
                data.at[index, 'conversionlot'] = 10
            elif int(data.at[index, 'reversallot']) == 0 and int(data.at[index, 'reversalrate']) > 0:
                data.at[index, 'reversallot'] = 10

    return data


def modified_rate(stock_price, rate, current_rate):
    if rate == 'NC':
        return current_rate
    modified_value = stock_price * float(rate)
    modified_value = .05 * round(modified_value / .05)
    modified_value = round(modified_value, 2)
    return modified_value


def read_stt_rates():
    df = pd.read_csv(get_prop("excel_path") + "/../STTRateInput.csv", index_col=0, delimiter=';', header=0)
    return df


update_stt_data()

