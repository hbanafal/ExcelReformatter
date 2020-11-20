import pandas as pd
from read_prop import get_prop
import glob
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
    current_month = get_month(datetime.now().month)
    next_month = get_month(datetime.now().month + 1)
    rates_df = read_stt_rates()

    max_order_lot = rates_df.at["MAX_ORDER_LOT", "Cutting>10"]
    generic_cutting_rate_gt_10 = rates_df.at["Rate", "Cutting>10"]
    generic_min_cutting_rate_gt_10 = rates_df.at["Rate", "MinCut>10"]
    generic_cutting_rate_lt_10 = rates_df.at["Rate", "Cutting<10"]
    generic_making_rate_lt_10 = rates_df.at["Rate", "Making<10"]
    generic_min_cutting_rate_lt_10 = rates_df.at["Rate", "MinCut<10"]
    generic_min_making_rate_lt_10 = rates_df.at["Rate", "MinMak<10"]

    for index, row in data.iterrows():

        if str(row['Portfolio']) in {"", "nan", "NaN", "null", "NULL"}:
            data.drop(index=index, inplace=True)
            continue
        stock_range = str(row['Portfolio']).split('_')[2]
        stock_range = float(stock_range.split("TO")[0])
        stock_name = str(row['Portfolio']).split('_')[0]
        stock_price = 0
        if current_month in stock_name:
            stock_price = float(stock_name.split(current_month)[1])
            stock_name = stock_name.split(current_month)[0]
        elif next_month in stock_name:
            stock_price = float(stock_name.split(next_month)[1])
            stock_name = stock_name.split(next_month)[0]
        else:
            print("There is no current month ", current_month, " or next month ", next_month, " in Portfolio name ",
                  stock_name)

        data.at[index, 'maxorderlot'] = max_order_lot

        # This if block handles the stocks with range greater than 10
        if stock_range >= 10:
            cutting_rate_gt_10 = generic_cutting_rate_gt_10
            min_cutting_rate_gt_10 = generic_min_cutting_rate_gt_10

            # Updating cutting rate and minimum cutting rate if the stock is listed in Input file
            if stock_name in rates_df.index:
                cutting_rate_gt_10 = rates_df.at[stock_name, "Cutting>10"]
                if rates_df.at[stock_name, "MinCut>10"] not in {"", "nan", "NaN", "null", "NULL"}:
                    min_cutting_rate_gt_10 = rates_df.at[stock_name, "MinCut>10"]

            # If cutting rate is NC (No Change) then skipping that stock and not updating anything except maxorderlot.
            if cutting_rate_gt_10 == 'NC':
                continue

            print("Stock name is ", stock_name, "Rate is ", cutting_rate_gt_10)
            data.at[index, 'Otm'] = 'no'

            # Updating conversion rate or reversal rate.
            if float(data.at[index, 'conversionrate']) > 0:
                data.at[index, 'conversionrate'] = modified_rate(stock_price, cutting_rate_gt_10, None, min_cutting_rate_gt_10)
            else:
                data.at[index, 'reversalrate'] = modified_rate(stock_price, cutting_rate_gt_10, None, min_cutting_rate_gt_10)

        # This else block handles the stocks with range less than 10
        elif stock_range < 10:
            cutting_rate_lt_10 = generic_cutting_rate_lt_10
            making_rate_lt_10 = generic_making_rate_lt_10
            min_cutting_rate_lt_10 = generic_min_cutting_rate_lt_10
            min_making_rate_lt_10 = generic_min_making_rate_lt_10

            # Updating cutting rate, making rate, minimum cutting rate and minimum making rate if the stock is listed
            # in Input file
            if stock_name in rates_df.index:
                cutting_rate_lt_10 = rates_df.at[stock_name, "Cutting<10"]
                making_rate_lt_10 = rates_df.at[stock_name, "Making<10"]
                if rates_df.at[stock_name, "MinMak<10"] not in {"", "nan", "NaN", "null", "NULL"}:
                    min_making_rate_lt_10 = rates_df.at[stock_name, "MinMak<10"]
                if rates_df.at[stock_name, "MinCut<10"] not in {"", "nan", "NaN", "null", "NULL"}:
                    min_cutting_rate_lt_10 = rates_df.at[stock_name, "MinCut<10"]

            # If cutting rate and making rate is NC (No Change) then skipping that stock and not updating anything
            # except maxorderlot.
            if cutting_rate_lt_10 == 'NC' and making_rate_lt_10 == 'NC':
                continue

            print("Stock name is ", stock_name, "Rates are ", cutting_rate_lt_10, making_rate_lt_10)

            # Updating conversion rate or reversal rate.
            if float(data.at[index, 'conversionrate']) == 0:
                data.at[index, 'conversionrate'] = modified_rate(stock_price, making_rate_lt_10, data.at[index, 'conversionrate'], min_making_rate_lt_10)
                data.at[index, 'reversalrate'] = modified_rate(stock_price, cutting_rate_lt_10, data.at[index, 'reversalrate'], min_cutting_rate_lt_10)
            else :
                data.at[index, 'conversionrate'] = modified_rate(stock_price, cutting_rate_lt_10, data.at[index, 'conversionrate'], min_cutting_rate_lt_10)
                data.at[index, 'reversalrate'] = modified_rate(stock_price, making_rate_lt_10, data.at[index, 'reversalrate'], min_making_rate_lt_10)

            data.at[index, 'Otm'] = 'yes'

            # Updating conversion lot or reversal lot.
            if int(data.at[index, 'conversionlot']) == 0 and int(data.at[index, 'conversionrate']) > 0:
                data.at[index, 'conversionlot'] = 10
            elif int(data.at[index, 'reversallot']) == 0 and int(data.at[index, 'reversalrate']) > 0:
                data.at[index, 'reversallot'] = 10

    return data


def modified_rate(stock_price, rate, current_rate, min_value):
    if rate == 'NC':
        return current_rate
    modified_value = stock_price * float(rate)
    modified_value = .05 * round(modified_value / .05)
    modified_value = round(modified_value, 2)
    if modified_value < min_value:
        modified_value = min_value
    return modified_value


def read_stt_rates():
    df = pd.read_csv(get_prop("excel_path") + "/../STTRateInput.csv", index_col=0, delimiter=';', header=0)
    return df


def get_month(month):
    months = {1: "JAN", 2: "FEB", 3: "MAR", 4: "APR", 5: "MAY", 6: "JUN", 7: "JUL", 8: "AUG", 9: "SEP", 10: "OCT",
              11: "NOV", 12: "DEC"}
    if month > 12:
        return months.get(1)
    else:
        return months.get(month)

update_stt_data()