import pandas as pd
from read_prop import get_prop
import glob


def combine_excel():
    combined_data = pd.DataFrame()
    new_data = pd.DataFrame()
    files = glob.glob(get_prop("excel_path") + "/*.csv")
    if len(files) == 0:
        print("There are not input files on the path given in property file.")
        exit(1)
    for f in glob.glob(get_prop("excel_path") + "/*.csv"):
        if 'BIG' in f:
            print("New stocks file is present")
            new_data = pd.read_csv(f, na_filter=False, skip_blank_lines=True)
            continue

        df = pd.read_csv(f, na_filter=False, skip_blank_lines=True)
        combined_data = combined_data.append(df, ignore_index=True)
    print("All Files are read successfully")

    combined_data = combined_data.drop_duplicates(subset=['Portfolio', 'TradingSymbol'])
    print("Duplicate Stocks are removed!!")

    if len(new_data.index) > 1:
        new_data = check_new_stocks(combined_data, new_data)
        print("Total new stocks are ", len(new_data.index)/4)
        new_data.to_csv(path_or_buf=get_prop("excel_path") + "/../NewStocks.csv", index=False, header=True)

    combined_data = update_rates(combined_data)
    print("All files are combined and conversion & reversal rates updated")

    return combined_data


def check_new_stocks(old_data, new_data):
    for index, row in new_data.iterrows():
        if row['Portfolio'] in old_data['Portfolio'].values:
            new_data.drop(index=index, inplace=True)
            continue

    return new_data


def update_rates(combined_data):
    for index, row in combined_data.iterrows():
        if str(row['Portfolio']) in {"", "nan", "NaN", "null", "NULL"}:
            combined_data.drop(index=index, inplace=True)
            continue
        stock_price = str(row['Portfolio']).split('_')[2]
        stock_name = str(row['Portfolio']).split('_')[0]
        rates_df = read_rates()
        generic_rate = rates_df.at["Rate", "Value"]
        min_value = rates_df.at["Rate", "Min"]
        if stock_name in rates_df.index:
            generic_rate = rates_df.at[stock_name, "Value"]
            if rates_df.at[stock_name, "Min"] not in {"", "nan", "NaN", "null", "NULL"}:
                min_value = rates_df.at[stock_name, "Min"]

        if generic_rate == 'NC':
            continue
        elif generic_rate == 'DEL':
            combined_data.drop(index=index, inplace=True)
            continue

        modified_value = float(stock_price) * float(generic_rate)
        modified_value = .05 * round(modified_value / .05)
        modified_value = round(modified_value, 2)
        if modified_value < min_value:
            modified_value = min_value

        combined_data.at[index, 'conversionrate'] = modified_value
        combined_data.at[index, 'reversalrate'] = modified_value

    return combined_data


def read_rates():
    df = pd.read_csv(get_prop("excel_path") + "/../RateInput.csv", index_col=0, delimiter=';', header=0)
    return df


def create_output_csv(no_of_files):
    df = combine_excel()
    new_df = pd.DataFrame(columns=df.columns)
    for i in range(no_of_files):
            new_df.to_csv(path_or_buf=get_prop("excel_path") + "/../Output" + str(i+1) + ".csv", index=False, header=True)

    i = 1
    for group_name, df in df.groupby('Portfolio'):
        with open(get_prop("excel_path") + "/../Output" + str(i) + ".csv", 'a') as f:
            df.to_csv(f, header=False, index=False, mode='a')
            i += 1
        if i > no_of_files:
            i = 1


create_output_csv(1)
