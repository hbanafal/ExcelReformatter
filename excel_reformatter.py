import pandas as pd
from read_prop import get_prop
import glob


def combine_excel():
    combined_data = pd.DataFrame()
    files = glob.glob(get_prop("excel_path") + "/*.csv")
    if len(files) == 0:
        print("There are not input files on the path given in property file.")
        exit(1)
    for f in glob.glob(get_prop("excel_path") + "/*.csv"):
        df = pd.read_csv(f, na_filter=False, skip_blank_lines=True)
        combined_data = combined_data.append(df, ignore_index=True)
    print("All Files are read successfully")

    if len(combined_data.index) == 1:
        print("There is no data in the input CSV files.")
        exit(1)
    combined_data = combined_data.drop_duplicates(subset=['Portfolio', 'TradingSymbol'])
    print("Duplicate Stocks are removed!!")

    combined_data = update_rates(combined_data)
    print("All files are combined and conversion & reversal rates updated")

    return combined_data


def update_rates(combined_data):
    for index, row in combined_data.iterrows():
        if str(row['Portfolio']) in {"", "nan", "NaN", "null", "NULL"}:
            combined_data.drop(index=index, inplace=True)
            continue
        stock_price = str(row['Portfolio']).split('_')[2]
        stock_name = str(row['Portfolio']).split('_')[0]
        rates = read_rates()[0]
        generic_rate = rates['Rate']
        if stock_name in rates:
            generic_rate = rates[stock_name]

        if generic_rate != 'NC':
            modified_value = float(stock_price) * float(generic_rate)
            modified_value = .05 * round(modified_value / .05)
            modified_value = round(modified_value, 2)
            combined_data.at[index, 'conversionrate'] = modified_value
            combined_data.at[index, 'reversalrate'] = modified_value

    return combined_data


def read_rates():
    df = pd.read_csv(get_prop("excel_path") + "/../RateInput.csv", names=['Rates', 'Value'], delimiter=';', header=0)
    input_rates = df.set_index('Rates').T.to_dict('records')
    return input_rates


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


create_output_csv(10)
