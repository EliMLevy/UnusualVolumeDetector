import datetime as dt
import http.client
import pandas as pd
import io
import calendar
from stocklist import NasdaqController
import numpy as np
import dateutil.relativedelta
import time
from tqdm import tqdm
import os
from grapher import make_graph
from s3handler import (get_client, put_file, put_string)


from dotenv import load_dotenv
import os

load_dotenv()
base_dir = os.getenv("BASE_DIR")


def getVolume(ticker, startDate, endDate): # dates in ISO format

    fromDate = calendar.timegm(startDate.utctimetuple()) # To Unix timestamp format used by Yahoo
    toDate = calendar.timegm(endDate.utctimetuple())

    domain = "query1.finance.yahoo.com"
    path = "/v7/finance/download/"+str(ticker)+"?period1="+str(fromDate)+"&period2="+str(toDate)+"&interval=1d&events=history&includeAdjustedClose=true"

    time.sleep(1)
    conn = http.client.HTTPSConnection(domain)
    payload = ''
    headers = {
        'Cookie': 'B=8jaeb81gvdbdo&b=3&s=gu'
    }
    conn.request("GET", path, payload, headers)
    res = conn.getresponse()
    data = res.read()
    decoded_data = data.decode("utf-8")

    if res.status < 200 or res.status > 299:
        return None
    else:
        csv = io.StringIO(decoded_data)
        df = pd.read_csv(csv, index_col='Date')
        # print(df)
        return df[["Volume"]]

def find_anomalies(data, threshold):
    dates = []
    unusual_volume = []
    data_std = np.std(data['Volume'])
    data_mean = np.mean(data['Volume'])
    anomaly_cut_off = data_std * threshold
    upper_limit = data_mean + anomaly_cut_off
    data.reset_index(level=0, inplace=True)
    for i in range(len(data)):
        temp = data.iloc[i]
        if temp["Volume"] > upper_limit:
            dates.append(str(temp["Date"]))
            unusual_volume.append(temp["Volume"])
    result = {'Dates': dates, 'Volume': unusual_volume, 'Mean':[round(data_mean,2) for n in range(len(dates))]}
    return result


def scan_market(threshold):
    s3_client = get_client()
    StocksController = NasdaqController(True)
    list_of_tickers = StocksController.getList()
    endDate = dt.datetime.strptime(dt.date.today().strftime("%Y-%m-%d"), "%Y-%m-%d")
    startDate = endDate - dateutil.relativedelta.relativedelta(months=5)
    start_time = time.time()
    result_df = pd.DataFrame(columns=["Symbol", "Date", "Volume", "Mean", "Dist. to Mean"])
    result_df.to_csv(base_dir + "output/" + str(dt.date.today()) + ".csv", index=False)
    for ticker in tqdm(list_of_tickers):
        # print(ticker)
        if "$" not in ticker and "." not in ticker:
            volume = getVolume(ticker, startDate, endDate)
            if volume is not None:
                # print(ticker)
                anomolies = find_anomalies(volume, threshold)
                if len(anomolies["Dates"]) > 0:
                    filled_volume = fill_data_gaps(volume)
                    for i in range(len(anomolies["Dates"])):
                        if endDate - dt.datetime.strptime(anomolies["Dates"][i], "%Y-%m-%d") <= dt.timedelta(days=3):
                            # if not os.path.isdir("output/" + str(ticker.upper())):
                            #     os.mkdir("output/" + str(ticker.upper()))
                            filled_volume.to_json(base_dir + "output/volume.json", orient="records")
                            put_file(s3_client, "mysecfilings", base_dir + "output/volume.json", "data/unusualVolume/volume/" + str(ticker.upper()) + ".json")
                            # filled_volume.to_csv("output/" + str(ticker.upper()) + "/volume.csv", index=False)
                            # make_graph(dt.datetime.strptime(filled_volume.iloc[0]["Date"], "%Y-%m-%d"), dt.datetime.strptime(filled_volume.iloc[-1]["Date"], "%Y-%m-%d"), filled_volume["Volume"], str(ticker) + " Volume", ticker)
                            df = pd.DataFrame({
                                "Symbol":[ticker], 
                                "Date": anomolies["Dates"][i], 
                                "Volume": anomolies["Volume"][i],
                                "Mean": anomolies["Mean"][i],
                                "Dist. to Mean": round(anomolies["Volume"][i] - anomolies["Mean"][i],2)
                            })
                            # print(df)
                            df.to_csv(base_dir + "output/" + str(dt.date.today()) + ".csv", header=False, index=False, mode='a')

            else:
                print(ticker)


    pd.read_csv(base_dir + "output/" + str(dt.date.today()) + ".csv").to_json(base_dir + "output/volume.json", orient="records")
    put_file(s3_client, "mysecfilings", base_dir + "output/volume.json", "data/unusualVolume/"+ str(dt.date.today()) + ".json")
    put_file(s3_client, "mysecfilings", base_dir + "output/volume.json", "data/unusualVolume/"+ str(dt.date.today() + dt.timedelta(days=1)) + ".json")


def fill_data_gaps(data):
    end_date = dt.datetime.strptime(data.iloc[-1]["Date"], "%Y-%m-%d")
    start_date = dt.datetime.strptime(data.iloc[0]["Date"], "%Y-%m-%d")
    for day in pd.date_range(start=start_date,end=end_date):
        if str(day.date()) not in list(data["Date"]):
            data = pd.concat([data[data["Date"] < str(day.date())], pd.DataFrame({"Date":[str(day.date())], "Volume":[None]}), data[data["Date"] > str(day.date())]]).reset_index(drop=True)

    for i in range(len(data)):
        if data.iloc[i]["Volume"] is None:
            data.iloc[i]["Volume"] = data["Volume"][i - 1]

    return data

def main():
    # print("hello")
    # data = getVolume("GOOG", dt.datetime(2022,2,28) - dt.timedelta(days=30), dt.datetime(2022,2,28))
    # anomolies = find_anomalies(data, 2)
    # print(data)
    # print(anomolies)
    scan_market(3)
    # StocksController = NasdaqController(True)
    # list_of_tickers = StocksController.getList()
    # print(list_of_tickers)

if __name__ == "__main__":
    main()