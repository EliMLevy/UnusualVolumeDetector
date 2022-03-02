import os
import time
import yfinance as yf
import dateutil.relativedelta
from datetime import date
from datetime import timedelta
from datetime import datetime as dt
import calendar
import io
import requests

import http.client
import pandas as pd


import numpy as np
import sys
from stocklist import NasdaqController
from tqdm import tqdm


class mainObj:
    def getData(self, ticker):
        currentDate = dt.strptime(
            date.today().strftime("%Y-%m-%d"), "%Y-%m-%d")
        pastDate = currentDate - dateutil.relativedelta.relativedelta(months=5)
        sys.stdout = open(os.devnull, "w")
        data = yf.download(ticker, pastDate, currentDate)
        sys.stdout = sys.__stdout__
        return data[["Volume"]]
    
    def getYahooDf(self, ticker, startDate, endDate): # dates in ISO format

        fromDate = calendar.timegm(startDate.utctimetuple()) # To Unix timestamp format used by Yahoo
        toDate = calendar.timegm(endDate.utctimetuple())

        domain = "query1.finance.yahoo.com"
        url = "/v7/finance/download/"+str(ticker)+"?period1="+str(fromDate)+"&period2="+str(toDate)+"&interval=1d&events=history&includeAdjustedClose=true"

        conn = http.client.HTTPSConnection(domain)
        payload = ''
        headers = {
        'Cookie': 'B=8jaeb81gvdbdo&b=3&s=gu'
        }
        conn.request("GET", url, payload, headers)
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

    def find_anomalies(self, data, cutoff):
        anomalies = []
        data_std = np.std(data['Volume'])
        data_mean = np.mean(data['Volume'])
        anomaly_cut_off = data_std * cutoff
        upper_limit = data_mean + anomaly_cut_off
        indexs = data[data['Volume'] > upper_limit].index.tolist()
        outliers = data[data['Volume'] > upper_limit].Volume.tolist()
        index_clean = [str(x)[:-9] for x in indexs]
        d = {'Dates': index_clean, 'Volume': outliers}
        return d

    def find_anomalies_two(self, data, cutoff):
        indexs = []
        outliers = []
        data_std = np.std(data['Volume'])
        data_mean = np.mean(data['Volume'])
        anomaly_cut_off = data_std * cutoff
        upper_limit = data_mean + anomaly_cut_off
        data.reset_index(level=0, inplace=True)
        for i in range(len(data)):
            temp = data['Volume'].iloc[i]
            if temp > upper_limit:
                indexs.append(str(data['Date'].iloc[i])[:-9])
                outliers.append(temp)
        d = {'Dates': indexs, 'Volume': outliers}
        return d

    def customPrint(self, d, tick):
        print("\n\n\n*******  " + tick.upper() + "  *******")
        print("Ticker is: "+tick.upper())
        for i in range(len(d['Dates'])):
            str1 = str(d['Dates'][i])
            str2 = str(d['Volume'][i])
            print(str1 + " - " + str2)
        print("*********************\n\n\n")

    def days_between(self, d1, d2):
        d1 = dt.strptime(d1, "%Y-%m-%d")
        d2 = dt.strptime(d2, "%Y-%m-%d")
        return abs((d2 - d1).days)

    def main_func(self, cutoff):
        StocksController = NasdaqController(True)
        list_of_tickers = StocksController.getList()
        currentDate = dt.strptime(
            date.today().strftime("%Y-%m-%d"), "%Y-%m-%d")
        earlierDate = currentDate - dateutil.relativedelta.relativedelta(months=5)
        start_time = time.time()
        for x in tqdm(list_of_tickers):
            print(x)
            d = (self.find_anomalies_two(self.getYahooDf(x, earlierDate, currentDate), cutoff))
            print(d)
            if d['Dates']:
                for i in range(len(d['Dates'])):
                    print( str(d['Dates'][i]))
                    if self.days_between(str(currentDate)[:-9], str(d['Dates'][i])) <= 3:
                        self.customPrint(d, x)

        print("\n\n\n\n--- this took %s seconds to run ---" %
              (time.time() - start_time))


# input desired anomaly standard deviation cuttoff
# run time around 50 minutes for every single ticker.
mainObj().main_func(10)
