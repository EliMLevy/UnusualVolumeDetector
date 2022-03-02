# importing the required module
import matplotlib.pyplot as plt
import math
import numpy as np
import datetime as dt
import pandas as pd
import json
import dateutil.relativedelta



# import output from sim_data

def make_graph(start_time, end_time, y_vals, output_name, ticker):
    # Set up figure
    plt.title(output_name)
    plt.figure(figsize=(10,5))
    plt.xticks(rotation=40)
    plt.xlabel('Time')
    plt.ylabel('Volume')
    plt.minorticks_on()
    x = [n for n in pd.date_range(start=start_time,end=end_time)]
    # Plot portfolio
    plt.plot(x, y_vals)
    plt.savefig('./output/' + str(ticker.upper()) + "/" + output_name + '.png')
    plt.clf()
    plt.close('all')





def main():
    # end_time = dt.date.today()
    # start_time =  end_time - dt.timedelta(months=5)

    all_tickers_df = pd.read_csv("output/2022-03-02.csv")
    for ticker in all_tickers_df["Symbol"]:
        # ticker = "AEL"
        print(ticker)

        data = pd.read_csv("output/"+str(ticker)+"/volume.csv")

        endDate = dt.datetime.strptime(data.iloc[len(data) - 1]["Date"], "%Y-%m-%d")
        startDate = dt.datetime.strptime(data.iloc[0]["Date"], "%Y-%m-%d")
        # print(("2022-02-24" in list(data["Date"])))
        for day in pd.date_range(start=startDate,end=endDate):
            if str(day.date()) not in list(data["Date"]):
                data = pd.concat([data[data["Date"] < str(day.date())], pd.DataFrame({"Date":[str(day.date())], "Volume":[None]}), data[data["Date"] > str(day.date())]]).reset_index(drop=True)

        for i in range(len(data)):
            if data.iloc[i]["Volume"] is None:
                # print(i)
                data.iloc[i]["Volume"] = data["Volume"][i - 1]

        # data.to_csv("output/ACCO/volumeV2.csv", index=False)

        y_vals = data["Volume"]
        # x_vals = data["Date"]
        make_graph(startDate, endDate, y_vals, str(ticker) + " Volume", ticker)



if __name__ == "__main__":
    main()