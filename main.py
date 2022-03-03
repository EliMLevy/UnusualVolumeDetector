from s3handler import (get_client, put_string)
import pandas as pd
from unusual_volume import scan_market

from dotenv import load_dotenv
import os

def main():
    scan_market(3)

    # load_dotenv()
    # base_dir = os.getenv("BASE_DIR")
    # data = pd.read_csv(base_dir + "output/2022-03-02.csv")
    # client = get_client()
    # put_string(client, "mysecfilings", data.to_json(orient="records"), "data/unusualVolume/2022-03-02.json")

if __name__ == "__main__":
    main()