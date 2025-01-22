import sys
import time 
from datetime import datetime 
import pandas as pd 

def parse_args():

    if len(sys.argv) != 4:
        print("Error: Usage must be in the format YYYY-MM-DD HH YYYY-MM-DD HH filename.csv")
        sys.exit(1)

    time_format = "%Y-%m-%d %H"
    try: 
        start_dt = datetime.strptime(sys.argv[1], time_format)
        end_dt = datetime.strptime(sys.argv[2], time_format)
    except ValueError:
        print("Error: Start and end times must be in the format YYYY-MM-DD HH")
        sys.exit(1) 

    if start_dt > end_dt:
        print("Error: Start time must be less than or equal to end time")
        sys.exit(1) 

    return start_dt, end_dt, sys.argv[3]


def main():

    runtime_start = time.perf_counter_ns() 

    start_dt, end_dt, filename = parse_args() 

    pixel_freq = pd.Series({})
    color_freq = pd.Series({})

    n = m = 0

    while n == 0 or m == 20000000: 

        df = pd.read_csv(filename, usecols=['timestamp', 'pixel_color', 'coordinate'], skiprows=range(1,n), nrows=20000000)

        m = len(df)
        n += m

        df['timestamp'] = pd.to_datetime(df['timestamp'].str[:19], format="%Y-%m-%d %H:%M:%S")

        df = df[(end_dt >= df['timestamp']) & (df['timestamp'] > start_dt)]

        pixel_counts = df['pixel_color'].value_counts() 
        color_counts = df['coordinate'].value_counts() 

        pixel_freq = pixel_freq.add(pixel_counts, fill_value=0).astype(int) 
        color_freq = color_freq.add(color_counts, fill_value=0).astype(int)   

    print("Most Placed Color: ", pixel_freq.idxmax())
    print("Most Placed Coord: ", color_freq.idxmax())

    runtime_end = time.perf_counter_ns()

    elapsed_runtime = runtime_end - runtime_start

    print(f"Script runtime: {elapsed_runtime / 1_000_000:.4f} ms")

if __name__ == "__main__":
    main()

