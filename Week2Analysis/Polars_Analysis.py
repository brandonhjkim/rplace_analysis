import sys
import time 
from datetime import datetime 
import polars as pl 

def preprocess_csv_to_parquet(csv_path, parquet_path):

    df = pl.read_csv(csv_path, columns=['timestamp', 'pixel_color', 'coordinate'])

    df = df.with_columns(df['timestamp'].str.slice(0, 19).str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S"))

    df.write_parquet(parquet_path) 

# preprocess_csv_to_parquet('2022_place_canvas_history.csv', '2022_place_canvas_history.parquet')

def parse_args():

    if len(sys.argv) != 4:
        print("Error: Usage must be in the format YYYY-MM-DD HH YYYY-MM-DD HH filename.parquet")
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

    df = pl.read_parquet(filename) 

    df = df.filter((pl.col('timestamp') >= pl.lit(start_dt)) & (pl.col('timestamp') <= pl.lit(end_dt)))

    most_common_color = (
        df
        .group_by('pixel_color')
        .count()
        .sort('count', descending=True)
        ["pixel_color"][0]
    )

    most_common_coord = (
        df
        .group_by('coordinate')
        .count()
        .sort('count', descending=True)
        ["coordinate"][0]
    )

    print("Most Placed Color: ", most_common_color)
    print("Most Placed Coord: ", most_common_coord)

    runtime_end = time.perf_counter_ns()

    elapsed_runtime = runtime_end - runtime_start

    print(f"Script runtime: {elapsed_runtime / 1_000_000:.4f} ms")

if __name__ == "__main__":
    main()

