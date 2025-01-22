import sys
import time 
from datetime import datetime 
import duckdb 

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

    con = duckdb.connect(database=':memory:')

    con.execute(f"""
        CREATE OR REPLACE TABLE my_table AS
        SELECT *
        FROM read_csv_auto('{filename}');
    """)

    df_pixel = con.execute(f"""
        SELECT 
            pixel_color,
            COUNT(*) AS frequency
        FROM my_table
        WHERE "timestamp" >= '{start_dt}'
          AND "timestamp" < '{end_dt}'
        GROUP BY pixel_color
        ORDER BY frequency DESC
        LIMIT 1;
    """).df()

    df_coord = con.execute(f"""
        SELECT 
            coordinate,
            COUNT(*) AS frequency
        FROM my_table
        WHERE "timestamp" >= '{start_dt}'
          AND "timestamp" < '{end_dt}'
        GROUP BY coordinate
        ORDER BY frequency DESC
        LIMIT 1;
    """).df()

    print("Most Placed Color: ", df_pixel)
    print("Most Placed Coord: ", df_coord)

    runtime_end = time.perf_counter_ns()

    elapsed_runtime = runtime_end - runtime_start

    print(f"Script runtime: {elapsed_runtime / 1_000_000:.4f} ms")


if __name__ == "__main__":
    main()

