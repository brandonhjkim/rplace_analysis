import sys
import csv
import time 
from datetime import datetime

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

    color_counts = {}
    coord_counts = {}

    with open(filename, 'r', encoding = 'utf-8') as f:
        reader = csv.reader(f) 
        next(reader, None)

        for row in reader:

            time_stamp, color, coord = row[0].strip()[:-4].strip(), row[2].strip(), row[3].strip() 

            try: 
                row_time = datetime.strptime(time_stamp, "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                try: 
                    row_time = datetime.strptime(time_stamp, "%Y-%m-%d %H:%M:%S")
                except ValueError: 
                    continue 

            if row_time >= start_dt and row_time < end_dt:
                color_counts[color] = color_counts.get(color, 0) + 1
                coord_counts[coord] = coord_counts.get(coord, 0) + 1 
        
    print("Most Placed Color: ", max(color_counts, key=color_counts.get))
    print("Most Placed Coord: ", max(coord_counts, key=coord_counts.get))

    runtime_end = time.perf_counter_ns()

    elapsed_runtime = runtime_end - runtime_start

    print(f"Script runtime: {elapsed_runtime / 1_000_000:.4f} ms") 

    
if __name__ == "__main__":
    main()