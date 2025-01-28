import sys
import time 
from datetime import datetime  
import polars as pl 

def parse_args():

    if len(sys.argv) != 4:
        print("Error: Usage must be in the format YYYY-MM-DD HH YYYY-MM-DD HH filename.parquet")
        sys.exit(1)

    time_format = "%Y-%m-%d %H:%M:%S"
    try: 
        start_dt = datetime.strptime(sys.argv[1]+':00:00', time_format)
        end_dt = datetime.strptime(sys.argv[2]+':00:00', time_format)
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

    # top 3 colors 
    top_cols = df.group_by('pixel_color').agg([pl.col('user_id').n_unique().alias('count')]).sort('count', descending=True)

    print('Top colors:\n')
    print(top_cols)
    print('\n')

    # quantiles 
    counts = df.group_by('user_id').agg([pl.count().alias('count')])
    q50 = counts.select(pl.quantile('count', 0.5)).item()
    q75 = counts.select(pl.quantile('count', 0.75)).item()
    q90 = counts.select(pl.quantile('count', 0.90)).item()
    q99 = counts.select(pl.quantile('count', 0.99)).item() 

    print('Q50: ', q50)
    print('Q75: ', q75)
    print('Q90: ', q90)
    print('Q99: ', q99)

    # num_first_users 
    num_first = df.select(pl.col('is_first')).filter('is_first').count().item()
    print('Number of new users: ', num_first)

    # avg session length
    df = df.sort(['user_id', 'timestamp']).with_columns((pl.col('timestamp') - pl.col('timestamp').shift(1)).fill_null(pl.duration(hours=0)).alias('dif'))
    df = df.with_columns((pl.col('dif') >= pl.duration(minutes=15)).cast(pl.Int32).alias('session'))
    df = df.group_by(['user_id', 'session']).agg(
        pl.col('timestamp').min().alias('start'),
        pl.col('timestamp').max().alias('end'),
        pl.count().alias('num_placements')  
    )
    df = df.filter(pl.col('num_placements') > 1).with_columns((pl.col('end') - pl.col("start")).alias('dur'))
    result = df.select(pl.mean('dur')).item()
    print('Average length: ', result)

    runtime_end = time.perf_counter_ns()

    elapsed_runtime = runtime_end - runtime_start

    print(f'Script runtime: {elapsed_runtime / 1_000_000:.4f} ms')

if __name__ == "__main__":
    main()





    