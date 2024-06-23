import pyarrow.parquet as pq

with pq.ParquetFile("test.parquet") as pf:
    while True:
        pf.read_row_group(0)
