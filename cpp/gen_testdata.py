import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


cols = {}
for i in range(40):
    cols[f"col_{i}"] = np.linspace(0, 100, 100000)

df = pd.DataFrame(cols)
print(df)

tb = pa.Table.from_pandas(df)
with pq.ParquetWriter("test.parquet", schema=tb.schema, compression="zstd") as pw:
    for _ in range(10):
        pw.write_table(tb)
