import pandas as pd
from pathlib import Path

data_dir = Path("data/historical")
files = list(data_dir.glob("*.csv"))

print(f"Found {len(files)} files\n")

for f in files:
    df = pd.read_csv(f, index_col=0, parse_dates=True)
    print(f"{f.stem}")
    print(f"  Rows      : {len(df):,}")
    print(f"  From      : {df.index.min()}")
    print(f"  To        : {df.index.max()}")
    print(f"  Columns   : {list(df.columns)}")
    print(f"  Nulls     : {df.isnull().sum().sum()}")
    print()
