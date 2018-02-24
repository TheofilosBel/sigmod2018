#!/usr/bin/env python
import argparse
import sys
import pandas as pd

parser = argparse.ArgumentParser(description='Give me a .tbl path')
parser.add_argument('path', metavar='P', type=str, nargs='+',
                   help='a path for the accumulator')

args = parser.parse_args()

if len(sys.argv) > 2:
    sys.exit(1)

path = sys.argv[1]

with open(path, 'r') as f:
    data = pd.read_csv(path, sep="|", header=None, index_col=False)
    data.drop(data.columns[[-1,]], axis=1, inplace=True)
    print(data)

    num_rows = data.shape[0]
    num_columns = data.shape[1]
    print(str(num_rows) + " rows")
    print(str(num_columns) + " columns")

f.close()
