#!/usr/bin/env python

# CREATED BY ORESTIS
# CONTINUED BY YORGOS

import matplotlib.pyplot as plt
import argparse
import sys
import pandas as pd
from decimal import *

# parse command line arguments
parser = argparse.ArgumentParser(description='Give me path to directory of .tbl files. Give me number of .tbl files. I will start from 0 and try to reach your given number minus 1. Also, give me an order.')
parser.add_argument('path', metavar='P', type=str, nargs='+',
                   help='path to the directory of .tbl files')
parser.add_argument('number', metavar='N', type=int, nargs='+',
                   help='number of .tbl files')
parser.add_argument('order', metavar='O', type=str, nargs='+',
                   help='order')

args = parser.parse_args()

if len(sys.argv) > 4:
    sys.exit(1)

# path to directory of .tbl files
path = sys.argv[1]
if path[len(path)-1] != '\n':
    path += '/'

# Number of relations
relations = int(sys.argv[2])

# is order ascending
is_ascending = False
if sys.argv[3].lower() == "asc" or sys.argv[3].lower() == "ascending" or sys.argv[3].lower() == "a":
    is_ascending = True

# Relations' numbers
relations_numbers = [i for i in range(relations)]

# Count the columns of every relation
columns_counter = [0] * relations
# Count the rows of every relation
rows_counter = [0] * relations
# Count the sorted columns of every relation
sorted_columns_counter = [0] * relations
# Dictionary that maps the sorted columns of every relation
# to their respective relation
sorted_columns_dict = {}
# Dictionary that maps the number of unique elements per column
# to their respective table
unique_elements_dict = {}

for i in range(relations):
    relation_file = "r" + str(i) + ".tbl"

    with open(path + relation_file, "r") as file:
        # the latest element read from each column
        latest_element = []
        # can a column be sorted from what we have seen so far?
        is_sorted = []
        # an array of sets that hold the (unique) elements of each column
        column_elements_set = []

        for line in file:
            # count columns
            if columns_counter[i] == 0:
                columns_counter[i] = line.count('|')
                latest_element = [-1] * columns_counter[i]
                is_sorted = [True] * columns_counter[i]
                column_elements_set = [set() for _ in xrange(columns_counter[i])]

            # Remove the trailing "|" + newline characters
            line = line.split("|\n")[0];

            columns = line.split("|")
            columns = map(int, columns)

            index = 0
            for column in columns:
                # if is sorted so far, go ahead and try to prove otherwise
                if is_sorted[index]:
                    if latest_element[index] != -1:
                        # we want ascending order
                        if is_ascending:
                            if latest_element[index] > column:
                                is_sorted[index] = False
                        # we want descending order
                        else:
                            if latest_element[index] < column:
                                is_sorted[index] = False

                latest_element[index] = column
                column_elements_set[index].add(column)

                index += 1
            # END for column

            # count rows
            rows_counter[i] += 1
        # END for line

        if i in unique_elements_dict:
            for index in range(0, columns_counter[i]):
                unique_elements_dict[i][index] = len(column_elements_set[index])
        else:
            unique_elements_dict[i] = [-1] * columns_counter[i]
            for index in range(0, columns_counter[i]):
                unique_elements_dict[i][index] = len(column_elements_set[index])

        sorted_columns_dict[i] = []

        index = 0
        for s in is_sorted:
            # print(s)
            # print("*")
            if s:
                # print(s)
                sorted_columns_counter[i] += 1
                sorted_columns_dict[i].append(index)

            index += 1
        # END for s
    # END with file open

    # close table's file
    file.close()

# Plot the columns data
plt.bar(range(len(columns_counter)), columns_counter, align='center')
plt.xticks(range(len(relations_numbers)), relations_numbers)
plt.title("Number of columns per relation")
plt.xlabel("Relations")
plt.ylabel("Number of columns")
plt.show()

# Plot the rows data
plt.bar(range(len(rows_counter)), rows_counter, align='center')
plt.xticks(range(len(relations_numbers)), relations_numbers)
plt.title("Number of rows per relation")
plt.xlabel("Relations")
plt.ylabel("Number of rows")
plt.show()

# Plot the sorted columns data
plt.bar(range(len(sorted_columns_counter)), sorted_columns_counter, align='center')
plt.xticks(range(len(relations_numbers)), relations_numbers)
plt.title("Number of sorted columns per relation")
plt.xlabel("Relations")
plt.ylabel("Number of sorted columns")
plt.show()

# More details regarding sorted columns
for table in sorted_columns_dict:
    if len(sorted_columns_dict[table]) == 0:
        print("Table " + str(table) + " has no sorted columns.")
    else:
        print("Table " + str(table) + " has the column(s) ")
        for i in sorted_columns_dict[table]:
            print(str(i) + " ")
        print("sorted.")

# Details regarding unique elements
getcontext().prec = 3 # set precision
for table in unique_elements_dict:
    print("Table " + str(table) + " has " + str(rows_counter[table]) + " rows and:")
    index = 0
    for column in unique_elements_dict[table]:
        print("Column " + str(index) + " with " + str(column) + " (Fq = " + str(Decimal(column)/ Decimal(rows_counter[table])) + ") unique elements.")
        index += 1
