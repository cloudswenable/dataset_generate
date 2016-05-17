#!/usr/bin/env python

import sys
import os
import argparse

def process(fname):
    file = open(fname)
    ofile = open("out.txt", "w")
    rowid = 1
    for line in file:
        out = []
        fields = line.split(",")
        cid=1
        for data in fields:
            if(data.strip() != "0".strip()):
                ofile.write(str(rowid)+","+str(cid)+","+data.strip()+"\n")
            cid += 1
        rowid += 1
        


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-f','--file', help='input file path for process', required=True, dest="file")
    args = parser.parse_args()
    fname = args.file
    process(fname)
