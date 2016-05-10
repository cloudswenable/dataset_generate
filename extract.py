#!/usr/bin/env python


import sys

import os


def process():
    file=open(sys.argv[1])
    ofile = open("out.txt", "w")
    rowid = 1
    for line in file:
        out = []
        fields = line.split(",")
        cid=1
        for data in fields:
            if(data.strip() != "0".strip()):
                ofile.write(str(rowid)+","+str(cid)+","+data+"\n")
            cid += 1
        rowid += 1
        


if __name__ == "__main__":
    process()
