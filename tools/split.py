#!/usr/bin/env python

import argparse
import sys
import os

def createOutFiles(outdir, file, rangelist):
    path = os.path.abspath(file)
    fname = os.path.basename(file).split('.')[0]
    
    outdir = path.split(fname)[0]   + outdir
    print outdir
    ofile = []
    i = 0
    if (not os.path.exists(outdir)):
        os.mkdir(outdir)
    for count in rangelist:
        ofname = outdir+'/'+fname+'_'+str(i+1)+'.csv'
        print ofname
        ofile[i] = open(fname,'w')
        i+=1
    return ofile


def hashSplit(slices):
    return

def rangeSplit(slices, ranges, file, outdir):
    infile = open(file)
    rangelist = ranges.split(',')
    ofile = createOutFiles(outdir, file, rangelist)
    pre = -1
    item = 0 
    i = 0
    ofid = 0
    count = int(rangelist[i])
    for line in infile:
        keys = line.split(',')
        if (keys[0] == pre):
            ofile[ofid].write(keys[0]+','+keys[1]+','+keys[2])
        else:
            pre = keys[0]
            item += 1
            if item <= count:
                ofile[ofid].write(keys[0]+','+keys[1]+','+keys[2])
            else:
                i += 1
                count = int(rangelist[i])
                item = 1
                ofid = i
                if (item<=count):
                    ofile[ofid].write(keys[0]+','+keys[1]+','+keys[2])
                else:
                    print("error number for slice")
                    sys.exit(2)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--method', help='split method: hash or range', dest='method', required=True)
    parser.add_argument('-s', '--slices', help='slices for splict resutl: Int', dest='slices')
    parser.add_argument('-f', '--file', help='file for split: string', dest='file', required=True)
    parser.add_argument('-o', '--outdir', help='dir for split result: string', dest='outdir')
    parser.add_argument('-r', '--ranges', help='ranges for method range: comma split like for 4 slices: 1,2,3,4', dest='ranges')
    args = parser.parse_args()
    if args.method == 'hash':
        hashSplit(args.slicesi, args.file, args.outdir)
    elif args.method == 'range':
        rangeSplit(args.slices, args.ranges, args.file, args.outdir)
    else:
        print('unsupported split method ' + args.method )
        sys.exit(2)
