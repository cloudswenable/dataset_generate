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
        ofile.append(open(ofname,'w'))
        i+=1
    return ofile


def compare(x, y):
    if(x[0]<y[0]):
        return -1
    elif(x[0] > y[0]):
        return 1
    else:
        if(x[1] < y[1]):
            return -1
        elif(x[1] > y[1]):
            return 1
        else:
            return 0
    
def trans(file, outdir):
    path = os.path.abspath(file)
    fname = os.path.basename(file).split('.')[0]

    outdir = path.split(fname)[0]+outdir
    print outdir

    if(not os.path.exists(outdir)):
        os.mkdir(outdir)

    ofname = outdir+ '/' + fname + '_trans.csv'
    print ofname
    infile = open(file)
    ofile = open(ofname, 'w')
    ratings = []
    for line in infile:
        keys = line.strip().split(',')
        ratings.append((int(keys[1]), int(keys[0]), float(keys[2])))
    
    ratings.sort(lambda x,y: compare(x,y))
    #ratings = sorted(ratings, key=lambda x:x[0])
    print(ratings)
    print len(ratings)
    for r in ratings:
        ofile.write(str(r[0]) + ',' + str(r[1]) + ',' + str(r[2]) + '\n')

    ofile.close()

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
    for f in ofile:
        f.close()


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
    elif args.method == 'trans':
        trans(args.file, args.outdir)
    else:
        print('unsupported split method ' + args.method )
        sys.exit(2)
