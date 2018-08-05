import csv, threading
import thread, psutil, time

import operator
import pandas
from rwcsv import Singlecsv
from cpu_usage import CPUmonitor

from multiprocessing import Process
import threading

class Seperatecsv(Singlecsv):
    def __init__(self):
        Singlecsv.__init__(self)
        self.csvR = []
        self.csvW = []
        
        self.count = 0
        self.namelist1 = ['07','08','09','10','11']
        self.namelist2 = ['0','1','2','3','4','6']
        
    def seperateRW(self):
        self.csvR = []
        self.csvW = []
        for row in self.rows:
            if(row[2] == "R"):
                self.csvR.append(row)
            else:
                self.csvW.append(row)
        self.count += 1
        self.write_csv("systor17-01_result/"+str(self.count)+"R", self.csvR)
        self.write_csv("systor17-01_result/"+str(self.count)+"W", self.csvW)

    def sort(self, filename):
        self.get_fields_rows(filename)
        data = self.rows 
        sortedlist = sorted(data, key = lambda x: (int(x[5]), float(x[0])) )
        
        self.write_csv(filename+"sorted", sortedlist)


def cmprow(row1, row2):
    if int(row1[5]) < int(row2[5]):
        return True
    elif int(row1[5]) > int(row2[5]):
        return False
    else:
        if float(row1[0]) <= float(row2[0]):
            return True 
        else:
            return False

def combinecsv(file1, file2, fileto):
    with open(fileto+'.csv', 'w') as csvfile:
        writer = csv.writer(csvfile)
        with open(file1+".csv", 'r') as csvfile1, open(file2+".csv", 'r') as csvfile2:
            csvreader1 = csv.reader(csvfile1)
            csvreader2 = csv.reader(csvfile2)

            count = 1

            done = object()
            csv1Next = next(csvreader1, done)
            csv2Next = next(csvreader2, done)

            a = [];  a.append(csv1Next)
            writer.writerows(a)

            csv1Next = next(csvreader1, done)
            csv2Next = next(csvreader2, done)

            while (csv1Next is not done) or (csv2Next is not done):
                count+=1
                if (csv2Next is done) or ((csv1Next is not done) and cmprow(csv1Next, csv2Next)):
                    writer.writerow(csv1Next)
                    csv1Next = next(csvreader1, done)
                else:
                    writer.writerow(csv2Next)
                    csv2Next = next(csvreader2, done)
            print count

def get_size_count(filein, fileto):
    with open(fileto+'.csv', 'w') as csvfile:
        writer = csv.writer(csvfile)
        with open(filein+'.csv', 'r') as csvfile2:
            csvreader = csv.reader(csvfile2)

            done = object();  csv2Next = next(csvreader, done)
            a = [];  a.append(csv2Next)
            writer.writerows(a)

            csv2Next = next(csvreader, done)
            index = 0;  size = int(csv2Next[5]); ans = []
            ans.append([size, 1])
            while csv2Next is not done:
                # print csv2Next
                if size != int(csv2Next[5]):
                    ans.append([csv2Next[5],1])
                    size = int(csv2Next[5])
                    index += 1
                else:
                    ans[index][1] += 1
                writer.writerow(csv2Next)
                csv2Next = next(csvreader, done)
            writer.writerow([])
            title = [['size', 'number']]
            writer.writerows(title)
            writer.writerows(ans)





print 'start'

t1 = time.time()
## step1 task1
csv2 = Seperatecsv()
for i in csv2.namelist1:
    for j in csv2.namelist2:
        # csv file name
        filename = "systor17-01/20160222"+i+"-LUN"+j
        # print filename
        csv2.get_fields_rows(filename)
        csv2.seperateRW()

filename = "systor17-01/20160222"+'12'+"-LUN"+'0'
csv2.get_fields_rows(filename)
csv2.seperateRW()

filename = "systor17-01/20160222"+'12'+"-LUN"+'1'
csv2.get_fields_rows(filename)
csv2.seperateRW()

t2 = time.time()
## step2 task2
# quick sort for every single file
for i in range(1,33):
    filename = 'systor17-01_result/'+str(i)+'R'
    csv3 = Seperatecsv()
    csv3.sort(filename)
    filename = 'systor17-01_result/'+str(i)+'W'
    csv3 = Seperatecsv()
    csv3.sort(filename)

t3 = time.time()
## step3 task2
# combine all sorted files
combinecsv('systor17-01_result/1Rsorted', 'systor17-01_result/2Rsorted', 'systor17-01_result/1bigRsorted')
combinecsv('systor17-01_result/1Wsorted', 'systor17-01_result/2Wsorted', 'systor17-01_result/1bigWsorted')
flag = 1; flag2 = 2
for i in range(3,33):
    print i
    if flag == 1:
        flag = 2; flag2 = 1
    else:
        flag = 1; flag2 = 2
    # print 'systor17-01_result/'+str(flag2)+'bigRsorted', 'systor17-01_result/'+str(i)+'Rsorted', 'systor17-01_result/'+str(flag)+'bigRsorted'
    combinecsv('systor17-01_result/'+str(flag2)+'bigRsorted', 'systor17-01_result/'+str(i)+'Rsorted', 'systor17-01_result/'+str(flag)+'bigRsorted')
    combinecsv('systor17-01_result/'+str(flag2)+'bigWsorted', 'systor17-01_result/'+str(i)+'Wsorted', 'systor17-01_result/'+str(flag)+'bigWsorted')

t4 = time.time()
## step4 task3
get_size_count('systor17-01_result/1bigRsorted','systor17-01_result/R')
get_size_count('systor17-01_result/1bigWsorted','systor17-01_result/W')

t5 = time.time()

print 'time usage:',t5-t1
current = psutil.Process()
print current.io_counters()