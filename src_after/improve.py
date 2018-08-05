import csv
import tarfile, gzip
import os 

import psutil, time

import threading

class Singlecsv():

    def __init__(self):
        # initializing the titles and rows list
        self.fields = []
        self.rows = []
        self.count = 0
        self.namelist1 = ['07','08','09','10','11']
        self.namelist2 = ['0','1','2','3','4','6']
        self.csvR = []
        self.csvW = []
        pass

    def get_fields_rows(self, filename):
        self.fields = []
        # reading csv file
        with open(filename+".csv", 'r') as csvfile:
            # creating a csv reader object
            csvreader = csv.reader(csvfile)
            
            # extracting field names through first row
            self.fields = csvreader.next()
        
            # extracting each data row one by one
            for row in csvreader:
                self.rows.append(row)
        
            # get total number of rows
            print("Total no. of rows: %d"%(csvreader.line_num))

    def write_csv(self, filename, mydict):
        # writing to csv file
        with open(filename+".csv", 'w') as csvfile:
            # creating a csv dict writer object
            writer = csv.writer(csvfile)
            # writing headers (field names)
            a = [];  a.append(self.fields)
            writer.writerows(a)
            # writing data rows
            writer.writerows(mydict)

    def seperateRW(self):
        for row in self.rows:
            if(row[2] == "R"):
                self.csvR.append(row)
            else:
                self.csvW.append(row)
        self.rows = []


    def sort(self, filer,filew):
        sortedR = sorted(self.csvR, key = lambda x: (int(x[5]), float(x[0])))
        sortedW = sorted(self.csvW, key = lambda x: (int(x[5]), float(x[0])))
        self.write_csv(filer, sortedR)
        self.write_csv(filew, sortedW)

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
            buffer = []

            while (csv1Next is not done) or (csv2Next is not done):
                count+=1
                if (csv2Next is done) or ((csv1Next is not done) and cmprow(csv1Next, csv2Next)):
                    buffer.append(csv1Next)
                    # writer.writerow(csv1Next)
                    csv1Next = next(csvreader1, done)
                else:
                    buffer.append(csv2Next)
                    # writer.writerow(csv2Next)
                    csv2Next = next(csvreader2, done)
                if(len(buffer) == 10000):
                    writer.writerows(buffer)
                    buffer = []
            if buffer != []:
                writer.writerows(buffer)
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

def untar(fname, dirs): 
    with tarfile.open(fname) as t:
        t.extractall(path = dirs)
def ungz(file_name,fileto):
    with gzip.open(file_name) as g:
        with open(fileto, "w+") as op:
            op.write(g.read())


t1 = time.time()

# step0 task0
if not os.path.exists('improve'):
    os.mkdir('improve')
untar('systor17-01.tar','improve/')
csv1 = Singlecsv()
count = 1
for i in csv1.namelist1:
    for j in csv1.namelist2:
        filename = "improve/20160222"+i+"-LUN"+j+'.csv.gz'
        ungz(filename, 'improve/'+str(count)+'.csv')
        print 'ungz '+filename,count
        count += 1
filename = "improve/20160222"+'12'+"-LUN"+'0'+'.csv.gz'
ungz(filename,'improve/31.csv')
filename = "improve/20160222"+'12'+"-LUN"+'1'+'.csv.gz'
ungz(filename,'improve/32.csv')

t2 = time.time();  print 'time usage:',t2-t1
## step1 task1 & task2
def partcsv_toRW(m,n,k):
    csv1 = Singlecsv()
    for i in range(m,n):
        filename = 'improve/'+str(i)
        print 'csvget&seperate '+filename
        csv1.get_fields_rows(filename)
        csv1.seperateRW()
    csv1.sort('improve/R'+str(k)+'temp','improve/W'+str(k)+'temp')
partcsv_toRW(1,5,1)
partcsv_toRW(5,9,2)
partcsv_toRW(9,13,3)
partcsv_toRW(13,17,4)
partcsv_toRW(17,21,5)
partcsv_toRW(21,25,6)
partcsv_toRW(25,29,7)
partcsv_toRW(29,33,8)

t3 = time.time();  print 'time usage:',t3-t1
## step3 task3
# combine all sorted files


td1 = threading.Thread(target = combinecsv, args = ('improve/R1temp', 'improve/R2temp', 'improve/R11temp',)); td1.start()
td2 = threading.Thread(target = combinecsv, args = ('improve/W1temp', 'improve/W2temp', 'improve/W11temp',)); td2.start()
td3 = threading.Thread(target = combinecsv, args = ('improve/R3temp', 'improve/R4temp', 'improve/R22temp',)); td3.start()
td4 = threading.Thread(target = combinecsv, args = ('improve/W3temp', 'improve/W4temp', 'improve/W22temp',)); td4.start()
td5 = threading.Thread(target = combinecsv, args = ('improve/R5temp', 'improve/R6temp', 'improve/R33temp',)); td5.start()
td6 = threading.Thread(target = combinecsv, args = ('improve/W5temp', 'improve/W6temp', 'improve/W33temp',)); td6.start()
td7 = threading.Thread(target = combinecsv, args = ('improve/R7temp', 'improve/R8temp', 'improve/R44temp',)); td7.start()
td8 = threading.Thread(target = combinecsv, args = ('improve/W7temp', 'improve/W8temp', 'improve/W44temp',)); td8.start()
td1.join(); td2.join(); td3.join(); td4.join(); td5.join(); td6.join(); td7.join(); td8.join()

td1 = threading.Thread(target = combinecsv, args = ('improve/R11temp', 'improve/R22temp', 'improve/R111temp',)); td1.start()
td2 = threading.Thread(target = combinecsv, args = ('improve/W11temp', 'improve/W22temp', 'improve/W111temp',)); td2.start()
td3 = threading.Thread(target = combinecsv, args = ('improve/R33temp', 'improve/R44temp', 'improve/R222temp',)); td3.start()
td4 = threading.Thread(target = combinecsv, args = ('improve/W33temp', 'improve/W44temp', 'improve/W222temp',)); td4.start()
td1.join(); td2.join(); td3.join(); td4.join()

td1 = threading.Thread(target = combinecsv, args = ('improve/R111temp', 'improve/R222temp', 'improve/Rtemp',)); td1.start()
td2 = threading.Thread(target = combinecsv, args = ('improve/W111temp', 'improve/W222temp', 'improve/Wtemp',)); td2.start()
td1.join(); td2.join()

t4 = time.time();  print 'time usage:',t4-t1
## step4 task3
get_size_count('improve/Rtemp','improve/R')
get_size_count('improve/Wtemp','improve/W')

t5 = time.time();  print 'time usage:',t5-t1
current = psutil.Process()
print current.io_counters()
# f.write('time usage:%f\n',(t5-t1))
# f.write(str(current.io_counters())


