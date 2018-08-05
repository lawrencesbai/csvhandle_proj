# importing csv module
import csv

 

class Singlecsv():

    def __init__(self):
        # initializing the titles and rows list
        self.fields = []
        self.rows = []
        pass


    def get_fields_rows(self, filename):
        self.fields = []
        self.rows = []
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
        
        # printing the field names
        print('Field names are:' + ', '.join(field for field in self.fields))
        
        # printing first 5 rows
        # print('\nFirst 5 rows are:\n')
        # for row in self.rows[:5]:
        #     # parsing each column of a row
        #     for col in row:
        #         print("%10s"%col),
        #     print('\n') 
        print('\n\n')

    def write_csv(self, filename, mydict):
        # writing to csv file
        with open(filename+".csv", 'w') as csvfile:
            # creating a csv dict writer object
            # writer = csv.DictWriter(csvfile, fieldnames = fields)
            writer = csv.writer(csvfile)
            # writing headers (field names)
            a = []
            a.append(self.fields)
            writer.writerows(a)
            
            # writing data rows
            writer.writerows(mydict)

   

