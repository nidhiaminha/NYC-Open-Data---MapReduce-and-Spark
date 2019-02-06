import csv  
import os
result_store="./result_spark/"
result_folder = os.listdir(result_store)
result_folder.sort()
for i in result_folder:
    sub_folder=result_store+"/"+i
    if(os.path.isdir(sub_folder)):
        files = os.listdir(sub_folder)
        csv_f = filter(lambda x:x[-4:] == '.csv',files)
        if(len(csv_f)>0):
            for j in csv_f:
                with open(sub_folder+"/"+j) as csv_file:
                    csv_reader = csv.reader(csv_file, delimiter=',')
                    task_count=0
                    for row in csv_reader:
                        task_count=task_count+1
                        if(i=="Sparktask2.1"):
                            print "Result : Date on which maximum number of accidents took place" + repr(row)
                        if(i=="Sparktask2.2"):
                            print "Result : Borough with maximum count of accident fatality" + repr(row)
                        if(i=="Sparktask2.3"):
                            print "Result : Zip with maximum count of accident fatality" + repr(row)
                        if(i=="Sparktask2.4"):
                            print "Result : vehicle type involved in maximum accidents" + repr(row)
                        if(i=="Sparktask2.5"):
                            print "Result : Year in which maximum Number Of Persons and Pedestrians Injured" + repr(row)
                        if(i=="Sparktask2.6"):
                            print "Result : Year in which maximum Number Of Persons and Pedestrians Killed" + repr(row)
                        if(i=="Sparktask2.7"):
                            print "Result : Year in which maximum Number Of Cyclist Injured and Killed" + repr(row)
                        if(i=="Sparktask2.8"):
                            print "Result : Year in which maximum Number Of Motorist Injured and Killed" + repr(row)
