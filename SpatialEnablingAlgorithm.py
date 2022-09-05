# -*- coding: utf-8 -*-
import csv, time,os,multiprocessing
from multiprocessing import Process, Pool

addressIn = 'filepath/Addr_五結鄉.csv'
addressOut = 'filepath/Preprocessing_AddressData.csv'
taxIn = 'filepath/1091106_700_宜蘭五結.csv'
taxOut =  'filepath/Preprocessing_TaxData.csv'
finalOut = 'filepath/Algorithm_FinalOutput.csv'

def Find_Process_Count(): #計算CPU核心數
    count = multiprocessing.cpu_count()
    inputs = []
    for i in range(count):
        inputs.append(i)
    return inputs

def CreateTemporFile(filepath,encoding): #寫入新檔案
        open(filepath, 'w',encoding=encoding)
        
def ExtractingFloor(address): #從地址萃取樓層資訊
        if ( '十一樓' in address[11]):
                address[12] =11
        elif ( '十二樓' in address[11]):
                address[12] =12
        elif ( '十三樓' in address[11]):
                address[12] =13
        elif ( '十四樓' in address[11]):
                address[12] =14
        elif ( '十五樓' in address[11]):
                address[12] =15
        elif ( '十六樓' in address[11]):
                address[12] =16
        elif ( '一樓' in address[11]):
                address[12] =1
        elif ( '二樓' in address[11]):
                address[12] =2
        elif ( '三樓' in address[11]):
                address[12] =3
        elif ( '四樓' in address[11]):
                address[12] =4
        elif ( '五樓' in address[11]):
                address[12] =5
        elif ( '六樓' in address[11]):
                address[12] =6
        elif ( '七樓' in address[11]):
                address[12] =7
        elif ( '八樓' in address[11]):
                address[12] =8
        elif ( '九樓' in address[11]):
                address[12] =9
        elif ( '十樓' in address[11]):
                address[12] =10

def ShowMatchingResult(filepath,encoding): #顯示匹配比例
        with open(filepath, 'r', encoding=encoding) as csvRFile:
                csvReader = csv.reader(csvRFile)
                listReport = list(csvReader)
        csvRFile.close()
        total_record = 0
        first_record = 0
        first2_record = 0
        first21_record=0
        first3_record = 0
        second_record = 0
        third_record = 0
        fourth_record=0
        for row in listReport:
                total_record = total_record+1
                if ('1st' in row[18]):
                        first_record = first_record+1
                        if('2nd' in row[18]):
                                first2_record = first2_record+1
                                if(row[17]==row[16]):
                                        first21_record = first21_record+1
                        elif('3nd' in row[18]):
                                first3_record = first3_record+1
                elif('2nd' in row[18]):
                        second_record = second_record+1
                elif('3rd' in row[18]):
                        third_record = third_record+1
                else:
                        fourth_record = fourth_record+1
        print('\n---------------Result---------------')
        print(('1st Layer Get %d Records (%.2f)') %(int(first_record), float(first_record/total_record)*100))
        print((' |---Only 1st Layer Get %d Records (%.2f)') %(int(first_record-first2_record-first3_record), float((first_record-first2_record-first3_record)/first_record)*100))
        print((' |---1st, 2nd Layer Get %d Records (%.2f)') %(int(first2_record), float(first2_record/first_record)*100))
        print(('     |---Address, Tax Data MaxFloor %d Records Get Matching (%.2f)') %(int(first21_record), float(first21_record/first2_record)*100))
        print(('     |---Address, Tax Data MaxFloor %d Records Not Matching (%.2f)') %(int(first2_record-first21_record), float((first2_record-first21_record)/first2_record)*100))
        print((' |---1st, 3rd Layer Get %d Records (%.2f)') %(int(first3_record), float(first3_record/total_record)*100))
        print(('2nd Layer Get %d Records (%.2f)') %(int(second_record), float(second_record/total_record)*100))
        print(('3rd Layer Get %d Records (%.2f)') %(int(third_record), float(third_record/total_record)*100))
        print(('Total %d Records Get Matching (%.2f)') %(int(total_record-fourth_record), float((total_record-fourth_record)/total_record)*100))
        print('------------------------------------\n')


def TaxPreprocessing(i): #稅籍資料清洗
        with open(taxIn, 'r', encoding='big5') as csvRFile:
                csvReader = csv.reader(csvRFile)
                listReport = list(csvReader)
        csvRFile.close()
        Listrow = []
        for row in listReport:
                Listrow.append([row[0], row[4], row[5],row[12],row[10]])
        with open(taxOut, 'a', newline='',encoding='big5') as csvOFile:
                csvWriter = csv.writer(csvOFile)
                for row in range(i,len(Listrow),len(Find_Process_Count())):
                        if Listrow[row][0] =='':
                                break
                        csvWriter.writerow(Listrow[row])
        csvOFile.close()
        
def AddressPreprocessing(i): #地址資料清洗
        with open(addressIn, 'r', encoding='utf8') as csvRFile:
                csvReader = csv.reader(csvRFile)
                listReport = list(csvReader)
        csvRFile.close()
        Listrow = []
        for row in listReport:
                Listrow.append(row+[row[0]+row[1]+row[2]+row[4]+row[6]+row[7]+row[8]]+['']+[''])
        with open(addressOut, 'a', newline='',encoding='big5',errors='backslashreplace') as csvOFile:
                csvWriter = csv.writer(csvOFile)
                for row in range(i,len(Listrow),len(Find_Process_Count())):
                        if Listrow[row][0] =='':
                                break
                        ExtractingFloor(Listrow[row]) #擷取含有「樓」之資訊
                        for row3 in Listrow:
                                if((Listrow[row][9]==row3[9])&(Listrow[row][10] == row3[10])&(Listrow[row][11]!=row3[11])&(Listrow[row][12]!='NULL')):
                                        ExtractingFloor(row3)
                                        if((Listrow[row][12]=='')&(row3[4]!='')):  #檢查沒有「樓」資訊之一樓樓層
                                                Listrow[row][12]=1
                                        if(Listrow[row][13]==''):
                                                Listrow[row][13]=Listrow[row][12]
                                        if((row3[12]!='')&(row3[12]!='NULL')): #給予單點之MaxFloor資訊
                                                if (int(row3[12])>=int(Listrow[row][13])):
                                                        Listrow[row][13]=row3[12]
                        if((Listrow[row][12]!='')&(Listrow[row][13]=='')):
                                Listrow[row][13]=Listrow[row][12]
                        csvWriter.writerow([Listrow[row][0], Listrow[row][1], Listrow[row][2], Listrow[row][3], Listrow[row][4], Listrow[row][5], Listrow[row][6], Listrow[row][7],Listrow[row][8], Listrow[row][9], Listrow[row][10], Listrow[row][11], Listrow[row][12],Listrow[row][13]])
        csvOFile.close()

def  Algorithm(i): #總演算法
        with open(taxOut, 'r', encoding='big5',errors='backslashreplace') as csvRFile:
                csvReader = csv.reader(csvRFile)
                taxList = list(csvReader)
        csvRFile.close()
        with open(addressOut, 'r', encoding='big5',errors='backslashreplace') as csvRFile2:
                csvReader2 = csv.reader(csvRFile2)
                addressList = list(csvReader2)
        csvRFile2.close()
        with open(finalOut, 'a', newline='',encoding='big5',errors='backslashreplace') as csvOFile:
                csvWriter = csv.writer(csvOFile)
                for row in range(i,len(addressList),len(Find_Process_Count())):
                        if addressList[row][0] =='':
                                break
                        resultString=''
                        distance=99999 #賦予一個極大的距離數值避免每次都進入第三層演算
                        for row2 in taxList:                                        
                                if((addressList[row][12]!='')&(resultString=='')): #第一層：直接擷取樓層資訊
                                        resultString = '1st layer matching'
                                        pass
                                if (((row2[0] in addressList[row][11])or(addressList[row][11] in row2[0]))): #第二層：Full Address Matching
                                        if(addressList[row][12]==''):
                                                addressList[row][12]=1
                                        address = row2[0]
                                        tmx = row2[1]
                                        tmy = row2[2]
                                        maxfloor = row2[3]
                                        distance = (((float(addressList[row][9])-float(row2[1]))**2+(float(addressList[row][10])-float(row2[2]))**2)**0.5)
                                        if ('1st' in resultString):
                                                resultString = '1st,2nd layer matching'
                                        else:
                                                resultString = '2nd layer matching'
                                        break
                                if(((((float(addressList[row][9])-float(row2[1]))**2+(float(addressList[row][10])-float(row2[2]))**2)**0.5)<=10)&((((float(addressList[row][9])-float(row2[1]))**2+(float(addressList[row][10])-float(row2[2]))**2)**0.5)<distance)): #第三層：Nearest Distance Searching
                                        if(addressList[row][12]==''):
                                                addressList[row][12]=1
                                        address = row2[0]
                                        tmx = row2[1]
                                        tmy = row2[2]
                                        maxfloor = row2[3]
                                        distance = (((float(addressList[row][9])-float(row2[1]))**2+(float(addressList[row][10])-float(row2[2]))**2)**0.5)
                                        if ('1st' in resultString):
                                                resultString = '1st,3rd layer matching'
                                        else:
                                                resultString = '3rd layer matching'
                        if((addressList[row][12]!='')):
                                if('1st' in resultString):
                                        if(('2nd' in resultString)or('3rd' in resultString)):
                                                csvWriter.writerow([addressList[row][0], addressList[row][1], addressList[row][2], addressList[row][3], addressList[row][4], addressList[row][5], addressList[row][6], addressList[row][7], addressList[row][8], addressList[row][9], addressList[row][10], addressList[row][11], addressList[row][12], address,tmx,tmy,maxfloor,addressList[row][13],resultString,distance])
                                        else:
                                                csvWriter.writerow([addressList[row][0], addressList[row][1], addressList[row][2], addressList[row][3], addressList[row][4], addressList[row][5], addressList[row][6], addressList[row][7], addressList[row][8], addressList[row][9], addressList[row][10], addressList[row][11], addressList[row][12], '','','','',addressList[row][13],resultString,''])
                                else:
                                        csvWriter.writerow([addressList[row][0], addressList[row][1], addressList[row][2], addressList[row][3], addressList[row][4], addressList[row][5], addressList[row][6], addressList[row][7], addressList[row][8], addressList[row][9], addressList[row][10], addressList[row][11], addressList[row][12], address, tmx, tmy, maxfloor,maxfloor,resultString,distance])
                        if(addressList[row][12]==''): #第四層：Not Matching = 1
                                addressList[row][12]=1
                                csvWriter.writerow([addressList[row][0], addressList[row][1], addressList[row][2], addressList[row][3], addressList[row][4], addressList[row][5], addressList[row][6], addressList[row][7], addressList[row][8], addressList[row][9], addressList[row][10], addressList[row][11], addressList[row][12], '','','','',addressList[row][12],'Not matching',''])
        csvOFile.close()
                                
if __name__ == "__main__": #主程式 main()
    starttime = time.time()
    print('Reading Raw Data . . . ')
    inputs = Find_Process_Count()
    pool = Pool()
    CreateTemporFile(addressOut, 'big5')
    pool.map(AddressPreprocessing, inputs)
    print('Preprocessing of Address Data Finish !')
    CreateTemporFile(taxOut, 'big5')
    pool.map(TaxPreprocessing, inputs)
    print('Preprocessing of Tax Data Finish !')
    CreateTemporFile(finalOut, 'big5')
    csv.writer(open(finalOut,'a',newline='',encoding='big5')).writerow(['County', 'Town', 'Village', 'Zone', 'Road', 'Area', 'Lane', 'Long', 'Num', 'Tmx(Address)', 'Tmy(Address)', 'Address', 'Stoery', 'Address(Tax)', 'Tmx(Tax)', 'Tmy(Tax)', 'MaxFloor(Tax)', 'TotalMaxFloor','Result','Matching Distance'])
    pool.map(Algorithm, inputs)
    ShowMatchingResult(finalOut,'big5')
    endtime = time.time()
    print('Algorithm Finish !')
    print('Total Time: %.3f s' %(endtime-starttime))