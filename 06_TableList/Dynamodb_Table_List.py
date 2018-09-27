# -*- coding: utf-8 -*-
import sys,os
import subprocess
import json



profile=str(sys.argv[1])
command = 'aws --profile ' + profile + ' dynamodb list-tables'
os_result=subprocess.Popen(command,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT).communicate()[0]
table_list=json.loads(os_result)
for table in table_list["TableNames"]:
    module = table[:3]
    if module in ["CC_","PR_","SC_","GD_","DP_","CH_","AT_","ST_","ET_","SE_","PY_","OM_","MB_","LO_","CM_"]:
        if(module == "AT_"):
            module_name="상품속성"
        elif(module == "CC_"):
            module_name="고객센터"
        elif(module == "CH_"):
            module_name="채널"
        elif(module == "CM_"):
            module_name="FO공통"
        elif(module == "DP_"):
            module_name="전시"            
        elif(module == "ET_"):
            module_name="거래처"
        elif(module == "GD_"):
            module_name="상품"
        elif(module == "MB_"):
            module_name="회원"
        elif(module == "OM_"):
            module_name="주문"
        elif(module == "PR_"):
            module_name="판촉"
        elif(module == "SC_"):
            module_name="검색"
        elif(module == "SE_"):
            module_name="정산"
        elif(module == "ST_"):
            module_name="시스템"
        elif(module == "LO_"):
            module_name="배송"
        elif(module == "PY_"):
            module_name="결제"


        command = 'aws --profile ' + profile + ' dynamodb describe-table --table-name ' + table
        os_result=subprocess.Popen(command,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT).communicate()[0]
        table_describe=json.loads(os_result)
        itemcount=table_describe["Table"]["ItemCount"]
        table_size=table_describe["Table"]["TableSizeBytes"]
        rcu = table_describe["Table"]["ProvisionedThroughput"]["ReadCapacityUnits"]
        wcu = table_describe["Table"]["ProvisionedThroughput"]["WriteCapacityUnits"]
        index_count=0
        if "GlobalSecondaryIndexes" in table_describe["Table"]:
            index_count = len(table_describe["Table"]["GlobalSecondaryIndexes"])
        row_size=0
        if(itemcount != 0):
            row_size = round(table_size/itemcount)
            
        print(table + " "+ module_name + " " + str(row_size) + " " + str(rcu) + " " + str(wcu) + " " +  str(index_count))

