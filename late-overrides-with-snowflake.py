import os
from os.path import join, dirname
from dotenv import load_dotenv
import snowflake.connector
import csv
import datetime


dotenv_path = join(dirname(__file__),'.env')
load_dotenv(dotenv_path)
 # Get the credentials from .env
SF_ACCOUNT    = os.getenv('SF_ACCOUNT')
SF_USER       = os.getenv('SF_USER')
SF_WAREHOUSE  = os.getenv('SF_WAREHOUSE')
SF_DATABASE   = os.getenv('SF_DATABASE')
SF_SCHEMA     = os.getenv('SF_SCHEMA')
SF_PASSWORD   = os.getenv('SF_PASSWORD')
SF_ROLE       = os.getenv('SF_ROLE')


connection = snowflake.connector.connect(
authenticator='externalbrowser',
user = SF_USER,
role = SF_ROLE, 
account  = SF_ACCOUNT,
warehouse = SF_WAREHOUSE,
database = SF_DATABASE,
password = SF_PASSWORD,
schema   = SF_SCHEMA)


SQL_GUILD_AS_A_PAYOR_CONTROL_SPECIFICATIONS = connection.execute_string(
    """
    SELECT * 
    FROM GUILD.TA_ORCHESTRATOR_PUBLIC.GUILD_AS_A_PAYOR_CONTROL_SPECIFICATIONS 
    """
    )

SQL_STUDENT_TERM_LINE_ITEMS_UUID = connection.execute_string(
    """
    SELECT DISTINCT 
        CONCAT(TERM_CODE,'_', GUILD_UUID) AS KEY, 
        MAX(LIS.SET_ON)
    FROM TA_ORCHESTRATOR_PUBLIC.STUDENT_TERM_LINE_ITEMS STLI
    JOIN GUILD.TA_ORCHESTRATOR_PUBLIC.LINE_ITEM_STATES LIS ON LIS.STUDENT_TERM_LINE_ITEM_ID = STLI.ID
    JOIN GUILD.TA_ORCHESTRATOR_PUBLIC.PAYMENT_DECISIONS PD on PD.ID = STLI.CURRENT_PAYMENT_DECISION_ID
    WHERE LIS.NAME = 'Committed'
    AND STLI.CURRENT_STATE_NAME = 'Committed'
    GROUP BY KEY
    """
    )

SQL_STUDENT_TERM_LINE_ITEMS_STUDENT_ID = connection.execute_string(
    """
    SELECT DISTINCT 
        CONCAT(TERM_CODE,'_', PARTNER_STUDENT_ID) AS KEY, 
        MAX(LIS.SET_ON)
    FROM TA_ORCHESTRATOR_PUBLIC.STUDENT_TERM_LINE_ITEMS STLI
    JOIN GUILD.TA_ORCHESTRATOR_PUBLIC.LINE_ITEM_STATES LIS ON LIS.STUDENT_TERM_LINE_ITEM_ID = STLI.ID
    JOIN GUILD.TA_ORCHESTRATOR_PUBLIC.PAYMENT_DECISIONS PD on PD.ID = STLI.CURRENT_PAYMENT_DECISION_ID
    WHERE LIS.NAME = 'Committed'
    AND STLI.CURRENT_STATE_NAME = 'Committed'
    GROUP BY KEY
    """
    )


SQL_TUITION_ELIGIBILITY_OVERRIDES = connection.execute_string(
    """
    SELECT 
        CONCAT(O.TERM_CODE,'_', O.STUDENT_EXTERNAL_ID) as key,
        O.UPDATED_AT, 
        U.LAST_NAME as Override_Logged_By,
        AP.NAME as AP_NAME,
        O.REASON, 
        O.TUITION_ELIGIBLE, 
        O.STUDENT_EXTERNAL_ID, 
        O.TERM_CODE, 
        O.COMMENT, 
        CONCAT('https://ta-admin.guildeducation.com/member-payments?searchTerm=', O.STUDENT_EXTERNAL_ID, '&page=0&attribute=studentId') AS "MP SEARCH URL"
    FROM GUILD.TA_ORCHESTRATOR_PUBLIC.TUITION_ELIGIBILITY_OVERRIDES O
    JOIN GUILD.USER_PROFILE_SERVICE_PUBLIC.USERS U ON U.ID = O.ACTOR_ID
    JOIN GUILD.ACADEMIC_SERVICE_V2_PUBLIC.ACADEMIC_PARTNER AP ON AP.ID = O.ACADEMIC_PARTNER_ID
    ORDER BY O.CREATED_AT DESC
    """
    )


gapFlags = [['TERM_CODE_GUILD_UUID_', 'CREATED_AT', 'APPROVED_AMOUNT_CENTS', 'DESCRIPTION']]
for x in SQL_GUILD_AS_A_PAYOR_CONTROL_SPECIFICATIONS:
   for row in x:
      flag = []
      uniqueKey = row[3]+'_'+row[0]
      flag.extend([uniqueKey, row[4], row[13], row[2]])
      gapFlags.append(flag) 

mlbSTLIs_UUID = [['KEY', 'MAX(UPDATED_AT)']]
for x in SQL_STUDENT_TERM_LINE_ITEMS_UUID:
   for row in x:
      stli = []
      stli.extend([row[0], row[1]])
      mlbSTLIs_UUID.append(stli)

mlbSTLIs_StudentID = [['KEY', 'MAX(UPDATED_AT)']]
for x in SQL_STUDENT_TERM_LINE_ITEMS_STUDENT_ID:
   for row in x:
      stli = []
      stli.extend([row[0], row[1]])
      mlbSTLIs_StudentID.append(stli)
 
tuitionOverrides = [['KEY', 'UPDATED_AT', 'OVERRIDE_LOGGED_BY', 'AP_NAME', 'REASON', 'TUITION_ELIGIBLE', 'STUDENT_EXTERNAL_ID', 'TERM_CODE', 'COMMENT', 'MP SEARCH URL' ]]
for x in SQL_TUITION_ELIGIBILITY_OVERRIDES:
    for row in x:
        override = []
        override.extend([row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]])  # turns row into a list
        tuitionOverrides.append(override)


def createListfromCSV(csvFileName):
    """
    Takes a csv file as an argument and returns a list.
    File name argument is formatted as a string with .csv. Example: 'overrides.csv'
    """ 
    file=open(csvFileName)
    new_list = list(csv.reader(file))
    return new_list


# def combineLists(list1, list2):
#     """
#     Combines two lists where the values are dates. This function
#     maintains the value where the date is the most recent for a given key 
#     when there are duplicate keys across the two dictionaries. 
#     """ 
#     newList = list1 | list2
#     for k in newList:
#         if k in list1 and k in list2:
#             if list1[k] > list2[k]:
#                 newList[k] = list1[k]
#             else:
#                 newList[k] = list2[k]
#     return newList



def excludePermissables(permissables,overrides):
    """
    Takes one list (permissables) and one table (overrides) as parameters and 
    returns a new version of the overrides table that excludes items on the list. 
    Overrides contain the key in the first column that match the items in the 
    permissible list.
    """
    result = []
    for n in overrides:
        if [n[0]] not in permissables:
            result.append(n)
    return result


def lateOverrideCheck(overrides, lines):
    """
    Compares date of logged overrides/GAP flags to the date of the committed line item for the
    given term_studentID key. If the override date is after the line item date,
    the override is added to the late override list. This function
    returns a list of the late override list.
    """ 
    lateOverrideslist = []

    for ovrd in overrides:
        for line in lines:
            if ovrd[0] == line[0]:
                if ovrd[1] > line[1]:
                    lateOverrideslist.append(ovrd)
    return lateOverrideslist


def writeToCSV(list,filename):
    """
    Takes a list and a CSV file name as parameters and writes the contents of the 
    list to the csv file. File name is a string in quotes ''.
    """
    file = open(filename,'w',newline='')
    wrapper = csv.writer(file)
    for i in list:
        wrapper.writerow(i)
    file.close()

permissables = createListfromCSV('Permissables.csv')
ta1List = createListfromCSV('TA1.csv')  #Ideally pull this TA1.0 and IM data from snowflake as well? .......................................................................................................................................................
imList = createListfromCSV('invoicemanagement_lines.csv') #Ideally pull this TA1.0 and IM data from snowflake as well? ....................................................................................................................................



# gapResult = lateOverrideCheck(gapFlags,mlbSTLIs_UUID)  # Need to change this to also review TA 1.0 and IM line items. make sure not to exclude permissables for GAP.........................................................................................
# gapHeader = gapFlags[0]
# gapResult.insert(0,gapHeader)
# writeToCSV(gapResult,'_gapResults.csv')
# print("Count of late GAP flags is: ", len(gapResult)-1)

overridesMinusPermissables = excludePermissables(permissables,tuitionOverrides)
overridesResult1 = lateOverrideCheck(overridesMinusPermissables, mlbSTLIs_StudentID)
overrideHeader = tuitionOverrides[0]
overridesResult1.insert(0,overrideHeader)
writeToCSV(overridesResult1,'_overridesResults.csv')
print("Count of late overrides is: ", len(overridesResult1)-1)
##need to incorporate 1.0, Invoice Management, and Permissibles in the late override

# x=0
# while x < 10:
#     print(ta1List[x])
#     x+=1