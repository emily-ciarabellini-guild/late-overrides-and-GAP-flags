import os
from os.path import join, dirname
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector import DictCursor
import csv
import datetime
import pandas as pd


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

SQL_INVOICE_MGMT = connection.execute_string(
    """
    SELECT DISTINCT CONCAT(TLI.TERM_CODE,'_', SLI.STUDENT_EXTERNAL_ID) AS KEY, MAX(I.UPDATED_AT)
    FROM TA_ORCHESTRATOR_PUBLIC.TERM_LINE_ITEMS TLI
    JOIN TA_ORCHESTRATOR_PUBLIC.STUDENT_LINE_ITEMS SLI ON SLI.ID = TLI.STUDENT_LINE_ITEM_ID
    JOIN TA_ORCHESTRATOR_PUBLIC.INVOICES I ON I.ID = SLI.INVOICE_ID
    WHERE I.STATE = 'COMMITTED'
    GROUP BY KEY
    """
    )

SQL_TA1 = connection.execute_string(
    """
    SELECT DISTINCT CONCAT(TERM_CODE,'_', PARTNER_STUDENT_ID) AS KEY, MAX(BENEFIT_LOCKED_ON)
    FROM TA_ORCHESTRATOR_PUBLIC.LEGACY_SALESFORCE_LINE_ITEMS SFLI
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

tuitionOverrides = [['KEY', 'UPDATED_AT', 'OVERRIDE_LOGGED_BY', 'AP_NAME', 'REASON', 'TUITION_ELIGIBLE', 'STUDENT_EXTERNAL_ID', 'TERM_CODE', 'COMMENT', 'MP SEARCH URL' ]]
for x in SQL_TUITION_ELIGIBILITY_OVERRIDES:
    for row in x:
        override = []
        override.extend([row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]])  # turns row into a list
        tuitionOverrides.append(override)


def createDictfromCursor(cursor):
    """
    Takes a Snowflake Cursor object as an arguments and returns a dictionary. 
    The Snowflake Cursor object contains a nested list. In the resulting
    dictionary, the first value of a nested list will be the key and the second 
     will be the value.
    ex: {"key": "timestamp"}
    """ 
    dict = {"key": "timestamp"}
    for x in cursor:
        for row in x:
            dict[row[0]] =  row[1]
    return dict


def createListfromCSV(csvFileName):
    """
    Takes a csv file as an argument and returns a list.
    File name argument is formatted as a string with .csv. Example: 'overrides.csv'
    """ 
    file=open(csvFileName)
    new_list = list(csv.reader(file))
    return new_list


def combineDicts(dict1, dict2):
    """
    Combines two dictionaries where the values are dates. This function
    maintains the value where the date is the most recent for a given key 
    when there are duplicate keys across the two dictionaries. 
    """ 
    newDict = dict1 | dict2
    for k in newDict:
        if k in dict1 and k in dict2:
            if dict1[k] > dict2[k]:
                newDict[k] = dict1[k]
            else:
                newDict[k] = dict2[k]
    return newDict


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


def lateOverrideCheckWdict(overrides, linesDict):
    """
    Compares date of logged overrides to the date of the committed line item for the
    given term_studentID key. If the override is logged after the line item was 
    committed, the term_studentID key is added to the late override list. This function
    returns a list of the late override list and the overrides not found.
    Overrides is a list with termcode_studentID in column 1 and a date in 
    column 2. LinesDict is a dictionary with key:value pairs, ex: 
    {"key": "timestamp", "key": "timestamp", "key": "timestamp"}
    """ 
    lateOverrideslist = []
    overridesWithNoLoggedSTLIs = []

    for ovrd in overrides:
        if ovrd[0] in linesDict:
            if ovrd[1] > linesDict[ovrd[0]]:
                lateOverrideslist.append(ovrd)
        else:
            overridesWithNoLoggedSTLIs.append(ovrd)
    return [lateOverrideslist,overridesWithNoLoggedSTLIs]


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

mlbSTLIs_SID_dict = createDictfromCursor(SQL_STUDENT_TERM_LINE_ITEMS_STUDENT_ID)
InvMgmt_dict = createDictfromCursor(SQL_INVOICE_MGMT)
TA1_dict = createDictfromCursor(SQL_TA1)
ta1_imDict = combineDicts(TA1_dict,InvMgmt_dict)
allLinesDict = combineDicts(ta1_imDict,mlbSTLIs_SID_dict)

gapResult = lateOverrideCheck(gapFlags,mlbSTLIs_UUID)  #  GAP flags do not contain UUID, so this check is based on UUID ............... need to see if can pull in TA 1.0 and IM data with UUID...............
gapHeader = gapFlags[0]
gapResult.insert(0,gapHeader)
writeToCSV(gapResult,'_gapResults.csv')
print("Count of late GAP flags is: ", len(gapResult)-1)

permissables = createListfromCSV('Permissables.csv')
overridesMinusPermissables = excludePermissables(permissables,tuitionOverrides)
overridesResult = lateOverrideCheckWdict(overridesMinusPermissables, allLinesDict)
lateOverrides = overridesResult[0]
overrideHeader = tuitionOverrides[0]
lateOverrides.insert(0,overrideHeader)
writeToCSV(lateOverrides,'_lateOverridesResults.csv')
print("Count of late overrides is: ", len(lateOverrides)-1)
