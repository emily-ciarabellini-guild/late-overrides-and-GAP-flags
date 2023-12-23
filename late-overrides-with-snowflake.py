import os
from os.path import join, dirname
from dotenv import load_dotenv
import snowflake.connector
import csv

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
    SELECT  
        CONCAT(G.TERM_CODE,'_',G.GUILD_UUID) KEY,
        G.CREATED_AT,
        AP.NAME,
        G.APPROVED_AMOUNT*.01,
        G.DESCRIPTION,
        CONCAT('https://ta-admin.guildeducation.com/member-payments/',G.GUILD_UUID)
    FROM GUILD.TA_ORCHESTRATOR_PUBLIC.GUILD_AS_A_PAYOR_CONTROL_SPECIFICATIONS G
    JOIN GUILD.ACADEMIC_SERVICE_V2_PUBLIC.ACADEMIC_PARTNER AP ON AP.ID = G.ACADEMIC_PARTNER_ID
    """
    )

##SQL query retrieves most recent committed line item for a given TermCode_StudentID combination across TA 1.0, Invoice Management, and MLB line items. 
SQL_COMBINED_TA_LINES = connection.execute_string(
    """
    WITH CombinedList AS(
        WITH RankedRows AS (
            SELECT 
                CONCAT(STLI.TERM_CODE, '_', STLI.PARTNER_STUDENT_ID) AS KEY, 
                LIS.SET_ON AS COMMIT_DATE,
                PD.PAYMENT_REASON,
                STLI.CREATED_AT
            FROM TA_ORCHESTRATOR_PUBLIC.STUDENT_TERM_LINE_ITEMS STLI
            JOIN GUILD.TA_ORCHESTRATOR_PUBLIC.LINE_ITEM_STATES LIS ON LIS.STUDENT_TERM_LINE_ITEM_ID = STLI.ID
            JOIN GUILD.TA_ORCHESTRATOR_PUBLIC.PAYMENT_DECISIONS PD ON PD.ID = STLI.CURRENT_PAYMENT_DECISION_ID
            WHERE LIS.NAME = 'Committed'
            AND STLI.CURRENT_STATE_NAME = 'Committed'
        
            UNION ALL
            
            SELECT
                CONCAT(TLI.TERM_CODE,'_', SLI.STUDENT_EXTERNAL_ID) AS KEY, 
                I.UPDATED_AT AS COMMIT_DATE,
                CASE
                    WHEN (TLI.COST_CENTS = TLI.FUNDED_CENTS) THEN 'Full Payment Facilitated'
                    WHEN (TLI.FUNDED_CENTS > 0) THEN 'Hit funding cap'
                    ELSE 'Other Payment Status'
                    END AS PAYMENT_REASON,
                SLI.CREATED_AT    
            FROM TA_ORCHESTRATOR_PUBLIC.TERM_LINE_ITEMS TLI
            JOIN TA_ORCHESTRATOR_PUBLIC.STUDENT_LINE_ITEMS SLI ON SLI.ID = TLI.STUDENT_LINE_ITEM_ID
            JOIN TA_ORCHESTRATOR_PUBLIC.INVOICES I ON I.ID = SLI.INVOICE_ID
            JOIN ACADEMIC_SERVICE_V2_PUBLIC.ACADEMIC_PARTNER AP ON AP.ID = SLI.ACADEMIC_PARTNER_ID
            WHERE I.STATE = 'COMMITTED'
        
            UNION ALL
        
            SELECT 
                CONCAT(TERM_CODE,'_', PARTNER_STUDENT_ID) AS KEY, 
                BENEFIT_LOCKED_ON AS COMMIT_DATE,
                CASE
                    WHEN SFLI.NET_TA_CENTS = SFLI.EMPLOYER_PAYMENT_OBLIGATION THEN 'Full Payment Facilitated'
                    WHEN SFLI.EMPLOYER_PAYMENT_OBLIGATION > 0 THEN 'Hit funding cap'
                    ELSE 'Other Payment Status'
                    END AS PAYMENT_REASON,
                BENEFIT_LOCKED_ON AS CREATED_AT
            FROM TA_ORCHESTRATOR_PUBLIC.LEGACY_SALESFORCE_LINE_ITEMS SFLI
        )
        SELECT
            KEY,
            COMMIT_DATE,
            PAYMENT_REASON,
            CREATED_AT,
            ROW_NUMBER() OVER (PARTITION BY KEY ORDER BY COMMIT_DATE DESC) AS RowNum
                //ROW_NUMBER() function assigns a rank to each row within each distinct combination of TERM_CODE and PARTNER_STUDENT_ID, based on the COMMIT_DATE date in descending order.
                //'partition by' resets the row number for each distinct combination of TERM_CODE and PARTNER_STUDENT_ID.
        FROM RankedRows
    )

    SELECT
        KEY,
        COMMIT_DATE,
        PAYMENT_REASON,
        CREATED_AT
    FROM CombinedList
    WHERE RowNum = 1
    --AND KEY = '202150_873541637'
    """
)


SQL_COMBINED_TA_LINES_UUID = connection.execute_string(
    """
    WITH CombinedList AS(
        WITH RankedRows AS (
            SELECT 
                CONCAT(STLI.TERM_CODE, '_', STLI.GUILD_UUID) AS KEY, 
                LIS.SET_ON AS COMMIT_DATE,
                PD.PAYMENT_REASON,
                STLI.CREATED_AT
            FROM TA_ORCHESTRATOR_PUBLIC.STUDENT_TERM_LINE_ITEMS STLI
            JOIN GUILD.TA_ORCHESTRATOR_PUBLIC.LINE_ITEM_STATES LIS ON LIS.STUDENT_TERM_LINE_ITEM_ID = STLI.ID
            JOIN GUILD.TA_ORCHESTRATOR_PUBLIC.PAYMENT_DECISIONS PD ON PD.ID = STLI.CURRENT_PAYMENT_DECISION_ID
            WHERE LIS.NAME = 'Committed'
            AND STLI.CURRENT_STATE_NAME = 'Committed'
        
            UNION ALL
            
            SELECT
                CONCAT(TLI.TERM_CODE,'_', SLI.GUILD_UUID) AS KEY, 
                I.UPDATED_AT AS COMMIT_DATE,
                CASE
                    WHEN (TLI.COST_CENTS = TLI.FUNDED_CENTS) THEN 'Full Payment Facilitated'
                    WHEN (TLI.FUNDED_CENTS > 0) THEN 'Hit funding cap'
                    ELSE 'Other Payment Status'
                    END AS PAYMENT_REASON,
                SLI.CREATED_AT    
            FROM TA_ORCHESTRATOR_PUBLIC.TERM_LINE_ITEMS TLI
            JOIN TA_ORCHESTRATOR_PUBLIC.STUDENT_LINE_ITEMS SLI ON SLI.ID = TLI.STUDENT_LINE_ITEM_ID
            JOIN TA_ORCHESTRATOR_PUBLIC.INVOICES I ON I.ID = SLI.INVOICE_ID
            JOIN ACADEMIC_SERVICE_V2_PUBLIC.ACADEMIC_PARTNER AP ON AP.ID = SLI.ACADEMIC_PARTNER_ID
            WHERE I.STATE = 'COMMITTED'
        
            UNION ALL
        
            SELECT 
                CONCAT(TERM_CODE,'_', GUILD_UUID) AS KEY, 
                BENEFIT_LOCKED_ON AS COMMIT_DATE,
                CASE
                    WHEN SFLI.NET_TA_CENTS = SFLI.EMPLOYER_PAYMENT_OBLIGATION THEN 'Full Payment Facilitated'
                    WHEN SFLI.EMPLOYER_PAYMENT_OBLIGATION > 0 THEN 'Hit funding cap'
                    ELSE 'Other Payment Status'
                    END AS PAYMENT_REASON,
                BENEFIT_LOCKED_ON AS CREATED_AT
            FROM TA_ORCHESTRATOR_PUBLIC.LEGACY_SALESFORCE_LINE_ITEMS SFLI
        )
        SELECT
            KEY,
            COMMIT_DATE,
            PAYMENT_REASON,
            CREATED_AT,
            ROW_NUMBER() OVER (PARTITION BY KEY ORDER BY COMMIT_DATE DESC) AS RowNum
                //ROW_NUMBER() function assigns a rank to each row within each distinct combination of TERM_CODE and PARTNER_STUDENT_ID, based on the COMMIT_DATE date in descending order.
                //'partition by' resets the row number for each distinct combination of TERM_CODE and PARTNER_STUDENT_ID.
        FROM RankedRows
    )

    SELECT
        KEY,
        COMMIT_DATE,
        PAYMENT_REASON,
        CREATED_AT
    FROM CombinedList
    WHERE RowNum = 1

    """

)


SQL_TUITION_ELIGIBILITY_OVERRIDES = connection.execute_string(
    """
    WITH COMMITTED_STLIS AS (
                SELECT DISTINCT
                    STLI.GUILD_UUID, 
                    STLI.PARTNER_STUDENT_ID as Student_ID, 
                    AP.NAME as AP_NAME,
                    AP.ID as AP_UUID,
                    REPLACE(PD.SUPPORTING_INFORMATION:user:employeeId,'"','') AS EP_ID,                          
                    REPLACE(PD.SUPPORTING_INFORMATION:user:employerId,'"','') AS EMPLOYER_UUID,
                    EP.NAME AS EP_NAME
                FROM TA_ORCHESTRATOR_PUBLIC.STUDENT_TERM_LINE_ITEMS  STLI
                JOIN TA_ORCHESTRATOR_PUBLIC.PAYMENT_DECISIONS PD ON PD.ID = STLI.CURRENT_PAYMENT_DECISION_ID
                JOIN ACADEMIC_SERVICE_V2_PUBLIC.ACADEMIC_PARTNER AP ON AP.ID = STLI.ACADEMIC_PARTNER_ID
                LEFT JOIN TA_ORCHESTRATOR_PUBLIC.INVOICING_CYCLES IC ON IC.ID = STLI.INVOICING_CYCLE_ID
                JOIN GUILD.CATALOG_SERVICE_PUBLIC.EMPLOYERS EP ON EP.UUID = EMPLOYER_UUID
                WHERE STLI.CURRENT_STATE_NAME = 'Committed'
                )
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
        CONCAT('https://ta-admin.guildeducation.com/member-payments?searchTerm=', O.STUDENT_EXTERNAL_ID, '&page=0&attribute=studentId') AS "MP SEARCH URL",
        EP_NAME
    FROM GUILD.TA_ORCHESTRATOR_PUBLIC.TUITION_ELIGIBILITY_OVERRIDES O
    JOIN GUILD.USER_PROFILE_SERVICE_PUBLIC.USERS U ON U.ID = O.ACTOR_ID
    JOIN GUILD.ACADEMIC_SERVICE_V2_PUBLIC.ACADEMIC_PARTNER AP ON AP.ID = O.ACADEMIC_PARTNER_ID
    LEFT JOIN COMMITTED_STLIS CS ON CS.STUDENT_ID =  O.STUDENT_EXTERNAL_ID
    ORDER BY O.CREATED_AT DESC
    """
    )



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


def lateOverrideCheckLists(overrides, lines):
    """
    Compares date of logged overrides to the date of the committed line item for the
    given term_studentID key. If the override is created after the line was created AND the 
    override reason is MP3, then the override is added to the late override list
    Or, if the override was created after the line was committed, then the override is
    added to the late override list.
    This function returns 2 lists: late overrides and overrides where line items 
    were not found.
    overrides is a list with termcode_studentID in index 0, date at index 1, 
    and override reason at index 4. lines is a nested list containing lists 
    with termcode_studentID at index 0, commit date at index 1, and create date 
    at index 3.
    """ 
    lateOverrideslist = []
    overridesWithNoCommittedSTLIs = []

    for ovrd in overrides:
        for line in lines:
            if ovrd[0] == line[0]:
                if ((ovrd[1] > line[3]) and (ovrd[4] == 'mp3override')) or (ovrd[1] > line[1]):  ##If override created after line created AND override reason is MP3 -OR- override created after line was committed, then the override is late override
                    if(ovrd[5]==True) and (line[2]=='Full Payment Facilitated' or line[2]=='Hit Annual TA Cap'): ##committed line was already eligible and override was eligible
                        continue
                    if(ovrd[5]==False) and (line[2]=='Ineligible' or line[2]=='Did Not Meet Corporate Requirement(s)'): ##committed line was already ineligible and override is ineligible
                        continue
                    else:
                        lateOverrideslist.append(ovrd)
        else:
            overridesWithNoCommittedSTLIs.append(ovrd)
    return [lateOverrideslist,overridesWithNoCommittedSTLIs]


def writeToCSV(list,filename):
    """
    Takes a list and a CSV file name as parameters and writes the contents of the 
    list to the csv file. File name is a string in quotes ''.
    """
    file = open(filename,'w',newline='',encoding='utf-8')
    wrapper = csv.writer(file)
    for i in list:
        wrapper.writerow(i)
    file.close()


gapFlags = [['TERM-CODE_GUILD-UUID', 'CREATED_AT', 'AP_NAME', 'APPROVED_AMOUNT', 'DESCRIPTION', 'LINK']]
for x in SQL_GUILD_AS_A_PAYOR_CONTROL_SPECIFICATIONS:
   for row in x:
      flag = []
      flag.extend([row[0],row[1],row[2],row[3],row[4],row[5]])
      gapFlags.append(flag) 


tuitionOverrides = [['KEY', 'UPDATED_AT', 'OVERRIDE_LOGGED_BY', 'AP_NAME', 'REASON', 'TUITION_ELIGIBLE', 'STUDENT_EXTERNAL_ID', 'TERM_CODE', 'COMMENT', 'MP SEARCH URL','EP_NAME' ]]
for x in SQL_TUITION_ELIGIBILITY_OVERRIDES:
    for row in x:
        override = []
        override.extend([row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10]])  # turns row into a list
        tuitionOverrides.append(override)


taLineItems = [['KEY', 'COMMIT_DATE', 'PAYMENT_REASON', 'CREATED_AT']]
for x in SQL_COMBINED_TA_LINES:
    for row in x:
        lineItem = []
        lineItem.extend([row[0],row[1],row[2],row[3]])
        taLineItems.append(lineItem)


# # create & combine dictionaries from line item data with UUIDs for GAP Flags comparison
allLinesDict_UUID = createDictfromCursor(SQL_COMBINED_TA_LINES_UUID)
gapResult = lateOverrideCheckWdict(gapFlags,allLinesDict_UUID)
gapHeader = gapFlags[0]
lateGAP = gapResult[0]
GAPwithoutSTLIs = gapResult[1]
lateGAP.insert(0,gapHeader)
writeToCSV(lateGAP,'_lateGAPflags.csv')
writeToCSV(GAPwithoutSTLIs,'_gapFlagsWithNoSTLIs.csv')
print("Count of late GAP flags is: ", len(lateGAP)-1)


permissables = createListfromCSV('Permissables.csv')
overridesMinusPermissables = excludePermissables(permissables,tuitionOverrides)
overridesResult = lateOverrideCheckLists(overridesMinusPermissables, taLineItems) 
writeToCSV(overridesResult[0],'_lateOverridesResults.csv')
print("Count of late overrides is: ", len(lateOverrides)-1)

