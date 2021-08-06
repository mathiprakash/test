# -----------------------------------------------------------------------------
#
# This file is the copyrighted property of Tableau Software and is protected
# by registered patents and other applicable U.S. and international laws and
# regulations.
#
# You may adapt this file and modify it to fit into your context and use it
# as a template to start your own projects.
#
# -----------------------------------------------------------------------------
#import shutil
#from pathlib import Path

from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    TableName, \
    HyperException


    # Make a copy of the superstore denormalized sample Hyper file
path_to_database = "C:\Mathi\PythonCode\superstore_sample.hyper"
print ("Source Path of the hyper file : ", path_to_database)

def fntablerowformat(ColumnList, RowArray):
    DisplayColumn = "X"
    for column in ColumnList:
        DisplayColumn = DisplayColumn + ", " + str(column.name).replace('"', '')

    # Print Header followed by actual rows
    print(DisplayColumn.replace("X, ", ""))
    print("--------------------------------------------------------------")
    for Row in RowArray:
        print(Row)

def fnqueryrowformat(RowArray):
    for Row in RowArray:
        print(Row)

with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:

    # Connect to existing Hyper file
    with Connection(endpoint=hyper.endpoint,
                    database=path_to_database) as connection:
     
        print("--------------------------------------------------------------")

        # Print all rows from a sample table
        table_name = TableName("public", 'Products')


        strSchemaName = "medecon"
        strCreateSchema = "CREATE SCHEMA IF NOT EXISTS " + strSchemaName
        strCreateTable_Claim = "CREATE TABLE medecon.claim (ClaimID  char(5), ClaimLine integer, DOS Date,  AmountPaid integer, ASSUMED PRIMARY KEY(ClaimID, ClaimLine) );"
        strDropTable = "Drop table medecon.claim"
        strInsertTable_Claim = "Insert into medecon.claim (ClaimID, ClaimLine, DOS, AmountPaid) values ('C3', 2, '2021-05-09', 1000)"
        strDeleteTable_Claim = " Delete from medecon.claim where claimline > 1 and DOS = '2020-10-20' "
        strUpdateTable_Claim = "UPDATE medecon.claim SET DOS = '2019-09-29' WHERE AmountPaid != 0;"

        strCreateTable_DimDate = "CREATE TABLE medecon.dimdate (DateKey Date, Year integer, month integer) "
        strInsertTable_DimDate = "Insert into medecon.dimdate values ('2021-05-09', 2021, 5) "
        #, 
        #connection.execute_command(strInsertTable_DimDate)

        strQuery_0 = "SELECT * FROM medecon.claim limit 3"
        strQuery_1 = "SELECT ClaimID, count(1) as RowCount, sum(AmountPaid) as AmountPaid FROM medecon.claim group by ClaimID order by 3 desc"
        strQuery_2 = "SELECT DOS, DateKey, count(1) as RowCount, sum(AmountPaid) as AmountPaid FROM medecon.claim C left join medecon.dimdate D on C.DOS = D.DateKey group by DOS, DateKey order by 3 desc"

        #print Table data
        SelectResult = connection.execute_list_query("SELECT * FROM medecon.dimdate")
        objtable_name = TableName("medecon", 'claim')
        objColumn_list = connection.catalog.get_table_definition(name=objtable_name)
        fntablerowformat(objColumn_list.columns, SelectResult)

        print("--------------------------------------------------------------")

        #print Query data
        QueryResult = connection.execute_list_query(strQuery_0)
        fnqueryrowformat(QueryResult)

        print("--------------------------------------------------------------")

        # The table names in the "Extract" schema (the default schema).
        schemaname_list = connection.catalog.get_schema_names()  
        print ("Schema Name List : ", schemaname_list)
        tablename_list = connection.catalog.get_table_names('medecon')
        print ("Table Name List : ", tablename_list)

    print("The connection to the Hyper file has been closed.")
print("The Hyper process has been shut down.")
print("--------------------------------------------------------------")


