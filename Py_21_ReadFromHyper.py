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


def run_read_data_from_existing_hyper_file():

     # Make a copy of the superstore denormalized sample Hyper file
    path_to_database = "C:\Mathi\PythonCode\superstore_sample.hyper"
    print ("Source Path of the hyper file : ", path_to_database)


    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:

        # Connect to existing Hyper file
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database) as connection:
            
            # The table names in the "Extract" schema (the default schema).
            schemaname_list = connection.catalog.get_schema_names()  
            print ("Schema Name List : ", schemaname_list)
            
            for schema_name in schemaname_list :
                #print("Schema Name : ", schema_name.name)
               
                tablename_list = connection.catalog.get_table_names(schema=schema_name.name)
                print ("Table Name List : ", tablename_list)

                for table in tablename_list:
                    print(f"Table {table.name} has qualified name: {table}")

                    column_definition = connection.catalog.get_table_definition(name=table)
                    #print("Structure of the Column : ", type(column_definition))                    ## Class

                    #for column in column_definition.columns:
                    #    print(f"{column} -  Column {column.name} has type={column.type} and nullability={column.nullability}")
                    #print(" --------- ")

            print("--------------------------------------------------------------")


            # Print all rows from a sample table
            table_name = TableName("public", 'Products')

            #print(f"These are all rows in the table {table_name}:")
            
            # `execute_list_query` executes a SQL query and returns the result as list of rows of data,
            # each represented by a list of objects.
            rows_in_table = connection.execute_list_query(' SELECT distinct "Category" FROM "public"."Products" ')
            print(rows_in_table)


            RowCount_Table = connection.execute_list_query(query=f"SELECT count(1) FROM {table_name}")[0][0]
            print("Row Count in the table ", {table_name}, ":", RowCount_Table, "", type(RowCount_Table))


        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")


if __name__ == '__main__':
    try:
        run_read_data_from_existing_hyper_file()
    except HyperException as ex:
        print(ex)
        exit(1)
