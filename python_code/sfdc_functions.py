
def extract_from_source(sfdc_conn, sql_path,debug):
    
    with open(sql_path,'r') as file:
        sql = file.read()

    if debug:
        print('==============> Extract sql')
        print(sql)
        print('===============')
    query = sfdc_conn.query(sql)
    records = query['records']
    total_records = query['size']
    query_locator = query['queryLocator']

    while query['done'] is False and len(records) < total_records:
        query = sfdc_conn.queryMore(query_locator)
        query_locator = query['queryLocator']   # get updated query locator
        records = records + query['records']    # append the next set of data to the records dictionary
        
    return records


def load_to_stage(records, curr_tgt, conn_tgt, table,debug):
    curr_tgt.execute('truncate table bi_stage.stg_sfdc_' + table)

    if table == 'user':
        try:
            data_set = ','.join\
                (curr_tgt.mogrify
                 ('(%s, %s, %s , %s, %s)',
                        (
                            row['Id'], row['FirstName'], row['LastName'], row['CreatedDate'], row['LastModifiedDate']
                        )
                 ) for row in records
                )
            curr_tgt.execute('insert into bi_stage.stg_sfdc_user  values ' + data_set)
        except Exception as e:
            if debug:
                print("Error while executing SQL Query for : %s : %s " % (table, e))
            raise RuntimeError("Error while executing SQL Query for  : %s : %s" % (table , e))

    if table == 'account':
        try:
            data_set = ','.join\
                (curr_tgt.mogrify
                 ('(%s, %s, %s , %s, %s, %s, %s, %s , %s, %s, %s, %s, %s , %s, %s, %s, %s, %s , %s , %s)',
                        (
                            row['Id'], row['Name'], row['BillingCity'], row['BillingState'], row['BillingCountry'],
                            row['OwnerId'], row['CreatedDate'], row['LastModifiedDate'], row['Size__c'],
                            row['Account_Manager__c'], row['Brand_Account__c'], row['Account_Balance__c'],
                            row['Account_Overdue_Balance__c'], row['Days_Overdue__c'], row['NetSuite_Id__c'],
                            row['Unbilled_Orders__c'], row['AdTech_Partner__c'], row['Region__c'], row['AdTech_SPM__c'],
                            row['Industry']
                        )
                 ) for row in records
                )
            curr_tgt.execute('insert into bi_stage.stg_sfdc_account  values ' + data_set)
        except Exception as e:
            if debug:
                print("Error while executing SQL Query for  %s : %s" % (table, e))
            raise RuntimeError("Error while executing SQL Query for  : %s : %s" % (table , e))

    if table == 'opportunity':
        try:
            data_set = ','.join \
                (curr_tgt.mogrify
                 ('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                    (
                        row['Id'], row['AccountId'], row['RecordTypeId'], row['Name'], row['StageName'],
                        row['Amount'], row['Probability'], row['CloseDate'], row['LeadSource'], row['CurrencyIsoCode'],
                        row['CampaignId'], row['OwnerId'], row['Lost_Reason__c'], row['Net_Total_Value__c'],
                        row['Monthly_Recurring_Revenue__c'], row['Annual_Contract_Value__c'],
                        row['Contract_End_Date__c'], row['Contract_Start_Date__c'], row['NetSuite_ContractId__c'],
                        row['NetSuite_Id__c'], row['Autorenew_Days__c'], row['Autorenew_Date__c'], row['Order_Type__c'],
                        row['Contract_Term__c'], row['AdTech_Partners__c'], row['Campaign_Start_Date__c'],
                        row['Campaign_End_Date__c'], row['Lost_Reason_picklist__c'], row['Direct_to_Brand__c'],
                        row['Opportunity_Revenue_Type__c'], row['Region__c'], row['Opportunity_Won_By__c'],
                        row['Upsell_Cross_Device__c'], row['CreatedDate'], row['LastModifiedDate']
                    )
                 ) for row in records
                )
            curr_tgt.execute('insert into bi_stage.stg_sfdc_opportunity  values ' + data_set)
        except Exception as e:
            if debug:
                print("Error while executing SQL Query for :  %s : %s" % (table, e))
                raise RuntimeError("Error while executing SQL Query for  : %s : %s" % (table, e))

    if table == 'opportunity_line_item':
            try:
                data_set = ','.join \
                    (curr_tgt.mogrify
                        ('(%s, %s, %s , %s, %s, %s, %s, %s , %s, %s, %s)',
                         (
                            row['Id'], row['OpportunityId'], row['ProductCode'], row['CurrencyIsoCode'],
                            row['NetSuite_ItemId__c'], row['Use_Case__c'], row['Revenue_Type__c'], row['End_Date__c'],
                            row['Start_Date__c'], row['CreatedDate'], row['LastModifiedDate']
                         )
                        ) for row in records
                    )
                curr_tgt.execute('insert into bi_stage.stg_sfdc_opportunity_line_item  values ' + data_set)
            except Exception as e:
                if debug:
                    print("Error while executing SQL Query for : %s : %s" % (table, e))
                    raise RuntimeError("Error while executing SQL Query for : %s : %s" % (table, e))
    if table == 'opportunity_line_item_schedule':
        try:
            data_set = ','.join \
                (curr_tgt.mogrify
                    ('(%s, %s, %s , %s, %s, %s, %s)',
                     (
                        row['Id'], row['OpportunityLineItemId'], row['Revenue'], row['ScheduleDate'],
                        row['CurrencyIsoCode'], row['CreatedDate'], row['LastModifiedDate']
                     )
                    ) for row in records
                )
            curr_tgt.execute('insert into bi_stage.stg_sfdc_opportunity_line_item_schedule  values ' + data_set)

        except Exception as e:
            if debug:
                print("Error while executing SQL Query for : %s : %s " % (table, e))
                raise RuntimeError("Error while executing SQL Query for : %s : %s " % (table, e))
    conn_tgt.commit()

def load_to_target(file, curr_tgt, conn_tgt,debug):

    with open(file, "r") as fp:
        sql = fp.read()

    if debug:
        print('==============> Target sql')
        print(sql)
        print('===============')
    curr_tgt.execute(sql)
    conn_tgt.commit()
