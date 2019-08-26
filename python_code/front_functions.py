import json


def extract_from_source(front_conn,front_curr, sql_path,debug):
    
    with open(sql_path,'r') as file:
        sql = file.read()

    if debug:
        print('==============> Extract sql')
        print(sql)
        print('===============')

    front_curr.execute(sql)
    records=front_curr.fetchall()
    return records


def load_to_stage(records, curr_tgt, conn_tgt, table,debug):
    curr_tgt.execute('truncate table bi_stage.stg_front_' + table)
    print('processing table '+table)

    if table == 'orgs':
        try:
            data_set = ','.join\
                (curr_tgt.mogrify
                 ('(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                        (
                            row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7],
                            json.dumps(row[8]), row[9], row[10], row[11], json.dumps(row[12])
                        )
                 ) for row in records
                )
            curr_tgt.execute('insert into bi_stage.stg_front_orgs  values ' + data_set)
        except Exception as e:
            if debug:
                print("Error while executing SQL Query for : %s : %s " % (table, e))
            raise RuntimeError("Error while executing SQL Query for  : %s : %s" % (table , e))

    if table == 'users':
        try:
            data_set = ','.join\
                (curr_tgt.mogrify
                 ('(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                        (
                            row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7],
                            row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15],
                            row[16], row[17], row[18], row[19], row[20], row[21], row[22], row[23],
                            row[24], row[25], row[26], row[27], row[28], row[29], row[30], row[31],
                            row[32], row[33], row[34], row[35], row[36], row[37]
                        )
                 ) for row in records
                )
            curr_tgt.execute('insert into bi_stage.stg_front_users  values ' + data_set)
        except Exception as e:
            if debug:
                print("Error while executing SQL Query for  %s : %s" % (table, e))
            raise RuntimeError("Error while executing SQL Query for  : %s : %s" % (table , e))

    if table == 'designs':
        try:
            data_set = ','.join \
                (curr_tgt.mogrify
                 ('(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                    (
                        row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7],
                        row[8], row[9], row[10], row[11], row[12], row[13], row[14],
                        row[15], row[16], row[17], row[18], row[19], row[20], row[21], row[22],
                        json.dumps(row[23]), row[24]
                    )
                 ) for row in records
                )
            curr_tgt.execute('insert into bi_stage.stg_front_designs  values ' + data_set)
        except Exception as e:
            if debug:
                print("Error while executing SQL Query for :  %s : %s" % (table, e))
                raise RuntimeError("Error while executing SQL Query for  : %s : %s" % (table, e))

    if table == 'builds':
            try:
                data_set = ','.join \
                    (curr_tgt.mogrify
                        ('(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                         (
                             row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7],
                             row[8], json.dumps(row[9]), row[10], row[11], row[12], json.dumps(row[13]), row[14],
                             row[15], row[16], json.dumps(row[17]), row[18], row[19], row[20], row[21], row[22],
                             row[23]
                         )
                        ) for row in records
                    )
                curr_tgt.execute('insert into bi_stage.stg_front_builds  values ' + data_set)
            except Exception as e:
                if debug:
                    print("Error while executing SQL Query for : %s : %s" % (table, e))
                    raise RuntimeError("Error while executing SQL Query for : %s : %s" % (table, e))
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
