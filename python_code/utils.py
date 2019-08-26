import logging
import json
from os.path import dirname
from datetime import date
import pandas as pd
from io import BytesIO as StringIO
import io


def fetch_json_data(for_key):
    path = dirname(__file__)
    cred_file = "{}/{}".format(path, "credentials.json")
    with open(cred_file) as f:
        data = json.load(f)
    return data[for_key]


def check_dependencies(job_name, postgres_prod_cur, logger,debug):
        sql = "select bi_audit.check_dependencies('"+job_name+"')"
        if debug:
            print(sql)
        postgres_prod_cur.execute(sql)
        row_count = postgres_prod_cur.rowcount
        if row_count > 0:
            dependencies = []
            for row in postgres_prod_cur:
                dependencies.append(row)
            logger.error('Dependencies not completed %s --> %s' % (row_count, dependencies))
            print('Email sent dependency')
            exit()


def check_job_prev_status(job_name, postgres_prod_cur, logger,debug):
        sql = "select bi_audit.get_job_status('"+job_name+"')"
        if debug:
            print(sql)
        postgres_prod_cur.execute(sql)
        for row in postgres_prod_cur:
            if row[0] == 'running':
                logger.error('Previous istance of job is running')
                print('Email sent running')
                exit()


def create_job_log(job_name, postgres_prod_cur, postgres_prod_conn,debug):
    sql = "select bi_audit.get_last_successfull_id('" + job_name + "')"
    if debug:
        print(sql)
    postgres_prod_cur.execute(sql)
    for row in postgres_prod_cur:
        prev_success_id = row[0]
    sql = "select bi_audit.get_last_load_date('"+job_name+"')"
    if debug:
        print(sql)
    postgres_prod_cur.execute(sql)
    for row in postgres_prod_cur:
        prev_success_date = row[0]
    if prev_success_date is None:
        prev_success_date = date.today()
    sql = "select bi_audit.create_job_log('"+job_name+"','"+str(prev_success_date)+"'::date)"
    if debug:
        print(sql)
    postgres_prod_cur.execute(sql)
    for row in postgres_prod_cur:
        new_job_log_id = row[0]
    postgres_prod_conn.commit()
    return prev_success_id,prev_success_date, new_job_log_id


def insert_task_log(new_job_log_id, task_name, postgres_prod_cur, postgres_prod_conn,debug):
    sql = "select bi_audit.create_task_log( %s,%s)" % (new_job_log_id, "'%s'" % task_name)
    if debug:
        print(sql)
    postgres_prod_cur.execute(sql)
    for row in postgres_prod_cur:
        task_log_id = row[0]
    postgres_prod_conn.commit()
    return task_log_id


def update_task_log(task_log_id, task_status, postgres_prod_cur,debug):
    sql = "select bi_audit.update_task_status( '%s','%s')" % (task_log_id, task_status)
    if debug:
        print(sql)
    postgres_prod_cur.execute(sql)


def update_job_status(new_job_log_id, job_status, postgres_prod_cur,debug):
    sql = "select bi_audit.update_job_status( '%s','%s')" % (new_job_log_id, job_status)
    if debug:
        print(sql)
    postgres_prod_cur.execute(sql)

def update_file_extract_status(file_key, status, job_log_id, email_time, postgres_prod_cur, debug):
    sql = "select bi_audit.update_file_extract_status( '%s','%s','%s','%s')" % (file_key, status, job_log_id, email_time)
    if debug:
        print(sql)
    postgres_prod_cur.execute(sql)

def update_file_load_status(file_key, status, postgres_prod_cur, debug):
    sql = "select bi_audit.update_file_load_status( '%s','%s')" % (file_key, status)
    if debug:
        print(sql)
    postgres_prod_cur.execute(sql)

def get_logger():
    log_file = './log/log_' + str(date.today()) + '.log'
    logger=logging.basicConfig \
            (
                filename=log_file, level=logging.DEBUG,
                format="%(asctime)s:%(levelname)s:%(message)s"
            )
    return logging.getLogger()

def upload_file_to_db(table_name, data, cur_tgt, conn_tgt):
    cur_tgt.copy_expert("""COPY {} FROM STDIN WITH(FORMAT CSV)""".format(table_name), data)
    conn_tgt.commit()


def read_file_to_csv_pandas(file_obj, delimit):
    #delimiter = "\t" if delimit == "\\t" else delimit
    
    fdata = file_obj['Body'].read()
    df = pd.read_csv(io.BytesIO(fdata), sep=delimit)

    # converting dataframe to csv
    sio = StringIO()
    sio.write(df.to_csv(index=None, header=None))
    sio.seek(0)
    return sio
