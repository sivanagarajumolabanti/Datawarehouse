import argparse
import os
import io
import pandas as pd
import sys

from io import BytesIO as StringIO

from python_code.utils import (
    get_logger, create_job_log, update_job_status, update_task_log,
    insert_task_log, upload_file_to_db, read_file_to_csv_pandas,update_file_load_status
)
from python_code.connections import *
from python_code.partner_email_extract import download_s3_folder, s3_copy_file, s3_delete_file
from python_code.sfdc_functions import load_to_target

root = os.path.join(os.path.dirname(__file__))
base_path = os.path.join(root, "partner_ingest_daily", "sql_scripts", "dml")


def call_partners(env, debug):

    job_name = 'partner_ingest_daily'
    task_log_id = None

    conn_tgt = bi_connection(env)
    curr_tgt = conn_tgt.cursor()
    logger = get_logger()

    if debug:
        print('Environment : %s' % env)

    logger.info('****************** Starting job %s ******************' % job_name)
    # Create a new log entry for the job
    prev_success_id, prev_success_date, new_job_log_id = create_job_log(job_name, curr_tgt, conn_tgt, debug)

    logger.info('===================> Initialized variables')
    logger.info('job_name :%s' % job_name)
    logger.info('old_job_log_id :%s' % prev_success_id)
    logger.info('new_job_log_id :%s' % new_job_log_id)
    logger.info('env :%s' % env)
    logger.info('debug :%s' % debug)
    logger.info('===================>')

    try:
        sql = """
                select file_key,file_pickup_date,tgt_file_name,s3_bucket,s3_raw_loc,
                file_delimiter,s3_processed_loc,staging_table,transformation_file, s3_raw_loc,
                s3_processed_loc 
                from bi_audit.file_master 
                where 1=1  
                and enabled=1 
                and is_extract_done=1
                and is_load_done=0
                and file_frequency in ('d','w') order by file_key;
            """
        curr_tgt.execute(sql)
        rows = curr_tgt.fetchall()

        try:
            for row in rows:

                file_obj = download_s3_folder(row,env)

                transformation_file = os.path.join(base_path, row[8])
                # Load data to stage

                logger.info('===================> Started task Loading to Stage %s' % row[7])

                task_log_id = insert_task_log(
                     new_job_log_id, "%s -> Loading to Stage" % row[7], curr_tgt, conn_tgt, debug)

                # command to truncate the database table here first : row[7]
                curr_tgt.execute("Truncate {} Cascade".format(row[7]))

                # command to insert file to RDS Postgres database..(Postgres database on AWS instance).
                # Before insertion we need to truncate the table and call transformation query...

                sio = read_file_to_csv_pandas(file_obj, row[5])

                upload_file_to_db(row[7], sio,curr_tgt,conn_tgt)

                update_task_log(task_log_id, "completed", curr_tgt, debug)

                logger.info('===================> Finished task Loading to Stage %s' % row[2])

                # Load data to target
                logger.info('===================> Started task Loading to Target %s' % row[0])

                task_log_id = insert_task_log(
                    new_job_log_id, "%s -> Loading to target" % row[0], curr_tgt, conn_tgt, debug)

                load_to_target(transformation_file, curr_tgt, conn_tgt, debug)

                update_task_log(task_log_id, "completed", curr_tgt, debug)
                update_file_load_status(row[0], 1 , curr_tgt, debug)

                logger.info('===================> Finished task Loading to Target %s' % row[0])


                logger.info('===================> Started task Sending File to Processed Location %s' % row[0])

                src_path=row[3] + '/' + row[4] + row[2]
                dest_path = row[3] + '/' + row[6]+ row[2]

                s3_copy_file(src_path, dest_path,env)
                s3_delete_file(src_path,env)
                logger.info('===================> Finished task Sending File to Processed Location %s' % row[0])

            # Update the job status
            update_job_status(new_job_log_id, "completed", curr_tgt, debug)
            logger.info('****************** Job %s finished successfully ******************' % job_name)

        except Exception as e:
            print('====== Job failed Alert email sent =====')
            print(e)
            logger.error(e)
            logger.error('****************** Job %s failed ******************' % job_name)
            conn_tgt.close()
            conn_tgt = bi_connection(env)
            curr_tgt = conn_tgt.cursor()
            update_task_log(task_log_id, "failed", curr_tgt, debug)
            update_job_status(new_job_log_id, "failed", curr_tgt, debug)
            conn_tgt.commit()
            conn_tgt.close()
            sys.exit(0)

        conn_tgt.commit()
        conn_tgt.close()

    except Exception as e:
        print('====== Job failed Alert email sent =====')
        print(e)
        logger.error(e)
        logger.error('****************** Job %s failed ******************' % job_name)
        conn_tgt.close()
        conn_tgt = bi_connection(env)
        curr_tgt = conn_tgt.cursor()
        update_task_log(task_log_id, "failed", curr_tgt, debug)
        update_job_status(new_job_log_id, "failed", curr_tgt, debug)
        conn_tgt.commit()
        conn_tgt.close()
        sys.exit(0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-e', '--env',
        action='store',
        default="dev",
        help='Will pick default date if date not entered, Please enter date in (DD-MM-YYYY) eg.. 02-Jul-1989'
    )

    parser.add_argument(
        '-d', '--debug',
        action='store',
        default=False,
        help='Will pick default date if date not entered, Please enter date in (DD-MM-YYYY) eg.. 02-Jul-1989'
    )

    args = parser.parse_args()

    env = args.env
    debug = True if args.debug == 'True' else False
    call_partners(env, debug)


