'''
Job description: Extracts daily/monthly partner files from the emails that are received from various partners. This job is driven from the file_master table in bi_audit schema. Files are extracted
                 and dumped on s3 based on the location and other criteria's in the file_master table.
Arguments:
    env: dev/prod   "dev points to local postgres env and different s3 account"
    debug: True/False
    email: reporting/ldap "email account that should be searched for extracting the attachments"
'''

import sys
import argparse
from datetime import datetime
from python_code.utils import (
    update_file_extract_status, update_file_load_status, get_logger, create_job_log, update_job_status, update_task_log
)
from python_code.connections import *
from python_code.partner_email_extract import AccessEmail
import os

root = os.path.join(os.path.dirname(__file__))
base_path = os.path.join(root, "python_code/attachments")

def call_partners(env, debug,email_account):
    try:
        job_name = 'partners_extract_file_daily'
        task_log_id = None

        conn_tgt = bi_connection(env)
        curr_tgt = conn_tgt.cursor()
        logger = get_logger()

        if debug:
            print('Environment : %s' % env)

        logger.info('****************** Starting job %s ******************' % job_name)

        # Create a new log entry for the job
        prev_success_id, prev_success_date, new_job_log_id = create_job_log(job_name, curr_tgt, conn_tgt,debug)

        logger.info('===================> Initialized variables')
        logger.info('job_name :%s' % job_name)
        logger.info('old_job_log_id :%s' % prev_success_id)
        logger.info('new_job_log_id :%s' % new_job_log_id)
        logger.info('env :%s' % env)
        logger.info('debug :%s' % debug)
        logger.info('email account :%s' % email_account)
        logger.info('===================>')

        scrap = AccessEmail()
        scrap.login(email_account)

        # creating the attachements direc if if does not exist. this is where the attachments will the downloaded

        if not os.path.exists(base_path):
            os.makedirs(base_path)

        sql =\
        """
            select 
                file_key, file_pickup_date, file_source, src_file_name, tgt_file_name,
                s3_raw_loc, file_columns, api, email_from, email_subject, 
                alert_email, s3_raw_loc,s3_processed_loc, file_delimiter,s3_bucket,
                email_search_date_range 
            from bi_audit.file_master
            where 1=1 
            and enabled=1 
            and is_extract_done=0
            and file_pickup_date <= now() 
            and file_frequency in ('d','w') 
            order by file_key;
        """
        curr_tgt.execute(sql)
        rows = curr_tgt.fetchall()

        for row in rows:
            # look for the emails based on the info in the filemaster (row). return the status along with the emails found
            status, emails = scrap.search_emails(row)

            if status and len(emails[0])>0:
                # looks for the email to be processed
                eid, payload, dt = scrap.get_matched_attachments_email_ids(emails[0], row)
                # downloads the attachement
                scrap.process_downloading(payload, row)

                #scrap.file_formatting(row)

                # uploads to ftp
                scrap.upload_to_s3(row,env)
                update_file_extract_status(row[0], 1, new_job_log_id, '2018-07-30', curr_tgt, debug)
                conn_tgt.commit()
        update_job_status(new_job_log_id, "completed", curr_tgt, debug)

    except Exception as e:
        print('====== Job failed Alert email sent =====')
        print(e)
        logger.error(e)
        logger.error('****************** Job %s failed ******************' % job_name)
        conn_tgt.close()
        conn_tgt = bi_connection(env)
        curr_tgt = conn_tgt.cursor()
        #update_task_log(task_log_id, "failed", curr_tgt,debug)
        update_job_status(new_job_log_id, "failed", curr_tgt,debug)
        conn_tgt.commit()
        conn_tgt.close()
        sys.exit(0)

    conn_tgt.commit()
    conn_tgt.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # parser.add_argument(
    #     '-dt', '--date',
    #     action='store',
    #     default=datetime.today().strftime("%d-%b-%Y"),
    #     help='Will pick default date if date not entered, Please enter date in (DD-MM-YYYY) eg.. 02-Jul-1989'
    # )

    parser.add_argument(
        '-e', '--env',
        action='store',
        default="prod",
        help='Will pick default value'
    )
    parser.add_argument(
        '-d', '--debug',
        action='store',
        default=True,
        help='Will pick default value'
    )
    parser.add_argument(
        '-a', '--email',
        action='store',
        default="ldap",   # ldap: connecting to personal company acct   reporting: connecting to the main reporting account where all emails end up
        help='Will pick default value for the email account'
    )

    args = parser.parse_args()
    #dt = args.date
    env = args.env
    debug = True if args.debug == 'True' else False
    email_account=args.email

    # if not isinstance(dt, str):
    #     print("Need to convert String to date")
    #     try:
    #         dt = dt.strftime("%d-%b-%Y")
    #     except Exception as e:
    #         print("Please enter date in the format:: %d-%b-%Y")

    call_partners(env, debug,email_account)
