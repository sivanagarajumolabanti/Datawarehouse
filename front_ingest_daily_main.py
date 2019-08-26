import os
import sys
import ast
import argparse

from python_code.front_functions import extract_from_source, load_to_target, load_to_stage
from python_code.utils import *
from python_code.connections import *

job_name = 'front_ingest_daily'
old_job_log_id = None
new_job_log_id = None 
task_log_id = None
#env = sys.argv[1]
#debug = False if sys.argv[2]=='0' else True #ast.literal_eval(sys.argv[2])
root = os.path.join(os.path.dirname(__file__))
base_path = os.path.join(root, "front_ingest_daily", "sql_scripts", "dml")


def call_front_ingest (env, debug):

    front_conn = front_connection()
    front_curr = front_conn.cursor()
    conn_tgt = bi_connection(env)
    curr_tgt = conn_tgt.cursor()
    logger = get_logger()

    if debug:
        print('Environment :' + env)

    logger.info('******************Starting job %s ******************' % job_name)

    # Check for dependencies. If dependent jobs did not run then exit
    check_dependencies(job_name, curr_tgt, logger, debug)

    # Check for previous run if still running then exit
    check_job_prev_status(job_name, curr_tgt, logger, debug)

    # Create a new log entry for the job
    prev_success_id, prev_success_date, new_job_log_id = create_job_log(job_name, curr_tgt, conn_tgt, debug)

    logger.info('===================> Initialized variables')
    logger.info('job_name :%s' % job_name)
    logger.info('old_job_log_id :%s' % prev_success_id)
    logger.info('new_job_log_id :%s' % new_job_log_id)
    logger.info('env :%s' % env)
    logger.info('debug :%s' % debug)
    logger.info('root :%s' % root)
    logger.info('base_path :%s' % base_path)

    logger.info('===================>')

    try:
        for data in ['orgs','users','designs','builds']:

                extract = "extract_front_{}.sql".format(data)
                transform = "transformation_front_{}.sql".format(data)

                extract_file = os.path.join(base_path, extract)
                transform_file = os.path.join(base_path, transform)

                print(extract)
                print(transform)

                # Extract object from source

                logger.info('===================> Started task Extract from source %s' % data)

                task_log_id = insert_task_log(new_job_log_id, "%s -> Extract from source" % data, curr_tgt, conn_tgt,debug)

                records = extract_from_source(front_conn,front_curr, extract_file,debug)

                update_task_log(task_log_id, "completed", curr_tgt,debug)

                logger.info('===================> Finished task Extract from source %s' % data)

                # Load data to stage

                logger.info('===================> Started task Loading to Stage %s' % data)

                task_log_id = insert_task_log(new_job_log_id, "%s -> Loading to Stage" % data, curr_tgt, conn_tgt,debug)

                load_to_stage(records, curr_tgt, conn_tgt, data , debug)

                update_task_log(task_log_id, "completed", curr_tgt,debug)

                logger.info('===================> Finished task Loading to Stage %s' % data)

                # Load data to target

                logger.info('===================> Started task Loading to Target %s' % data)

                task_log_id = insert_task_log(new_job_log_id, "%s -> Loading to target" % data, curr_tgt, conn_tgt,debug)

                load_to_target(transform_file, curr_tgt, conn_tgt,debug)

                update_task_log(task_log_id, "completed", curr_tgt,debug)

                logger.info('===================> Finished task Loading to Target %s' % data)

        # Load data to design opportunity history
        data='design_opportunity_hist'
        transform = "transformation_front_{}.sql".format(data)
        transform_file = os.path.join(base_path, transform)

        logger.info('===================> Started task Loading table %s' % data)

        task_log_id = insert_task_log(new_job_log_id, "%s -> Loading to target" % data, curr_tgt, conn_tgt, debug)

        load_to_target(transform_file, curr_tgt, conn_tgt, debug)

        update_task_log(task_log_id, "completed", curr_tgt, debug)

        logger.info('===================> Finished task Loading to Target %s' % data)

        # Load data to uinque designs

        data = 'unique_designs'
        transform = "transformation_front_{}.sql".format(data)
        transform_file = os.path.join(base_path, transform)

        logger.info('===================> Started task Loading table %s' % data)

        task_log_id = insert_task_log(new_job_log_id, "%s -> Loading to target" % data, curr_tgt, conn_tgt, debug)

        load_to_target(transform_file, curr_tgt, conn_tgt, debug)

        update_task_log(task_log_id, "completed", curr_tgt, debug)

        logger.info('===================> Finished task Loading to Target %s' % data)

        # Load data to uinque designs

        data = 'targeting_codes'
        transform = "transformation_front_{}.sql".format(data)
        transform_file = os.path.join(base_path, transform)

        logger.info('===================> Started task Loading table %s' % data)

        task_log_id = insert_task_log(new_job_log_id, "%s -> Loading to target" % data, curr_tgt, conn_tgt, debug)

        load_to_target(transform_file, curr_tgt, conn_tgt, debug)

        update_task_log(task_log_id, "completed", curr_tgt, debug)

        logger.info('===================> Finished task Loading to Target %s' % data)

        # Update the job status
        update_job_status(new_job_log_id, "completed", curr_tgt,debug)
        logger.info('****************** Job %s finished successfully ******************' % job_name)

    except Exception as e:
        print('====== Job failed Alert email sent =====')
        print(e)
        logger.error(e)
        logger.error('****************** Job %s failed ******************' % job_name)
        conn_tgt.close()
        conn_tgt = bi_connection(env)
        curr_tgt = conn_tgt.cursor()
        update_task_log(task_log_id, "failed", curr_tgt,debug)
        update_job_status(new_job_log_id, "failed", curr_tgt,debug)
        conn_tgt.commit()
        conn_tgt.close()
        sys.exit(0)

    conn_tgt.commit()
    conn_tgt.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-e', '--env',
        action='store',
        default="dev",
        help='Decides Environment'
    )

    parser.add_argument(
        '-d', '--debug',
        action='store',
        default=False,
        help='Decides debug'
    )

    args = parser.parse_args()

    env = args.env
    debug = True if args.debug == 'True' else False
    call_front_ingest(env, debug)
