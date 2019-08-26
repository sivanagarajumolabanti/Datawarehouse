import boto3
import email
import imaplib
import logging
import os,re,os.path
import pandas as pd
import requests
from zipfile import ZipFile
from botocore.client import Config
from dateutil import  parser
import csv
from python_code.utils import fetch_json_data
from python_code.connections import *


logger = logging.getLogger(__name__)

root = os.path.join(os.path.dirname(__file__))
base_path = os.path.join(root, "attachments")
dest_path = os.path.join(root, "formatted")

body_search = "Download your report by clicking the link below:"
end_sign = "The Trade Desk"


class AccessEmail(object):

    def __init__(self):
        self.mail = None
        self.data = None

    def login(self,email_account):
        print(email_account)
        try:
            logger.info("Logging into Gmail")
            self.mail = imaplib.IMAP4_SSL('imap.gmail.com', 993)
            if email_account == 'ldap':
                data = fetch_json_data('gmail_ldap')
            elif email_account == 'reporting':
                data = fetch_json_data('gmail_reporting')
            self.mail.login(data['user'], data['pwd'])
            logger.info("Email connection established successfully")
            print("Login Successfully")
        except Exception as E:
            logger.error("Error occurred while establishing connection")
            logger.error("Error :: %s" % E)
            print("Error while downloading data from Json file::", E)
            print('Error connecting to email account')
            raise(0)

    def logout(self):
        logger.info("Email successfully signed out")
        self.mail.logout()
        print("Logout Successfully")

    def search_emails(self, info):
        global result
        try:
            logger.info(
                "Trying to Fetch Data from Email on the date : %s for the email : %s for subject %s" %
                (info[1], info[8], info[9])
            )
            self.mail.select('INBOX')
            mail_id = info[8] # email_from
            day = info[1].strftime("%d-%b-%Y") # file_pickup_date
            print(day)
            if info[15] == 'y':
                print('yes')
                result, data = self.mail.search(None, '(FROM "{}" SINCE "{}" SUBJECT "{}")'.format(mail_id, day, info[9]))
            elif info[15] == 'n':
                print('no')
                result, data = self.mail.search(None,'(FROM "{}" SENTON "{}" SUBJECT "{}")'.format(mail_id, day, info[9]))
                print(result)
                print(data)
            logger.info("Status of fetching data from Email:: %s" % result)
            return result, data

        except Exception as E:
            logger.error("Error while Searching data in Email:: %s" % E)

    # def get_recent_timestamp(self, emails):
    #     try:
    #         for inbox in emails[0].split():
    #             result2, email_data = self.mail.fetch(inbox, '(RFC822)')
    #             raw_email = email_data[0][1].decode("utf-8")
    #             email_message = email.message_from_string(raw_email)
    #             timestamp = [data for data in email_message._headers if 'Received' in data][0][1]
    #
    #
    #     except Exception as e:
    #         logging.error("Error while fetching latest email")

    def base_email_reading(self, emails, row):
        try:
            email_list = []
            for info in emails.split():
                result2, email_data = self.mail.fetch(info, '(RFC822)')
                raw_email = email_data[0][1].decode("utf-8")
                email_message = email.message_from_string(raw_email)

                if row[2] == 'email':
                    payload = email_message.get_payload()
                    for attachment in payload[1:]:
                        filename = "".join(attachment.get_filename().splitlines())
                        if filename.lower() == row[3].lower():
                            email_list.append((info, payload, email_message._headers))
                if row[2] == 'link':
                    subject = [data for data in email_message._headers if 'Subject' in data][0][1]

                    logger.info("Subject data and Database data :: %s, %s" % (subject.lower(), row[9].lower()))

                    if row[9].lower() in subject.lower():
                        payload = email_message.get_payload()
                        email_list.append((info, payload, email_message._headers))

                        #payload[0].get_payload()[0].get_payload()
            return email_list

        except Exception as e:
            logging.error("Error while reading base email format.")

    def get_latest_email_from_attached(self, list_emails):
        try:
            logging.info("Process started to find the latest timestamp of all the emails")
            eid = None
            edate = None
            payload = None
            for data in list_emails:
                try:
                    timestamp = data[-1][1][1].split("\n")[1].strip()
                    dt = parser.parse(timestamp)
                    if edate and edate < dt:
                        edate = dt
                        eid = data[0]
                        payload = data[1]
                    if not edate:
                        edate = dt
                        eid = data[0]
                        payload = data[1]
                except Exception as e:
                    logging.error("Error while format date", data[-1], e)
            return eid, payload, edate
        except Exception as e:
            logging.error("Error while getting latest email...", data[-1], e)

    def get_matched_attachments_email_ids(self, emails, row):

        try:
            logging.info("Processes started to know the matched filename subjects..")
            list_emails = self.base_email_reading(emails, row)
            em_id, payload, em_date = self.get_latest_email_from_attached(list_emails)
            return em_id, payload, em_date

        except Exception as e:
            logging.error("Error while searching the attachment..")

    def process_downloading(self, payload, row):

        """
            Function to process Inbox downloading list....
            If we have multiple emails we need to extract data from most recent one..
        """
        try:
            logger.info("Process initiated for downloading email from attachments")
            # Here we need to check whether the attachment payload is a file or not.
            # if the attachment is a file, then we need to download simply otherwise we need to extract it again

            if row[2] == 'email':
                self.download_attachments(payload, row)

            if row[2] == "link":
                self.download_from_url(payload, row)

            # for inbox in emails[0].split():
            #     result2, email_data = self.mail.fetch(inbox, '(RFC822)')
            #     raw_email = email_data[0][1].decode("utf-8")
            #     email_message = email.message_from_string(raw_email)
            #     subject = [data for data in email_message._headers if 'Subject' in data][0][1]
            #
            #     logger.info("Subject data and Database data :: %s, %s" % (subject.lower(), row[9].lower()))
            #     if row[9].lower() in subject.lower():
            #         payload = email_message.get_payload()
            #
            #         logger.info("Count of payload object for the Email:: %s " % len(payload))
            #         if row[2] == 'email':
            #             return self.download_attachments(payload, row)
            #
            #         if row[2] == 'link':
            #             return self.download_from_url(payload, row)
        except Exception as E:
            logger.error("Error while downloading data from Email::%s" % E)

    @staticmethod
    def download_attachments(payload, row):
        try:
            logger.info("Attachment download process started")

            attachments = payload[1:]

            for data in attachments:
                #filename = data.get_filename()
                filename = "".join(data.get_filename().splitlines())
                # Checking the File name whether the filename matches with the database filename or not...
                if filename.strip() == row[3].strip():
                    try:
                        logger.info("Deleting existing files in the folder")
                        directory = os.path.join(base_path + '/')
                        for root, dirs, files in os.walk(directory):
                            for file in files:
                                os.remove(os.path.join(root, file))

                        logger.info("Trying to download data from Email : %s" % row[3])
                        if row[3].endswith(".zip"):
                            open(base_path + '/' + row[3], 'wb').write(data.get_payload(decode=True))  # download the zip file
                            with ZipFile(base_path + '/' + row[3]) as zf:                              # grab the actual files
                                with open(base_path + '/' + row[4], 'w') as file:
                                    file.write(zf.read(zf.namelist()[0]))
                            if row[9].__contains__('Pixel Targeting'):                                  # if mediamath pixel file then remove first 2 lines
                                with open(base_path + '/' + row[4], 'rb') as csvfile:
                                    lines = csvfile.readlines()[2:]
                                    csvfile.close()

                                with open(base_path + '/' + row[4], 'wb') as csvfile:          # write the clensed data to the target file
                                    csvfile.writelines(lines)
                                    csvfile.close()

                            # if row[9].__contains__('Pixel Targeting'):
                            #     with ZipFile(base_path + '/' + row[3]) as zf:
                            #         with open(base_path + '/' + row[4], 'w') as file:
                            #             file.write(zf.namelist()[0].split('/').pop())
                            # else:
                            #     with ZipFile(base_path + '/' + row[3]) as zf:
                            #         with open(base_path + '/' + row[4], 'w') as file:
                            #             file.write(zf.read(zf.namelist()[0]))
                        else:
                            open(base_path + '/' + row[4], 'wb').write(data.get_payload(decode=True))


                        logger.info("Email Attachment process ended....")
                        return True
                    except Exception as E:
                        logger.error("Error occurred while Downloading attachment from Email: %s" % E)
        except Exception as E:
            print("Error while downloading attachment", E)

    @staticmethod
    def download_from_url(payload, row):
        try:
            logger.info("Process started for downloading data from URL..")
            body = payload#[0].get_payload()[0].get_payload()
            if body_search in body:
                url = body.split(body_search)[-1].split(end_sign)[0].strip()
                r = requests.get(url, stream=True)
                try:
                    logger.info("Trying to downloading data from URL")
                    with open(base_path + '/' + row[4], 'wb') as f:
                        for chunk in r.iter_content(chunk_size=1024):
                            if chunk:
                                f.write(chunk)
                    logger.info("Email Attachment process ended....")
                    return True
                except Exception as E:
                    logger.info("Error while downloading data from URL : %s" % E)
                logger.info("Process finished for downloading data from URL..")
        except Exception as E:
            logger.error("Process ended for downloading data from URL...")
            print("Error while fetching data from URL", E)

    def file_formatting(self, row):
        """
            Function to format rows based up on the requirement...
        :param row:
        :return:
        """
        try:
            if not row[6].strip():
                logging.error("File Format return None Values... Process quit Here...")
                return None

            # Checking the first row of the file
            logging.info("File Validation begins, checking the first row of file..")
            file_path = os.path.exists(base_path + '/' + row[4])

            if file_path:
                file_name = os.path.join(base_path + '/' + row[4])
                logger.info("File existed, file formatting process started...")
                columns = row[6]
                df = pd.read_csv(file_name , sep= row[13])
                df = df.filter(items=columns.split(','))
                #df.to_csv(dest_path+'/'+row[4], sep=[13])

        except Exception as e:
            logger.error("Error while formatting the file", e)

    def upload_to_s3(self, row,env):
        """
            Functionality to upload files to S3..
        :param row:
        :return:
        """
        try:

            # s3_bucket = fetch_json_data('s3_bucket')
            #
            # s3 = boto3.resource(
            #     's3',
            #     aws_access_key_id=s3_bucket['ACCESS_KEY_ID'],
            #     aws_secret_access_key=s3_bucket['ACCESS_SECRET_KEY'],
            #     config=Config(signature_version='s3v4')
            # )

            s3 = get_s3_resource(env)

            # writing processed_file to s3
            filename = base_path + '/' + row[4]
            bucket_name = row[14]
            bucket_folder= row[5]

            logging.info('Uploading %s to Amazon S3 bucket %s %s' % (filename, bucket_name,bucket_folder))
            s3.Object(bucket_name, bucket_folder+row[4]).put(Body=open(filename, 'rb'))
            logging.info('Uploaded %s to Amazon S3 bucket %s %s' % (filename, bucket_name,bucket_folder))
            #print('File uploaded to https://s3.amazonaws.com/%s/%s' % (bucket_name, filename))

        except Exception as e:
            logger.error("Error while uploading files to S3... ::")
            print('Connection to s3  failed')
            raise(0)

def download_s3_folder(row,env):
    try:
        # if env == 'prod':
        #     s3_bucket = fetch_json_data('s3_bucket_prod')
        # elif env == 'dev':
        #     s3_bucket = fetch_json_data('s3_bucket_dev')

        # s3_client = boto3.client(
        #     's3', aws_access_key_id=s3_bucket['ACCESS_KEY_ID'],
        #     aws_secret_access_key=s3_bucket['ACCESS_SECRET_KEY'],
        #     config=Config(signature_version='s3v4'))
        s3_client = get_s3_client(env)
        try:
            base_path = row[3]+'/'+row[4]+row[2]#row[9].replace("https://s3.amazonaws.com/", "")
            bucket, key = base_path.split('/', 1)
            return s3_client.get_object(Bucket=row[3], Key=key)
        except Exception as e:
            logging.error("S3 Bucket not found.......")
            print("Bucket not found")
    except Exception as e:
        logger.error("Error while downloading files from S3... ::", e)
    return False

def s3_copy_file(src_path, target_path,env):
    # if env == 'prod':
    #     s3_bucket = fetch_json_data('s3_bucket_prod')
    # elif env == 'dev':
    #     s3_bucket = fetch_json_data('s3_bucket_dev')
    #
    # s3_client = boto3.client(
    #     's3', aws_access_key_id=s3_bucket['ACCESS_KEY_ID'],
    #     aws_secret_access_key=s3_bucket['ACCESS_SECRET_KEY'],
    #     config=Config(signature_version='s3v4'))

    s3_client = get_s3_client(env)
    src_path = src_path.replace("https://s3.amazonaws.com/", "")
    tgt_path = target_path.replace("https://s3.amazonaws.com/", "")

    src_bucket, src_key = src_path.split('/', 1)
    dest_bucket, dest_key = tgt_path.split('/', 1)

    try:
        copy_source = {'Bucket': src_bucket, 'Key': src_key}
        s3_client.copy(copy_source, dest_bucket, dest_key)
    except Exception as e:
        logger.error("Error while copying file from raw to processed location:: %s", e)

def s3_delete_file(s3file,env):
    # if env == 'prod':
    #     s3_bucket = fetch_json_data('s3_bucket_prod')
    # elif env == 'dev':
    #     s3_bucket = fetch_json_data('s3_bucket_dev')
    #
    # s3_client = boto3.client(
    #     's3', aws_access_key_id=s3_bucket['ACCESS_KEY_ID'],
    #     aws_secret_access_key=s3_bucket['ACCESS_SECRET_KEY'],
    #     config=Config(signature_version='s3v4'))

    s3_client = get_s3_client(env)
    src_path = s3file.replace("https://s3.amazonaws.com/", "")
    bucket, key = src_path.split('/', 1)

    try:
        logger.info("Process started to Delete file from s3 bucket:: %s, %s" % (bucket, key))
        status = s3_client.delete_object(Bucket=bucket, Key=key)
        logger.info("Deleting file from s3 bucket:: %s" % status)
        return status
    except Exception as e:
        logger.error("Error while copying file from raw to processed location:: %s", e)
