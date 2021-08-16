# -*- coding: utf-8 -*-
"""
Created on Mon Nov 2 22:22:34 2020
@author: aaggar15
"""

import boto3
import logging
import sys
import configparser as cp
import os

log = logging.getLogger('root')
FORMAT = "[%(levelname)s: %(filename)s:%(lineno)s - %(funcName)20s() ]  %(message)s"
logging.basicConfig(format=FORMAT)
log.setLevel(logging.DEBUG)

config_dict = {}

def check_for_configuration():  # reading the configuration property file
    log.info("reading configuration file")
    config = cp.ConfigParser()
    config.read("/App/B6744PRD/ML6744JP/PROD/python/src/config.properties")

    error_list = []

    for key, value in config['config'].items():
        if value is None or value.strip() == '':
            error_list.append(f'{key} \"{value}\" not found or incorrect\n')
        else:
            config_dict[key] = value

    if len(error_list) != 0:
        log.error("something went wrong while reading configuration=%s", error_list)
        sys.exit(-1)

    log.info("configuration file read successfully config_size=%s", len(config_dict))

def find_files():
    log.info("Searching for files in the path")
    try:
        mf_path = config_dict['mf_path']
        key_trigger = config_dict['cdf_trigger']
        orbit_trigger = config_dict['orbit_trigger']
        file_type = []
        file_types=config_dict['file_types']
        for fltypnm in file_types.split(','):
            file_type.append(fltypnm)
        log.info('This is the total list of type of files for ECG %s', file_type)
        list_of_files = os.listdir(mf_path)
        files_processed = []
        count = 0
        for file_to_send in list_of_files:
                    if file_to_send.startswith(key_trigger):
                        for i in file_type:
                            if i in file_to_send:
                                make_s3_conn()
                                bucket_name = config_dict['cdf_bucket_name']
                                send_files_to_s3(file_to_send,bucket_name)
                                files_processed.append(file_to_send)
                                count = count + 1
                    elif file_to_send.startswith(orbit_trigger):
                        make_s3_conn()
                        bucket_name = config_dict['orbit_bucket_name']
                        send_files_to_s3(file_to_send,bucket_name)
                        files_processed.append(file_to_send)
                        count = count + 1
        if count == 0:
            log.error('No File found on the path')
        else:
            msg = f'the number of files sent are {count} and the files processed are {files_processed}'
            log.info(msg)
    except Exception as e:
        log.error("Something went wrong while searching for file on path - %s, message - %s",mf_path,e)
        sys.exit(-1)


def make_s3_conn():
    log.info("Making Connection to amazon S3")
    access_key = config_dict['access_key']
    secret_key = config_dict['secret_key']
    url = config_dict['url']
    try:
        global s3_conn
        s3_conn = boto3.resource('s3', endpoint_url=url,aws_access_key_id=access_key,aws_secret_access_key=secret_key)
    except Exception as e:
        log.error("Something went wrong while connecting to S3. Url - %s, Access Key - %s, message - %s",url,access_key,e)
        sys.exit(-1)
    log.info('Connection Successful with Amazon S3')

def send_files_to_s3(file_to_send,bucket_name):
    log.info("Sending File to Amazon S3")
    try:
        bucket_name = bucket_name
        mf_path = config_dict['mf_path']
        log.info("BUCKET NAME IS %s",bucket_name)
        file_to_send1 = mf_path + file_to_send
        mainframe_file_name = file_to_send1
        s3_file_name = file_to_send
        s3_conn.meta.client.upload_file(mainframe_file_name,bucket_name,s3_file_name)
        os.remove(mainframe_file_name)
    except Exception as e:
        log.error("Something went wrong while sending file to S3. Bucket - %s, File - %s, message - %s",bucket_name,s3_file_name,e)
        sys.exit(-1)
    log.info('File - %s, successfully transferred to amazon S3', file_to_send)

# Calling the main functions:
if __name__ == "__main__":

# checking configuration file
    check_for_configuration()

# Checking for files in the path
    find_files()
