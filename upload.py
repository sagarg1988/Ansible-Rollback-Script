import boto3
import os
import socket
import gzip
rootDir = 'collect'
hostname = str(socket.gethostname())

rootDir = 'collect/'
bucket_name='hsbcbucket'
session = boto3.Session(
    aws_access_key_id='AKIAJJ2OORQFJQYXHHDQ',
    aws_secret_access_key='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',
)
s3 = session.resource('s3')
# Filename - File to upload
# Bucket - Bucket to upload to (the top level directory under AWS S3)
# Key - S3 object name (can contain subdirectories). If not specified then file_name is used
s3.meta.client.upload_file(Filename='/home/ubuntu/soft.py', Bucket='hsbcbucket',Key='soft.py')


def connect():
    session = boto3.Session(
        aws_access_key_id='AKIAJJ2OORQFJQYXHHDQ',
        aws_secret_access_key='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',
    )
    return session

def upload(session, name, bucket_name):
    s3 = session.resource('s3')
    # Filename - File to upload
    # Bucket - Bucket to upload to (the top level directory under AWS S3)
    # Key - S3 object name (can contain subdirectories). If not specified then file_name is used
    s3.meta.client.upload_file(Filename=name, Bucket='hsbcbucket', Key='soft.py')

def traversing_files():
    for dirName, subdirList, fileList in os.walk(rootDir):
        # print(dirName)

        for fname in fileList:
            print('\t%s' % fname)

            # with open(dirName+"/"+fname, "r") as fd:
            #     print(fname)
            with gzip.open(dirName + "/" + fname, 'r') as fin:
                all_content = fin.readlines()

                command_details = fname.split(hostname)[1].replace('-', '').split(".")
                command = command_details[0]
                command_run_status = command_details[1]  # stderr/stdout
                print(command)

            # write dump data logic here
            upload(session, fname, bucket_name)

            # Put logic of copy here


if __name__ == '__main__':
    traversing_files()