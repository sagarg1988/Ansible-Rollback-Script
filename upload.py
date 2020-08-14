import boto3
import os
import socket
import gzip
from boto3.dynamodb.conditions import Key, Attr

bucket_name='hsbcbucket'
hostname = str(socket.gethostname())
id= 0
rootDir = 'collect/'

def connect():
    with open('../cred.txt') as reader:
        key = reader.readline().split('key=')[1].replace('\n', '')
        secret = reader.readline().split('secret=')[1].replace('\n', '')
        # print(key, secret)
    session = boto3.Session(
        aws_access_key_id=key,
        aws_secret_access_key=secret,
    )
    return session

def upload(session, name, bucket_name):
    s3 = session.resource('s3')
    outer_name = hostname+'/'+name
    s3.meta.client.upload_file(Filename=name, Bucket=bucket_name, Key=outer_name)

def put_data(session, id, fname, all_content, command_type):
    client = session.resource('dynamodb', region_name='ap-south-1')

    # this will search for dynamoDB table
    # your table name may be different
    table = client.Table("HSBC")
    print(table.table_status)

    response = table.put_item(
       Item={
            'ID': id,
            'host':hostname,
           'title': fname,
            'Command Type': command_type,
            'output': all_content
        }
    )
    # return response

def get_last_record():
    client = session.resource('dynamodb', region_name='ap-south-1')
    table = client.Table('HSBC')

    response = table.query(
        KeyConditionExpression=Key('ID').eq('latest_entry_identifier'))
    items = response['Items']
    print(items)
    return items

def traversing_files(session):
    id = 0 # get_last_record()
    for dirName, subdirList, fileList in os.walk(rootDir):
        print(dirName)
        for fname in fileList:
            print('\t%s' % fname)

            # with open(dirName+"/"+fname, "r") as fd:
            #     print(fname)
            file_name = dirName + "/" + fname
            print(file_name)
            # with gzip.open(dirName + "/" + fname, 'r') as fin:
            # with open(dirName + "/" + fname, 'r') as fin:
            #     all_content = fin.readlines()
            #
            #     command_details = fname.split(hostname)[1].replace('-', '').split(".")
            #     command = command_details[0]
            #     command_run_status = command_details[1]  # stderr/stdout
            #     print(command)
            #     put_data(session, id, fname, all_content, dirName)
            #     id = id+1
            # # write dump data logic here
            # upload(session, file_name, bucket_name)

            # Put logic of copy here



if __name__ == '__main__':
    session = connect()
    traversing_files(session)
