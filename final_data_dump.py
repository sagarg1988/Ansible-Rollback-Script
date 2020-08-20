import boto3
import os
import socket
import gzip
import tarfile


bucket_name='hsbcbucketdilshad'
hostname = 'ip-172-31-19-141'#str(socket.gethostname())
id= 0
rootDir = '/home/ubuntu/new_zip/'

def connect():
    #with open('/home/ubuntu/cred.txt', 'r') as reader:
    #    key = reader.readline().split('key=')[1].replace('\n', '')
    #    secret = reader.readline().split('secret=')[1].replace('\n', '')
    #    print(key, secret)
    session = boto3.Session(
        aws_access_key_id='',
        aws_secret_access_key='',
    )
    return session


def put_data(session, id, fname, all_content, command_type):
    client = session.resource('dynamodb', region_name='ap-south-1')

    # this will search for dynamoDB table
    # your table name may be different
    table = client.Table("HSBC1")
    print(table.table_status)

    response = table.put_item(
       Item={
            'ID': str(id),
            'host':hostname,
            'title': fname,
            'Command Type': command_type,
            'output': all_content
        }
    )
    # return response


def extract_gz():
    #import pdb;pdb.set_trace()
    for root, directories, files in os.walk(rootDir):
        # for directory in directories:
        #    print(directory)
        for file in files:
            #import pdb;pdb.set_trace()
            hostname=file.split('tar.gz')[0]
            if 'ip' in file and 'tar.gz' in file:
                print(file)
                my_tar = tarfile.open(rootDir + file, "r:gz")
                my_tar.extractall('/home/ubuntu/extracted/')
                my_tar.close()

def traversing_files(session):

    id = 0 # get_last_record()
    for dirName, subdirList, fileList in os.walk('/home/ubuntu/extracted/collect/'):
        print(dirName)
        for fname in fileList:
            print('\t%s' % fname)
            file_name = dirName + "/" + fname
            print(file_name)
            if '.gz' in fname:
                with gzip.open(dirName+'/' + fname, 'r') as fin:
                    all_content = fin.readlines()
                    command_details = fname.split(hostname)[1].replace('-', '').split(".")
                    command = command_details[0]
                    command_run_status = command_details[1]  # stderr/stdout
                    print(command)
                    put_data(session, id, fname, all_content, dirName)
                    id = id+1
            # write dump data logic here


if __name__ == '__main__':
    session = connect()
    extract_gz()
    traversing_files(session)