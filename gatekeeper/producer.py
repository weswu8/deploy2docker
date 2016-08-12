#!/usr/bin/python
'''
Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at

http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
'''
#------------------------------------------------------
# Name:      producer
# Purpose:   Put gatekeeper logs to AWS kinesis
# Version:   1.0
# Author:    Jack Tian
# Created:   2016-7-26
# Modified:  N/A
# Copyright:  (c) JackTian 2016
#------------------------------------------------------

from __future__ import print_function
import sys, os, string, time, datetime, json, base64, ConfigParser, argparse, logging
from boto import kinesis

# define global variables
region_name = 'us-east-1'
stream_name = ''
initialPositionInStream = 'LATEST'
config_file = 'producer.conf'
access_log_file = 'gatekeeper.access.log'
logging_file = './producer.log'
put_records_count = 2
put_records_interval = 1

def parse_config_file():
    '''
    parse config file
    '''
    global region_name
    global stream_name
    global initialPositionInStream
    global access_log_file
    global logging_file
    global put_records_count
    global put_records_interval

    cf = ConfigParser.ConfigParser()
    cf.read(config_file)

    #return all section
    secs = cf.sections()

    #read by key-value
    try:
        opts = cf.options(secs[0])
    except:
        print('Usage: Can not find {conf} !!!'.format(conf=config_file))
        logging.error('Error: Can not find - ' + config_file + ' !!!')
        sys.exit(1)

    # get every parameter
    for i in range(0,len(opts)):
        if opts[i] == 'regionname':
            region_name = cf.get(secs[0],opts[i])
        elif opts[i] == 'streamname':
            stream_name = cf.get(secs[0],opts[i])
        elif opts[i] == 'initialpositioninstream':
            initialPositionInStream = cf.get(secs[0],opts[i])
        elif opts[i] == 'access_log_file':
            access_log_file = cf.get(secs[0],opts[i])
        elif opts[i] == 'logging_file':
            logging_file = cf.get(secs[0],opts[i])
        elif opts[i] == 'put_records_count':
            put_records_count = int(cf.get(secs[0],opts[i]))
        elif opts[i] == 'put_records_interval':
            put_records_interval = int(cf.get(secs[0],opts[i]))
        else:
            print('Some parameters is missing ! Please correct it.')
            logging.error('Some parameters is missing ! Please correct it.')
            sys.exit(1)

def parse_command_line():
    '''
    parse command line parameters
    NOT BE USED HERE
    '''
    global region_name
    global stream_name
    global access_log_file

    parser = argparse.ArgumentParser(description='''
Puts HTTP server logs into a stream.

producer.py -s STREAM_NAME -f HTTP__LOG_FILENAME -r REGION_NAME

''')
    parser.add_argument("-s", "--stream", dest="stream_name", required=True,
                      help="The stream you'd like to put into.",)
    parser.add_argument("-f", "--filename", dest="logfilename",required=True,
                      help="HTTP log file to be processed.",)
    parser.add_argument("-r", "--regionName", "--region", dest="region", default="us-east-1",
                      help="The region you'd like to make this stream in. Default is 'us-east-1'", metavar="REGION_NAME",)

    args = parser.parse_args()
    stream_name = args.stream_name
    log_file = args.logfilename
    if args.region != '':
        region_name = args.region

class Tail(object):
    '''
    Represents a tail command.
    '''
    def __init__(self, tailed_file):
        ''' Initiate a Tail instance.
            Check for file validity, assigns callback function to standard out.
            
            Arguments:
                tailed_file - File to be followed. '''

        self.check_file_validity(tailed_file)
        self.tailed_file = tailed_file
        self.callback = sys.stdout.write

    def follow(self, record_count, interval):
        ''' Do a tail follow. If a callback function is registered it is called with every new line. 
        Else printed to standard out.
    
        Arguments:
            record_count - Number of accumulated logs together to be put to kinesis. Defaults to 2.
            interval - period of force to put accumulated logs to kinesis even less. Defaults to 1. '''

        with open(self.tailed_file) as file_:
            # Go to the end of file
            file_.seek(0,0)
            timer_begin = datetime.datetime.now()
            count = 0
            records = []
            while True:
                curr_position = file_.tell()
                # accumulate some[put_records_count] logs together to put to kinesis
                line = file_.readline()
                line = line.strip('\n')
                if not line:
                    file_.seek(curr_position)
                    time.sleep(1)
                else:
                    record = { 'Data':line, 'PartitionKey':str(hash(line))}
                    records.append(record)
                    count += 1
                timer_delta = (datetime.datetime.now() - timer_begin).total_seconds()
                if count == record_count or (count > 0 and count < record_count and timer_delta > interval):  # need to be put to kinesis
                    print('Accumulated {count} logs to be put ...'.format(count=count))
                    logging.info('Accumulated ' + str(count) + ' logs to be put ...')
                    self.callback(records)
                    count = 0
                    records = []
                    timer_begin = datetime.datetime.now()

    def register_callback(self, func):
        ''' Overrides default callback function to provided function. '''
        self.callback = func

    def check_file_validity(self, file_):
        ''' Check whether the a given file exists, readable and is a file '''
        if not os.access(file_, os.F_OK):
            raise TailError("File '%s' does not exist" % (file_))
        if not os.access(file_, os.R_OK):
            raise TailError("File '%s' not readable" % (file_))
        if os.path.isdir(file_):
            raise TailError("File '%s' is a directory" % (file_))

class TailError(Exception):
    def __init__(self, msg):
        self.message = msg
    def __str__(self):
        return self.message

# get stream status
def get_stream_status(conn, stream_name):
    '''
    Query this provided connection object for the provided stream's status.

    :type conn: boto.kinesis.layer1.KinesisConnection
    :param conn: A connection to Amazon Kinesis

    :type stream_name: str
    :param stream_name: The name of a stream.

    :rtype: str
    :return: The stream's status
    '''

    tries = 0
    while tries < 10:
        tries += 1
        time.sleep(1)
        try:
            response = conn.describe_stream(stream_name)
            StreamStatus = response['StreamDescription']['StreamStatus']
            if StreamStatus != 'ACTIVE':
                raise TimeoutError('Stream is still not active, aborting...')
            else:
                break
        except Exception as e:
            print('Error while trying to describe kinesis stream: {e} '.format(e=e))
            logging.error('Error while trying to describe kinesis stream: ' + e)
    return StreamStatus

# Wait for here if the stream s not ready
def wait_for_stream(conn, stream_name):
    '''
    Wait for the provided stream to become active.

    :type conn: boto.kinesis.layer1.KinesisConnection
    :param conn: A connection to Amazon Kinesis

    :type stream_name: str
    :param stream_name: The name of a stream.
    '''
    SLEEP_TIME_SECONDS = 3
    status = get_stream_status(conn, stream_name)
    while status != 'ACTIVE':
        print('{stream_name} has status: {status}, sleeping for {secs} seconds'.format(
                stream_name = stream_name,
                status      = status,
                secs        = SLEEP_TIME_SECONDS))
        time.sleep(SLEEP_TIME_SECONDS) # sleep for 3 seconds
        status = get_stream_status(conn, stream_name)

def put_to_kinesis(records):
    try:
        conn.put_records(records, stream_name)
    except Exception as e:
#        sys.stderr.write('Encountered an exception while trying to put records into kinesis {ks} exception was: {error}'.format(ks=stream_name,error=str(e)))
        print('Encountered an exception while trying to put records into kinesis {ks} exception was: {error}'.format(ks=stream_name,error=str(e)))
        logging.error('Encountered an exception while trying to put records into kinesis')

def process_logs(conn, stream_name):
    '''
    Put each log in the provided list of logs into the stream.

    :type conn: boto.kinesis.layer1.KinesisConnection
    :param conn: A connection to Amazon Kinesis

    :type stream_name: str
    :param stream_name: The name of a stream.

    :type log_file: str
    :param log_file: log file name to be processed
    '''

    t = Tail(access_log_file)
    t.register_callback(put_to_kinesis)
    t.follow(put_records_count,put_records_interval)       # DO NOT sleep in fact



#########################
# Main                  #
#########################

if __name__ == "__main__":

#    parse_config_file()

# get mandatory parameters from config file
    parse_config_file()

    logging.basicConfig(level=logging.INFO,  
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',  
                    datefmt='%a, %d %b %Y %H:%M:%S',  
                    filename=logging_file,  
                    filemode='w')
    logging.info('|-> Starting ...')

# connect to kinesis
    try:
        conn = kinesis.connect_to_region(region_name)
    except Exception as e:
        print('Failed to connect to region: {r} ! Error: {e}'.format(r=region_name,e=e))
        logging.error('Failed to connect to region: ' + region_name + ' !')
        sys.exit(1)

    try:
        status = get_stream_status(conn, stream_name)
        if 'DELETING' == status:
            print('The stream: {s} is being deleted, please rerun the script.'.format(s=stream_name))
            sys.exit(1)
        elif 'ACTIVE' != status:
            wait_for_stream(conn, stream_name)
    except Exception as e:
        # We'll assume the stream didn't exist so we will try to create it with just one shard
        print('Failed to connect to kinesis ! Error: {e} '.format(e=e))
        logging.error('Failed to connect to kinesis ! Error: ' + str(e))
        sys.exit(1)

# Now the stream should exist
# process it
    print('|-> Connected to Kinesis: "{s}" in "{r}" '.format(s=stream_name,r=region_name))
    logging.info('|-> Connected to Kinesis "' + stream_name + '" ... in ' + region_name)
    process_logs(conn, stream_name)


