import logging
import json
import os
from decimal import Decimal
from common_src import dynamo,apis
from botocore.exceptions import ClientError


def respond(err, res=None):
    return {
        'statusCode': '400' if err else '200',
        'body': err.message if err else json.dumps(res),
        'headers': {
            'Content-Type': 'application/json',
        },
    }


def GET(payload,conn):
    message=""
    status =400
    try:
        results=conn.scan()
    except:
        status = 200
        message = "Call fail"
        results = ""
    

    return status,results,message;


def POST(payload,conn):
    '''
    POST 기능 
    - Lat, Lng, Radius 를 입력했을 시 반응, 
    - Single ID
    - 
    '''
    
    
    
    
    
    
    return 0;


def PUT(payload,conn):
    ### PUT은 work 등의 데이터를 갱신용,
    conn
    
    
    
    
    
    
    if status == 200:
        return respond(ValueError(f'Unsupported method "{payload}"'))
            
    return respond()
    

    
def common_response(payload, operation, dynamoDB):
    '''
    reponse에 대한 분기 main 함수
    
    
    status  -> statusCode : 400 / 200
    body    -> 내용 전달
    message -> debug 용으로, 남겨둘 것
    '''
    operations = {
        'GET':  0,
        'POST': 1, ### post medge
        'PUT':  2, ### PUT 
    }
    
    if operations[operation] == 0:                    ### GET
        err,result,message= GET(payload,dynamoDB)
        logging.info(message)
        
        return respond(err,res=result)
            
    elif operations[operation] == 1:                  ### POST
        return POST(payload,dynamoDB)
        
    elif operations[operation] == 2:
        return PUT(payload, dynamoDB)
    else:
        
        return ValueError('Parameter fail " : {}"')
    


def lambda_handler(event, context):
    logging.basicConfig(format='[%(asctime)s] %(message)s')
    aws_env      = os.environ['AWSENV']
    dev_env      = os.environ['DEVENV']
    region       = os.environ['REGION']
    table_name   = os.environ['TABLE']
    
    logging.info(f"env:{aws_env},{dev_env},")
    
    dynamoDB=dynamo.dynamoApi(aws_env,dev_env,region,table_name)
    
    operation = event['httpMethod']
    operations = {
        'GET':  0,
        'POST': 1, ### post medge
        'PUT':  2, ### PUT 
    }
   
    if operation in operations:
        payload = event['queryStringParameters'] if operation == 'GET' else event['body'] ### 유의미한 데이터 받아오기
        try:
            logging.info('[ Normal Method ] {operation} : {time.time}')
            common_response(payload, operation,dynamoDB)
        except:
            return respond(ValueError('Unsupported query "{}"'.format(payload)))
    else:
        return respond(ValueError('Unsupported method "{}"'.format(operation)))
    