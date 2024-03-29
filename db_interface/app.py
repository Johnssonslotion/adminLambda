import os
import sys
sys.path.append(os.getcwd())
import logging

logger = logging.getLogger("INTERFACE-LAMBDA")
logger.info("DETAILS - START")
logger.setLevel(logging.INFO)


import json


from decimal import Decimal
import time

import dynamo,apis,utils ### in layer ~

# try:
#     import dynamo,apis,utils ### in layer 
# except:
#     from common_src import dynamo,apis,utils ### in local python code


def respond(err, res=None, step=None): ### error 
    ### boto3 code convention와 양식을 일치시키기 위해서
    TotalCnt = 0
    if type(res) is list:
        TotalCnt = len(res)    
    #print(f"BODY : {json.dumps(res,ensure_ascii=False)}")
    meta={
             'status': err.message if err else 'normal_condition',
             'step': step,
             'TotalCnt': TotalCnt
        }
    
    return {
        'statusCode': '400' if err else '200',
        'body': json.dumps( {
            "items":'[]',
            "meta" :meta
            },ensure_ascii=False) if err else json.dumps(res,ensure_ascii=False),
        'headers': {
            'Content-Type': 'application/json',
        },
    }

def GET(event,conn):
    # 해당 기능 POST 에 Merge 
    status =200
    err=utils.err_("None acceptable methods 'GET'")
    return respond(err)


def POST(event,conn):
    '''
    POST 기능 
    - Lat, Lng, Radius 를 입력했을 시 반응, 
    - Single ID
    - UPDATE
    - QUARY_STRING
    '''
    
    payload,err=utils.base64_body_parser(event) ## base64로 parser 
    if err is not None:
        return respond(err,step="PARSING STEP")
    else:
        ################################
        if 'lat' in payload.keys():
            if 'lng' in payload.keys():
                if 'radius' in payload.keys():
                    try:
                        lat= float(payload["lat"])
                        lng= float(payload["lng"])
                        radius = int(payload["radius"])
                    except:
                        err=utils.err_(f" Lat, Lng type error")
                        return respond(err=err)
                    
                    if lat < 0 or lat > 90:
                        err=utils.err_(f"Lat value error : 0 < {lat} < 90",step='CALL_RADIUS')
                        return respond(err=err)
                    if lng < 0 or lng > 180:
                        err=utils.err_(f"Lng value error : 0 < {lng} < 180",step='CALL_RADIUS')
                        return respond(err=err)
                    if radius < 0 or lng > 20000:
                        err=utils.err_(f"Radius value error : 0 < {radius} < 20000",step='CALL_RADIUS')
                        return respond(err=err)
                    logging.info(f"Normal radius search")
                    query_results,err=conn.query_radius(lat,lng,radius,Table='TEST_CASE_0_build_info',Index='geohash-opt')
                    if err is None:
                        return respond(err,res=query_results,step='CALL_RADIUS')
                    else:
                        return respond(err=err)
        #################################
        elif 'PK' in payload.keys():
            logging.info(f"Normal single search")
            query_results,err=conn.query_single_PK(payload["PK"])
            if err is None:
                logging.info(f"Normal process in CALL_PK ")
                return respond(err,res=query_results,step='CALL_PK');
            else:
                return respond(err,step='CALL_PK')
        #################################
        elif 'Input' in payload.keys():
            if 'UserId' in payload.keys():
                #TODO TEST CASE and FUNCTION
                Input= payload["Input"]
                if 'PK' in Input.keys():    
                    logging.info(f"Normal process in UPDATE_INFO ")
                    query_results,err=conn.update_information(Input,Table='TEST_CASE_0_build_info')
                    if err is None:
                        return respond(err,res=query_results,step='UPDATE_INFO');
                    else:
                        #TODO ERROR CASE
                        logging.info("long")
                else:
                    err=utils.err_("Input.PK should be required")
                    return respond(err,step='UPDATE_INFO')
            else:
                err=utils.err_("UserID should be required")
                return respond(err,step='UPDATE_INFO')
        else:
            logging.error(f"Method error : {event}")
            err=utils.err_("parameter err")
            return respond(err=err)
        #################################
        
def PUT(event,conn):
    ### PUT은 work 등의 데이터를 갱신용, 
    err=utils.err_("Not support method")
    return respond(err)
    

    
def common_response(event, operation, dynamoDB):
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
        err,result,message= GET(event,dynamoDB)
        logging.info(message)
        return respond(err,res=result)
            
    elif operations[operation] == 1:                  ### POST
        return POST(event,dynamoDB)
        
    elif operations[operation] == 2:
        return PUT(event, dynamoDB)
    else:
        logging.error(f"payload err : {event}")
        return 
    


def lambda_handler(event, context):
    logging.basicConfig(format='[%(asctime)s] %(message)s')

    aws_env      = os.environ['AWSENV']
    dev_env      = os.environ['DEVENV']
    region       = os.environ['REGION']
    table_name   = os.environ['TABLE']
    
    logging.info(f"env:{aws_env},{dev_env},")    
    dynamoDB=dynamo.dynamoApi(aws_env,dev_env,region,table_name)
    # print(type(event))
    # print(event)
    
    #### lambda apigateway payload 2.0 parsing
    if "version" in event.keys():
        if event["version"]=='2.0': 
            operation = event['requestContext']["http"]["method"]
        else:
            err=utils.err_("Not supported payload")
            logging.error(f"payload version : {event},{context}")
            return respond(err)
    else:
        operation = event['httpMethod']
    
    operations = {
        'GET':  0,
        'POST': 1, ### post medge
        'PUT':  2, ### PUT 
    }
   
    if operation in operations:
        try:
            payload = event['queryStringParameters'] if operation == 'GET' else event #['body'] ### 유의미한 데이터 받아오기 -> 바디 내부
            logging.info('[ Normal Method ] {operation} : {time.time}')
            return common_response(payload, operation,dynamoDB)
        except:
            err=utils.err_("payload err")
            logging.error(f"payload err : {event},{context}")
            return respond(err)
    else: ## 호출될 일이 없음 (이 함수는 POST에 매칭되어있음)
        err=utils.err_("unsupported method")
        return respond(err)

if __name__=="__main__":
    ## TODO lambda local testing 
    print("lambda local testing")
    
    aws_env      = os.environ['AWSENV']
    dev_env      = os.environ['DEVENV']
    region       = os.environ['REGION']
    table_name   = os.environ['TABLE']
    
    aws_env= "AWS_SAM",
    
    default_cord={
        'lat':36.4977,
        'lng':127.2067,
        'radius':1000 
    }
    
    lat = default_cord['lat']
    lng = default_cord['lng']
    radius = default_cord['radius']
    
    conn=dynamo.dynamoApi(aws_env,dev_env,region,table_name)
    r,_=conn.query_radius(lat,lng,radius,Table='TEST_CASE_0_build_info',Index='geohash-opt')
    print(r)
    ## local 이상무
    