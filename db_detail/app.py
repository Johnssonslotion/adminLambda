import os
import sys
sys.path.append(os.getcwd())

import logging
import json

from decimal import Decimal
import time

try:
    import dynamo,apis,utils ### in layer 
except:
    from common_src import dynamo,apis,utils ### in local python code


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
    if res != None: 
        res["meta"]=meta
    return {
        'statusCode': '400' if err else '200',
        'body': {
            "items":'[]',
            "meta" :json.dumps(meta)
            } if err else json.dumps(res,ensure_ascii=False),
        'headers': {
            'Content-Type': 'application/json',
        },
    }
    
    
    
def common_action(conn,payload,debug=None):
    method=payload["method"]
    if method == "GET_INFO":
        '''
        PK에 대한 디테일 값 호출
        전부 호출 -> 일부 프로젝션으로 수정 
        Body에 method로 호출
        '''
        return get_info(conn,payload,debug=None)
    elif method == "GET_HISTORY":
        
        return get_history(conn,payload,debug=None)
    elif method == "DROP_HISTORY":
        '''
        이력 전체삭제
        필수 method / PK / num -> 권한문제는 lambda 앞단 auth단에서 해결
        '''
        return drop_history(conn,payload,debug=None)
    elif method == "SELECT_HISTORY":
        '''
        이력 선택
        필수 method / PK / update -> 권한문제는 lambda 앞단 auth단에서 해결
        
        '''
        return select_history(conn,payload,debug=None)
    elif method == "DELETE_HISTORY":
        '''
        이력 단일삭제
        필수 method / PK / num -> 권한문제는 lambda 앞단 auth단에서 해결
        '''
        return delete_history(conn,payload,debug=None)
    elif method == "UPDATE_ITEM":
        '''
        히스토리 업데이트 및 미반영
        UPDATE_ITEM -> SELECT_HISTORY를 통해서 적용
        '''
        return update_item(conn,payload,debug=None);
    
    
    
def get_info(conn,payload,debug=None):
    '''
    Test일 때 TestDB에서만 작동
    임의의 화장실 hardcoding
    '''
    if 'test' in payload.keys():
        TableName="_test_db"
        payload["PK"]="361101000028326"
    else:
        TableName=None
    if 'PK' in payload.keys():
        logging.info(f"Normal single search")
        query_results,err=conn.query_single_PK(payload["PK"],TableName=TableName)
        if "HISTORY" in query_results["Items"][0].keys():
            query_results["Items"][0].pop("HISTORY")
        if debug is not None:
            item=query_results
        else:
        ## TODO 
        ## candidate_key = target
        ## candidate_value = [query_results[i] if i in target for i in query_results]    
        ##
            item=query_results
        
        if err is None:
            #logging.info(f"Normal process in CALL_PK : ID:{info}")
            return respond(err,res=item,step='GET_INFO');
        else:
            return respond(err,step='GET_INFO')
    else:
        err=utils.err_("Need PK")
        return respond(err,step='GET_INFO')

def get_history(conn,payload,debug=None):
    '''
    Test일 때 TestDB에서만 작동
    임의의 화장실 hardcoding
    '''
    if 'test' in payload.keys():
        TableName="_test_db"
        payload["PK"]="361101000028326"
    else:
        TableName=None
    assert type(payload) == dict,"payload type err"
    if 'PK' in payload.keys():
        logging.info(f"Normal single search")
        query_results,err=conn.query_single_PK(payload["PK"],TableName=TableName)
        
        ### items 가 없을 경우 (검색 성공했으나, 리턴값 없을 떄)
        if "Items" in query_results.keys():
        ### items 가 단일 값인가? 
            if len(query_results['Items']) == 1:
            ### history가 등록되어있는가?
                if "history" in query_results['Items'][0].keys():
                    item=query_results["history"]
                else:
                    item=[]
                res={
                    "PK":payload["PK"],
                    "history":item
                }
            else:
                logging.error("Many Items : Request : {PK} ")
                err=utils.err_("Many Items : Request : {PK} ") 
        else: 
            err=utils.err_("Need : Request : {payload} ")
    else:
        err=utils.err_("Need PK")
    if err == None:
            #logging.info(f"Normal process in CALL_PK : ID:{info}")
        return respond(err,res=res,step='GET_HISTORY');
    else:
        return respond(err,step='GET_HISTORY')

    
def drop_history(conn,payload,debug=None):
    if 'test' in payload.keys():
        TableName="_test_db"
        payload["PK"]="361101000028326"
    else:
        TableName=None
    result,err=conn.drop_history(payload["PK"],TableName=TableName)
    return respond(err,res=result,step="DROP_HISTORY");

def update_item(conn,payload,debug=None):
    if 'test' in payload.keys():
        TableName="_test_db"
        payload["PK"]="361101000028326"
        payload["user"]="user00"
    else:
        TableName=None
    if "user" in payload.keys():
        if "PK" in payload.keys():
            if 'input' in payload.keys():
                check=payload["input"]
                if check == None:
                    err = utils.err_(f"input item error, {type(check)}")
                else: 
                    status, confirmed, message = conn.data_validation(check)
                    if status == 200:
                        target=confirmed['done']
                        target["user"]=payload["user"]
                        result,err=conn.add_history(payload["PK"],target,TableName=TableName)
                    else:
                        err = utils.err_(message)
            else:
                err = utils.err_("No item for update")
        else:
            err=utils.err_("Need PK")
    
    if err == None:
            #logging.info(f"Normal process in CALL_PK : ID:{info}")
        return respond(err,res=result,step='UPDATE_ITEM');
    else:
        return respond(err,step='UPDATE_ITEM')
    

def select_history(conn,payload,debug=None):
    return 0;

def delete_history(conn,payload,debug=None):
    return 0;

    

def lambda_handler(event, context):
    logging.basicConfig(format='[%(asctime)s] %(message)s')

    aws_env      = os.environ['AWSENV']
    dev_env      = os.environ['DEVENV']
    region       = os.environ['REGION']
    table_name   = os.environ['TABLE']
    
    logging.info(f"env:{aws_env},{dev_env},")    
    conn=dynamo.dynamoApi(aws_env,dev_env,region,table_name)
    
    ### validate payload 
    if "version" in event.keys():
        if event["version"]=='2.0': 
            operation = event['requestContext']["http"]["method"]
        else:
            err=utils.err_("Not supported payload")
            logging.error(f"payload version {event['version']}: {event},{context}")
            return respond(err)
    else:
        operation = event['httpMethod'] ## proxy
    
    if operation != "POST":
        err=utils.err_("abnormal method")
        return respond(err)
    
    
    ### parse payload
    payload,info,err=utils.base64_body_parser(event) ## base64로 parser 
    ### TODO User parsing and store
    if "debug" in event["headers"].keys():
        if event["headers"]["debug"]== True:
            DEBUG=True
        else:
            DEBUG=False
    else:
        DEBUG=False
    
    if "method" in payload.keys():
        return common_action(conn,payload,DEBUG)

    
def testcase_gen(method,pk,input,user=None):     
    event={}
    context=None
    headers={
        "debug":True
    }
    version="2.0"
    requestContext={
        "http":{"method":"POST"}
    }
    
    body={
        "method":method,
        "PK":pk,
        "input":input,
        "test":True,
        "user":user
    }
    #str=base64.b64encode(json.dumps(body).encode('utf-8'))
    event["headers"]=headers
    event["body"]=body
    event["version"]=version
    event["requestContext"]=requestContext
    event["isBase64Encoded"]=False
    return event,context
    
    
    
    
    
if __name__ == "__main__":
    event,context=testcase_gen(method="GET_INFO",pk="1234",input=None)
    case1=lambda_handler(event, context)
    print("GETINFO:/t/t/t/t/t",case1)
    del case1
    event,context=testcase_gen(method="GET_HISTORY",pk="1234",input=None)
    case2=lambda_handler(event, context)
    print("GETHISTORY:/t/t/t/t/t",case2)
    del case2
    event,context=testcase_gen(method="DROP_HISTORY",pk="1234",input=None)
    case3=lambda_handler(event, context)
    print("DROPHISTORY:/t/t/t/t/t",case3)
    del case3
    
    
    input= {
        "lat"        : '34.444',
        "lng"        : '123.444',
        "toiletType" : 'N',   ###"화장실타입", 'G 범용화장실 | S 장애인전용 | 
        "entryLoc"   : False,
        "entryFind"  : False,
        "entryThold" : False,
        "entryDotB"  : False,
        "entryDotS"  : False,
        "entryAuto"  : False,
        "entrySpace" : False,
        "maleFamale" : False,
        "floarMatr"  : False,
        "washExist"  : False,
        "washMalF"   : False,
        "washEasy"   : False,
        "handRailEX" : False,
        "handRailMF" : False,
        "childKeepEX": False,
        "childKeepMF": False,
        "backKeepEX": False,
        "backKeepMF": False,
        "trashBin": False,
        "emerCall": False,
        "cleanChartEx": False,
        "cleanChartMg": False,
        "cleanToolLf": False,
        "noclean": False,
        "noSupplies": False
        }
    
    
    event,context=testcase_gen(method="UPDATE_ITEM",pk="1234",input=input, user="user01")
    case4=lambda_handler(event, context)
    print("UPDATEITEM:/t/t/t/t/t",case4)
    del case4
    event,context=testcase_gen(method="GET_HISTORY",pk="1234",input=None)
    case2=lambda_handler(event, context)
    print("GET_HISTORY:/t/t/t/t/t",case2)
    event,context=testcase_gen(method="UPDATE_ITEM",pk="1234",input=input, user="user01")
    case4=lambda_handler(event, context)
    print("UPDATE_ITEM:/t/t/t/t/t",case4)
    event,context=testcase_gen(method="GET_HISTORY",pk="1234",input=None)
    case2=lambda_handler(event, context)
    print("GET_HISTORY:/t/t/t/t/t",case2)
    event,context=testcase_gen(method="DROP_HISTORY",pk="1234",input=input, user="user01")
    case4=lambda_handler(event, context)
    print("DROP_HISTORY:/t/t/t/t/t",case4)
    del case4
    
    
    

    
    
    