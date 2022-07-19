
import json
from logging import PlaceHolder
import boto3
import DBconstants
import os
from decimal import Decimal

# import requests

class tApi(object):
    def __init__(self, *args):
        '''
        ### 목적 : 
        ### T equip CRUD 및 공공 api 검색
        ### Lambda 특성 상 3000ms 넘으면안됨
        '''
        
        super(tApi, self).__init__(*args)
        ###### DB settings ######
        self._client=boto3.resource('dynamodb')
        self.targetDB=self._client.Table("toilet_infomation")
        
        
    
    def search_query(self, payload):
        
        
        
        
        
        return status, message, respose
    
    def data_validation(self, payload):
        '''
        python dict으로 변환된 입력값
        ### 무결성검사 -> 입력값에 대한 데이터 Type 검증
        ### unknown은 UI 상에서 구현하기 어려움(x) checkbox + switch 
        ### Rule 1. 장애인 노인 임산부 등의 편의증진 보장에 관 법률 시행규칙 
        ### Rule 2. 교통약자 이동편의 증진법 시행규칙
        ### 모든 변수는 nullable 로 넘어올 것,
        '''
        
        ### Github 배포를 위한 encapsulation
        #payload_holder = copy.deepcopy(payload)
        ko_KR       = DBconstants.ko_KR
        type_check  = DBconstants.type_check 
        place_holder = DBconstants.empty_check
        
        ######## EPSG:3857

        ######## 검증 프로토콜 : type check , mapping check ##########
        #### Key only #####
        ## null_input   =   dict{} ### 입력받지 않은 케이스들은 여기에 둔다.
        error_input     =   {} ### type 이 다른 케이스는 여기에 둔다.
        anomaly_input   =   {} ### 입력값에 해당되지 않는 경우는 여기에 둔다.
        done            =   {} ### 입력완료는 여기에 둔다.
        ######## 타입이 다른 경우를 제외하고 입력하기 ###########
        
        for i in payload.keys():                    
            if i not in type_check.keys():                       #### 예외 케이스 1. 해당되지 않은 것을 입력받았을 떄
                anomaly_input[i]=payload[i]
            elif type(payload[i])!=type_check[i]:
                error_input[i]=payload[i]                        #### 예외 케이스 2. 타입이 다른 것을 입력 받았을 때,
            else: 
                done[i]=payload[i]                               #### 필터링 완료
                                                                 #### 입력받지 않은 부분들에 대해서,
        null_input_keys=set(payload.keys())-set(done.keys())     #### 예외 케이스 3. Null 값 처리여부    
        null_input=dict.fromkeys(null_input_keys,None) 
        
        
        string_notupdated=",".join([str(i) for i in null_input_keys])
        string_notupdated=f'[{string_notupdated}]'
        string_updated   =",".join([ko_KR[str(i)] for i in done.keys()])
        string_updated   =f'[{string_updated}]'
        
        for i in done.keys():
            place_holder[i] = done[i]   
        
        ### TODO
        ### DB 호출 
        ### 추후에 DB 유저별 권한분리 필요 
        
        if done is None:
            status = 400
            results={
            "done"          : None,
            "type_error"    : error_input,
            "anomaly_error" : anomaly_input,
            "null_error"    : null_input,
            }
        else:
            status = 200
            results={
            "done"          : place_holder,
            "type_error"    : error_input,
            "anomaly_error" : anomaly_input,
            "null_error"    : null_input,
            }
        
        
        #print(result)
        message=f'valid : {string_updated}, invalid : {string_notupdated}'
        print(message)
        return status,results,message
        


def respond(err, res=None):
    return {
        'statusCode': '400' if err else '200',
        'body': err.message if err else json.dumps(res),
        'headers': {
            'Content-Type': 'application/json',
        },
    }

def lambda_handler(event, context):
    
    
    c=tApi()
    operation = event['httpMethod']
    
    operations = {
        'GET':  0,
        'POST': 2, ### post medge
        'PUT':  2,
    }
    
    #print("Received event: " + json.dumps(event, indent=2))
    
    if operation in operations:
        payload = event['queryStringParameters'] if operation == 'GET' else event['body'] ### 유의미한 데이터 받아오기
        if operations[operation] == 0:                    ### GET
            res= c.targetDB.scan(),        
        elif operations[operation] == 1:                  ### POST
            pass
        elif operations[operation] == 2:
            status,result,message = c.data_validation(payload)          ### PUT
            if status == 400:
                respond(ValueError('Parameter fail " : {}"'.format(message)),)
            else:
                item=result["done"]
                item = json.loads(json.dumps(item), parse_float=Decimal)
                try:
                    res= c.targetDB.put_item(Item=item)
                except:
                    respond(ValueError('Parameter fail " : {}"'.format(message)),)
        return respond(None, res)
    else:
        return respond(ValueError('Unsupported method "{}"'.format(operation)))
    