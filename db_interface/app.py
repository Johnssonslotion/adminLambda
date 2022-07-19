import json
from xmlrpc.client import Boolean
import boto3
import DBconstants


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
        self.targetDB=self._client.table["toilet_infomation"]
        
    def adminchecknput(self, dict):
        '''
        python dict으로 변환된 입력값
        ### 무결성검사 -> 입력값에 대한 데이터 Type 검증
        ### unknown은 UI 상에서 구현하기 어려움(x) checkbox + switch 
        ### Rule 1. 장애인 노인 임산부 등의 편의증진 보장에 관 법률 시행규칙 
        ### Rule 2. 교통약자 이동편의 증진법 시행규칙
        ### 모든 변수는 nullable 로 넘어올 것,
        '''
        
        
        ### Github 배포를 위한 encapsulation
        ko_KR       = DBconstants.ko_KR
        type_check  = DBconstants.type_check 
        empty_check = DBconstants.empty_check

        
        ######## EPSG:3857
        ######## 검증 프로토콜 : type check , mapping check ##########
        null_input=[] ### 입력받지 않은 케이스들은 여기에 둔다.
        error_input=[] ### type 이 다른 케이스는 여기에 둔다.
        anomaly_input=[] ### 입력값에 해당되지 않는 경우는 여기에 둔다.
        ######## 타입이 다른 경우를 제외하고 입력하기 ###########
        
        for i in dict.keys:
            if i not in self.type_check:           #### 예외 케이스 1. 해당되지 않은 것을 입력받았을 떄
                anomaly_input.append[{i:dict[i]}]
                del dict[i]
            else:
                if type(dict[i])!=type_check[i]:
                    error_input.append[{i:dict[i]}] #### 예외 케이스 2. 타입이 다른 것을 입력 받았을 때,
                    del dict[i]
                else: 
                    ### 입력받지 않은 부분들에 대해서,
                    print("TODO")
                        
        
        
        ### TODO
        ### DB 호출 
        ### 추후에 DB 유저별 권한분리 필요 
        
        targetDB=self._client.table["toilet_infomation"].putitem(dict)
        message=f'111111'
        status = 200
        
        # "empty_dict"
        return status,message, 
        


def respond(err, res=None):
    return {
        'statusCode': '400' if err else '200',
        'body': err.message if err else json.dumps(res),
        'headers': {
            'Content-Type': 'application/json',
        },
    }


def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """
    c=tApi()
    c.targetDB
    
    operation = event['httpMethod']
    operations = {
        'GET':  0,
        'POST': 1,
        'PUT':  2,
    }
    
    
    print("Received event: " + json.dumps(event, indent=2))
    
    if operation in operations:
        payload = event['queryStringParameters'] if operation == 'GET' else json.loads(event['body']) ### 유의미한 데이터 받아오기
        if operations[operation] == 0:            ### GET
            res= c.targetDB.scan(payload),        
        elif operations[operation] == 1:          ### POST
            res= c.targetDB.scan(payload),
        else:
            status,message=c.adminchecknput(payload)         ### PUT
            if status == 400:
                respond(ValueError('Unsupported method "{} : {}"'.format(operation,message)),)
            else:
                res= message
        return respond(None, res)
    else:
        return respond(ValueError('Unsupported method "{}"'.format(operation)))
    
    


    