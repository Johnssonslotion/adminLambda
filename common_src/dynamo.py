import os
import sys


sys.path.append(os.getcwd())

import logging
import time
import boto3
import pandas as pd
import dynamodbgeo

from botocore.exceptions import ClientError

### try -> lambda
### except -> local

try:
    from src import constant
    import apis
    
except:
    from common_src.src import constant
    from common_src import apis ### local condition
    
    
try:
    from ..tests.benckmark import TEST_HASHOPT, TEST_NORMALSCAN    
except:
    from tests.benckmark import TEST_HASHOPT, TEST_NORMALSCAN
    


class dynamoApi(object):
    def __init__(self, aws_env, dev_env, region_name,table_name, cli=None):
        self._aws_env= aws_env
        self._dev_env= dev_env
        self._region_name=region_name
        self._table_name=table_name
        
        '''
        ### 목적 : 
        ### T equip CRUD 및 공공 api 검색
        ### Lambda 특성 상 3000ms 넘으면안됨 -> 설정 바꾸면 됨
        '''
        
        print(time.ctime(time.time()))
        time_ck = time.time()
        if cli != None:
            self.client = cli
            
        else:
            
            self.resource, self.client = self.dynamoDB_setting()
            ####### for local Test
            try:
                if self._table_name in self.client.list_tables()["TableNames"]:
                    if len(self.resource.Table(self._table_name).scan()['Items'])==0:
                        logging.warning("No Items")
                        self.dynamoDB_dataInit()
                else:
                    self.dynamoDB_tableInit()
                    self.dynamoDB_dataInit()
            except Exception as e:
                print(e)
                logging.warning("No init")
                
        init_time=time_ck - time.time()
        logging.info(f"Init done : {init_time}")
        
        #self.targetDB=self.resource.Table(self._table_name)
        
        ##### DB settings ######
    def dynamoDB_setting(self):
        if self._aws_env == "AWS_SAM_LOCAL":
            if self._dev_env == "OSX":
                client= boto3.client('dynamodb',endpoint_url="http://docker.for.mac.localhost:8000/")
                resource=boto3.resource('dynamodb',endpoint_url="http://docker.for.mac.localhost:8000/")
            elif self._dev_env == "Windows":
                client=boto3.client('dynamodb',endpoint_url="http://docker.for.windows.localhost:8000/")
                resource=boto3.resource('dynamodb',endpoint_url="http://docker.for.windows.localhost:8000/")
            else: ## linux
                client=boto3.client('dynamodb',endpoint_url="http://127.0.0.1:8000/")
                resource=boto3.resource('dynamodb',endpoint_url="http://127.0.0.1:8000/")
        else:
            
            client = boto3.client(
                                        'dynamodb',region_name=self._region_name,
                                         aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                                         aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
                                        )
            resource=boto3.resource('dynamodb',
                                         region_name=self._region_name,
                                         aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                                         aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
                                         )
        return resource, client
    
                
    def dynamoDB_tableInit(self):
        '''
        내부 실험용 (SAM Docker용)
        '''
        try:
            self.resource.create_table(
                TableName='build_info',
                AttributeDefinitions=[
                    {
                        'AttributeName': 'PK',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'GEOHASH',
                        'AttributeType': 'S'
                    },    
                    {
                        'AttributeName': 'Nm',
                        'AttributeType': 'S'
                    },    
                    {
                        'AttributeName': 'LOCALCODE',
                        'AttributeType': 'S'
                    },    
                ],
                KeySchema=[
                    {
                        'AttributeName': 'PK',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'GEOHASH',
                        'KeyType': 'RANGE'
                    },
                ],
                GlobalSecondaryIndexes=[
                {
                    'IndexName': 'LocalBase',
                    'KeySchema': [
                        {
                            'AttributeName': 'LOCALCODE',
                            'KeyType': 'HASH'
                            
                        },
                        {
                            'AttributeName': 'Nm',
                            'KeyType': 'RANGE'
                        },
                    ],
                    'Projection': {
                        'ProjectionType': 'INCLUDE',
                        'NonKeyAttributes': [
                            'GEOHASH',
                            'PK',
                            'LAT',
                            'LNG',
                        ]
                    },
                },
                {
                    'IndexName': 'NameBase',
                    'KeySchema': [
                        {
                            'AttributeName': 'Nm',
                            'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': 'LOCALCODE',
                            'KeyType': 'RANGE'
                        },
                    ],
                    'Projection': {
                        'ProjectionType': 'INCLUDE',
                        'NonKeyAttributes': [
                            'GEOHASH',
                            'PK',
                            'LAT',
                            'LNG',
                        ]
                    },
                },
              ],
              TableClass='STANDARD',
              BillingMode='PAY_PER_REQUEST',
            )
        except ClientError as err:
            return err
            
        
    def dynamoDB_dataInit(self):
        ''' 
        기본 데이터 적제용
        '''
        df=pd.read_csv("src/default.csv")
        
        ## 스키마 확인
        
        
    
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
        ko_KR       = constant.ko_KR
        type_check  = constant.type_check 
        place_holder = constant.empty_check
        
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
        return status, results, message
    def create_item(self, items, basedb=False):
        status = 200
        message = None
        results = None
        if type(items) is list:
            logging.info(f"[System] : 입력된 아이템은 List 입니다. Items:{len(items)}")
            for i,item in enumerate(items):
                err=[]
                try:
                    logging.info(f"{i+1}번째 입력시도")
                    if basedb == False:
                        status, results, message = self.data_validation(item)
                        self.targetDB.put_item(item=results["done"])
                        logging.info(f"{i+1}번째 입력성공")
                    else:
                        self.targetDB.put_item(item=item)
                        logging.info(f"{i+1}번째 입력성공")
                    status = 400
                except :
                    err.append(i)
                    logging.warn(f"{i+1}번쨰 입력실패,[{message}]")
                    results = None
                    pass        
                results=err
            return status, results, message 
        else:
            logging.info("[System] : 입력된 아이템은 dict 입니다.")
            status, results, message = self.data_validation(item)
            e=self.targetDB.put_item(item=results["done"])
            return status, results, message   
    def geodynamodb(self,Table,Index=None,HASHKEY=None,Create_table=None):
        config = dynamodbgeo.GeoDataManagerConfiguration(self.client, Table)
        if Index != None:
            config.geohashIndexName=Index
        if HASHKEY is None:
            config.hashKeyLength = 6    
        config.hashKeyLength = HASHKEY 
        geoDataManager = dynamodbgeo.GeoDataManager(config)
        if Create_table is not None:
            table_util = dynamodbgeo.GeoTableUtil(config)
            create_table_input=table_util.getCreateTableRequest()
            create_table_input["ProvisionedThroughput"]['ReadCapacityUnits']=5
            table_util.create_table(create_table_input)
        return geoDataManager
    

    def single_input(self,s,geoDataManager):
    ## TODO : Input
    
        '''
        단일 입력값 넣기    
        
        '''
        _s=s
        item_dict={}
        err_0=False
        err_1=False
        err_2=False 

        res_geo,status=apis.vworld_address(_s['newPlatPlc'],road=True)
        if status=='status : no':
            err_0=True
            res_geo,status=apis.vworld_address(_s['platPlc'],road=False)
            if status=='status : no':
                err_1=True
                res_geo['lng']=None
                res_geo['lat']=None
                res_geo['crs']=None
        _pk=str(_s['mgmBldrgstPk'].replace("-",""))
        
        item_dict={
                "PK":                   {"S":str(_pk)},
                "mainPurpsCd":          {"S":_s['mainPurpsCd']},
                "mainPurpsCdNm":        {"S":_s['mainPurpsCdNm']},
                }
        ## 주소 무결성 체크 했나? 
        localcode=str(_s['sigunguCd']+_s['bjdongCd'])
        item_dict['LOCALCODE']={'S':localcode}
        
        target=constant.columns_target_dict
        for i in target.keys():
            if _s[i] is not None:
                item_dict[i]={'S':_s[i]}
            
        if res_geo['crs'] is not None:
            item_dict['CRS']={'S':res_geo['crs']}
        if res_geo['lat'] is not None:
            item_dict['lat']={'S':res_geo['lat']}
        if res_geo['lng'] is not None:
            item_dict['lng']={'S':res_geo['lng']}
        
        PutItemInput = {
                'Item':item_dict,
                'ConditionExpression': "attribute_not_exists(PK)"
                # ... Anything else to pass through to `putItem`, eg ConditionExpression
        }
        try:
            info=geoDataManager.put_Point(
            dynamodbgeo.PutPointInput(
                dynamodbgeo.GeoPoint(float(res_geo['lat']), float(res_geo['lng'])), # latitude then latitude longitude
                str(_pk),
                #str( uuid.uuid4()), # Use this to ensure uniqueness of the hash/range pairs.
                PutItemInput # pass the dict here
                ))
        except:
            err_2=True
            # print("------------ABNORMALCASE------------")
            # print(f"{item_dict}")
            # print(f"GEO INFO: {res_geo} / STATUS:{status}")
            
        result ={
            "PK"                  : _pk,
            "case_1_addr_failures": err_0,
            "case_2_addr_failures": err_1,
            "case_3_put_failures" : err_2,
            'tried_item'          : item_dict,
        }

        return result


        
    def abnormal_table_init(self,target):
        if target in self.client.list_tables()['TableNames']:
            info="done"
            logging.warn(f"Table [{target}] already exists. Skipping table creation.")
        else:
            info=self.client.create_table(
                        TableName=target,
                        AttributeDefinitions=[ 
                            {'AttributeName': 'PK','AttributeType': 'S'},
                            {'AttributeName': 'LOCALCODE','AttributeType': 'S'},
                        ],
                        KeySchema=[
                            {'AttributeName': 'PK','KeyType': 'HASH'},
                            {'AttributeName': 'LOCALCODE','KeyType': 'RANGE'},
                        ],
                        TableClass='STANDARD',
                        BillingMode='PAY_PER_REQUEST',
                    )
        return info
    
    def query_radius(self,Lat,Lng,radius,Table=None,Index=None,Hash=None):
        err=None
        if Hash is None:
            Hash = 5 ## default 값 설정
        if Index==None:
            if Table==None:
                geoDataManager=self.geodynamodb(Table=self._table_name,HASHKEY=Hash)
            else:
                geoDataManager=self.geodynamodb(Table=Table,HASHKEY=Hash)
        else:
            if Table==None:
                geoDataManager=self.geodynamodb(Table=self._table_name,Index=Index,HASHKEY=Hash)
            else:
                geoDataManager=self.geodynamodb(Table=Table,Index=Index,HASHKEY=Hash)
        start_time = time.time()
        try:
            query_reduis_result=geoDataManager.queryRadius(
            dynamodbgeo.QueryRadiusRequest(
                dynamodbgeo.GeoPoint(Lat, Lng), # center point
                radius,  sort = True)) # diameter

            logging.info(f"Qtime:{time.time()-start_time}")
        except ClientError as e:
            err=e
        return query_reduis_result, err
    
    def query_single_PK(self,PK):
        assert type(PK) != 'str','Type == STR'
        err={}
        try:
            query_single_results=self.client.query(
                TableName='TEST_CASE_0_build_info',
                IndexName='PK-index',   
                Select='ALL_ATTRIBUTES',
                KeyConditions = {'PK': {
                    'AttributeValueList':[
                    {
                        'S':PK,
                    },
                        ],
                    'ComparisonOperator': 'EQ'
                        }
                    }
            )
        except ClientError as e:
            err=e
        return query_single_results,err
    
    def string_query(string):
        # TODO : 검색을 위한 내용 추가
        '''
        Elastic search를 통한 검색기능 이전,
        '''
        
        return 0
    



if __name__ =="__main__":
    print("dynamo uni test")
    
    logging.basicConfig(format='[%(asctime)s] %(message)s')
    aws_env                     = os.environ['AWSENV']
    dev_env                     = os.environ['DEVENV']
    region                      = os.environ['REGION']
    table_name                  = os.environ['TABLE']
    aws_access_key_id           = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret_access_key       = os.environ['AWS_SECRET_ACCESS_KEY']
    
    logging.info(f"env:{aws_env},{dev_env},")
    
    '''
    UNIT TEST GEOHASH 별 속도 차이 테스트
    '''
    
    
    cli = boto3.client(
        'dynamodb', 
        region_name=region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    
    conn=dynamoApi(aws_env,dev_env,region,table_name,cli=cli)
    
    TEST_NORMALSCAN(conn)
    # TEST_HASHOPT(conn)
    
    
    
    
    
    
    
    #conn.client.close()
    ## benchmark for geohash opti by grid search
    
    
    
    