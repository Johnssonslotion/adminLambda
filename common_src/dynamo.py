import os
import sys
from time import strftime
import logging
logger=logging.getLogger("DYNAMODB")
logger.info("DYNAMODB - START")

import uuid

sys.path.append(os.getcwd())

import time
import boto3
import pandas as pd
import dynamodbgeo
import json

from botocore.exceptions import ClientError
from dynamodbgeo.s2 import S2Manager


### try -> lambda
### except -> local

try:
    from src import constant
    import apis,utils

except:
    from common_src.src import constant
    from common_src import apis,utils ### local condition
    
    
# try:
#     from ..tests.benckmark import TEST_HASHOPT, TEST_NORMALSCAN    
# except:
#     from tests.benckmark import TEST_HASHOPT, TEST_NORMALSCAN
    


class dynamoApi(object):
    def __init__(self, aws_env=None, dev_env=None, region_name=None,table_name=None, cli=None):
        
        if aws_env == None:
            self._aws_env= os.environ['AWSENV']
        else:    
            self._aws_env= aws_env
        
        if dev_env== None:        
            self._dev_env= os.environ['DEVENV']
        else:
            self._dev_env= dev_env
            
        if region_name==None:
            self._region_name=os.environ['REGION']
        else:
            self._region_name=region_name
        
        if table_name==None:
            self._table_name=os.environ['TABLE']
        else: 
            self._table_name=table_name
            
        '''
        ### 목적 : 
        ### T equip CRUD 및 공공 api 검색
        ### Lambda 특성 상 3000ms 넘으면안됨 -> 설정 바꾸면 됨
        '''
        
        logging.info("START DEBUG")
        time_ck = time.time()
        if cli != None:
            self.client = cli
            
        else:
            
            self.resource, self.client = self.dynamoDB_setting()
            ####### for local Test
            # try:
            #     if self._table_name in self.client.list_tables()["TableNames"]:
            #         if len(self.resource.Table(self._table_name).scan()['Items'])==0:
            #             logging.warning("No Items")
            #             self.dynamoDB_dataInit()
            #     else:
            #         self.dynamoDB_tableInit()
            #         self.dynamoDB_dataInit()
            # except Exception as e:
            #     print(e)
            #     logging.warning("No init")
                
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
                                        #  aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                                        #  aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
                                        )
            resource=boto3.resource('dynamodb',
                                         region_name=self._region_name,
                                        #  aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                                        #  aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
                                         )
        return resource, client
    
                
    def dynamoDB_tableInit(self):
        '''
        내부 실험용 (SAM Docker용)
        '''
        try:
            self.resource.create_table(
                TableName='build_info',
                AttributeDefinitions=[{
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
        place_holder = {i:None for i in constant.type_check.keys()}
        
        ######## EPSG:3857

        ######## 검증 프로토콜 : type check , mapping check ##########
        #### Key only #####
        ## null_input   =   dict{} ### 입력받지 않은 케이스들은 여기에 둔다.
        error_input     =   {} ### type 이 다른 케이스는 여기에 둔다.
        anomaly_input   =   {} ### 입력값에 해당되지 않는 경우는 여기에 둔다.
        done            =   {} ### 입력완료는 여기에 둔다.
        ######## 타입이 다른 경우를 제외하고 입력하기 ###########
        
        if "lat" in payload.keys():
            payload["lat"]=float(payload["lat"])
        if "lng" in payload.keys():
            payload["lng"]=float(payload["lng"])
        
        
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
     
    def geodynamodb(self,Table,Index=None,HASHKEY=None,Create_table=None):
        '''
        GEODYNAMODB에 대한 사전설정 GEOHASHING 및 GEOJSON 생성 및 관리 쪽은 이쪽에서
        
        '''
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
        단일 입력값 넣기, 초기값 입력
        
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
            item_dict['lat']={'S':res_geo['lng']}
        
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
        except Exception as e:
            print(e)
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
        '''
        lat, lng, radius 값을 받아서, 해당되는 범위를 검색해서 리턴
        '''
        print(Lat,Lng)
        query_redius_result=[]
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
            query_redius_result=geoDataManager.queryRadius(
            dynamodbgeo.QueryRadiusRequest(
                dynamodbgeo.GeoPoint(Lat, Lng), # center point
                radius,  sort = True)) # diameter

            logging.info(f"Qtime:{time.time()-start_time}")
        except ClientError as e:
            err=e
            print(e)
        return query_redius_result, err
    
    def query_single_PK(self,PK,TableName=None):
        ### 단일 쿼리 조회기능
        assert type(PK) != 'str','Type == STR'
        err=None
        query_single_results="Fail"
        if TableName == None:
            TableName = self._table_name
        try:
            query_single_results=self.client.query(
                TableName=TableName,
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
            print(e)
        return query_single_results,err
    
    def drop_history(self,PK,TableName=None):
        '''
        history 
        
        '''
        err=None
        result=None
        if TableName == None:
            TableName = self._table_name
            
        ### 요청 1. 기존 데이터 확인
        ### 쿼리 치고 partition key & sortkey 가져온 후 업데이트
        res=self.client.query(TableName=TableName,IndexName='PK-index',Select='ALL_PROJECTED_ATTRIBUTES',KeyConditions={'PK':{'AttributeValueList':[{'S':PK}],'ComparisonOperator': 'EQ'}})
        ### 정상이면, httpcode 200 -> count =1
        if res['ResponseMetadata']['HTTPStatusCode'] == 200 and res['Count']== 1:
            k1=res['Items'][0]['hashKey']
            k2=res['Items'][0]['rangeKey']
            Key={
                'hashKey':k1,
                'rangeKey':k2
            }     
            if 'Items' in res.keys():
                before_item=res["Items"]
            else:
                err=utils.err_(f"error noitem: ${res['ResponseMetadata']}")
                return result,err
        else:
            err=utils.err_(f"error connection: ${res['ResponseMetadata']}")
            return result,err
        
        ### 요청 2. 업데이트 데이터 확인
        updated_res=self.client.update_item(
                    TableName=TableName,
                    Key=Key,
                    UpdateExpression="SET #H=:D",
                    ExpressionAttributeNames={"#H": "HISTORY"},
                    ExpressionAttributeValues={":D":{"L": []}},
                    ReturnValues='UPDATED_NEW',
                )
        if updated_res['ResponseMetadata']["HTTPStatusCode"]==200:
            if "Attributes" in updated_res.keys():
                new_item=updated_res["Attributes"]
            else:
                err=utils.err_(f"error noitem: ${updated_res['ResponseMetadata']}")
                return result,err
        else:
            err=utils.err_(f"error connection: ${updated_res['ResponseMetadata']}")
            return result,err
        before_item[0].pop("HISTORY")
        before_item[0].pop("Duuid")
        result = {
            "PK"    :PK,
            "before":before_item,
            "result":new_item, 
        }
        return result, err
    
    def add_history(self,PK,Update,USER,TableName=None):
        '''
        Update 된 내용을 parsing
        Update에 들어가 있을 내용 : 
        
        '''
        logging.info("START:ADD_HISTORY")
        err=None
        result=None
        if TableName == None:
            TableName = self._table_name
        ### 요청 1. 기존 데이터 확인
        logging.info(f"INFO : Tablename : {TableName}")
        res=self.client.query(TableName=TableName,IndexName='PK-index',Select='ALL_PROJECTED_ATTRIBUTES',KeyConditions={'PK':{'AttributeValueList':[{'S':PK}],'ComparisonOperator': 'EQ'}})
        ### 정상이면, httpcode 200 -> count =1
        
        if res['ResponseMetadata']['HTTPStatusCode'] == 200 and res['Count']== 1:
            ### single c
            
            
            k1=res['Items'][0]['hashKey']
            k2=res['Items'][0]['rangeKey']
            Key={
                "hashKey":k1,
                "rangeKey":k2
            }
            #Key = json.dumps(Key,ensure_ascii=False)
            logging.info(f"KEY:{Key}")
            if 'Items' in res.keys():
                before_item=res["Items"]
            else:
                err=utils.err_(f"error noitem: ${res['ResponseMetadata']}")
                return result,err
        else:
            err=utils.err_(f"error connection: ${res['ResponseMetadata']}")
            return result,err
        
        if "HISTORY" in res["Items"][0].keys():
            if len(res["Items"][0]["HISTORY"]['L'])!=0:
                entryTime=json.loads(res['Items'][0]['HISTORY']['L'][-1]['S'])["updateTime"]["N"]
                if time.time()-entryTime < 60:
                    err=utils.err_(f"retry should be over 60s {strftime('%Y-%m-%d %I:%M:%S %p',time.localtime(entryTime))}")
                    return result,err
        else: ### 없을 떄 history 초기화
            logging.info("NO HISTORY : INIT")
            updated_res=self.client.update_item(
                    TableName=TableName,
                    Key=Key,
                    UpdateExpression="SET #H=:D",
                    ExpressionAttributeNames={"#H": "HISTORY"},
                    ExpressionAttributeValues={":D":{"L": []}},
                    ReturnValues='UPDATED_NEW',
                )
            logging.info(updated_res)
        
        ### 요청 2. 데이터 추가할 데이터셋 생성 
        
        if Update['lat'] == None or Update['lng'] == None :
            ### No update
            nGeohash=before_item[0]["geohash"]
            nGeojson=before_item[0]["geoJson"]
        else:
            nGeohash=S2Manager().generateGeohash(dynamodbgeo.GeoPoint(Update['lat'],Update['lng']))
            nGeojson=f"{Update['lat']},{Update['lng']}"
        
        if "HISTORY" in res['Items'][0].keys():
            index=len(res['Items'][0]['HISTORY']['L'])
        else:
            index=0
        
        update_time=time.time()
        update_user=USER
        update_UUID=str(uuid.uuid4())
        ###############
        Update.pop('lat')
        Update.pop('lng')
        
        placeholder={i:None for i in Update.keys()}
        
        for i in placeholder.keys():
            if constant.type_check[i]==int:
                placeholder[i] = {'N':Update[i]}
            elif constant.type_check[i]==bool:
                placeholder[i] = {'BOOL':Update[i]}
            else: ## str
                placeholder[i] = {'S':Update[i]}
        
        placeholder['index']        =  {"N":index}
        placeholder['uuid']         =  {"B":update_UUID}
        placeholder['updateTime']   =  {"N":update_time}
        placeholder['updateUser']   =  {"S":update_user}
        placeholder['nGeohash']     =  {"N":nGeohash}
        placeholder['nGeojson']     =  {"S":nGeojson}
        
        str_info=json.dumps(placeholder,ensure_ascii=False)
        ### 요청 3. 데이터 삽입
        # test=self.client.get_item(TableName=TableName,Key=Key)
        # logging.info(test)
        
        updated_res=self.client.update_item(
                    TableName=TableName,
                    Key=Key,
                    UpdateExpression="SET #H=list_append(:D,#H)",
                    ExpressionAttributeNames={"#H": "HISTORY"},
                    ExpressionAttributeValues={":D":{"L": [{"S":str_info}]}},
                    ReturnValues='UPDATED_NEW',
                )
        if updated_res['ResponseMetadata']["HTTPStatusCode"]==200:
            if "Attributes" in updated_res.keys():
                updated_item=updated_res["Attributes"]
            else:
                err=utils.err_(f"error no item: ${updated_res['ResponseMetadata']}")
                return result, err
        else:
            err=utils.err_(f"error connection: ${updated_res['ResponseMetadata']}")
            return result, err
        
        placeholder['updateUser']
        result={
            "PK"    :PK,
            "before":list(before_item[0].keys()),
            "result":{
                "n"   :len(updated_item["HISTORY"]['L']),
                "user":placeholder['updateUser']["S"],
                "time":strftime('%Y-%m-%d %I:%M:%S %p',time.localtime(placeholder['updateTime']['N'])),
            }
        }
        return result, err

    def apply_history(self,PK,NUM,USER,TableName=None):
        '''
        HISTORY를 적용함
        예외 사항 1. HISTORY 항목이 없을 경우 -> 초기화
        예외 사항 2. HISTORY 항목이 있으나, 내용이 없을 경우 -> 넘어가기
        예외 사항 3. HISTORY 항목이 있으나, 호출 index가 밖에 있을 떄,
        예외 사항 4. HISTORY 항목에 있고, 적용하는 내용이 동일할 경우 -> Duuid == uuid 일경우 update 취소
        
        업데이트 사항 : constant.py 에 정의된 값 포함 geojson, geohash, lat, lng, apply-timestamp apply-user 생성 후 값 추가 
        
        '''
        ### 
        logging.info("START:APPLY_HISTORY")
        NUM=int(NUM) 
        assert type(NUM)==int,"입력된 NUM은 상수여야함"
        err=None
        result=None
        if TableName == None:
            TableName = self._table_name
        ### 요청 1. 기존 데이터 확인
        logging.info(f"INFO : Tablename : {TableName}")
        res=self.client.query(TableName=TableName,IndexName='PK-index',Select='ALL_PROJECTED_ATTRIBUTES',KeyConditions={'PK':{'AttributeValueList':[{'S':PK}],'ComparisonOperator': 'EQ'}})
        ### 정상이면, httpcode 200 -> count =1
        
        if res['ResponseMetadata']['HTTPStatusCode'] == 200 and res['Count']== 1:
            ### single c
            k1=res['Items'][0]['hashKey']
            k2=res['Items'][0]['rangeKey']
            Key={
                "hashKey":k1,
                "rangeKey":k2
            }
            #Key = json.dumps(Key,ensure_ascii=False)
            logging.info(f"KEY:{Key}")
            if 'Items' in res.keys():
                before_item=res["Items"]
            else:
                err=utils.err_(f"error noitem: ${res['ResponseMetadata']}")
                return result,err
        else:
            err=utils.err_(f"error connection: ${res['ResponseMetadata']}")
            return result,err
        
        
        ## HISTORY 항목에 대한 예외처리 / 1. HISTORY가 없을때, 2. HISTORY가 있는데 아무것도 없을때, 3. HISTORY가 있으나, 
        
        if "HISTORY" not in res["Items"][0].keys():
            logging.info("NO HISTORY : INIT")
            updated_res=self.client.update_item(
                    TableName=TableName,
                    Key=Key,
                    UpdateExpression="SET #H=:D",
                    ExpressionAttributeNames={"#H": "HISTORY"},
                    ExpressionAttributeValues={":D":{"L": []}},
                    ReturnValues='UPDATED_NEW',
                )
            logging.info(updated_res)
            err=utils.err_("NO HISTORY: RETURN")
            return result,err
            
        elif len(res["Items"][0]["HISTORY"]['L'])== 0: ### 없을 떄 history 초기화
            logging.info("NO HISTORY ELEMENTS : RETURN")
            err=utils.err_("NO HISTORY ELEMENTS : RETURN")
            return result,err
        
        elif len(res["Items"][0]["HISTORY"]['L']) < NUM:
            logging.info("HISTORY Range OVER")
            err=utils.err_("HISTORY Range OVER")
            return result,err
            
        else:
            ## 필요값 생성
            str_info=res['Items'][0]['HISTORY']['L'][NUM]['S']
            info=json.loads(str_info)
            
            if "Duuid" in res["Items"][0].keys(): #중복 체크 일단 disable
                if res["Items"][0]["Duuid"]["B"] == info["uuid"]["B"]:
                    result={"status":"updated"}
                    return result,err
            
            applyTimestamp=str(time.time())
            applyUser=str(USER)
            
            exp_gen=0
            Names={}
            Values={}
            Exp_string="SET"
            
            ### Null 을 제외한 값 업데이트 constant에 정의된 형태 => 수정
            target_column=list(constant.type_check.keys())
            #target_column=[f"D{i}" for i in target_column] 
            target_column.append('uuid')
            try: 
                target_column.remove('lat') ##DB이전 후 아래로 옮기자
            except:
                target_column.remove('lat_orgin')
            try: 
                target_column.remove('lng')
            except:
                target_column.remove('lng_orgin')
            logging.info(f"column add: {target_column}")
            
            for i in target_column:
                if info[i]!=None:
                    if list(info[i].values())[0]!=None:
                        #### 왜 Type 이 지정된 None값이 들어가지? 버그 validation 항목 체크
                        
                        Names[f"#AT{exp_gen}"]=f"D{i}"
                        Values[f":V{exp_gen}"]=info[i]
                        Exp_string=f"{Exp_string} #AT{exp_gen}=:V{exp_gen},"
                        exp_gen+=1
                else:
                    continue
            ### 추가 데이터 삽입
            Names[f"#AT{exp_gen}"]="geoJson"
            try:
                Values[f":V{exp_gen}"]={"S":str(info["nGeojson"]["S"])}
                geojson=info["nGeojson"]["S"]
            except:
                logging.error(f'abnormal type: {PK},{info["nGeojson"]}')
                geojson=info["nGeojson"]["N"]
                Values[f":V{exp_gen}"]={"S":str(info["nGeojson"]["N"])}
            Exp_string=f"{Exp_string} #AT{exp_gen}=:V{exp_gen},"
            exp_gen+=1
            Names[f"#AT{exp_gen}"]="geohash"
            try:
                Values[f":V{exp_gen}"]={"N":str(info["nGeohash"]["S"])}
                
            except:
                logging.error(f'abnormal type: {PK},{info["nGeohash"]}')
                Values[f":V{exp_gen}"]={"N":str(info["nGeohash"]["N"])}
            Exp_string=f"{Exp_string} #AT{exp_gen}=:V{exp_gen},"
            exp_gen+=1
            Names[f"#AT{exp_gen}"]="DapplyTimestamp"
            Values[f":V{exp_gen}"]={"S":str(applyTimestamp)}
            Exp_string=f"{Exp_string} #AT{exp_gen}=:V{exp_gen},"
            exp_gen+=1
            Names[f"#AT{exp_gen}"]="DapplyUser"
            Values[f":V{exp_gen}"]={"S":applyUser}
            Exp_string=f"{Exp_string} #AT{exp_gen}=:V{exp_gen},"
            Exp_string=Exp_string[:len(Exp_string)-1]
            
            logging.info(f"S: {Exp_string}")   
            logging.info(f"N: {Names}")   
            logging.info(f"V: {Values}")   
            
            updated_res=self.client.update_item(
                    TableName=TableName,
                    Key=Key,
                    UpdateExpression=Exp_string,
                    ExpressionAttributeNames=Names,
                    ExpressionAttributeValues=Values,
                    ReturnValues='UPDATED_NEW',
                )
            logging.info(f"R:{updated_res}")                
        
        if updated_res['ResponseMetadata']["HTTPStatusCode"]==200:
            if "Attributes" in updated_res.keys():
                updated_item=updated_res["Attributes"]
                updated_item.pop('Duuid')
                logging.info(f"Attr:{updated_item}")
            else:
                err=utils.err_(f"error no item: ${updated_res['ResponseMetadata']}")
                return result, err
        else:
            err=utils.err_(f"error connection: ${updated_res['ResponseMetadata']}")
            return result, err
        
        result={
            "PK"    :PK,
            "result":{
                "update":updated_item,
            }
        }
        return result, err
    def clear_info(self,PK,USER,TableName=None):
        '''
        적용된 HITORY를 취소함
        예외 사항 1. 항목이 없을 경우 -> 있는 항목만
        
        업데이트 사항 : constant.py 에 정의된 값 포함 geojson, geohash, lat, lng, apply-timestamp apply-user 생성 후 값 추가 
        
        '''
        ### 
        logging.info("START:APPLY_HISTORY")
        # NUM=int(NUM) 
        # assert type(NUM)==int,"입력된 NUM은 상수여야함"
        err=None
        result=None
        if TableName == None:
            TableName = self._table_name
        ### 요청 1. 기존 데이터 확인
        logging.info(f"INFO : Tablename : {TableName}")
        res=self.client.query(TableName=TableName,IndexName='PK-index',Select='ALL_PROJECTED_ATTRIBUTES',KeyConditions={'PK':{'AttributeValueList':[{'S':PK}],'ComparisonOperator': 'EQ'}})
        ### 정상이면, httpcode 200 -> count =1
        
        if res['ResponseMetadata']['HTTPStatusCode'] == 200 and res['Count']== 1:
            ### single c
            k1=res['Items'][0]['hashKey']
            k2=res['Items'][0]['rangeKey']
            Key={
                "hashKey":k1,
                "rangeKey":k2
            }
            #Key = json.dumps(Key,ensure_ascii=False)
            logging.info(f"KEY:{Key}")
            if 'Items' in res.keys():
                before_item=res["Items"]
            else:
                err=utils.err_(f"error noitem: ${res['ResponseMetadata']}")
                return result,err
        else:
            err=utils.err_(f"error connection: ${res['ResponseMetadata']}")
            return result,err
        
        
        ## HISTORY 항목에 대한 예외처리 / 1. HISTORY가 없을때, 2. HISTORY가 있는데 아무것도 없을때, 3. HISTORY가 있으나, 
        
        if "HISTORY" not in res["Items"][0].keys():
            logging.info("NO HISTORY : INIT")
            updated_res=self.client.update_item(
                    TableName=TableName,
                    Key=Key,
                    UpdateExpression="SET #H=:D",
                    ExpressionAttributeNames={"#H": "HISTORY"},
                    ExpressionAttributeValues={":D":{"L": []}},
                    ReturnValues='UPDATED_NEW',
                )
            logging.info(updated_res)
            err=utils.err_("NO HISTORY: RETURN")
            return result,err
            
        elif len(res["Items"][0]["HISTORY"])== 0: ### 없을 떄 history 초기화
            logging.info("NO HISTORY ELEMENTS : RETURN") 
            err=utils.err_("NO HISTORY ELEMENTS : RETURN")
            return result,err
        
        # elif len(res["Items"][0]["HISTORY"]) < NUM:
        #     logging.info("HISTORY Range OVER")
        #     err=utils.err_("HISTORY Range OVER")
        #     return result,err
            
        else:
            ## 필요값 생성

            exp_gen=0
            Exp_string="REMOVE"
            
            target_column=list(constant.type_check.keys())
            try: 
                target_column.remove('lat') ##DB이전 후 아래로 옮기자
            except:
                target_column.remove('lat_orgin')
            try: 
                target_column.remove('lng')
            except:
                target_column.remove('lng_orgin')
                
            target_column.append('uuid')
            target_column=[f"D{i}" for i in target_column] 
            logging.info(f"column add: {target_column}")
            
            ### Null 을 제외한 값 업데이트 constant에 정의된 형태
            ###
            target_column=list(constant.type_check.keys())
            target_column=[f"D{i}" for i in target_column] 
            target_column.append('Duuid')
            logging.info(f"column add: {target_column}")
                        
            for i in target_column:
                if i in res["Items"][0].keys():
                        #### 왜 Type 이 지정된 None값이 들어가지? 버그 validation 항목 체크
                    Exp_string=f"{Exp_string} {i},"
                    exp_gen+=1
                else:
                    continue
            ### 추가 데이터 삽입
            
            if "DapplyTimestamp" in res["Items"][0].keys():
                Exp_string=f"{Exp_string} DapplyTimestamp,"    
            if "DapplyUser" in res["Items"][0].keys():
                Exp_string=f"{Exp_string} DapplyUser,"
            if Exp_string=="REMOVE":
                logging.info("NO APPLIED ELEMENTS : RETURN") 
                err=utils.err_("NO APPLIED ELEMENTS : RETURN")
                return result,err
            
            
            Exp_string=Exp_string[:len(Exp_string)-1]
            
            logging.info(f"S: {Exp_string}")            
            
            updated_res=self.client.update_item(
                    TableName=TableName,
                    Key=Key,
                    UpdateExpression=Exp_string,
                    ReturnValues='UPDATED_NEW',
                )
            logging.info(f"R:{updated_res}")                
        
        if updated_res['ResponseMetadata']["HTTPStatusCode"]==200:
            if "Attributes" in updated_res.keys():
                updated_item=updated_res["Attributes"]
                updated_item.pop('Duuid')
                logging.info(f"Attr:{updated_item}")
            else:
                updated_item=None
                err=None
                
        else:
            err=utils.err_(f"error connection: ${updated_res['ResponseMetadata']}")
            return result, err
        
        result={
            "PK"    :PK,
            "result":{
                "update":updated_item,
            }
        }
        return result, err
    
    
    
    def delete_history(self,PK,Update,TableName=None):
        return 0
        
    
    
    
        
        
    
    def add_management(self,PK,issue,USER,TableName=None):
        err=None
        result=None
        key=None
        
        if TableName==None:
            TableName=self._table_name
        res=self.client.query(
                TableName=TableName,
                IndexName='PK-index',   
                Select='ALL_ATTRIBUTES',
                KeyConditions = {'PK': {'AttributeValueList':[{'S':PK,},],'ComparisonOperator': 'EQ'}} 
        )
        if "ResponseMetadata" not in res.keys():
            logging.error("None connection")
            err="None connection"
            return result,err
        elif res["ResponseMetadata"]["HTTPStatusCode"]!=200:
            logging.error("HTTPCode connection error")
            err="HTTPCode connection error"
            return result,err
        elif "Items" in res.keys():
            Key={
                'hashKey':res["Items"][0]["hashKey"],
                'rangeKey':res["Items"][0]["rangeKey"],
            }
            ########
            
            Exp_string=""
            Names=""
            Values=""
            
            
            updated_res=self.client.update_item(
                    TableName=TableName,
                    Key=Key,
                    UpdateExpression=Exp_string,
                    ExpressionAttributeNames=Names,
                    ExpressionAttributeValues=Values,
                    ReturnValues='UPDATED_NEW',
                )
            logging.info(f"R:{updated_res}")                
            
                
                
                    
        
    
    
    
    def string_query(string):
        # TODO : 검색을 위한 내용 추가
        '''
        Elastic search를 통한 검색기능 이전,
        '''
        
        return 0
    



if __name__ =="__main__":
    from dotenv import load_dotenv
    load_dotenv(verbose=True)
    
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
    
    # TEST_NORMALSCAN(conn)
    # TEST_HASHOPT(conn)
    
    print(time.time())
    print(time.localtime())
    tm = time.localtime()
    print(strftime('%Y-%m-%d %I:%M:%S %p', tm))

    default_cord={
        'lat':36.4977,
        'lng':127.2067,
        'radius':1000 
    }
    
    a=conn.query_radius(Lat=default_cord['lat'],Lng=default_cord['lng'],radius=default_cord['radius'],Table='TEST_CASE_0_build_info',Index=None,Hash=5)
    

    PK='1234'
    
    
    ### check data_validation
    
    PK="361101000028326"
    
    
    
    
    
    
    
    

# T=conn.resource.Table("test_db")



# print(i)
#     #conn.client.close()
    
    
    
    
    