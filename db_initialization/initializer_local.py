import logging
import json
import os
import requests
import xmltodict
import pandas as pd
import datetime
import dynamodbgeo
import uuid
import boto3

#from src impot const



'''
외부에 노출되지 않은 내부 DB 개선용
결과물에 대한 저장은 
Amazon CloudWatch logs 에 따름,
'''


# def vworld_address(address,road=True):
#     addr= str(address)
#     e = "normal"
#     if road == True:
#       type_str="ROAD"
#     else:
#       type_str="PaRCEL"
#     urls="http://api.vworld.kr/req/address"

#     result_placeholder={
#         "crs": None,
#         "lat": None,
#         "lng": None,
#     }
#     params={
#       "service" : "address",
#       "version" : "2.0",
#       "request" : "GetCoord",
#       "key"     : os.environ["vworldKey"],
#       "format"  : "json",
#       "crs"     : "epsg:4326",
#       "type"    : type_str,
#       "address" : addr,
#       "refine"  : "false",
#       "crs"     : "EPSG:4326"
#     }
    
#     response_json=json.loads(requests.get(urls,params).text)
    
#     try:
#       if response_json['response']['status'] == "OK":
#         result_placeholder["crs"]=response_json['response']['result']["crs"]
#         result_placeholder["lat"]=response_json['response']['result']["point"]["y"]
#         result_placeholder["lng"]=response_json['response']['result']["point"]["x"]
#         e = ""
#         return result_placeholder, e
#       else:
#         e="status : no"
#         return result_placeholder, e
#     except:
#       e = "except"
#       return result_placeholder, e


# def call_api(urls,locate_code_1,locate_code_2,startDate=None,endDate=None,num=100,pageNo=1):
#   params ={
#     'serviceKey' : os.environ["govopenKey"],
#     'sigunguCd'  : locate_code_1,
#     'bjdongCd'   : locate_code_2,
#     'numOfRows'  : num,
#     'pageNo'     : f'{pageNo}' }
#   if startDate is not None:
#     params['startDate']= startDate,
#     params['endDate'] = endDate,
  
#   response=requests.get(urls,params=params)
#   if response.status_code!= 200:
#     return response.status_code
#   elif 'xml' in response.headers['content-Type']:
#     df=xmltodict.parse(response.text)
#     return df
#   elif 'json' in response.headers['content-Type']:
#     df = json.loads(response.text)
#     return df




def single_input(s,geoDataManager):
  ##
  _s=s
  item_dict={}
  err_0=False
  err_1=False
  err_2=False 

  res_geo,status=vworld_address(_s['newPlatPlc'],road=True)
  if status=='status : no':
    err_0=True
    res_geo,status=vworld_address(_s['platPlc'],road=False)
    if status=='status : no':
      err_1=True

      res_geo['lng']=None
      res_geo['lat']=None
      res_geo['crs']=None
  item_dict={
          "PK":                   {"S":_s['mgmBldrgstPk']},
          "mainPurpsCd":          {"S":_s['mainPurpsCd']},
          "mainPurpsCdNm":        {"S":_s['mainPurpsCdNm']},
          }
  ## 주소 무결성 체크 했나? 
  localcode=str(_s['sigunguCd']+_s['bjdongCd'])
  item_dict['LOCALCODE']={'S':localcode}
  
  if s["bldNm"] is not None:
    item_dict["bldNm"]={'S':_s['bldNm']}
  if s['splotNm'] is not None:
    item_dict['splotNm']={'S':_s['splotNm']}
  if s['dongNm'] is not None:
    item_dict['dongNm']={'S':_s['dongNm']}
  if s['crtnDay'] is not None:
    item_dict['crtnDay']={'S':_s['crtnDay']}
  if s['platPlc'] is not None:
    item_dict['platPlc']={'S':_s['platPlc']}
  if s['rnum'] is not None:
    item_dict['rnum']={'S':_s['rnum']}
  if s['sigunguCd'] is not None:
    item_dict['sigunguCd']={'S':_s['sigunguCd']}
  if s['bjdongCd'] is not None:
    item_dict['bjdongCd']={'S':_s['bjdongCd']}
  if s['platGbCd'] is not None:
    item_dict['platGbCd']={'S':_s['platGbCd']}
  if s['bun'] is not None:
    item_dict['bun']={'S':_s['bun']}
  if s['ji'] is not None:
    item_dict['ji']={'S':_s['ji']}
  if s['newPlatPlc'] is not None:
    item_dict['newPlatPlc']={'S':_s['newPlatPlc']}
  if s['naRoadCd'] is not None:
    item_dict['naRoadCd']={'S':_s['naRoadCd']}
  if s['naBjdongCd'] is not None:
    item_dict['naBjdongCd']={'S':_s['naBjdongCd']}
  if s['naMainBun'] is not None:
    item_dict['naMainBun']={'S':_s['naMainBun']}
  if s['naSubBun'] is not None:
    item_dict['naSubBun']={'S':_s['naSubBun']}
  if s['naUgrndCd'] is not None:
    item_dict['naUgrndCd']={'S':_s['naUgrndCd']}
  if s['block'] is not None:
    item_dict['block']={'S':_s['block']}
  if s['lot'] is not None:
    item_dict['lot']={'S':_s['lot']}
  if s['mainAtchGbCd'] is not None:
    item_dict['mainAtchGbCd']={'S':_s['mainAtchGbCd']}
  if s['mainAtchGbCdNm'] is not None:
    item_dict['mainAtchGbCdNm']={'S':_s['mainAtchGbCdNm']}
  if s['heit'] is not None:
    item_dict['heit']={'S':_s['heit']}
  if s['lot'] is not None:
    item_dict['lot']={'S':_s['lot']}
  if s['mainAtchGbCd'] is not None:
    item_dict['mainAtchGbCd']={'S':_s['mainAtchGbCd']}
  if s['mainAtchGbCdNm'] is not None:
    item_dict['mainAtchGbCdNm']={'S':_s['mainAtchGbCdNm']}
  if s['grndFlrCnt'] is not None:
    item_dict['grndFlrCnt']={'S':_s['grndFlrCnt']}
  if s['grndFlrCnt'] is not None:
    item_dict['ugrndFlrCnt']={'S':_s['ugrndFlrCnt']}


    
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
    geoDataManager.put_Point(
      dynamodbgeo.PutPointInput(
          dynamodbgeo.GeoPoint(float(res_geo['lat']), float(res_geo['lng'])), # latitude then latitude longitude
          str(_s['mgmBldrgstPk']),
          #str( uuid.uuid4()), # Use this to ensure uniqueness of the hash/range pairs.
          PutItemInput # pass the dict here
          ))
  except:
    err_2=True
    # print("------------ABNORMALCASE------------")
    # print(f"{item_dict}")
    # print(f"GEO INFO: {res_geo} / STATUS:{status}")
      
  result ={
      "PK"                  : s["mgmBldrgstPk"],
      "case_1_addr_failures": err_0,
      "case_2_addr_failures": err_1,
      "case_3_put_failures" : err_2,
      'tried_item'          : item_dict,
  }

  return result


def abnormal_table_init(target):
  if target in cli.list_tables()['TableNames']:
    info="done"
    logging.warn(f"Table [{target}] already exists. Skipping table creation.")
  else:
    info=cli.create_table(
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
                  ProvisionedThroughput={
                                          'ReadCapacityUnits': 2000,
                                          'WriteCapacityUnits': 2000,
                                      }
              )
  return info







########## 람다 이식시 Handler 로 변경 필요 -> 
# def lambda_handler(event, context):
if __name__=='__main__':
    url_target_1 = 'http://apis.data.go.kr/1613000/BldRgstService_v2/getBrBasisOulnInfo'
    url_target_2 = 'http://apis.data.go.kr/1613000/BldRgstService_v2/getBrTitleInfo'
    
    cli = boto3.client(
    'dynamodb', 
    region_name='ap-northeast-2',
    aws_access_key_id=accessKey,
    aws_secret_access_key=secretKey,
    #aws_session_token=SESSION_TOKEN
    )

    try: 
      local_code = pd.read_pickle('/opt/local_code.pickle')
    except:
      local_code = pd.read_pickle('src/local_code.pickle') ## for local

    geoDataManager          = geodynamodb(cli,'build_info')
    geoDataManager_case_1   = geodynamodb(cli,'build_info_review_nobldnm')
    geoDataManager_case_2   = geodynamodb(cli,'build_info_review_noNewAddr')
    abnormal_table_init('build_info_review_address_failure')

    ####################### 진도 찾아서 자동으로 재시작 #######################
    S=cli.scan(TableName='build_info_review_address_failure')
    last=sorted(list(set([i['S'] for i in pd.DataFrame.from_dict(S['Items'])['LOCALCODE']])))[-1]
    progress_index=local_code[(local_code['bjdongCd']==last[5:]) & (local_code['sigunguCd']==last[:5])].index

    for index,i in enumerate(local_code.iloc):
      if i['exist']=="폐지":
        continue;
      
      if index <= progress_index:
         continue;
      
      # if index == 5:
      #   break;
      target_dict=call_api(
        url_target_2,
        locate_code_1=i['sigunguCd'],
        locate_code_2=i['bjdongCd'],
        #startDate = datetime.today().strftime('%Y%m%d'),
        #endDate   = 22001231,
        num= 20000,
        )
      print(target_dict)
      try:
        print(f"index: {index}, {i['Nm']}, \t\t\t\t\t\t n of items:{target_dict['response']['body']['totalCount']} \t\t\t {i['exist']}")
      except:
        if 'OpenAPI_ServiceResponse' in target_dict.keys():
          continue
        target_dict=target_dict[0]
        print(f"index: {index}, {i['Nm']}, \t\t\t\t\t\t n of items:{target_dict['response']['body']['totalCount']} ")
        
        
      if target_dict['response']['header']['resultCode']=='00':
        ### Normal State 
        logging.info("Normal Service")
        if target_dict['response']['body']['totalCount']!='0':
        ###### Sorting Target #######
          try:
            TT=pd.DataFrame.from_dict(target_dict['response']['body']['items']['item'])
          except:
            TT=pd.DataFrame() ### list return 이 아니라 단일 dict 리턴시 에러발생
            TT=TT.append(target_dict['response']['body']["items"]["item"],ignore_index=True)
          TT=TT[constant.columns_target_dict.keys()] ## 1차 column 필터
          TT=TT[TT['mainPurpsCd'].isin(constant.mainPurpsMapping.keys())] ## 2차 경우 필터
          TT_CASE_0=TT[~TT['bldNm'].isna()]
          TT_CASE_1=TT[TT['bldNm'].isna()]                      ## abnormal case_1 ## 빌딩 이름 없는 케이스 SORTING
          TT_CASE_01=TT_CASE_0[TT_CASE_0['newPlatPlc'].isna()]  ## abnormal case_2 ## 새 도로명 주소가 없는 케이스
          TT_CASE_00=TT_CASE_0[~TT_CASE_0['newPlatPlc'].isna()] ## clean_case -> 제공될 데이터
          ##TT_CASE_00=TT_CASE_00.rename({'mgmBldrgstPk':'Pk'})   ## PK 값 변경, 입력단 로직에서 변경해서 들어감,
          err_import=[]
          for s in TT_CASE_00.iloc:
            #### 입력 Step
            s=s.to_dict()
            result=single_input(s,geoDataManager)
            if result["case_3_put_failures"]==True:
              err_import.append(result)
              logging.warn(f"{s['mgmBldrgstPk']} fail // fail_count: {len(err_import)}")
              #### abnormal case_3 db_input 필요

          for s in TT_CASE_1.iloc:
            s=s.to_dict()
            result=single_input(s,geoDataManager_case_1)
            if result["case_3_put_failures"]==True:
              err_import.append(result)
              logging.warn(f"{s['mgmBldrgstPk']} fail // fail_count: {len(err_import)}")
              #### abnormal case_3 db_input 필요
          
          for s in TT_CASE_01.iloc:
            s=s.to_dict()
            result=single_input(s,geoDataManager_case_2)
            if result["case_3_put_failures"]==True:
              err_import.append(result)
              logging.warn(f"{s['mgmBldrgstPk']} fail // fail_count: {len(err_import)}")
          

          if len(err_import) != 0:
            print(f"error_check : retry : {len(err_import)}")
          for f in err_import:
            
            try:
              cli.put_item(
                  TableName='build_info_review_address_failure',
                  Item=f['tried_item'],
                  ConditionExpression="attribute_not_exists(PK)",
              )
            except:
              continue
        else:
          logging.warn(f"{index}: No update {i['Nm']}")
          continue


    # T=pd.DataFrame.from_dict(target_dict[0]['response']['body']['items']['item'])
    
    
    
    
      
  
    
    
    
    
    
    
    
    
    
    
    