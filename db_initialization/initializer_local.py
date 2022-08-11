import os
import sys
sys.path.append(os.getcwd())


import logging
import pandas as pd

from common_src import dynamo,apis
from common_src.src import constant
import boto3


'''
외부에 노출되지 않은 내부 DB 개선용
결과물에 대한 저장은 
Amazon CloudWatch logs 에 따름,
'''



def INPUT_DB(confirmed_df,conn,DBSET):
  '''
  API 호출 이후 정리
  '''
      
      
  target_dict=confirmed_df
  assert len(DBSET) == 4, "DB는 4개 이상"
  geoDataManager_case_0=DBSET[0]
  geoDataManager_case_1=DBSET[1]
  geoDataManager_case_2=DBSET[2]
  geoDataManager_case_3=DBSET[3]
  
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
  
  ##입력에 대한 플레이스 홀더
  err_import=[]
  for s in TT_CASE_00.iloc:
    #### 입력 Step
    s=s.to_dict()
    result=conn.single_input(s,geoDataManager_case_0)
    if result["case_3_put_failures"]==True:
      err_import.append(result)
      _a=s['mgmBldrgstPk']
      _b=len(err_import)
      _c=f"{_a} fail // fail_count: {_b}"
      logging.warning(_c)
      #### abnormal case_3 db_input 필요

  for s in TT_CASE_1.iloc:
    s=s.to_dict()
    result=conn.single_input(s,geoDataManager_case_1)
    if result["case_3_put_failures"]==True:
      err_import.append(result)
      _a=s['mgmBldrgstPk']
      _b=len(err_import)
      _c=f"{_a} fail // fail_count: {_b}"
      logging.warning(_c)
      #### abnormal case_3 db_input 필요
  
  for s in TT_CASE_01.iloc:
    s=s.to_dict()
    result=conn.single_input(s,geoDataManager_case_2)
    if result["case_3_put_failures"]==True:
      err_import.append(result)
      _a=s['mgmBldrgstPk']
      _b=len(err_import)
      _c=f"{_a} fail // fail_count: {_b}"
      logging.warning(_c)
      
  
  if len(err_import) != 0:
    print(f"error_check : retry : {len(err_import)}")
  for f in err_import:
    try:
      conn.client.put_item(
          TableName=geoDataManager_case_3,
          Item=f['tried_item'],
          ConditionExpression="attribute_not_exists(PK)",
      )
    except:
      continue

    
    




########## 람다 이식시 Handler 로 변경 필요 -> 
# def lambda_handler(event, context):
if __name__=='__main__':
    
    url_target_1 = 'http://apis.data.go.kr/1613000/BldRgstService_v2/getBrBasisOulnInfo'
    url_target_2 = 'http://apis.data.go.kr/1613000/BldRgstService_v2/getBrTitleInfo'
    
    # print(constant.columns_target_dict)
    
    logging.info("INITIALIZE START")

    aws_env                     = os.environ['AWSENV']
    dev_env                     = os.environ['DEVENV']
    region                      = os.environ['REGION']
    table_name                  = os.environ['TABLE']
    aws_access_key_id           = os.environ['AWSACCESSKEY']
    aws_secret_access_key       = os.environ['AWSSECRETKEY']

    
    cli = boto3.client(
    'dynamodb', 
    region_name='ap-northeast-2',
    aws_access_key_id=os.environ["AWSACCESSKEY"],
    aws_secret_access_key=os.environ["AWSSECRETKEY"]
    )

    local_path = os.path.join(os.getcwd(),'common_src/src/local_code.pickle')

    try: 
      local_code = pd.read_pickle('/opt/local_code.pickle')
    except:
      local_code = pd.read_pickle(local_path) ## for local

    
    
    conn=dynamo.dynamoApi(aws_env,dev_env,region,table_name,cli=cli)


    geoDataManager_case_00   = conn.geodynamodb('_build_info',HASHKEY=7)
    geoDataManager_case_01   = conn.geodynamodb('_build_info_review_nobldnm',HASHKEY=7)
    geoDataManager_case_02   = conn.geodynamodb('_build_info_review_noNewAddr',HASHKEY=7)
    # geoDataManager_case_10   = conn.geodynamodb('TEST_CASE_1_build_info',7)
    # geoDataManager_case_11   = conn.geodynamodb('TEST_CASE_1_build_info_review_nobldnm',7)
    # geoDataManager_case_12   = conn.geodynamodb('TEST_CASE_1_build_info_review_noNewAddr',7)
    # geoDataManager_case_20   = conn.geodynamodb('TEST_CASE_2_build_info',9)
    # geoDataManager_case_21   = conn.geodynamodb('TEST_CASE_2_build_info_review_nobldnm',9)
    # geoDataManager_case_22   = conn.geodynamodb('TEST_CASE_2_build_info_review_noNewAddr',9)
    conn.abnormal_table_init('_build_info_review_address_failure')
    # conn.abnormal_table_init('TEST_CASE_1_build_info_review_address_failure')
    # conn.abnormal_table_init('TEST_CASE_2_build_info_review_address_failure')
    
    
    ###############################################################
    
    
    DBSET_0=[]
    DBSET_0.append(geoDataManager_case_00)
    DBSET_0.append(geoDataManager_case_01)
    DBSET_0.append(geoDataManager_case_02)
    DBSET_0.append('_0_build_info_review_address_failure')
    
    
    # DBSET_1=[]
    # DBSET_1.append(geoDataManager_case_00)
    # DBSET_1.append(geoDataManager_case_01)
    # DBSET_1.append(geoDataManager_case_02)
    # DBSET_1.append('_0_build_info_review_address_failure')

    
    # DBSET_2=[]
    # DBSET_2.append(geoDataManager_case_10)
    # DBSET_2.append(geoDataManager_case_11)
    # DBSET_2.append(geoDataManager_case_12)
    # DBSET_2.append('TEST_CASE_1_build_info_review_address_failure')
    
    # DBSET_3=[]
    # DBSET_3.append(geoDataManager_case_20)
    # DBSET_3.append(geoDataManager_case_21)
    # DBSET_3.append(geoDataManager_case_22)
    # DBSET_3.append('TEST_CASE_3_build_info_review_address_failure')


    ####################### 진도 찾아서 자동으로 재시작 #######################
    
    # S=cli.scan(TableName='TEST_build_info_review_address_failure')
    # last=sorted(list(set([i['S'] for i in pd.DataFrame.from_dict(S['Items'])['LOCALCODE']])))[-1]
    # progress_index=local_code[(local_code['bjdongCd']==last[5:]) & (local_code['sigunguCd']==last[:5])].index

    # print(local_code.head())
    # print(local_code[local_code["Nm"].str.contains("세종")])


    for index,i in enumerate(local_code.iloc):
      if i['exist']=="폐지":
            continue
      # if index <= 4316: ## 세종시 시작
      #       continue
      # if index == 4475: ## 경기도 시작 
      #       break
    #   # if index == 5:
    #   #   break;
      #print(f"{index} : {i['Nm']} {i['exist']}")
      target_dict=apis.call_api(
        url_target_2,
        locate_code_1=i['sigunguCd'],
        locate_code_2=i['bjdongCd'],
        #startDate = datetime.today().strftime('%Y%m%d'),
        #endDate   = 22001231,
        num= 20000,
        )
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
          INPUT_DB(target_dict,conn,DBSET_0)
          # INPUT_DB(target_dict,conn,DBSET_2)
          # INPUT_DB(target_dict,conn,DBSET_3)
        else:
            _a=i['Nm']
            logging.warning(f"{index}: No update {_a}")
          


    # # T=pd.DataFrame.from_dict(target_dict[0]['response']['body']['items']['item'])
    
    
    
    
      
  
    
    
    
    
    
    
    
    
    
    
