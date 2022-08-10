import json
import requests
import os
import xmltodict
import logging

def vworld_address(address,road=True):
    addr= str(address)
    e = "normal"
    if road == True:
      type_str="ROAD"
    else:
      type_str="PaRCEL"
    urls="http://api.vworld.kr/req/address"

    result_placeholder={
        "crs": None,
        "lat": None,
        "lng": None,
    }
    params={
      "service" : "address",
      "version" : "2.0",
      "request" : "GetCoord",
      "key"     : os.environ["vworldKey"],
      "format"  : "json",
      "crs"     : "epsg:4326",
      "type"    : type_str,
      "address" : addr,
      "refine"  : "false",
      "crs"     : "EPSG:4326"
    }
    
    response_json=json.loads(requests.get(urls,params).text)
    
    try:
      if response_json['response']['status'] == "OK":
        result_placeholder["crs"]=response_json['response']['result']["crs"]
        result_placeholder["lat"]=response_json['response']['result']["point"]["y"]
        result_placeholder["lng"]=response_json['response']['result']["point"]["x"]
        e = ""
        return result_placeholder, e
      else:
        e="status : no"
        return result_placeholder, e
    except:
      e = "except"
      return result_placeholder, e


def call_api(urls,locate_code_1,locate_code_2,startDate=None,endDate=None,num=100,pageNo=1):
  params ={
    'serviceKey' : os.environ["govopenKey"],
    'sigunguCd'  : locate_code_1,
    'bjdongCd'   : locate_code_2,
    'numOfRows'  : num,
    'pageNo'     : f'{pageNo}' }
  if startDate is not None:
    params['startDate']= startDate,
    params['endDate'] = endDate,
  
  response=requests.get(urls,params=params)
  if response.status_code!= 200:
    return response.status_code
  elif 'xml' in response.headers['content-Type']:
    df=xmltodict.parse(response.text)
    return df
  elif 'json' in response.headers['content-Type']:
    df = json.loads(response.text)
    return df

if __name__ =="__main__":
    logging.info("Single unit test for api connection")
    
    