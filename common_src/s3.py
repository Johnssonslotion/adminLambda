import json
import os
import boto3
import logging

from PIL import Image
from io import BytesIO
import base64



# ###
# import multipart
# from multipart.multipart import parse_options_header
# ###


class s3Api(object):
    '''
    s3를 통해서, 폴더명은 
    입력값 받아서 72p의 thumb생성,
    그 후
    - image/$PK/thumbnail/$uuid.png 
    - image/$PK/normal/$uuid.png
    
    1. 확장자 parsing
    2. 
    '''
    
    def __init__(self):
        logging.info(f"init : s3")
        self.cli=boto3.client('s3')
        self.rsc=boto3.resource('s3')
        
    def upload_image(self,local_file,s3_file):
        '''
        내부 함수 사용안함
        '''
        logging.info(f"upload_to_aws : {local_file},{s3_file}")
        err=None
        try:
            self.cli.upload_file(
                local_file,
                os.environ['BUCKET_NAME'], 
                s3_file
                )
            return s3_file, err
        except FileNotFoundError as e:
            logging.error("The file was not found")
            err=e
            return None,err 
        except Exception as e:
            logging.error(f"error : {e}")
            err=e
            return None,err
        
    def generate_url(self,s3_file,method='get'):
        '''
        업로드를 위한 url 생성
        
        '''
        logging.info(f"generating url from file :{s3_file}")
        url = self.cli.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': os.environ['BUCKET_NAME'],
                    'Key': s3_file
                },
                ExpiresIn=3600
            )
        logging.info(f"Upload Successful : {url}")
        return url
        
        
    def get_list_bucket(self,path):
        '''
        input 경로
        output Key값을 포함한 기타정보
        '''
        res=self.cli.list_objects(Bucket=os.environ['BUCKET_NAME'],Prefix=path)
        if res["ResponseMetadata"]['HTTPStatusCode']==200:
            logging.info("Normal process : list_bucket")
            if "Contents" in res.keys():
                logging.info(f'return items:{len(res["Contents"])}')
                index=len(res["Contents"])
                items=res["Contents"]
                #logging.info(f"list bucke itemsßßß:{items}")
            else:
                index=0
                items=[]
        return index,items
    def del_list_bucket(self,indexes):
        '''
        indexes should be checked
        '''
        err=None
        
        if len(indexes)==1:
            res=self.cli.delete_object(
                Bucket= os.environ['BUCKET_NAME'],
                Key=indexes[0]
            )
            if res['ResponseMetadata']['HTTPStatusCode']!=200:
                result=None
                err="abnormal connection"
            elif "Deleted" in res.keys():
                result=None
                err="not deleted"
            else:
                result=res['Deleted']
                err=None
            return result, err
            
        elif len(indexes)>=1:
            Keys=[]
            for i in indexes:
                Keys.append({'Key':i})
            logging.info(f"target string:{Keys}")
            res=self.cli.delete_objects(
                Bucket=os.environ['BUCKET_NAME'],
                Delete={'Objects':Keys}
            )
            if res['ResponseMetadata']['HTTPStatusCode']!=200:
                result=None 
                err="abnormal connection"
            elif "Deleted" in res.keys():
                result=None
                err="not deleted"
            else:
                result=res['Deleted']
                err=None
            return result, err
        else:
            ''' None items'''
            result =None
            err= "No validated request"
            return result, err
            

        
        
if __name__=="__main__":
    from dotenv import load_dotenv
    load_dotenv(verbose=True)
    ## root logger info level 로
    logger=logging.getLogger()
    logging.basicConfig(format='[%(asctime)s] %(message)s')
    logger.setLevel(logging.INFO)
    logging.info("s3 unit test")
    conn=s3Api()
    