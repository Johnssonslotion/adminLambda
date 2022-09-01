import os
import boto3
import logging

class s3(object):
    '''
    s3를 통해서, 폴더명은 
    입력값 받아서 72p의 thumb생성,
    그 후
    - image/$PK/thumbnail/image_$index.$ext 
    - image/$PK/normal/image_$index.$ext
    
    1. 확장자 parsing
    2. 
    '''
    
    def __init__(self):
        logging.info(f"init : s3")
        self.cli=boto3.client('s3')
        self.rsc=boto3.resource('s3')
        
    def upload_image(self,local_file, s3_file):
        '''
        내부 함수 사용안함
        
        '''
        logging.info(f"upload_to_aws : {local_file},{s3_file}")
        err=None
        try:
            self.cli.upload_file(local_file, os.environ['BUCKET_NAME'], s3_file)
            return s3_file, err
        except FileNotFoundError as e:
            logging.error("The file was not found")
            err=e.message
            return None,err 
        except Exception as e:
            logging.error(f"error : {e}")
            err=e.message
            return None,err
        
    def generate_url(self,s3_file,method='get'):
        '''
        업로드를 위한 url 생성
        
        '''
        logging.info(f"generating url from file :{s3_file}")
        url = self.cli.generate_presigned_url(
                ClientMethod='get_object' if method == 'get' else 'put_object',
                Params={
                    'Bucket': os.environ['BUCKET_NAME'],
                    'Key': s3_file
                },
                ExpiresIn=12 * 3600
            )
        logging.info(f"Upload Successful : {url}")
        return url
        
        
    def get_list_bucket(self,PK):
        res=self.cli.list_objects(Bucket=os.environ['BUCKET_NAME'],Prefix=f"{PK}/main/")
        if res["ResponseMetadata"]['HTTPStatusCode']==200:
            logging.info("Normal process : list_bucket")
            if "Contents" in res.keys():
                logging.info(f'return items:{len(res["Contents"])}')
                index=len(res["Contents"])
            else:
                index=1
        return 
        
if __name__=="__main__":
    from dotenv import load_dotenv
    load_dotenv(verbose=True)
    ## root logger info level 로
    logger=logging.getLogger()
    logging.basicConfig(format='[%(asctime)s] %(message)s')
    logger.setLevel(logging.INFO)
    logging.info("s3 unit test")
    conn=s3()
    res=conn.upload_to_aws("./test_image")
    
    ### s3 naming rule
    
    