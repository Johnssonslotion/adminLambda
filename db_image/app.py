import logging
import os
import json
import uuid
from PIL import Image



import apis,utils,s3 ### in layer 
    
# except:
#     from common_src import dynamo,apis,utils,s3 ### in local python code


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
    if err == None:
        res["meta"]=meta
    
    return {
        'statusCode': '400' if err else '200',
        'body': json.dumps({"items":'[]',"meta" :meta},ensure_ascii=False) if err else json.dumps(res,ensure_ascii=False),
        'headers': {
            'Content-Type': 'application/json',
        },
    }

def get_images(conn_s3,payload):
    items=[]
    urls=[]
    err=None
    PK=payload['PK']
    items=[]
    index,get_items=conn_s3.get_list_bucket(f"image/{PK}/normal")
    #logging.info(f"get_items:{get_items}")
    if index==0:
        logging.error("error image get")
        err=utils.err_("get images fail")
        return respond(err)
    else:
        for i in get_items:
            logging.info(f"try get url {i}")
            items.append(i['Key'])
            urls.append(conn_s3.generate_url(i['Key']))
    
    logging.info(f'final check:{err}')
    results= {
        "PK":payload['PK'],
        "item":items,
        "urls":urls,
    }
    
    return respond(err,res=results,step="get_images") 

def get_thumbs(conn_s3,payload):
    items=[]
    urls=[]
    err=None
    PK=payload['PK']
    index,get_items=conn_s3.get_list_bucket(f"image/{PK}/thumbs")
    #logging.info(f"get_items:{get_items}")
    if index==0:
        logging.error("error image get")
        err=utils.err_("get images fail")
        return respond(err)
    else:
        for i in get_items:
            logging.info(f"try get url {i}")
            items.append(i['Key'])
            urls.append(conn_s3.generate_url(i['Key']))
    
    logging.info(f'final check:{err}')
    results= {
        "PK":payload['PK'],
        "item":get_items,
        "urls":urls,
    }
    
    return respond(err,res=results,step="get_thumbs") 
    
    
def upload_images(conn_s3,payload):
    items=[]
    urls=[]
    target_image=[i for i in os.listdir("/tmp/") if i.startswith("image")]
    if len(target_image) == 0:
        logging.error('No "image" in payload ')
        err=utils.err_('No "image" in payload ')
        return respond(err, step="upload_image")
    else:
        for i in target_image:
            ## upload할 url
            
            PK=payload["PK"]
            id=str(uuid.uuid4())
            path_thum=f"image/{PK}/thumbnail/{id}.png"
            path_norm=f"image/{PK}/normal/{id}.png"
            logging.info(f"upload step: target:{i}, path:{path_norm}")
            ###
            
            img=Image.open(f"/tmp/{i}")
            
            width,height=img.size
            logging.info(f"upload step: open imagefile, size:{width},{height}")
            ### 1440x1080 으로 맞춤
            ### 사용성 측면에서 추후 보정
            
            if width > height:
                logging.info(f"upload step: horizental")
                ratio=1080/height
                new=(int(width*ratio),1080)
                img_norm=img.resize(new)
                width_target=(width*ratio-1440)/2
                img_norm=img_norm.crop((width_target,0,1440,1080))
            else:
                logging.info(f"upload step: vertical")
                ratio=1440/width
                new=(1440,int(height*ratio))
                img_norm=img.resize(new)
                height_target=int((height*ratio-1080)/2)
                img_norm=img_norm.crop((0,height_target,1440,1080))
            
            #######
            img_norm.save('/tmp/norm.png')
            logging.info(f"save norm size:{img_norm.size[0]},{img_norm.size[1]}")
            
            ######## thumbnail gen
            ratio=72/img_norm.size[0]
            resize_target=int(72*(img_norm.size[0]/img_norm.size[1])),72
            img_thumb=img_norm.resize(resize_target)
            img_thumb.save('/tmp/thumb.png')
            logging.info("save thumb size:{img_thumb.size[0]},{img_thumb.size[1]}")
            img_thumb_path='/tmp/thumb.png'
            img_norm_path='/tmp/norm.png'
            
            res_1,err=conn_s3.upload_image(img_thumb_path,path_thum)
            if res_1 == None:
                logging.error(err)
            res_1,err=conn_s3.upload_image(img_norm_path,path_norm)
            if res_1 == None:
                logging.error(err)
            
            items.append(path_thum)
            items.append(path_norm)
            
            logging.info(f"items:{items}")
        
        current_items=[]
        logging.info("check infomation")
        index,get_items=conn_s3.get_list_bucket(f"image/{PK}")
        #logging.info(f"get_items:{get_items}")
        if index==0:
            logging.error("error image get")
            err=utils.err_("get images fail")
            return respond(err)
        else:
            for i in get_items:
                #logging.info(f"try get url {i}")
                current_items.append(i['Key'])
                urls.append(conn_s3.generate_url(i['Key']))
        
        logging.info(f'final check:{err}')
        results= {
            "PK":payload['PK'],
            "uploaded_items":items,
            "items":current_items,
            "urls":urls,
        }
        
        return respond(err,res=results,step="upload_images") 
            

def del_image(conn_s3,payload):
    ### return items
    normal_items=[]
    thumbs_items=[]
    ### return urls
    normal_urls=[]
    thumbs_urls=[]
    err=None
    index=payload["index"]
    
    ### validated index
    val_index=[]
    ### target keys
    target_items=[]
    
    PK=payload["PK"]
    ###items call 
    get_index,get_items=conn_s3.get_list_bucket(f"image/{PK}/normal")
    for i in index:
        try:
            if i < get_index:
                val_index.append(int(i)) ### 상수일때만 추가
            else:
                logging.error(f"{i} is not included in contents")
        except:
                logging.error(f"{i} is not included in contents")    
        
    
    #### after cheking indexes, 
    if len(val_index)==0:
        logging.error("No valided items")
        err=utils.err_("invalided items")
        return respond(err, step="del image")
    
    else:
        logging.info("Start delete step")
        for i in val_index:
            target_items.append(get_items[i]['Keys'])
        deleted=conn_s3.del_list_bucket(target_items)    
        get_items=conn_s3.get_list_bucket(f"image/{PK}/normal")
        get_thumbs=conn_s3.get_list_bucket(f"image/{PK}/thumbnail")
        if len(get_items)==0:
            logging.info("No items")
        else:
            for i in get_items:
                logging.info("Start : generating normal urls")
                normal_items.append(i['Key'])
                normal_urls.append(conn_s3.generate_url(i['Key']))
            for i in get_thumbs:
                logging.info("Start : generating thumbs urls")
                thumbs_items.append(i['Key'])
                thumbs_urls.append(conn_s3.generate_url(i['Key']))
        results= {
                "PK":payload['PK'],
                "delected_indexes":val_index,
                "delected_items":deleted,
                "normal_items":normal_items,
                "normal_urls": normal_urls,
                "thumb_items": thumbs_items,
                "thumb_urls":  thumbs_urls,
            }
            
        return respond(err,res=results,step="del_image") 
    
    
    
    
    
    
    
    

def common_action(conn_s3,payload):
    logging.info("start common_action")
    method=payload["method"]
    if method == "GET_IMAGES":
        '''
        PK에 대한 이미지 값 호출
        '''
        logging.info("get_images start")
        return get_images(conn_s3,payload)
    elif method == "GET_THUMB":
        '''
        Thumbnail 획득
        '''
        logging.info("get_thumbs start")
        return get_thumbs(conn_s3,payload)
    elif method == "CLEAR_IMAGES":
        '''
        이력 전체삭제
        필수 method / PK / 
        '''
        logging.info("clear_images start")
        #return clear_images(conn_s3,payload)
    elif method == "DEL_IMAGE":
        '''
        이력 선택삭제
        필수 method / PK / num
        
        '''
        logging.info("del_images start")
        return del_image(conn_s3,payload)
    elif method == "UPLOAD_IMAGES":
        '''
        이력 단일삭제
        필수 method / PK / num -> 권한문제는 lambda 앞단 auth단에서 해결
        '''
        logging.info("upload_images start")
        return upload_images(conn_s3,payload)
    else:
        err=utils.err_("No method :")
        return respond(err)




def lambda_handler(event,context):
    os.system('rm -rf /tmp/image*')
    logging.basicConfig(format='[%(asctime)s] %(message)s')
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    aws_env      = os.environ['AWSENV']
    dev_env      = os.environ['DEVENV']
    region       = os.environ['REGION']
    table_name   = os.environ['TABLE']
    logging.info(f"env:{aws_env},{dev_env},")
    
    conn_s3=s3.s3Api()
    logging.info(f"init:conn")
    logging.info(f"event:{event.keys()}")
    
    if "body" not in event.keys():
        err=utils.err_("No Body")
        return respond(err, step="init")
    else:
        payload=utils.base64_parser_with_image(event['body'])
        if 'PK' not in payload.keys():
            err=utils.err_("No PK")
            return respond(err, step="init")
        elif 'PK' not in payload.keys():
            err=utils.err_("No method")
            return respond(err, step="init")
        else:
            return common_action(conn_s3,payload)
            
        
    
    
    