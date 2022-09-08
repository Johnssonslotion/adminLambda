import base64
import logging
import json
import os

from io import BytesIO
from PIL import Image

def base64_body_parser(event):
        '''
        parser for lambda base encoding 
        '''
        logging.info("BASE64PARSER - START")
        logging.info(event)
        body = event["body"] ## body 인 경우
        logging.info(type(body))
        info=[]
        keys=[]
        values=[]
        err=None
        payload=None
        ### dict 개체일경우
        if type(body) is dict:
            logging.info("BODY TYPE :DICT, BODY :{body}")
            payload=body
            info.append("No encoded")
            err=None
            return payload,err
        ### base64 인 str인 경우
        elif event['isBase64Encoded'] == True:
            n = 4-len(body)%4
            body=f"{body}{n*'='}"
            body=base64.b64decode(body)
            case=body.decode().split('\n')
            print(case)
            for i,j in enumerate(case):
                print(i,":",j)
                if 'Content-Disposition:' in j:
                    keys.append(j.split("name=")[1].split("\r")[0].replace('"',''))
                    values.append(case[i+2].split('\r')[0])
                elif '-' in j:
                    j=j.replace("-","").split("\r")[0]
                    info.append(j)
            logging.info(f"BODY TYPE :BASE64, BODY :{keys},{values}")
            payload={k:v for k,v in zip(keys,values)}
            logging.info(f"CONVERTED :{payload}")
            return payload,err
        ### json string 인 경우
        else:
            logging.info(f"NOT SORTED - ABNORMAL")
            payload=json.loads(body)
            logging.info(payload)
            return payload,err


def base64_parser_with_image(body,debug=None):
    
    '''
    base64 parser version 2, 
    file + parameter 조합을 parsing 하는 기본
    
    '''
    logging.basicConfig(level=logging.INFO)
    logging.info("post base64 parser version 2.0")
    payload=base64.b64decode(body).split(b'\r\n')
    keys=[]
    values=[]
    file_names=[]
    file_order=[]
    image_order=[]
    item_curser=-1
    value=[]
    for i,j in enumerate(payload):
        #logging.info(f"{i}:{j[:100]}")
        if b'----'in j:
            item_curser+=1  ## 입력 값 갯수저장
            if value!=[]:
                logging.info(f"item: {item_curser} end of index:{i} \t len of v : {len(value)}")
                if len(value)==1:
                    values.append(str(value[0].decode()))
                    logging.info(f"KEY:{key}\t\t VALUES:{value}")
                else:
                    #logging.info(f"check_first : {value[0]}")
                    value=b'\r\n'.join(value[1:])
                    values.append(value)
                    logging.info(f"KEY:{key}\t\t VALUES:{value[:30]}\t\t length:{len(value)} file_order:{file_order}")
                value=[]
            start_index=i ### 시작지점 저장
        if b'Content-Disposition:' in j:
            for k in j.split(b';'):
                if k.split(b"=")[0]==b' name':
                    key=k.split(b"=")[1].replace(b'"',b'').decode('utf8')
                    keys.append(key)
                if k.split(b"=")[0]==b' filename':
                    file_name=k.split(b"=")[1].replace(b'"',b'').decode('utf8')
                    logging.info(f"filename:{file_name}")
                    file_names.append(file_name)
                    file_order.append(item_curser)
                    
        elif b'Content-Type:' in j:
            content_type=j.split(b':')[1]
            if content_type.startswith(b' image/'):
                image_order.append(item_curser)
            
        curser=i-start_index
        # if curser==0:
        #     logging.info(f"index:{i}, curser 1: {str(j)[:100]}")
        # if curser==1:
        #     logging.info(f"index:{i}, curser 1: {str(j)[:100]}")
        # if curser==2:
        #     logging.info(f"index:{i}, curser 2: {str(j)[:100]}")
        if curser==3:
            # logging.info(f"index:{i}, curser 3: {str(j)[:100]}")
            if i=='':
                continue
            else:
                value.append(j)    
        if curser>=4:
            value.append(j)
        
    
    
    logging.info(f"keys:{keys} values:EA{len(values)} {[f'v:{i[:10]}~,len:{len(i)}' for i in values]},file_order:{file_order}")
    
    for i,j in enumerate(image_order):
        logging.info(f"image_processing: \t step:start \t target:{i},\t key:{keys[j]}, \t filename:{file_names[i]}")
        logging.info(f"image_processing: \t step:open \t target:{i}, \t key:{keys[j]}, \t filename:{file_names[i]}")
        img=Image.open(BytesIO(values[j]))
        ext=os.path.splitext(file_name[i])[1]
        logging.info(f"image_processing: \t step:save /tmp/image{i}.png \t target:{i}, \t key:{keys[j]}, \t filename:{file_names[i]}")
        img.save(f"/tmp/image{i}.png")
        #img.save(f"image{i}.png")
        logging.info(f"image_processing end target:{i},\t key:{keys[j]}, \t filename:{file_names[i]}")

    
    
    if file_order != None:
        rev_file_order=list(range(item_curser))
        rev_file_order=[i for i in rev_file_order if i not in file_order] ### file이 아닌 값만 저장
        keys=[keys[i] for i in rev_file_order] ### index
        values=[values[i] for i in rev_file_order]
    
    logging.info(f"keys:{keys} values:EA{len(values)} {[f'v:{i[:10]}~,len:{len(i)}' for i in values]},file_order:{file_order},image_order:{image_order}")
    
    
    payload={k:v for k,v in zip(keys,values)}
    return payload

class err_(object):
    def __init__(self, message):
        logging.error(f"{message}")
        self.message=message


if __name__ == "__main__":
    logger=logging.getLogger()
    logger.setLevel(level=logging.INFO)
    with open("./events/event.json") as f:
        r=json.load(f)
    y=base64_parser_with_image(r['body'])
    
    
    print("Test") ### unit test 완료 freeze
    