import base64
import logging
import json

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

class err_(object):
    def __init__(self, message):
        logging.error(f"{message}")
        self.message=message

if __name__ == "__main__":
    print("Test") ### unit test 완료 freeze
    