import base64
import logging

def base64_body_parser(event):
        '''
        parser for lambda base encoding 
        '''
        body = event["body"] ## body 인 경우
        info=[]
        keys=[]
        values=[]
        err=None
        if type(body) is dict:
            payload=body
            info.append("No encoded")
            err=None
            return payload,info,err
        else:
            if event['isBase64Encoded'] == True:
                n = 4-len(body)%4
                body=f"{body}{n*'='}"
                body=base64.b64decode(body)
                case=body.decode().split('\n')
            else:
                case=body.decode().split('\n')
            for i,j in enumerate(case):
                print(i,":",j)
                if 'Content-Disposition:' in j:
                    keys.append(j.split("name=")[1].split("\r")[0].replace('"',''))
                    values.append(case[i+2].split('\r')[0])
                elif '-' in j:
                    j=j.replace("-","").split("\r")[0]
                    info.append(j)
            try:
                payload={k:v for k,v in zip(keys,values)}
            except :
                err=err_("dict err")
            try:
                info = list(set(info))[0]
            except Exception as e:
                err=err_("infomation get error")
        return payload,info,err

class err_(object):
    def __init__(self, message):
        logging.warning(f"{message}")
        self.message=message

if __name__ == "__main__":
    print("Test") ### unit test 완료 freeze
    