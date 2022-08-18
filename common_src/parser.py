import base64

def base64_body_parser(event):
        '''
        parser for lambda base encoding 
        '''
        body = event["body"]
        info=[]
        keys=[]
        values=[]
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
        
        payload={k:v for k,v in zip(keys,values)}
        info = list(set(info))[0]
        return payload,info
    
if __name__ == "__main__":
    print("Test") ### unit test 완료 freeze
    