import json







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
    
    return {
        'statusCode': '400' if err else '200',
        'body': json.dumps( {
            "items":'[]',
            "meta" :meta
            },ensure_ascii=False) if err else json.dumps(res,ensure_ascii=False),
        'headers': {
            'Content-Type': 'application/json',
        },
    }


def lambda_handler(event,context):
    print(event)