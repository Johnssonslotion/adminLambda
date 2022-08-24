import json


def respond(err, res=None):
    return {
		'statusCode': '400',
		'body': json.dumps(res),
		'headers': {
			'Comtent-Type': 'application/json',
		},
	}

def lambda_handler(event, context):
    print(event)
    print(context)
    return respond(None,res="API STACK HEALTH CHECK")