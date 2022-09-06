'''
checklist 미확정임에 따라 pre-mapping 과정
+
ORM의 임시적 구현

'''
from boto3.dynamodb.conditions import Key
import boto3
import os
import logging
#from dynamo import dynamoApi

class model_manage_toilet(object):
    ### fixed column
    
    def __init__(self,PK,conn=None,table_name=None):
        self.target_dict={}
        self.total={}
        if conn == None:
            self._conn=boto3.resource('dynamodb')
        else: 
            self._conn=conn
        if table_name==None:
            self._table_name=os.environ["TABLE"]
        else:
            self._table_name=table_name
        
        self._Table=self._conn.Table("_test_db")
        res=self._Table.query(
            IndexName='PK-index',
            KeyConditionExpression=Key('PK').eq(str(PK))
            )
        
        self.Key={
            "hashKey":res['Items'][0]['hashKey'],
            "rangeKey":res['Items'][0]['rangeKey'],
        }
        self.total=res['Items'][0]
        #####################
        ### 개별 컬럼 확정짓기 ###
        for i in self.total.keys():
            setattr(self,i,self.item_control(self,i))

    def keys(self):
        return print(self.total.keys())
    
    def values(self,*args):
        val_keys=[]
        return_dict={}
        if type(args) == list:
            for i in args:
                if i == self.total.keys():
                    val_keys.append(i)
            for j in val_keys:
                return_dict[j]=self.total[j]
            return return_dict                               
        else:
            return self.total.keys()
    
    def create_col(self,col):
        if hasattr(self,col):
            logging.info(f"ID : {self.total['PK']} has attribute'{col}'")
            return None
        else:
            logging.info(f"New column has been generated in {self.total['PK']}")
            setattr(self,col,self.item_control(self,col))
            res=self._Table.update_item(
                Key=self.Key,
                UpdateExpression="SET #name=:val",
                ExpressionAttributeNames={"#name":col},
                ExpressionAttributeValues={":val":None},
                ReturnValues="ALL_NEW"
            )
            self._super.total=res["Attributes"]
            return res["Attributes"][col]
        
    def create_item(self,*args):
        target_dict={}
        Names={}
        Values={}
        updateEx="SET "
        if len(args)==0:
            return res['Items']
        else:
            for i,j in enumerate(args):
                assert type(args[i])== dict,"check key & value"
                assert len(j.keys())!=len(j.values()), "check n of keys, n of values "
                target_dict.update(j)
            
            order=0
            for i in target_dict.keys():
                Names[f"#C{order:02d}"]=i
                Values[f":V{order:02d}"]=target_dict[i]
                updateEx+="#C{order:02d}=:V{order:02d},"
            
            updateEx=updateEx[:len(updateEx)-1]
            
            res=self._Table.update_item(
                Key=self.Key,
                UpdateExpression=updateEx,
                ExpressionAttributeNames= Names,
                ExpressionAttributeValues=Values,
                ReturnValues="ALL NEW"
            )
        return res["Attributes"]
    
    ### inner class
    
    class item_control():
        def __init__(self,_super,_col):
            self._super=_super
            self._col=_col
        def get_value(self):
            return self._super.total[self._col]
        def set_value(self,value):
            logging.info(f"set_value : [ Key:{self._col}, Value:{value} ]" )
            res=self._super._Table.update_item(
                Key=self._super.Key,
                UpdateExpression="SET #name=:val",
                ExpressionAttributeNames={"#name":self._col},
                ExpressionAttributeValues={":val":value},
                ReturnValues="ALL_NEW"
            )
            self._super.total=res["Attributes"]
            return res["Attributes"][self._col]
    
                  
if __name__=="__main__":
    logger=logging.getLogger()
    logging.basicConfig(level=logging.INFO)
    
    
    
       
    
    
    logging.info("test_api")
    logging.info("test_api")
    logging.info("test_api")
    logging.info("test_api")
    
        
        
        
         
    
    
    

