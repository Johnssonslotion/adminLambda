import pytest, sys, logging, os
from time import sleep
from common_src.dynamo import dynamoApi




def setup_function(function):
    logging.info(sys._getframe(0).f_code.co_name)

def teardown_function(function):
    logging.info(sys._getframe(0).f_code.co_name)



def test_function_01():
    
    assert True == True
    
    

