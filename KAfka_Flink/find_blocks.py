
from web3 import Web3
from web3.middleware import geth_poa_middleware
import ray
from ray.util import inspect_serializability
import pymongo
import gc
from time import sleep  
from json import dumps  
from kafka import KafkaProducer  
import json
def connection():
    folderName = "/home/deq/Desktop/aiven_detail"
    producer = KafkaProducer(
        bootstrap_servers="kafka-prod-bepureme-d95a.aivencloud.com:12414",
        security_protocol="SSL",
        ssl_cafile=folderName+"/ca.pem",
        ssl_certfile=folderName+"/service.cert",
        ssl_keyfile=folderName+"/service.key",
        #value_serializer=lambda v: json.dumps(v).encode('ascii'))
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda y: json.dumps(y).encode('utf-8'))
    return producer


#@ray.remote
def fetchBlocks(block):
        producer=connection()
        web3 = Web3(Web3.WebsocketProvider("wss://mainnet.infura.io/ws/v3/8e968f37d20f434d8358908201ae6685"))
        web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
        for tx_hash in web3.eth.get_block(block).transactions:
            contractAddress = web3.eth.getTransactionReceipt(tx_hash).to
            #print(contractAddress)
            if contractAddress != None:
                print(contractAddress)
                #producer.send('test-topic', value = contractAddress)
                producer.send("con-add",
                key={"key": 1},
                value={"address": contractAddress}
            )
                sleep(0.005)              
        del web3
        del block
        gc.collect()

def start():   
    infura_url= "wss://mainnet.infura.io/ws/v3/8e968f37d20f434d8358908201ae6685"
    web3 = Web3(Web3.WebsocketProvider(infura_url))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
    ray.init()

    print('''This cluster consists of
    {} nodes in total
    {} CPU resources in total
'''.format(len(ray.nodes()), ray.cluster_resources()['CPU']))

    block= web3.eth.get_block('latest').number + 1
    del web3
    del infura_url
    while block>=0:
        block-=1
        fetchBlocks(block)
          
start()




# for n in range(500):  
#     my_data = {'num' : n}  
#     my_producer.send('testnum', value = my_data)  
#     sleep(0.005)  
