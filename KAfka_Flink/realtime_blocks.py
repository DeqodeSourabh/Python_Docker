import time
import json
from web3 import Web3
from web3.middleware import geth_poa_middleware
from kafka import KafkaProducer  
import ray
ray.init()
def connection():
    folderName = "/home/deq/Desktop/aiven_detail"
    producer = KafkaProducer(
        bootstrap_servers="kafka-prod-bepureme-d95a.aivencloud.com:12414",
        security_protocol="SSL",
        ssl_cafile=folderName+"/ca.pem",
        ssl_certfile=folderName+"/service.cert",
        ssl_keyfile=folderName+"/service.key",
        value_serializer=lambda v: json.dumps(v).encode('ascii'),
        key_serializer=lambda v: json.dumps(v).encode('ascii')

    )
    return producer

def realTime():
    producer=connection()
    web3 = Web3(Web3.WebsocketProvider("wss://mainnet.infura.io/ws/v3/8e968f37d20f434d8358908201ae6685"))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0)
    prev_block = 0
    while True:
        current_block= web3.eth.get_block('latest').number
        if (prev_block!=current_block):
            for tx_hash in web3.eth.get_block(current_block).transactions:
                contractAddress = web3.eth.getTransactionReceipt(tx_hash).to
            if contractAddress != None:
                print(contractAddress)
                #producer.send('test-topic', value = contractAddress)
                producer.send("test-topic",
                key={"key": 1},
                value={"message": contractAddress}
            )
        prev_block = current_block
        time.sleep(1)
    
realTime()