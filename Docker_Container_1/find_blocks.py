
from web3 import Web3
from web3.middleware import geth_poa_middleware
import asyncio
import ray
import os
#from find_contracts import holdersContract
from ray.util import inspect_serializability
import pymongo
import cluster_setup 

infura_url= "wss://mainnet.infura.io/ws/v3/57d8e5ec16764a3e86ce18fc505e640e"
web3 = Web3(Web3.WebsocketProvider(infura_url))
web3.middleware_onion.inject(geth_poa_middleware, layer=0) 


def mongo(contractAddress, block):
    connection_url = 'mongodb+srv://sourabh:sourabh@cluster0.il3sa.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
    client = pymongo.MongoClient(connection_url)
    Database = client.get_database('myFirstDB')
    blocksTable = Database.blocksTable
    queryObject = {
    'contractAddress': contractAddress,
    'block': block
    }
    if blocksTable.find_one({'contractAddress': contractAddress}) == None:
        print("inserted")
        blocksTable.insert_one(queryObject)
    


@ray.remote
def fetchBlocks(block):
        web3 = Web3(Web3.WebsocketProvider(infura_url))
        web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
        
        blockInfo = web3.eth.get_block(block)
        for tx_hash in blockInfo.transactions:
            contractAddress = web3.eth.getTransactionReceipt(tx_hash).to
            print(contractAddress)
            if contractAddress != None:
                mongo(contractAddress, block)
                print(contractAddress, block)
                return contractAddress
        



#inspect_serializability(fetchBlocks, name="contract")
def start():    
    cluster_setup.start_node()
    ray.init(address='auto', _redis_password='5241590000000000')
    
    cluster_setup.create_multiple_nodes()


    print('''This cluster consists of
    {} nodes in total
    {} CPU resources in total
'''.format(len(ray.nodes()), ray.cluster_resources()['CPU']))

    futures =[]
    latestBlock = web3.eth.get_block('latest').number
    print(latestBlock)
    block= latestBlock+1
    while block>=0:
        block-=1
        futures.append(fetchBlocks.remote(block))
    ray.get(futures)
       

start()
