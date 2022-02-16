
from web3 import Web3
from web3.middleware import geth_poa_middleware
import ray
from ray.util import inspect_serializability
import pymongo
import cluster_setup 
from memory_profiler import profile
import gc


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
    
    gc.collect()


@ray.remote
def fetchBlocks(block):

        web3 = Web3(Web3.WebsocketProvider("wss://mainnet.infura.io/ws/v3/8e968f37d20f434d8358908201ae6685"))
        web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
        for tx_hash in web3.eth.get_block(block).transactions:
            contractAddress = web3.eth.getTransactionReceipt(tx_hash).to
            #print(contractAddress)
            if contractAddress != None:
                mongo(contractAddress, block)
                print(contractAddress, block)
                #return contractAddress
        del web3
        del block



#inspect_serializability(fetchBlocks, name="contract")
@profile
def start():   
    infura_url= "wss://mainnet.infura.io/ws/v3/8e968f37d20f434d8358908201ae6685"
    web3 = Web3(Web3.WebsocketProvider(infura_url))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0) 

    #This will start a Ray new node and automatically creates
    #the multiple workers in your cluster
    cluster_setup.start_node()
    ray.init(address='auto', _redis_password='5241590000000000')
    cluster_setup.create_multiple_nodes()


    print('''This cluster consists of
    {} nodes in total
    {} CPU resources in total
'''.format(len(ray.nodes()), ray.cluster_resources()['CPU']))
    block= web3.eth.get_block('latest').number + 1
    del web3
    del infura_url
    while block>=0:
        block-=1
        fetchBlocks.remote(block)
          
start()
