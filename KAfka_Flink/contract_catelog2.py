import blocks_catelog1
from web3 import Web3
from web3.middleware import geth_poa_middleware
import asyncio
import ray
import os
import pymongo
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

infura_url= "wss://mainnet.infura.io/ws/v3/57d8e5ec16764a3e86ce18fc505e640e"
web3 = Web3(Web3.WebsocketProvider(infura_url))
web3.middleware_onion.inject(geth_poa_middleware, layer=0) 

def flink_sql(address,fromblock,toBlock):
    env = StreamExecutionEnvironment.get_execution_environment()
    table_env = StreamTableEnvironment.create(env)
    result = table_env.from_elements([(address,fromblock,toBlock)], ['address', 'fromBlock','toBlock'])
    result = result.to_pandas()
    table2 = result.reset_index()
    return table2


def holdersContract(address,block):
    web3 = Web3(Web3.WebsocketProvider(infura_url))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
    # abi = json.load(open('erc721.json','r'))
    # contract = web3.eth.contract(address=address,abi=abi)
    latest = web3.eth.blockNumber
    firstBlock = block
    totalResult = latest - firstBlock
    initial = firstBlock
    if totalResult >2000:
        while totalResult>=10000:
            fromBlock = initial
            toBlock = initial +2000
            #holdersEvent.remote(fromBlock,toBlock,contract
           # mongoDb(address, fromBlock, toBlock)
            flink_sql(address, fromBlock, toBlock)   
            totalResult = totalResult -2000
            initial = toBlock
        if totalResult != 0:
            fromBlock = initial
            toBlock = initial + totalResult
            #holdersEvent.remote(fromBlock,toBlock,contract)
            #mongoDb(address, fromBlock, toBlock)
            flink_sql(address, fromBlock, toBlock)
    else:
        flink_sql(address, fromBlock, toBlock)
        #c.holdersEvent.remote(firstBlock,latest,contract)
    return 0


if __name__== "__main__":
    ray.init()
    contract_table=blocks_catelog1.web3Functions()
    for index, row in contract_table.iterrows():
        holdersContract.remote(row['address'], row['block'])
    # for i in details:
    #     futures.append(holdersContract.remote(i['contractAddress'], i['block'])



