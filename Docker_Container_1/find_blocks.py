
from web3 import Web3
from web3.middleware import geth_poa_middleware
import asyncio
import ray
import os
from find_contracts import holdersContract

infura_url= "wss://mainnet.infura.io/ws/v3/57d8e5ec16764a3e86ce18fc505e640e"
web3 = Web3(Web3.WebsocketProvider(infura_url))
web3.middleware_onion.inject(geth_poa_middleware, layer=0) 


def fetchBlocks(block):
    while block>=0:
        block -=1
        blockInfo = web3.eth.get_block(block)
        for tx_hash in blockInfo.transactions:
            contractAddress = web3.eth.getTransactionReceipt(tx_hash).to
            if contractAddress != None:
                holdersContract(contractAddress,block)
            #print(contractAddress, tx_hash.hex())
    
if __name__=="__main__":
    #   print(os.cpu_count())
    ray.init()
    latestBlock = web3.eth.get_block('latest').number
    block= latestBlock+1
    while block>=0:
        fetchBlocks(block)
    

