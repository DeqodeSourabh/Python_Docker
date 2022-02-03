import json
import eth_abi
from web3 import Web3
from web3.middleware import geth_poa_middleware
import asyncio
import ray
import os
from find_holders import holdersEvent

infura_url= "wss://mainnet.infura.io/ws/v3/57d8e5ec16764a3e86ce18fc505e640e"
web3 = Web3(Web3.WebsocketProvider(infura_url))
web3.middleware_onion.inject(geth_poa_middleware, layer=0) 


def holdersContract(address,block):
    print('hello')
    abi = json.load(open('erc721.json','r'))
    contract = web3.eth.contract(address=address,abi=abi)
    latest = web3.eth.blockNumber
    firstBlock = block
    totalResult = latest - firstBlock
    initial = firstBlock
    if totalResult >2000:
        while totalResult>=2000:
            fromBlock = initial
            toBlock = initial +2000
            holdersEvent.remote(fromBlock,toBlock,contract)
            totalResult = totalResult -2000
            initial = toBlock
        if totalResult != 0:
            fromBlock = initial
            toBlock = initial + totalResult
            holdersEvent.remote(fromBlock,toBlock,contract)
    else:
        holdersEvent.remote(firstBlock,latest,contract)
