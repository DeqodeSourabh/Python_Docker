import contract_catelog2
import eth_abi
from web3 import Web3
from web3.middleware import geth_poa_middleware
import json
import ray
infura_url= "wss://mainnet.infura.io/ws/v3/57d8e5ec16764a3e86ce18fc505e640e"
web3 = Web3(Web3.WebsocketProvider(infura_url))
web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
def holdersEvent(_fromBlock, _toBlock,address):
    try:
        web3 = Web3(Web3.WebsocketProvider(infura_url))
        web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
        abi = json.load(open('erc721.json','r'))
        contract = web3.eth.contract(address=address,abi=abi)
    
        transferEvents = contract.events.Transfer.createFilter(fromBlock=_fromBlock, toBlock=_toBlock)
        try:
            for i in range(len(transferEvents.get_all_entries())):
            
                token_id =transferEvents.get_all_entries()[i].args.tokenId
                holder = transferEvents.get_all_entries()[i].args.to
                print(holder, token_id)
                mongo(token_id,holder)
                
        except eth_abi.exceptions.InsufficientDataBytes:
            pass           
    except : 
        pass

if __name__== "__main__":
    ray.init()
    data=contract_catelog2.flink_sql()
    for index, row in data.iterrows():
        holdersEvent.remote(row['fromBlock'], row['toBlock'], row['address'])
