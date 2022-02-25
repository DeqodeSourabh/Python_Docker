# from pyflink.table import EnvironmentSettings, TableEnvironment
# 
#using batch table environment to execute the queries
# env_settings = EnvironmentSettings.in_batch_mode()
# table_env = TableEnvironment.create(env_settings)
# 
# orders = table_env.from_elements([('Jack', 'FRANCE', 10), ('Rose', 'ENGLAND', 30), ('Jack', 'FRANCE', 20)],
                                #  ['name', 'country', 'revenue'])
# 
#compute revenue for all customers from France
# revenue = orders \
    # .select(orders.name, orders.country, orders.revenue) \
    # .where(orders.country == 'FRANCE') \
    # .group_by(orders.name) \
    # .select(orders.name, orders.revenue.sum.alias('rev_sum'))
# print(revenue.to_pandas())

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.common.typeinfo import Types
from web3 import Web3
from web3.middleware import geth_poa_middleware
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)
#table_env.create_temporary_view('table_api_table', table)
def web3Functions():
    block = 5134541 
    data = []
    tr = []
    web3 = Web3(Web3.WebsocketProvider("wss://mainnet.infura.io/ws/v3/8e968f37d20f434d8358908201ae6685"))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
    for tx_hash in web3.eth.get_block(block).transactions:
        contractAddress = web3.eth.getTransactionReceipt(tx_hash).to
        if contractAddress != None:
        #print(contractAddress)
          print(contractAddress, block)
          data.append(contractAddress, block)       #ye galat he sahi karna he
    del web3
    del block
    source = table_env.from_elements(data, ['address', 'block'])
    print(source.to_pandas())
    result = source.to_pandas()
    #this code is for iterate the value in each rows
    # 
    result = result.reset_index()
    # print(df)
    # for index, row in df.iterrows():
    #     print(row['address'])
    return result
web3Functions()