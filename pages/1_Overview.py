import streamlit as st
from shroomdk import ShroomDK
import pandas as pd

st.set_page_config(
    page_title="📊 Overview",
    layout= "wide",
    page_icon="📊",
)
st.title("📊 Overview")
st.sidebar.success("📊 Overview")

@st.cache(ttl=10000)
def querying_pagination(query_string):
    sdk = ShroomDK('8c37dc3a-fcf4-42a1-a860-337fa9931a2a')
    result_list = []
    for i in range(1,11): 
        data=sdk.query(query_string,page_size=100000,page_number=i)
        if data.run_stats.record_count == 0:  
            break
        else:
            result_list.append(data.records)
  
    result_df=pd.DataFrame()
    for idx, each_list in enumerate(result_list):
        if idx == 0:
            result_df=pd.json_normalize(each_list)
        else:
            result_df=pd.concat([result_df, pd.json_normalize(each_list)])

    return result_df

df_query="""
with price as(select
date_trunc('day',RECORDED_HOUR) AS date1, 
avg(close) AS price
from crosschain.core.fact_hourly_prices
where ID = 'near'
group by 1
  )
select 
block_timestamp::date as date,
price as "Near price",
count(DISTINCT tx_signer) as "active users count",
count(DISTINCT tx_hash) as "transactions count",
count(DISTINCT TX_RECEIVER) as "smart contracts count",
sum("active users count") over (order by date) as "cumulative active users count",
sum("transactions count") over (order by date) as "cumulative transactions count",
sum("smart contracts count") over (order by date) as "cumulative smart contracts count",
sum(TRANSACTION_FEE/pow(10,24)) as "transactions fee",
sum(GAS_USED/pow(10,12)) as "gas used"
from near.core.fact_transactions a join price b on date_trunc('day',a.block_timestamp)=b.date1
where TX_STATUS = 'Success'
group by 1,2
"""
df = querying_pagination(df_query)

df2_query="""
with price as(select
date_trunc('day',RECORDED_HOUR) AS date1, 
avg(close) AS price
from crosschain.core.fact_hourly_prices
where ID = 'near'
group by 1
  )
select 
count(DISTINCT tx_signer) as active_users,
count(DISTINCT tx_hash) as transactions_count,
count(DISTINCT TX_RECEIVER) as contracts_count,
sum(TRANSACTION_FEE/pow(10,24)) as transactions_fee,
sum(GAS_USED/pow(10,12)) as gas_used
from near.core.fact_transactions a join price b on date_trunc('day',a.block_timestamp)=b.date1
where TX_STATUS = 'Success'
"""
df2 = querying_pagination(df2_query)

df3_query="""
with price as(select
date_trunc('day',RECORDED_HOUR) AS date1, 
avg(close) AS price
from crosschain.core.fact_hourly_prices
where ID = 'near'
group by 1
  ), main as (select 
block_timestamp,
PARSE_JSON(args):new_account_id::string as user
from near.core.fact_actions_events_function_call
where method_name ILIKE 'create\\_account%'
and tx_hash NOT IN (select 
DISTINCT a.tx_hash 
from near.core.fact_receipts a JOIN near.core.fact_actions_events_function_call b ON a.tx_hash = b.tx_hash 
where method_name ILIKE 'create\\_account%' 
and REGEXP_SUBSTR(status_value, 'Failure') IS NOT NULL
    )
)
select
a.block_timestamp::date as date,
count(DISTINCT user) as "new users count"
from main a join price b on date_trunc('day',a.block_timestamp) = b.date1
group by 1
"""
df3 = querying_pagination(df3_query)

df4_query="""
with main as (select 
block_timestamp,
PARSE_JSON(args):new_account_id::string as user
from near.core.fact_actions_events_function_call
where method_name ILIKE 'create\\_account%'
and tx_hash NOT IN (select 
DISTINCT a.tx_hash 
from near.core.fact_receipts a JOIN near.core.fact_actions_events_function_call b ON a.tx_hash = b.tx_hash 
where method_name ILIKE 'create\\_account%' 
and REGEXP_SUBSTR(status_value, 'Failure') IS NOT NULL
    )
)
select
count(DISTINCT user) as new_users
from main
"""
df4 = querying_pagination(df4_query)

df5_query="""
with main as (
select
block_timestamp::date as date,
lag(block_timestamp,1) over ( order by block_timestamp) as "block time",
datediff(second,"block time",block_timestamp) as "block time diff"
from near.core.fact_blocks
)
select 
date,
avg("block time diff") as "average time between two block"
from main
where "block time diff" > 0
group by 1
"""
df5 = querying_pagination(df5_query)

df6_query="""
select 
block_timestamp::date as date,
count(TX_HASH) as "total transactions count",
count(case when tx_STATUS = 'Success' then TX_HASH end) as "success transactions count",
("success transactions count" / "total transactions count")*100 as "success transactions rate",
(100 - "success transactions rate") as "fail transactions rate",
count(case when tx_STATUS = 'Success' then TX_HASH end) / (24*60) as "TPM",
count(case when tx_STATUS = 'Success' then TX_HASH end) / (24*60*60) as "TPS"
from near.core.fact_transactions
group by 1
"""
df6 = querying_pagination(df6_query)

st.write("""
 # Active users #

 The Daily Number of Active Accounts is a measure of how many wallets on NEAR are making transactions on chain.

 """
)
st.metric(
    value="{0:,.0f}".format(df2["active_users"][0]),
    label="Total active users count",
)
st.bar_chart(df, x='date', y = 'active users count', width = 400, height = 400)

st.write("""
 # New users #

 The Daily Number of Active Accounts is a measure of how many wallets on NEAR are making transactions on chain. Over the last 2 month.

 """
)
st.metric(
    value="{0:,.0f}".format(df4["new_users"][0]),
    label="Total new users count",
)
st.bar_chart(df3, x='date', y = 'new users count', width = 400, height = 400)

st.write("""
 # Transaction count #

 The daily transaction count is a record of the number of times the blockchain has recorded a transaction.

 """
)
st.metric(
    value="{0:,.0f}".format(df2["transactions_count"][0]),
    label="Total number of transaction",
)
st.bar_chart(df, x='date', y = 'transactions count', width = 400, height = 400)

st.write("""
 # Active contracts #

 Contracts on NEAR are simply programs stored on a blockchain that run when predetermined conditions are met. The Daily Number of New Contracts is a valuable metric for understanding the health and growth of an ecosystem.The more active contracts there are, the more projects are actively engaging with the NEAR protocol. The chart below shows a cyclical rhythm to new contracts

 """
)
st.metric(
    value="{0:,.0f}".format(df2["contracts_count"][0]),
    label="Total number of active contract",
)
st.bar_chart(df, x='date', y = 'smart contracts count', width = 400, height = 400)

st.write("""
 # Transaction fee (Near) #

 Gas Fees are a catch all term for the cost of making transactions on the NEAR network. These fees are paid to validators for their services to the blockchain. Without these fees, there would be no incentive for anyone to keep the network secure.

 """
)
cc1, cc2 = st.columns([1, 1])

with cc1:
  st.metric(
    value="{0:,.0f}".format(df2["transactions_fee"][0]),
    label="Total transaction fee (Near) & Gas used",
)
with cc2:
  st.metric(
    value="{0:,.0f}".format(df2["gas_used"][0]),
    label="Gas used",
)

cc1, cc2 = st.columns([1, 1])

with cc1:
  st.bar_chart(df, x='date', y = 'transactions fee', width = 400, height = 400)
with cc2:
  st.bar_chart(df, x='date', y = 'gas used', width = 400, height = 400)

st.write("""
 # Network performance #

 In this section the network performance metrics are shown.

 """
)
st.subheader('success vs fail transactions rate')
cc1, cc2 = st.columns([1, 1])
with cc1:
  st.caption('success transactions rate')
  st.bar_chart(df6, x='date', y = 'success transactions rate', width = 400, height = 400)
with cc2:
  st.caption('fail transactions rate')
  st.bar_chart(df6, x='date', y = 'fail transactions rate', width = 400, height = 400)

st.subheader('TPS vs TPM (transaction per minute / secound')
cc1, cc2 = st.columns([1, 1])
with cc1:
  st.caption('TPS')
  st.line_chart(df6, x='date', y = 'tps', width = 400, height = 400)
with cc2:
  st.caption('TPM')
  st.line_chart(df6, x='date', y = 'tpm', width = 400, height = 400)

st.subheader('Average time between two block')
st.caption('Daily average time between two block')
st.line_chart(df5, x='date', y = 'average time between two block', width = 400, height = 400)
