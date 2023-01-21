import streamlit as st
from shroomdk import ShroomDK
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="🌃 NFT (minting & selling activity)",
    layout= "wide",
    page_icon="🌃",
)
st.title("🌃 NFT (minting & selling activity)")
st.sidebar.success("🌃 NFT (minting & selling activity)")

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
#daily nft sale
df_query="""
select
a.block_timestamp::date as date,
count(distinct a.TX_HASH) as sales_count,
count(DISTINCT b.tx_signer) AS sellers_count
from near.core.fact_actions_events_function_call a join near.core.fact_transactions b on a.tx_hash=b.tx_hash
where method_name = 'buy' 
and TRY_PARSE_JSON(args) is not null 
and TRY_CAST(TRY_PARSE_JSON(args):price::string as INT) is not null
group by 1
"""
df = querying_pagination(df_query)

df110_query="""
with price as(select 
RECORDED_HOUR::date as date,
avg(close) as price 
from crosschain.core.fact_hourly_prices
where ID = 'near'
group by 1)
select
a.block_timestamp::date as date,
count(distinct a.TX_HASH) as sales_count,
(sum(PARSE_JSON(args):price::int*c.price))/pow(10,24) as sales_volume,
sales_volume / sales_count as avg_price
from near.core.fact_actions_events_function_call a join near.core.fact_transactions b on a.tx_hash=b.tx_hash join price c on c.date=b.block_timestamp::date
where method_name = 'buy' 
and TRY_PARSE_JSON(args) is not null 
and TRY_CAST(TRY_PARSE_JSON(args):price::string as INT) is not null
group by 1
"""
df110 = querying_pagination(df110_query)

#total nft sale
df1_query="""
select
count(distinct a.TX_HASH) as sales_count,
count(DISTINCT b.tx_signer) AS sellers_count
from near.core.fact_actions_events_function_call a join near.core.fact_transactions b on a.tx_hash=b.tx_hash
where method_name = 'buy' 
and TRY_PARSE_JSON(args) is not null 
and TRY_CAST(TRY_PARSE_JSON(args):price::string as INT) is not null
"""
df1 = querying_pagination(df1_query)

df111_query="""
with price as(select 
RECORDED_HOUR::date as date,
avg(close) as price 
from crosschain.core.fact_hourly_prices
where ID = 'near'
group by 1)
select
count(distinct a.TX_HASH) as sales_count,
(sum(PARSE_JSON(args):price::int*c.price))/pow(10,24) as sales_volume,
sales_volume / sales_count as avg_price
from near.core.fact_actions_events_function_call a join near.core.fact_transactions b on a.tx_hash=b.tx_hash join price c on c.date=b.block_timestamp::date
where method_name = 'buy' 
and TRY_PARSE_JSON(args) is not null 
and TRY_CAST(TRY_PARSE_JSON(args):price::string as INT) is not null
"""
df111 = querying_pagination(df111_query)

#user categorize by count of sale
df2_query="""
with main as(select
DISTINCT b.tx_signer as user,
count(distinct a.TX_HASH) as sales_count
from near.core.fact_actions_events_function_call a join near.core.fact_transactions b on a.tx_hash=b.tx_hash
where method_name = 'buy' 
and TRY_PARSE_JSON(args) is not null 
and TRY_CAST(TRY_PARSE_JSON(args):price::string as INT) is not null
group by 1)
select 
case
when sales_count = 1 then 'transaction just once time' 
when sales_count  BETWEEN 2 and 5 then 'transaction 2 - 5 time'  
when sales_count  BETWEEN 6 and 15 then 'transaction 5 - 15 time'  
when sales_count BETWEEN 16 and 50 then 'transaction 15 - 50 time'  
when sales_count BETWEEN 51 and 100 then 'transaction 50 - 100 time'  
when sales_count BETWEEN 101 and 200 then 'transaction 100 - 200 time'  
else 'transaction more than 200 time' end as tier,
count(distinct user) as sellers_count
from main
where tier is not null
group by 1
"""
df2 = querying_pagination(df2_query)

#user categorize by volume (USD) of sale
df3_query="""
with price as(select 
RECORDED_HOUR::date as date,
avg(close) as price 
from crosschain.core.fact_hourly_prices
where ID = 'near'
group by 1)
, main as(select
DISTINCT b.tx_signer as user,
(sum(PARSE_JSON(args):price::int*c.price))/pow(10,24) as sales_volume
from near.core.fact_actions_events_function_call a join near.core.fact_transactions b on a.tx_hash=b.tx_hash join price c on c.date=b.block_timestamp::date
where method_name = 'buy' 
and TRY_PARSE_JSON(args) is not null 
and TRY_CAST(TRY_PARSE_JSON(args):price::string as INT) is not null
group by 1)
select 
case
when sales_volume BETWEEN 0 and 10 then '0 - 10 USD' 
when sales_volume  BETWEEN 10 and 100 then '10 - 100 USD'  
when sales_volume  BETWEEN 100 and 500 then '100 - 500 USD'  
when sales_volume BETWEEN 500 and 1000 then '500 - 1k USD'  
when sales_volume BETWEEN 1000 and 10000 then '1k - 5k USD'  
when sales_volume BETWEEN 10000 and 100000 then 'Shark 10k-100k USD'  
else 'Whale >100k USD' end as tier,
count(distinct user) as sellers_count
from main
where tier is not null
group by 1
"""
df3 = querying_pagination(df3_query)

#top 10 collection by count of sale
df4_query="""
select
replace((PARSE_JSON(args):nft_contract_id),'.near') as collection,
count(distinct a.TX_HASH) as sales_count
from near.core.fact_actions_events_function_call a join near.core.fact_transactions b on a.tx_hash=b.tx_hash
where method_name = 'buy' 
and TRY_PARSE_JSON(args) is not null 
and TRY_CAST(TRY_PARSE_JSON(args):price::string as INT) is not null
group by 1
order by 2 desc 
limit 10
"""
df4 = querying_pagination(df4_query)

#top 10 collection by count of seller
df5_query="""
select
replace((PARSE_JSON(args):nft_contract_id),'.near') as collection,
count(DISTINCT b.tx_signer) AS sellers_count
from near.core.fact_actions_events_function_call a join near.core.fact_transactions b on a.tx_hash=b.tx_hash
where method_name = 'buy' 
and TRY_PARSE_JSON(args) is not null 
and TRY_CAST(TRY_PARSE_JSON(args):price::string as INT) is not null
group by 1
order by 2 desc 
limit 10
"""
df5 = querying_pagination(df5_query)

#top 10 collection by volume (USD) of sale
df6_query="""
with price as(select 
RECORDED_HOUR::date as date,
avg(close) as price 
from crosschain.core.fact_hourly_prices
where ID = 'near'
group by 1)
select
replace((PARSE_JSON(args):nft_contract_id),'.near') as collection,
(sum(PARSE_JSON(args):price::int*c.price))/pow(10,24) as sales_volume
from near.core.fact_actions_events_function_call a join near.core.fact_transactions b on a.tx_hash=b.tx_hash join price c on c.date=b.block_timestamp::date
where method_name = 'buy' 
and TRY_PARSE_JSON(args) is not null 
and TRY_CAST(TRY_PARSE_JSON(args):price::string as INT) is not null
group by 1
order by 2 desc 
limit 10
"""
df6 = querying_pagination(df6_query)

#top 10 user by count of sale
df7_query="""
select
DISTINCT b.tx_signer as user,
count(distinct a.TX_HASH) as sales_count
from near.core.fact_actions_events_function_call a join near.core.fact_transactions b on a.tx_hash=b.tx_hash
where method_name = 'buy' 
and TRY_PARSE_JSON(args) is not null 
and TRY_CAST(TRY_PARSE_JSON(args):price::string as INT) is not null
group by 1
order by 2 desc 
limit 10
"""
df7 = querying_pagination(df7_query)

#top 10 user by volume (USD) of sale
df8_query="""
with price as(select 
RECORDED_HOUR::date as date,
avg(close) as price 
from crosschain.core.fact_hourly_prices
where ID = 'near'
group by 1)
select
DISTINCT b.tx_signer as user,
(sum(PARSE_JSON(args):price::int*c.price))/pow(10,24) as sales_volume
from near.core.fact_actions_events_function_call a join near.core.fact_transactions b on a.tx_hash=b.tx_hash join price c on c.date=b.block_timestamp::date
where method_name = 'buy' 
and TRY_PARSE_JSON(args) is not null 
and TRY_CAST(TRY_PARSE_JSON(args):price::string as INT) is not null
group by 1
order by 2 desc 
limit 10
"""
df8 = querying_pagination(df8_query)

#daily mint
df9_query="""
select 
block_timestamp::date as date,
count(TX_HASH) as mint_count,
count(DISTINCT TX_SIGNER) minters_count
from near.core.ez_nft_mints 
where method_name = 'nft_mint'
group by 1
"""
df9 = querying_pagination(df9_query)

#total mint
df10_query="""
select 
count(TX_HASH) as mint_count,
count(DISTINCT TX_SIGNER) minters_count
from near.core.ez_nft_mints 
where method_name = 'nft_mint'
"""
df10 = querying_pagination(df10_query)

#user categorize by count of mint
df11_query="""
with main as(select 
DISTINCT TX_SIGNER as user,
count(TX_HASH) as mint_count
from near.core.ez_nft_mints 
where method_name = 'nft_mint'
group by 1)
select 
case
when mint_count = 1 then 'mint just once time' 
when mint_count  BETWEEN 2 and 5 then 'mint 2 - 5 time'  
when mint_count  BETWEEN 6 and 15 then 'mint 5 - 15 time'  
when mint_count BETWEEN 16 and 50 then 'mint 15 - 50 time'  
when mint_count BETWEEN 51 and 100 then 'mint 50 - 100 time'  
when mint_count BETWEEN 101 and 200 then 'mint 100 - 200 time'  
else 'mint than 200 time' end as tier,
count(distinct user) as minters_count
from main
where tier is not null
group by 1
"""
df11 = querying_pagination(df11_query)

#top 10 minters by count of mint
df12_query="""
select 
DISTINCT TX_SIGNER as user,
count(TX_HASH) as mint_count
from near.core.ez_nft_mints 
where method_name = 'nft_mint'
group by 1
order by 2 desc 
limit 10
"""
df12 = querying_pagination(df12_query)

st.write("""
 # Overal nft sale activity #

 The Daily Number of sellers , sale count and sale volume (USD) metrics is a measure of how many wallets / transaction with how much volume (USD) on NEAR are making nft sale on chain.

 """
)

cc1, cc2 , cc3 , cc4= st.columns([1, 1 , 1, 1])

with cc1:
  st.metric(
    value="{0:,.0f}".format(df1["sales_count"][0]),
    label="Total count of nft sale",
)
with cc2:
  st.metric(
    value="{0:,.0f}".format(df1["sellers_count"][0]),
    label="Total count of nft sellers",
)
with cc3:
  st.metric(
    value="{0:,.0f}".format(df111["avg_price"][0]),
    label="Average NFT's price",
)
with cc4:
  st.metric(
    value="{0:,.0f}".format(df111["sales_volume"][0]),
    label="Total volume (USD) of nft sale",
)


st.subheader('Daily count of nft sales')
st.caption('Daily count of nft sales')
st.bar_chart(df, x='date', y = 'sales_count', width = 400, height = 400)

st.subheader('Daily count of nft sellers')
st.caption('Daily count of nft sellers')
st.bar_chart(df, x='date', y = 'sellers_count', width = 400, height = 400)

st.subheader('Daily Average nft prices')
st.caption('Daily Average nft prices')
st.line_chart(df110, x='date', y = 'avg_price', width = 400, height = 400)

st.subheader('Daily volume (USD) of nft sales')
st.caption('Daily volume (USD) of nft sales')
st.bar_chart(df110, x='date', y = 'sales_volume', width = 400, height = 400)

st.write("""
 # User categorize by count and volume (USD) of nft sale #

 Here the sellers are categorized based on the number and volume (USD) of the nft sale.

 """
)
cc1, cc2 = st.columns([1, 1])

with cc1:
 st.subheader('User categorize by count of nft sale')
 st.caption('User categorize by count of nft sale')
 st.bar_chart(df2, x='tier', y = 'sellers_count', width = 400, height = 400)
with cc2:
 fig = px.pie(df2, values='sellers_count', names='tier', title='User categorize by rate of nft sale count')
 fig.update_layout(legend_title=None, legend_y=0.5)
 fig.update_traces(textinfo='percent+label', textposition='inside')
 st.plotly_chart(fig, use_container_width=True, theme=None)

cc1, cc2 = st.columns([1, 1])

with cc1:
 st.subheader('User categorize by volume (USD) of nft sale')
 st.caption('User categorize by volume (USD) of nft sale')
 st.bar_chart(df3, x='tier', y = 'sellers_count', width = 400, height = 400)
with cc2:
 fig = px.pie(df3, values='sellers_count', names='tier', title='User categorize by rate of nft sale volume (USD)')
 fig.update_layout(legend_title=None, legend_y=0.5)
 fig.update_traces(textinfo='percent+label', textposition='inside')
 st.plotly_chart(fig, use_container_width=True, theme=None)

st.write("""
 # Top 10 collection #

 Top collection are based on the number and volume (USD) of the nft sale and sellers are:

 """
)
cc1, cc2, cc3 = st.columns([1, 1,1])

with cc1:
 st.caption('Top 5 collection by count of nft sale')
 st.bar_chart(df4, x='collection', y = 'sales_count', width = 400, height = 400)

with cc2:
 st.caption('Top 5 collection by count of nft seller')
 st.bar_chart(df5, x='collection', y = 'sellers_count', width = 400, height = 400)

with cc3:
 st.caption('Top 10 collection by volume (USD) of nft sale')
 st.bar_chart(df6, x='collection', y = 'sales_volume', width = 400, height = 400)


st.write("""
 # Top 10 User #

 Top users are based on the number and volume (USD) of the nft sale are:

 """
)

cc1, cc2 = st.columns([1, 1])

with cc1:
 st.caption('Top 10 user by count of nft sale')
 st.bar_chart(df7, x='user', y = 'sales_count', width = 400, height = 400)
with cc2:
 st.caption('Top 10 user by volume (USD) of nft sale')
 st.bar_chart(df8, x='user', y = 'sales_volume', width = 400, height = 400)

st.write("""
 # Overal mint activity #

 The Daily Number of minters and mint count metrics is a measure of how many wallets / transaction on NEAR are nft mint on chain.

 """
)

cc1, cc2 = st.columns([1, 1])

with cc1:
  st.metric(
    value="{0:,.0f}".format(df10["mint_count"][0]),
    label="Total count of nft mint",
)
with cc2:
  st.metric(
    value="{0:,.0f}".format(df10["minters_count"][0]),
    label="Total count of nft minters",
)

st.subheader('Daily count of nft mint')
st.caption('Daily count of nft mint')
st.bar_chart(df9, x='date', y = 'mint_count', width = 400, height = 400)

st.subheader('Daily count of nft minters')
st.caption('Daily count of nft minters')
st.bar_chart(df9, x='date', y = 'minters_count', width = 400, height = 400)


st.write("""
 # User categorize by count of nft mint #

 Here the sellers are categorized based on the number of the nft mint.

 """
)
cc1, cc2 = st.columns([1, 1])

with cc1:
 st.subheader('User categorize by count of nft mint')
 st.caption('User categorize by count of nft mint')
 st.bar_chart(df11, x='tier', y = 'minters_count', width = 400, height = 400)
with cc2:
 fig = px.pie(df11, values='minters_count', names='tier', title='User categorize by rate of nft mint count')
 fig.update_layout(legend_title=None, legend_y=0.5)
 fig.update_traces(textinfo='percent+label', textposition='inside')
 st.plotly_chart(fig, use_container_width=True, theme=None)

st.subheader('Top 10 user by count of nft mint')
st.caption('Top 10 user by count of nft mint')
st.bar_chart(df12, x='user', y = 'mint_count', width = 400, height = 400)
