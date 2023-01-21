import streamlit as st
from shroomdk import ShroomDK
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="💱 Swap activity",
    layout= "wide",
    page_icon="💱",
)
st.title("💱 Swap activity")
st.sidebar.success("💱 Swap activity")
 
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
#daily swap
df_query="""
with price as(select 
timestamp::date as date,
symbol,
price_usd as price
from near.core.fact_prices
group by 1,2,3
)
select 
block_timestamp::date as date,
count(tx_hash) as "count of swap",
count(distinct TRADER) as "count of swapper",
sum(AMOUNT_in*price) as "swap volume"
from near.core.ez_dex_swaps a join price b on a.token_in = b.symbol and a.block_timestamp::date = b.date
where AMOUNT_in is not null
group by 1
"""
df = querying_pagination(df_query)


df1_query="""
with price as(select 
timestamp::date as date,
symbol,
price_usd as price
from near.core.fact_prices
group by 1,2,3
)
select 
count(tx_hash) as swap_count,
count(distinct TRADER) as swapper_count,
sum(AMOUNT_in*price) as volume
from near.core.ez_dex_swaps a join price b on a.token_in = b.symbol and a.block_timestamp::date = b.date
where AMOUNT_in is not null
"""
df1 = querying_pagination(df1_query)

#Total swap
df2_query="""
with price as(select 
timestamp::date as date,
symbol,
price_usd as price
from near.core.fact_prices
group by 1,2,3
)
select 
concat(token_in,' - ',token_out) as "Pool",
count(tx_hash) as count_swap,
count(distinct TRADER) as count_swapper,
sum(AMOUNT_in*price) as volume_swap
from near.core.ez_dex_swaps a join price b on a.token_in = b.symbol and a.block_timestamp::date = b.date
where AMOUNT_in is not null
group by 1
"""
df2 = querying_pagination(df2_query)

#categorize (count of swap)
df3_query="""
with price as(select 
timestamp::date as date,
symbol,
price_usd as price
from near.core.fact_prices
group by 1,2,3
), main as(select 
distinct TRADER as user,
count(tx_hash) as count_swap,
sum(AMOUNT_in*price) as volume_swap
from near.core.ez_dex_swaps a join price b on a.token_in = b.symbol and a.block_timestamp::date = b.date
where AMOUNT_in is not null
group by 1
  )
select
case 
when count_swap = 1 then 'swap just one time'
when count_swap between 2 and 5 then 'swap 2 - 5 time'
when count_swap between 6 and 10 then 'swap 6 - 10 time'
when count_swap between 11 and 20 then 'swap 11 - 20 time'
when count_swap between 21 and 50 then 'swap 21 - 50 time'
when count_swap between 51 and 100 then 'swap 51 - 100 time'
when count_swap between 101 and 200 then 'swap 101 - 200 time'
when count_swap between 201 and 500 then 'swap 201 - 500 time'
when count_swap between 501 and 1000 then 'swap 501 - 1000 time'
when count_swap between 1001 and 5000 then 'swap 1001 - 5000 time'
when count_swap between 5001 and 10000 then 'swap 5001 - 10000 time'
else 'swap more than 10000 time'
end as swap_count,
count(user) as count_swappers
from main
group by 1
"""
df3 = querying_pagination(df3_query)

#Categorize (volume (usd) of swap)
df4_query="""
with price as(select 
timestamp::date as date,
symbol,
price_usd as price
from near.core.fact_prices
group by 1,2,3
), main as(select 
distinct TRADER as user,
count(tx_hash) as count_swap,
sum(AMOUNT_in*price) as volume_swap
from near.core.ez_dex_swaps a join price b on a.token_in = b.symbol and a.block_timestamp::date = b.date
where AMOUNT_in is not null
group by 1
  )
select
case 
when volume_swap < 100 then 'Swap less than 100 USD'
when volume_swap between 100 and 499.99 then 'Swap 100 - 500 USD'
when volume_swap between 500 and 999.99 then 'Swap 500 - 1 K USD'
when volume_swap between 10000 and 2499.99 then 'Swap 1 K - 2.5 K USD'
when volume_swap between 2500 and 4999.99 then 'Swap 2.5 K - 5 K USD'
when volume_swap between 5000 and 9999.99 then 'Swap 5 K - 10 K USD'
when volume_swap between 10000 and 19999.99 then 'Swap 10 K - 20 K USD'
when volume_swap between 20000 and 49999.99 then 'Swap 20 K - 50 K USD'
when volume_swap between 50000 and 99999.99 then 'Swap 50 K - 100 K USD'
when volume_swap between 100000 and 249999.99 then 'Swap 100 K - 250 K USD'
when volume_swap between 250000 and 499999.99 then 'Swap 250 K - 500 K USD'
when volume_swap between 500000 and 999999.99 then 'Swap 500 K - 1 M USD'
else 'Swap more than 1 M USD'
end as swap_volume,
count(user) as count_swappers
from main
group by 1
"""
df4 = querying_pagination(df4_query)

#Top 5 platform by count
df5_query="""
with price as(select 
timestamp::date as date,
symbol,
price_usd as price
from near.core.fact_prices
group by 1,2,3
)
select 
distinct platform as platform,
count(tx_hash) as count_swap
from near.core.ez_dex_swaps a join price b on a.token_in = b.symbol and a.block_timestamp::date = b.date
where AMOUNT_in is not null
and platform is not null
group by 1
order by 2 desc 
limit 10
"""
df5 = querying_pagination(df5_query)

#Top 5 platform by volume
df6_query="""
with price as(select 
timestamp::date as date,
symbol,
price_usd as price
from near.core.fact_prices
group by 1,2,3
)
select 
distinct platform as platform,
sum(AMOUNT_in*price) as volume_swap
from near.core.ez_dex_swaps a join price b on a.token_in = b.symbol and a.block_timestamp::date = b.date
where AMOUNT_in is not null
and platform is not null
group by 1
order by 2 desc 
limit 10
"""
df6 = querying_pagination(df6_query)

#Top 10 user by count
df7_query="""
select 
distinct TRADER as swapper,
count(tx_hash) as count_swap
from near.core.ez_dex_swaps
group by 1
order by 2 desc 
limit 10
"""
df7 = querying_pagination(df7_query)

#Top 10 user by volume
df8_query="""
with price as(select 
timestamp::date as date,
symbol,
price_usd as price
from near.core.fact_prices
group by 1,2,3
)
select 
distinct TRADER as swapper,
sum(AMOUNT_in*price) as volume_swap
from near.core.ez_dex_swaps a join price b on a.token_in = b.symbol and a.block_timestamp::date = b.date
where AMOUNT_in is not null
group by 1
order by 2 desc 
limit 10
"""
df8 = querying_pagination(df8_query)
st.write("""
 # Overal swap activity #

 The Daily Number of swappers , swap count and swap volume (USD) metrics is a measure of how many wallets / transaction with how much volume (USD) on NEAR are making swap on chain.

 """
)

cc1, cc2 , cc3 = st.columns([1, 3 , 2])

with cc1:
  st.metric(
    value="{0:,.0f}".format(df1["swap_count"][0]),
    label="Total count of swap",
)
with cc2:
  st.metric(
    value="{0:,.0f}".format(df1["swapper_count"][0]),
    label="Total count of swappers",
)
with cc3:
  st.metric(
    value="{0:,.0f}".format(df1["volume"][0]),
    label="Total volume (USD) of swap",
)

st.subheader('Daily count of swaps')
st.caption('Daily count of swaps')
st.bar_chart(df, x='date', y = 'count of swap', width = 400, height = 400)

st.subheader('Daily count of swappers')
st.caption('Daily count of swappers')
st.bar_chart(df, x='date', y = 'count of swapper', width = 400, height = 400)

st.subheader('Daily volume (USD) of swaps')
st.caption('Daily volume (USD) of swaps')
st.bar_chart(df, x='date', y = 'swap volume', width = 400, height = 400)

st.write("""
 # Swap activity at each pool #

 The Daily Number of swappers , swap count and swap volume (USD) metrics is a measure of how many wallets / transaction with how much volume (USD) on NEAR are making swap at each pool.

 """
)

st.caption('Total count of swaps at each pool')
st.bar_chart(df2, x='pool', y = 'count_swap', width = 400, height = 400)

st.caption('Total count of swappers at each pool')
st.bar_chart(df2, x='pool', y = 'count_swapper', width = 400, height = 400)

st.caption('Total volume (USD) of swaps at each pool')
st.bar_chart(df2, x='pool', y = 'volume_swap', width = 400, height = 400)

st.write("""
 # User categorize by count and volume (USD) of swap #

 Here the swappers are categorized based on the number and volume (USD) of the swap.

 """
)
cc1, cc2 = st.columns([1, 1])

with cc1:
 st.subheader('User categorize by count of swap')
 st.caption('User categorize by count of swap')
 st.bar_chart(df3, x='swap_count', y = 'count_swappers', width = 400, height = 400)
with cc2:
 fig = px.pie(df3, values='count_swappers', names='swap_count', title='User categorize by rate of swap count')
 fig.update_layout(legend_title=None, legend_y=0.5)
 fig.update_traces(textinfo='percent+label', textposition='inside')
 st.plotly_chart(fig, use_container_width=True, theme=None)

cc1, cc2 = st.columns([1, 1])

with cc1:
 st.subheader('User categorize by volume (USD) of swap')
 st.caption('User categorize by volume (USD) of swap')
 st.bar_chart(df4, x='swap_volume', y = 'count_swappers', width = 400, height = 400)
with cc2:
 fig = px.pie(df4, values='count_swappers', names='swap_volume', title='User categorize by rate of swap volume (USD)')
 fig.update_layout(legend_title=None, legend_y=0.5)
 fig.update_traces(textinfo='percent+label', textposition='inside')
 st.plotly_chart(fig, use_container_width=True, theme=None)

st.write("""
 # Top 10 platform #

 Top platform are based on the number and volume (USD) of the swap are:

 """
)

st.subheader('Top 10 platform by count of swap')
st.caption('Top 10 platform by count of swap')
st.bar_chart(df5, x='platform', y = 'count_swap', width = 400, height = 400)

st.subheader('Top 10 platform by volume (USD) of swap')
st.caption('Top 10 platform by volume (USD) of swap')
st.bar_chart(df6, x='platform', y = 'volume_swap', width = 400, height = 400)


st.write("""
 # Top 10 User #

 Top users are based on the number and volume (USD) of the swap are:

 """
)


st.subheader('Top 10 Swapper by count of swap')
st.caption('Top 10 Swapper by count of swap')
st.bar_chart(df7, x='swapper', y = 'count_swap', width = 400, height = 400)

st.subheader('Top 10 Swapper by volume (USD) of swap')
st.caption('Top 10 Swapper by volume (USD) of swap')
st.bar_chart(df8, x='swapper', y = 'volume_swap', width = 400, height = 400)
