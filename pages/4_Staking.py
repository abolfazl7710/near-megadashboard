import streamlit as st
from shroomdk import ShroomDK
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="🥩 Staking activity",
    layout= "wide",
    page_icon="🥩",
)
st.title("🥩 Staking activity")
st.sidebar.success("🥩 Staking activity")

 
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
#daily stake
df_query="""
with price as(select 
timestamp::date as date, 
avg(price_usd) as price
from near.core.fact_prices
where symbol ilike '%near%'
group by 1
  ),stake as(select 
block_timestamp::date as date, 
action,
tx_hash, 
tx_signer, 
stake_amount/pow(10,24) as raw_amount
from near.core.dim_staking_actions
  ),main as(select 
a.date, 
action,
tx_hash, 
tx_signer,
raw_amount,
raw_amount*price as stake_usd
from stake a left outer join price b on a.date = b.date
  )
select 
date,
action,
count(DISTINCT(tx_signer)) as count_staker,
count(DISTINCT(tx_hash)) as count_stake,
sum(raw_amount) as stake_volume_near,
sum(stake_usd) as stake_volume_usd
from main
group by 1,2
"""
df = querying_pagination(df_query)

#Avg day holding stake
df1_query="""
with stake as(select 
date(block_timestamp) as date,
tx_signer
from near.core.dim_staking_actions
where action = 'Stake'
group by 1,2
  ),unstake as(select 
date(block_timestamp) as date,
tx_signer
from near.core.dim_staking_actions
where action = 'Unstake'
group by 1,2
  ), main as(select 
b.date, 
datediff('day',a.date,b.date) as n_day
from stake a join unstake b on a.tx_signer=b.tx_signer
group by 1,2)
select 
date,
avg(n_day) as average_days
from main 
where n_day > 0
group by 1
"""
df1 = querying_pagination(df1_query)

#Total stake
df2_query="""
with price as(select 
timestamp::date as date, 
avg(price_usd) as price
from near.core.fact_prices
where symbol ilike '%near%'
group by 1
  ),stake as(select 
block_timestamp::date as date,
action,
tx_hash, 
tx_signer, 
stake_amount/pow(10,24) as raw_amount
from near.core.dim_staking_actions
  ),main as(select 
a.date, 
action,
tx_hash, 
tx_signer,
raw_amount,
raw_amount*price as stake_usd
from stake a left outer join price b on a.date = b.date
  )
select 
action,
count(DISTINCT(tx_signer)) as count_staker,
count(DISTINCT(tx_hash)) as count_stake,
sum(raw_amount) as stake_volume_near,
sum(stake_usd) as stake_volume_usd
from main
group by 1
"""
df2 = querying_pagination(df2_query)

#categorize (count of stake)
df3_query="""
with main as(select 
tx_signer as user,
count(DISTINCT(tx_hash)) as count_action
from near.core.dim_staking_actions
where action = 'Stake'
group by 1
  )
select
case 
when count_action = 1 then 'action just one time'
when count_action between 2 and 5 then 'action 2 - 5 time'
when count_action between 6 and 10 then 'action 6 - 10 time'
when count_action between 11 and 20 then 'action 11 - 20 time'
when count_action between 21 and 50 then 'action 21 - 50 time'
when count_action between 51 and 100 then 'action 51 - 100 time'
when count_action between 101 and 200 then 'action 101 - 200 time'
when count_action between 201 and 500 then 'action 201 - 500 time'
when count_action between 501 and 1000 then 'action 501 - 1000 time'
when count_action between 1001 and 5000 then 'action 1001 - 5000 time'
when count_action between 5001 and 10000 then 'action 5001 - 10000 time'
else 'action more than 10000 time'
end as stake_count,
count(user) as count_staker
from main
group by 1
"""
df3 = querying_pagination(df3_query)

#categorize (count of unstake)
df31_query="""
with main as(select 
tx_signer as user,
count(DISTINCT(tx_hash)) as count_action
from near.core.dim_staking_actions
where action = 'Unstake'
group by 1
  )
select
case 
when count_action = 1 then 'action just one time'
when count_action between 2 and 5 then 'action 2 - 5 time'
when count_action between 6 and 10 then 'action 6 - 10 time'
when count_action between 11 and 20 then 'action 11 - 20 time'
when count_action between 21 and 50 then 'action 21 - 50 time'
when count_action between 51 and 100 then 'action 51 - 100 time'
when count_action between 101 and 200 then 'action 101 - 200 time'
when count_action between 201 and 500 then 'action 201 - 500 time'
when count_action between 501 and 1000 then 'action 501 - 1000 time'
when count_action between 1001 and 5000 then 'action 1001 - 5000 time'
when count_action between 5001 and 10000 then 'action 5001 - 10000 time'
else 'action more than 10000 time'
end as unstake_count,
count(user) as count_unstaker
from main
group by 1
"""
df31 = querying_pagination(df31_query)

#Categorize (volume (NEAR) of stake)
df4_query="""
with main as(select 
tx_signer as user,
sum(stake_amount)/pow(10,24) as action_volume_NEAR
from near.core.dim_staking_actions
where action = 'Stake'
group by 1
  )
select
case 
when action_volume_NEAR < 100 then 'Action volume less than 100 NEAR'
when action_volume_NEAR between 100 and 499.99 then 'Action volume 100 - 500 NEAR'
when action_volume_NEAR between 500 and 999.99 then 'Action volume 500 - 1 K NEAR'
when action_volume_NEAR between 10000 and 2499.99 then 'Action volume 1 K - 2.5 K NEAR'
when action_volume_NEAR between 2500 and 4999.99 then 'Action volume 2.5 K - 5 K NEAR'
when action_volume_NEAR between 5000 and 9999.99 then 'Action volume 5 K - 10 K NEAR'
when action_volume_NEAR between 10000 and 19999.99 then 'Action volume 10 K - 20 K NEAR'
when action_volume_NEAR between 20000 and 49999.99 then 'Action volume 20 K - 50 K NEAR'
when action_volume_NEAR between 50000 and 99999.99 then 'Action volume 50 K - 100 K NEAR'
when action_volume_NEAR between 100000 and 249999.99 then 'Action volume 100 K - 250 K NEAR'
when action_volume_NEAR between 250000 and 499999.99 then 'Action volume 250 K - 500 K NEAR'
when action_volume_NEAR between 500000 and 999999.99 then 'Action volume 500 K - 1 M NEAR'
else 'Action volume more than 1 M NEAR'
end as stake_volume,
count(user) as count_staker
from main
group by 1
"""
df4 = querying_pagination(df4_query)

#Categorize (volume (NEAR) of unstake)
df41_query="""
with main as(select 
tx_signer as user,
sum(stake_amount)/pow(10,24) as action_volume_NEAR
from near.core.dim_staking_actions
where action = 'Unstake'
group by 1
  )
select
case 
when action_volume_NEAR < 100 then 'Action volume less than 100 NEAR'
when action_volume_NEAR between 100 and 499.99 then 'Action volume 100 - 500 NEAR'
when action_volume_NEAR between 500 and 999.99 then 'Action volume 500 - 1 K NEAR'
when action_volume_NEAR between 10000 and 2499.99 then 'Action volume 1 K - 2.5 K NEAR'
when action_volume_NEAR between 2500 and 4999.99 then 'Action volume 2.5 K - 5 K NEAR'
when action_volume_NEAR between 5000 and 9999.99 then 'Action volume 5 K - 10 K NEAR'
when action_volume_NEAR between 10000 and 19999.99 then 'Action volume 10 K - 20 K NEAR'
when action_volume_NEAR between 20000 and 49999.99 then 'Action volume 20 K - 50 K NEAR'
when action_volume_NEAR between 50000 and 99999.99 then 'Action volume 50 K - 100 K NEAR'
when action_volume_NEAR between 100000 and 249999.99 then 'Action volume 100 K - 250 K NEAR'
when action_volume_NEAR between 250000 and 499999.99 then 'Action volume 250 K - 500 K NEAR'
when action_volume_NEAR between 500000 and 999999.99 then 'Action volume 500 K - 1 M NEAR'
else 'Action volume more than 1 M NEAR'
end as unstake_volume,
count(user) as count_unstaker
from main
group by 1
"""
df41 = querying_pagination(df41_query)

#Top 10 stake pool by count
df5_query="""
select 
replace((pool_address),'.near') as pool,
count(DISTINCT(tx_hash)) as count_stake
from near.core.dim_staking_actions
where action = 'Stake'
group by 1
order by 2 desc
limit 10
"""
df5 = querying_pagination(df5_query)

#Top 10 stake pool by volume
df6_query="""
select 
replace((pool_address),'.near') as pool,
sum(stake_amount)/pow(10,24) as stake_volume_near
from near.core.dim_staking_actions
where action = 'Stake'
group by 1
order by 2 desc
limit 10
"""
df6 = querying_pagination(df6_query)

#Top 10 staker by count
df7_query="""
select 
tx_signer as user,
count(DISTINCT(tx_hash)) as count_stake
from near.core.dim_staking_actions
where action = 'Stake'
group by 1
order by 2 desc
limit 10
"""
df7 = querying_pagination(df7_query)

#Top 10 unstaker by count
df71_query="""
select 
tx_signer as user,
count(DISTINCT(tx_hash)) as count_unstake
from near.core.dim_staking_actions
where action = 'Unstake'
group by 1
order by 2 desc
limit 10
"""
df71 = querying_pagination(df71_query)

#Top 10 staker by volume
df8_query="""
select 
tx_signer as user,
sum(stake_amount)/pow(10,24) as stake_volume_near
from near.core.dim_staking_actions
where action = 'Stake'
group by 1
order by 2 desc
limit 10
"""
df8 = querying_pagination(df8_query)

#Top 10 unstaker by volume
df81_query="""
select 
tx_signer as user,
sum(stake_amount)/pow(10,24) as unstake_volume_near
from near.core.dim_staking_actions
where action = 'Unstake'
group by 1
order by 2 desc
limit 10
"""
df81 = querying_pagination(df81_query)
st.write("""
 # Overal stake activity #

 The Daily Number of stakers , stake count and stake volume metrics is a measure of how many wallets / transaction with how much volume on NEAR are staked on chain.

 """
)

cc1, cc2 , cc3 , cc4 = st.columns([1, 1 , 1 , 1])

with cc1:
 fig = px.bar(df, x='action', y='count_stake', color='action', title='Total count of stake at each action')
 st.plotly_chart(fig, use_container_width=True, theme=None)
with cc2:
 fig = px.bar(df, x='action', y='count_staker', color='action', title='Total count of staker at each action')
 st.plotly_chart(fig, use_container_width=True, theme=None)
with cc3:
 fig = px.bar(df, x='action', y='stake_volume_near', color='action', title='Total volume (Near) of stake at each action')
 st.plotly_chart(fig, use_container_width=True, theme=None)
with cc4:
 fig = px.bar(df, x='action', y='stake_volume_usd', color='action', title='Total volume (USD) of stake at each action')
 st.plotly_chart(fig, use_container_width=True, theme=None)

fig = px.bar(df, x='date', y='count_stake', color='action', title='Daily count of stake at each action')
st.plotly_chart(fig, use_container_width=True, theme=None)

fig = px.bar(df, x='date', y='count_staker', color='action', title='Daily count of staker at each action')
st.plotly_chart(fig, use_container_width=True, theme=None)

fig = px.bar(df, x='date', y='stake_volume_near', color='action', title='Daily volume (Near) of stake at each action')
st.plotly_chart(fig, use_container_width=True, theme=None)

fig = px.bar(df, x='date', y='stake_volume_usd', color='action', title='Daily volume (USD) of stake at each action')
st.plotly_chart(fig, use_container_width=True, theme=None)

st.subheader('Average day holding stake')
st.caption('Average day holding stake')
st.line_chart(df1, x='date', y = 'average_days', width = 400, height = 400)

st.write("""
 # User categorize by count and volume (USD) of stake / unstake #

 Here the stakers / unstakers are categorized based on the number and volume (USD) of the stake.

 """
)
cc1, cc2 = st.columns([1, 1])

with cc1:
 st.subheader('User categorize by count of stake')
 st.caption('User categorize by count of stake')
 st.bar_chart(df3, x='stake_count', y = 'count_staker', width = 400, height = 400)
with cc2:
 st.subheader('User categorize by rate of stake count')
 fig = px.pie(df3, values='count_staker', names='stake_count', title='User categorize by rate of stake count')
 fig.update_layout(legend_title=None, legend_y=0.5)
 fig.update_traces(textinfo='percent+label', textposition='inside')
 st.plotly_chart(fig, use_container_width=True, theme=None)

cc1, cc2 = st.columns([1, 1])

with cc1:
 st.subheader('User categorize by count of unstake')
 st.caption('User categorize by count of unstake')
 st.bar_chart(df31, x='unstake_count', y = 'count_unstaker', width = 400, height = 400)
with cc2:
 st.subheader('User categorize by rate of unstake count')
 fig = px.pie(df31, values='count_unstaker', names='unstake_count', title='User categorize by rate of unstake count')
 fig.update_layout(legend_title=None, legend_y=0.5)
 fig.update_traces(textinfo='percent+label', textposition='inside')
 st.plotly_chart(fig, use_container_width=True, theme=None)

cc1, cc2 = st.columns([1, 1])

with cc1:
 st.subheader('User categorize by volume (Near) of stake')
 st.caption('User categorize by volume (Near) of stake')
 st.bar_chart(df4, x='stake_volume', y = 'count_staker', width = 400, height = 400)
with cc2:
 st.subheader('User categorize by rate of stake volume (Near)')
 fig = px.pie(df4, values='count_staker', names='stake_volume', title='User categorize by rate of stake volume (Near)')
 fig.update_layout(legend_title=None, legend_y=0.5)
 fig.update_traces(textinfo='percent+label', textposition='inside')
 st.plotly_chart(fig, use_container_width=True, theme=None)

cc1, cc2 = st.columns([1, 1])

with cc1:
 st.subheader('User categorize by volume (Near) of unstake')
 st.caption('User categorize by volume (Near) of unstake')
 st.bar_chart(df41, x='unstake_volume', y = 'count_unstaker', width = 400, height = 400)
with cc2:
 st.subheader('User categorize by rate of unstake volume (Near)')
 fig = px.pie(df41, values='count_unstaker', names='unstake_volume', title='User categorize by rate of unstake volume (Near)')
 fig.update_layout(legend_title=None, legend_y=0.5)
 fig.update_traces(textinfo='percent+label', textposition='inside')
 st.plotly_chart(fig, use_container_width=True, theme=None)

st.write("""
 # Top 10 pool #

 Top pool are based on the number and volume (Near) of the stake are:

 """
)
cc1, cc2 = st.columns([1, 1])

with cc1:
 st.subheader('Top 10 pool by count and volume of stake')
 st.caption('Top 10 pool by count of stake')
 st.bar_chart(df5, x='pool', y = 'count_stake', width = 400, height = 400)
with cc2:
 st.subheader('Top 10 pool by volume (Near) of stake')
 st.caption('Top 10 pool by volume (Near) of stake')
 st.bar_chart(df6, x='pool', y = 'stake_volume_near', width = 400, height = 400)


st.write("""
 # Top 10 stakers and unstakers #

 Top stakers and unstakers are based on the number and volume (Near) of the stake are:

 """
)

cc1, cc2 = st.columns([1, 1])

with cc1:
 st.subheader('Top 10 stakers and unstakers by count of action')
 st.caption('Top 10 stakers by count of stake')
 st.bar_chart(df7, x='user', y = 'count_stake', width = 400, height = 400)
with cc2:
 st.subheader('Top 10 unstakers by count of unstake')
 st.caption('Top 10 unstakers by count of unstake')
 st.bar_chart(df71, x='user', y = 'count_unstake', width = 400, height = 400)

cc1, cc2 = st.columns([1, 1])

with cc1:
 st.subheader('Top 10 stakers and unstakers by volume (Near) of action')
 st.caption('Top 10 stakers by volume (Near) of stake')
 st.bar_chart(df8, x='user', y = 'stake_volume_near', width = 400, height = 400)
with cc2:
 st.subheader('Top 10 unstakers by volume (Near) of unstake')
 st.caption('Top 10 unstakers by volume (Near) of unstake')
 st.bar_chart(df81, x='user', y = 'unstake_volume_near', width = 400, height = 400)