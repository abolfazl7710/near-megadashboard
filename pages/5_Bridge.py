import streamlit as st
from shroomdk import ShroomDK
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="🌉 Bridge activity",
    layout= "wide",
    page_icon="🌉",
)
st.title("🌉 Bridge activity")
st.sidebar.success("🌉 Bridge activity")

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
with bridge_vol AS (select
date_trunc('week',block_timestamp) as date,
case when symbol = 'WETH' then 'ETH' else symbol end as symbol,
avg(current_bal) as balance
from ethereum.core.ez_balance_deltas
where user_address in ('0x23ddd3e3692d1861ed57ede224608875809e127f','0x6bfad42cfc4efc96f529d786d643ff4a8b89fa52')
and symbol is not null 
group by 1,2
order by 1,2
),current_price as(select
symbol as symbol,
avg(price) as usd
from ethereum.core.fact_hourly_token_prices
where symbol IN (select distinct symbol from bridge_vol)
and current_date - date(hour) < 1
group by 1
)
select 
a.symbol,
balance*usd as bal_usd,
sum(balance*usd) over (order by bal_usd asc) as tvl_usd
from bridge_vol a join current_price b on a.symbol = b.symbol
order by date, bal_usd desc
"""
df = querying_pagination(df_query)

df1_query="""
with bridge_vol AS (select
date_trunc('week',block_timestamp) as date,
case when symbol = 'WETH' then 'ETH' else symbol end as symbol,
avg(current_bal) as balance
from ethereum.core.ez_balance_deltas
where user_address in ('0x23ddd3e3692d1861ed57ede224608875809e127f','0x6bfad42cfc4efc96f529d786d643ff4a8b89fa52')
and symbol is not null 
group by 1,2
order by 1,2
),current_price as(select
symbol as symbol,
avg(price) as usd
from ethereum.core.fact_hourly_token_prices
where symbol IN (select distinct symbol from bridge_vol)
and current_date - date(hour) < 1
group by 1
)
select 
date::date as date,
a.symbol,
balance*usd as bal_usd,
sum(balance*usd) over (order by bal_usd asc) as tvl_usd
from bridge_vol a join current_price b on a.symbol = b.symbol
order by date, bal_usd desc
"""
df1 = querying_pagination(df1_query)

#credit:https://app.flipsidecrypto.com/velocity/queries/c9019161-5c94-43f0-9fe9-8aa5f28e4924  pinehearst
df2_query="""
with bridge_native_eth_tx AS  (
SELECT 
tx_hash as tx_bridge,
substr(data, 259, len(data)) as address_sub,
len(address_sub) as length, 
case when length = 64 and address_sub LIKE '%17200%' then CONCAT(split_part(address_sub, '17200', 0), '172')
when length = 64 and address_sub LIKE '%172172%' then CONCAT(split_part(address_sub, '172172', 0), '172') 
else address_sub
end as address_edited,
TRY_HEX_DECODE_STRING(address_edited) as recipient
FROM ethereum.core.fact_event_logs 
WHERE origin_function_signature = '0xa8eb3b51'
AND origin_to_address = '0x6bfad42cfc4efc96f529d786d643ff4a8b89fa52' 
AND tx_status = 'SUCCESS'
),
erc20_bridge_tx AS  (
SELECT 
tx_hash as tx_hash_erc20, 
case when event_inputs:accountId LIKE '%aurora:%' then 'AURORA' else 'NEAR' end as bridge_destination,
event_inputs:accountId::string as recipient, 
event_inputs:sender::string as sender
FROM ethereum.core.fact_Event_logs 
WHERE origin_function_signature IN ('0x0889bfe7') 
AND contract_address = '0x23ddd3e3692d1861ed57ede224608875809e127f' 
AND tx_status = 'SUCCESS'
AND bridge_destination = 'NEAR'
),
eth_bridge AS (
SELECT 
block_timestamp,
tx_hash,
'NEAR' as bridge_destination,
origin_from_address as sender,
recipient,
'ETH' as symbol,
amount,
amount_usd
FROM bridge_native_eth_tx 
LEFT JOIN ethereum.core.ez_eth_transfers ON tx_bridge = tx_hash 
WHERE block_timestamp IS NOT NULL 
),
erc20_bridge AS (
SELECT
block_timestamp,
tx_hash,
bridge_destination,
nvl(sender, origin_from_address) as sender,
recipient,
symbol,
amount,
amount_usd
FROM erc20_bridge_tx 
LEFT JOIN ethereum.core.ez_token_transfers ON tx_Hash = tx_hash_erc20
WHERE block_timestamp IS NOT NULL 
),
BRIDGE_TX_FINAL AS (
SELECT
block_timestamp,
tx_hash,
sender,
recipient,
'Bridge to NEAR' as event_action,
symbol,
amount,
amount_usd
FROM erc20_bridge
UNION
SELECT
block_timestamp,
tx_hash,
sender,
recipient, 
'Bridge to NEAR' as event_action,
symbol,
  amount,
  amount_usd
FROM eth_bridge
),
near_events AS (
 SELECT 
  block_timestamp,
  tx_hash,
  receiver_id,
  replace(value, 'EVENT_JSON:') as logs_cleaned,
  split(logs_cleaned, ' ') as split_logs,
  nvl(try_parse_json(logs_cleaned):event, try_parse_json(logs_cleaned):type) as event,
  case 
  when event = 'nft_mint' then 'Mint NFT'
  when receiver_id like '%pool%' and split_logs[1] = 'staking' then 'Stake NEAR'
  when receiver_id = 'meta-pool.near' and event = 'STAKE' then 'Liquid Stake NEAR'
  when receiver_id = 'linear-protocol.near' and event = 'stake' then 'Liquid Stake NEAR'
  when receiver_id = 'nearx.stader-labs.near' and event = 'deposit_and_stake' then 'Liquid Stake NEAR'
  when split_logs[0] = 'Liquidity' then 'Add Liquidity'
  when split_logs[0] = 'Swapped' then 'Swap on Dex' 
  when receiver_id = 'usn' and event = 'ft_mint' then 'Mint USN'
  when receiver_id = 'contract.main.burrow.near' and event = 'borrow' then 'Borrow on Burrow'
  when receiver_id = 'contract.main.burrow.near' and event = 'deposit' then 'Deposit on Burrow'
  else null end as event_action,
  regexp_substr(status_value, 'Success') as reg_success
FROM near.core.fact_receipts,
  table(flatten(input =>logs))
WHERE reg_success IS NOT NULL 
),
ALL_TX AS (
SELECT 
  block_timestamp,
  tx_hash,
  tx_signer as recipient,
  receiver_id,
  event_action
FROM near_events
  LEFT JOIN (SELECT tx_signer, tx_hash as tx_hash1 from near.core.fact_transactions) ON tx_hash = tx_hash1
WHERE tx_signer IN (SELECT distinct recipient FROM BRIDGE_TX_FINAL)
UNION 
  SELECT 
  block_timestamp,
  tx_hash,
  recipient, 
  'NEAR Bridge' as receiver_Id,
  'Bridge to NEAR' as event_action
FROM BRIDGE_TX_FINAL
),
LABEL_BRIDGE AS ( 
  SELECT 
  *,
  sum(case when event_action = 'Bridge to NEAR' then 1 else 0 end) over (partition by recipient order by block_timestamp asc) as group_id 
  FROM ALL_TX 
),
LABEL_STEPS AS (
  SELECT 
  *, 
  row_number() over (partition by recipient, group_id order by block_timestamp) as step 
FROM LABEL_BRIDGE 
),
STEPS_ORDERED AS (
  SELECT 
  step-1::string as steps,
  event_action,
  count(distinct recipient) as users,
  count(tx_hash) as tx_count_no_distinct,
  count(distinct tx_hash) as tx_count
FROM LABEL_STEPS 
  WHERE steps < 6 
  AND group_id > 0 
GROUP BY 1, 2
  ORDER BY tx_Count DESC
),
FINAL AS (
SELECT 
  *,
  rank() over (partition by steps order by tx_count DESC) as rank_volume,
  case when rank_volume > 10 then 'Others' else event_action end as event_action2
FROM STEPS_ORDERED
  WHERE event_action IS NOT NULL
)
SELECT 
event_action2, 
sum(users) as count_user,
sum(tx_count) as count_tx
from FINAL 
GROUP BY 1
ORDER BY 1
"""
df2 = querying_pagination(df2_query)

st.subheader('Total bridge Tvl(USD) at each symbol')
cc1, cc2 = st.columns([1, 1])

with cc1:
 st.caption('Total bridge Tvl(USD) at each symbol')
 st.bar_chart(df, x='symbol', y = 'tvl_usd', width = 400, height = 400)
with cc2:
 fig = px.pie(df, values='tvl_usd', names='symbol', title='Total bridge Tvl(USD) at each symbol')
 fig.update_layout(legend_title=None, legend_y=0.5)
 fig.update_traces(textinfo='percent+label', textposition='inside')
 st.plotly_chart(fig, use_container_width=True, theme='streamlit')

st.subheader('Daily bridge Tvl(USD) at each symbol')
fig = px.bar(df1, x='date', y='tvl_usd',color='symbol', title='Daily bridge Tvl(USD) at each symbol')
fig.update_layout(legend_title=None, legend_y=0.5)
st.plotly_chart(fig, use_container_width=True, theme='streamlit')

st.subheader('First activity after bridge (count of users & transactions)')
cc1, cc2 = st.columns([1, 1])

with cc1:
 st.caption('First activity after bridge (count of users)')
 st.bar_chart(df2, x='event_action2', y = 'count_user', width = 400, height = 400)
with cc2:
 fig = px.pie(df2, values='count_user', names='event_action2', title='First activity after bridge (count of users)')
 fig.update_layout(legend_title=None, legend_y=0.5)
 fig.update_traces(textinfo='percent+label', textposition='inside')
 st.plotly_chart(fig, use_container_width=True, theme='streamlit')

cc1, cc2 = st.columns([1, 1])

with cc1:
 st.caption('First activity after bridge (count of tranactions)')
 st.bar_chart(df2, x='event_action2', y = 'count_tx', width = 400, height = 400)
with cc2:
 fig = px.pie(df2, values='count_tx', names='event_action2', title='First activity after bridge (count of tranactions)')
 fig.update_layout(legend_title=None, legend_y=0.5)
 fig.update_traces(textinfo='percent+label', textposition='inside')
 st.plotly_chart(fig, use_container_width=True, theme='streamlit')