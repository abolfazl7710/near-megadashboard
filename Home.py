import streamlit as st

st.set_page_config(
    page_title="Home",
    layout= "wide",
    page_icon="🏠",
)
st.title("🏠 Home")
st.sidebar.success("🏠 Home")

st.write("""
 # Methodology #
 In this dashboard, I used python and streamlit library to display charts.
 The data used was obtained from shroomDk and Flipsidecrypto.
 All the codes of this web app can be seen in this link (Github). 
 
 https://github.com/abolfazl7710/Terradash
"""
)
st.write("""
 # 📝 Introduction #
 # What Is NEAR Protocol? #
 NEAR Protocol is a software that aims to incentivize a network of computers to operate a platform for developers to create and launch decentralized applications.
 Central to NEAR Protocol's design is the concept of sharding, a process that aims to split the network's infrastructure into several segments in order for computers, also known as nodes, to only have to handle a fraction of the network's transactions.
 By distributing segments of the blockchain, rather than the complete blockchain across network participants, sharding is expected to create a more efficient way to retrieve network data and scale the platform.

 NEAR operates in a similar manner to other centralized data storage systems like Amazon Web Services (AWS) that serve as the base layer on which applications are built. But rather than being run by a single entity, NEAR is operated and maintained by a distributed network of computers. 

 Just as AWS allows developers to deploy code in the cloud without needing to create their own infrastructure, NEAR Protocol facilitates a similar architecture built around a network of computers and its native cryptocurrency, the NEAR token. 
 # How Does NEAR Protocol Work? #
 NEAR Protocol is a Proof of Stake (PoS) blockchain that aims to compete with other platforms thanks to its sharding solution, which it calls ‘Nightshade.’ 

 Nightshade
 Sharding is a blockchain architecture that allows each participating node in the blockchain to only store a small subset of the platform’s data. Sharding should allow the blockchain to scale more efficiently, while enabling a greater amount of transactions per second and lower transaction fees.Nightshade allows NEAR Protocol to maintain a single chain of data, while distributing the computing required to maintain this data into “chunks.” These chunks are handled by nodes, who process the data and add the information to the main chain.One of the main benefits of Nightshade is that its architecture allows for fewer potential points of failure when it comes to security, as participating nodes are only responsible for maintaining smaller sections of the chain. 

 # Rainbow Bridge #
 NEAR Protocol includes an application called the Rainbow Bridge that allows participants to easily transfer Ethereum tokens back and forth between Ethereum and NEAR.In order to move tokens from Ethereum to NEAR Protocol, a user would first deposit tokens in an Ethereum smart contract. These tokens are then locked, and new tokens would be created on NEAR's platform representing the original ones.Since the original funds are held in storage through the smart contract, the process can be reversed when the user wishes to retrieve their original tokens. 

 # Aurora #
 Aurora is a Layer 2 scaling solution built on NEAR Protocol intended for developers to launch their Ethereum decentralized applications on NEAR's network.Aurora is built using Ethereum's coding technology, the Ethereum Virtual Machine (EVM), as well as a cross-chain bridge which enables developers to link their Ethereum smart contracts and assets seamlessly.Developers can use Aurora to gain the low fee and high throughput advantages of NEAR Protocol, with the familiarity and network of applications of Ethereum.
 """
 )

st.write("")
st.write("")
st.write("")
st.write("📓 Contact data")
c1, c2 = st.columns(2)
with c1:
    st.info('**Twitter: [@daryoshali](https://twitter.com/daryoshali)**')
with c2:
    st.info('**Data: [Github](https://github.com/abolfazl7710)**')

st.write("")
st.write("")
st.write("")
st.write("Thanks for MetricsDAO and flipsidecrypto team")