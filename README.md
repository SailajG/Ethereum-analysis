# Ethereum analysis
This repository contains the deliverables of the coursework on Ethereum analysis, using MapReduce and Spark.

# Input data
The data used in the coursework was stored in a shared HDFS cluster within the university. The data covered the Ethereum network from the genesis block in August 2015 until the end of June 2019.

## Blocks
The Ethereum blocks data were CSV files with the following fields:
- number: The block number
- hash: Hash of the block
- miner: The address of the beneficiary to whom the mining rewards were given
- difficulty: Integer of the difficulty for this block
- size: The size of this block in bytes
- gas_limit: The maximum gas allowed in this block
- gas_used: The total used gas by all transactions in this block
- timestamp: The timestamp for when the block was collated
- transaction_count: The number of transactions in the block

## Transactions
The Ethereum transactions data were CSV files with the following fields:
- block_number: Block number where this transaction was in
- from_address: Address of the sender
- to_address: Address of the receiver. null when it is a contract creation transaction
- value: Value transferred in Wei (the smallest denomination of ether)
- gas: Gas provided by the sender
- gas_price : Gas price provided by the sender in Wei
- block_timestamp: Timestamp the associated block was registered at (effectively timestamp of the transaction)

## Contracts
The Ethereum contracts data were CSV files with the following fields:
- address: Address of the contract
- is_erc20: Whether this contract is an ERC20 contract
- is_erc721: Whether this contract is an ERC721 contract
- block_number: Block number where this contract was created

## Scams
The data of known crypto scam addresses were in a JSON file.
- id: Unique ID for the reported scam
- name: Name of the Scam
- url: Hosting URL
- coin: Currency the scam is attempting to gain
- category: Category of scam - Phishing, Ransomware, Trust Trade, etc.
- subcategory: Subdivisions of Category
- description: Description of the scam provided by the reporter and datasource
- addresses: List of known addresses associated with the scam
- reporter: User/company who reported the scam first
- ip: IP address of the reporter
- status: If the scam is currently active, inactive or has been taken offline

# Deliverables
- The total number and average Ether amount of transactions grouped by month
- The most popular smart contracts
- The most active miners
- Mapping Ethereum price with forks
- Gas price analysis
- Scam analysis
- Plots of data generated from various sections
