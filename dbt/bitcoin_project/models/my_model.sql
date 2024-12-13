{{ config(materialized='table') }}

select
  block_hash,
  block_timestamp
from `bigquery-public-data.crypto_bitcoin.blocks`
limit 10

