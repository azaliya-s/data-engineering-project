{{ config(materialized='ephemeral') }}
select
  block_hash,
  block_timestamp
from `bigquery-public-data.crypto_bitcoin.blocks`
limit 10

