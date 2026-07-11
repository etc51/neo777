# Golden real-data slice

This report is generated from captured local Parquet. It is an inspection fixture, not a source of expected synthetic values.

- Source: archived schema-v4 raw and independently validated derived Parquet
- Feature timestamps: 5
- Candidate sides: LONG and SHORT
- Outcome horizons present: [5, 10, 30, 60, 180, 300, 600, 900, 1800]
- Stop distances present: [2, 3, 4, 5]

## 1. 2026-07-11T17:19:29.303660+00:00

- Feature: `feature_33358ffaa47e615dd2368110`; ready=True; regime=range
- Book: `2a829b85b196362bee2dba48fca2c6f8ab9f43d85759a5b094c98261bcefbae3`; levels=50/50; bid/ask=64245.1/64252.0
- Trades/flow: 2 events; independently summed 60s signed flow=708.000000
- Candle: `a83c5daf748a20de7fa6363254208c9d47815446601339b17c0247c6866804e8`; close=64259.0; volume=2025.0
- Candidates: [('SHORT', 'candidate_3e6eed5ec55b19e9b4674b0e'), ('LONG', 'candidate_bc733c3d664ed77d3e7e55c2')]
- Fills: {'FULL': 4, 'NO_FILL': 2}
- Targets/outcomes: 36 rows; horizons=[5, 10, 30, 60, 180, 300, 600, 900, 1800]
- Stops: 16 rows; triggered=16
- Exits: 28 rows; variants=['breakeven', 'dynamic_breakeven', 'fixed_take_profit', 'microstructure', 'orderbook', 'time_exit', 'trailing']

## 2. 2026-07-11T17:21:59.757339+00:00

- Feature: `feature_abd6bf527ff12ec47e55e94c`; ready=True; regime=range
- Book: `cd373aeb11ad96f0b229b73d000a17e9bf9c10c298edceca9e64ff040adb56cf`; levels=50/50; bid/ask=64203.0/64209.0
- Trades/flow: 6 events; independently summed 60s signed flow=-1307.000000
- Candle: `dc9234dd820389508f66263c3e4378148aaf39d18aca1f4512b02335d8ebff5b`; close=64192.0; volume=1307.0
- Candidates: [('LONG', 'candidate_9bf1da1596ec0d068e3223fc'), ('SHORT', 'candidate_a6cde3b4d9b6e35d284cad70')]
- Fills: {'NO_FILL': 2, 'FULL': 4}
- Targets/outcomes: 36 rows; horizons=[5, 10, 30, 60, 180, 300, 600, 900, 1800]
- Stops: 16 rows; triggered=16
- Exits: 28 rows; variants=['breakeven', 'dynamic_breakeven', 'fixed_take_profit', 'microstructure', 'orderbook', 'time_exit', 'trailing']

## 3. 2026-07-11T17:24:28.329388+00:00

- Feature: `feature_c188805795f95eed0f93bdde`; ready=True; regime=range
- Book: `83052178d76e3d1920b84d0453fa39e64397951912b60ebafdf87504c2797e97`; levels=50/50; bid/ask=64212.1/64213.4
- Trades/flow: 1 events; independently summed 60s signed flow=-708.000000
- Candle: `88dabd4772d8c65d3ed00c80b09781aa236f34801ad550d27a2286205c15ee35`; close=64208.8; volume=708.0
- Candidates: [('LONG', 'candidate_07ae126ea80d1bf6ecbd3c43'), ('SHORT', 'candidate_8c32c0a9aac021057b966d5d')]
- Fills: {'NO_FILL': 2, 'FULL': 4}
- Targets/outcomes: 36 rows; horizons=[5, 10, 30, 60, 180, 300, 600, 900, 1800]
- Stops: 16 rows; triggered=16
- Exits: 28 rows; variants=['breakeven', 'dynamic_breakeven', 'fixed_take_profit', 'microstructure', 'orderbook', 'time_exit', 'trailing']

## 4. 2026-07-11T17:26:28.940489+00:00

- Feature: `feature_09b593b9e804c2614d961ca2`; ready=True; regime=range
- Book: `35071328beedede23945ad8d97a986c93153aa9dfad9f275122c102901fd1e05`; levels=50/50; bid/ask=64268.0/64273.6
- Trades/flow: 16 events; independently summed 60s signed flow=15190.000000
- Candle: `81cff310d0b17d731ed0a0f26883259f645cdd3372341ef021618c01de199dcd`; close=64266.9; volume=15295.0
- Candidates: [('LONG', 'candidate_806cb103b411265b5cbcacb6'), ('SHORT', 'candidate_dadff65806b7b31600b7fc35')]
- Fills: {'NO_FILL': 2, 'FULL': 4}
- Targets/outcomes: 36 rows; horizons=[5, 10, 30, 60, 180, 300, 600, 900, 1800]
- Stops: 16 rows; triggered=16
- Exits: 28 rows; variants=['breakeven', 'dynamic_breakeven', 'fixed_take_profit', 'microstructure', 'orderbook', 'time_exit', 'trailing']

## 5. 2026-07-11T17:28:55.332135+00:00

- Feature: `feature_8f71329adc87957b559c5573`; ready=True; regime=down
- Book: `0ebf660dc5309509cfab527f22e32ea510fa442358148f727c5f3da1dc95cb7e`; levels=50/50; bid/ask=64297.3/64300.0
- Trades/flow: 5 events; independently summed 60s signed flow=-85.000000
- Candle: `6c6fdcec0b815b2585227c2f26cfdcf02be0a44a18bf5a56cf70fba474a1bb39`; close=64297.3; volume=113.0
- Candidates: [('SHORT', 'candidate_07163db9e726001470b77a5a'), ('LONG', 'candidate_7a7ca6c8c7a08357972f53f0')]
- Fills: {'FULL': 4, 'NO_FILL': 2}
- Targets/outcomes: 36 rows; horizons=[5, 10, 30, 60, 180, 300, 600, 900, 1800]
- Stops: 16 rows; triggered=16
- Exits: 28 rows; variants=['breakeven', 'dynamic_breakeven', 'fixed_take_profit', 'microstructure', 'orderbook', 'time_exit', 'trailing']
