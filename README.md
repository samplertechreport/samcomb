This is the repo for paper submission.

## Technical Report

The technical report can be found in _[samcomb_TR.pdf](https://github.com/samplertechreport/samcomb/blob/submission/samcomb_TR.pdf)_.

## Dataset

**Tpch-skew**: the tpch-skew dataset can be generated using the tool in _[TPCH-H-Skew.zip](https://github.com/samplertechreport/samcomb/blob/submission/TPC-H-Skew.zip)_. The official download link is https://www.microsoft.com/en-us/download/details.aspx?id=52430.

**Loan**: the loan dataset can be downloaded in https://www.kaggle.com/skihikingkevin/online-p2p-lending.

## Query

The generated query can be found in _[tpch_query.csv](https://github.com/samplertechreport/samcomb/blob/submission/tpch_query.csv)_ and _[loan_query.csv](https://github.com/samplertechreport/samcomb/blob/submission/loan_query.csv)_.

## Code

The code is in _[samcomb](https://github.com/samplertechreport/samcomb/tree/submission/samcomb)_ folder. The entry point is _com.samcomb.Main_ class. Other main components includes:

1. _com.samcomb.config_: the place to set experiment settings.

2. _com.samcomb.experiment_: experiment implementation.

3. _com.samcomb.sampler_: sampler implementation, where _SamCombSampler_ class is the proposed SamComb approach.

To run the code, first load the dataset into a DBMS:

1. For each dataset, create a schema. For example, we create schema _skew_s1_z2_ and _loan_ for dataset _tpch-skew_ and _load_, respectively.

2. Load each dataset into a table named _orgtable_ under its corresponding schema.

Next, set up or change the settings in _com.samcomb.config_(e.g., jdbc connection string). After that, execute _com.samcomb.Main_. It will print a message and ask for choosing actions. To choose an action, you only needs to input its corresponding id.
