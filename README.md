# Ranking and benchmarking framework for sampling algorithms on synthetic data streams #
<sup>This project was created in collaboration with [@horvath-martin](https://github.com/horvath-martin) and [@zzvara]( https://github.com/zzvara ).</sup>

In the fields of big data, AI, and streaming processing, we work with large amounts of data from multiple sources. Due to memory and network limitations, we process data streams on distributed systems to alleviate computational and network loads. When data streams with non-uniform distributions are processed, we often observe overloaded partitions due to the use of simple hash partitioning. To tackle this imbalance, we can use dynamic partitioning algorithms that require a sampling algorithm to precisely estimate the underlying distribution of the data stream. There is no standardized way to test these algorithms. We offer an extensible ranking framework with benchmark and hyperparameter optimization capabilities and supply our framework with a data generator that can handle concept drifts.
Our work includes a generator for dynamic micro-bursts that we can apply to any data stream. We provide algorithms that react to concept drifts and compare those against the state-of-the-art algorithms using our framework. 

You can read the full publication here: https://arxiv.org/abs/2006.09895
You can find all of the measurements on our auxiliary repository: https://github.com/g-jozsef/sampling-framework-aux

## About this repository ##


### Benchmark ###
You can find all of the measurement related main objects here with the OracleCalculator. The optimizer with the strategies can also be found here.
### Core ###
Core utilities and functionalities that are shared between the modules.
### Datagenerator ###
Data generators generate data from merging one ore more distributions (concept drift).
### Decisioncore ###
A reimplementation of Gedik's algorithm can be found here together with the repartitioning part of the benchmarking framework.
### Sampler ###
Implementations for Frequest, Landmark, Lossycounting, Spacesaving, Stickysampling, Oracle sampling algorithms.
### Visualization ###
This module only contains one object, the graph generator: 'Plotter'.

## How to run ##
This porject uses `sbt version 1.3.10`

First, you'll need to generate data sets (or use the ones provided by us in [our auxiliary repository](https://github.com/g-jozsef/sampling-framework-aux)).

You can generate custom datasets based on the `StreamGenerators` test object (located in `benchmark` module).

The easiest method of running our tests is to open the project in IntelliJ Idea, change the rootDirectory in `BenchmarkDecider` and `BenchmarkDirectory`, compile and run one of the provided configurations. (`.idea/runConfigurations`).

If you'd like to get something up and running as soon as possible, you can run `Benchmark` with command line arguments, you can find the documentation for it in its source code.
