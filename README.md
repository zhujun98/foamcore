# foamcore

![Build status](https://github.com/zhujun98/foamcore/actions/workflows/rust.yml/badge.svg)


## Installation

```shell
conda-env create -f environment-dev.yml
conda activate foamcore

git clone https://github.com/zhujun98/foamcore.git
cd foamcore
cargo install --path .
```

## Getting started

```shell
cd examples
foamcore datahouse.json
```