FROM rapidsai/rapidsai:latest
MAINTAINER jtomlinson@nvidia.com

RUN source activate rapids && \
    pip install --upgrade azureml-sdk && \
    conda install -n rapids matplotlib && \
    pip install cugraph-cuda100 && \
    pip install scapy 