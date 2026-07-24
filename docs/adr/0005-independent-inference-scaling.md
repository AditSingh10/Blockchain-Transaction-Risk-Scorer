# ADR 0005: Inference scales independently

Status: Accepted

Model workers load one private model/scaler instance, share a partitioned Kafka
consumer group, query durable subgraphs, and publish keyed results. This removes
PyTorch and replay work from API processes and allows compute capacity to
increase without duplicating gateways or changing client behavior. Worker
memory is bounded and no mutable model object crosses processes.
