# Neural Wave DHT

A distributed hash table that scales to 1M+ nodes with 99% success under 30% churn.

## Why this exists
Standard DHTs flood messages or break under churn. Neural Wave DHT uses 3 ideas from nature:
- **Small-world shortcuts**: 3 long-range peers per node = O(log N) hops
- **Gradient learning**: Nodes learn where keys live, like ant pheromones
- **Sparse activation**: Only 3 peers fire per hop. No message flood.

## Performance
Tested at 1M nodes, 30% churn, 1000 lookups:
- **Success rate**: 98.9%
- **P99 latency**: 267ms
- **Messages/lookup**: 7.2
- **Memory/node**: 5MB

## Install
```bash
go get github.com/best-e/wave-dht
