```markdown
# Contributing

Keep it simple.

## Rules
1. No external deps beyond bbolt and prometheus
2. Keep total code under 1000 lines
3. Every feature must work at 1M nodes in simulation

## Running tests
```bash
go run bench.go --nodes=10000 --churn=30
