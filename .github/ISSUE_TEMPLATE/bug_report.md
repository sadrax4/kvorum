---
name: Bug report
about: Something behaves incorrectly
labels: bug
---

**What happened**

**What you expected**

**How to reproduce**

Include the kvorum version (`kvorum --version`), cluster size, the commands or
client calls, and relevant log lines. For consensus bugs, a failing simulator
seed (`go test ./raft -run TestSimulation -sim.seed=N`) is ideal.
