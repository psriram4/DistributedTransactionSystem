# Raft-Account-Tracker
A distributed transaction system keeping track of account balances across multiple servers using the Raft consensus algorithm. 

# Motivation
Within any distributed system, it is often difficult to address the "consensus" problem. The Raft consensus algorithm, an alternative to Paxos, is one such algorithm that addresses this problem and helps ensure that our system follows ACID properties. A great tutorial and walkthrough of the Raft algorithm can be found at https://raft.github.io and the actual paper at https://raft.github.io/raft.pdf. 

# Run
To run this program,

```
go build client.go
go build server.go
go run client.go
go run server.go {server-name} {server-config}
```

# Acknowledgement
Credit to Prof. Nikita Borisov from UIUC for the initial design of this MP. Credit to Prof. Radhika Mittal and ECE 428 staff for the further design of this MP.
