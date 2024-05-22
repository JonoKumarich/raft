Things that are missing:
- Servers retry RPCs if they do not receive a response in a timely manner, and they issue RPCs in parallel
for best performance.

- The leader
appends the command to its log as a new entry, then issues AppendEntries RPCs in parallel to each of the other
servers to replicate the entry. When the entry has been
safely replicated (as described below), the leader applies
the entry to its state machine and returns the result of that
execution to the client.




Each candidate restarts its randomized election timeout at the start of an
election. I think this means that the timeout interval stays the same???

COMMITTING = Applying log entries to the state machines
A log entry is committed once the leader that created the entry has replicated it on a majority of the servers
