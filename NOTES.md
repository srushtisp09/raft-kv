# Day 5 – Raft Persistence: Interview Notes

## 1. Why persist `currentTerm` and `votedFor`, but not `commitIndex` or `nextIndex`?

`currentTerm` and `votedFor` must be persisted because they are important for Raft safety.

- `currentTerm` tells a server which election term it is in.
- `votedFor` remembers which candidate the server already voted for in that term.
- If these values are lost after a restart, a server might vote for another candidate in the same term. This can break the one-vote-per-term rule.

`commitIndex` is volatile. It can be rebuilt as the leader communicates its commit index through AppendEntries RPCs.

`nextIndex` and `matchIndex` are leader-maintained replication progress values. They are initialized again when a server becomes leader.

## 2. Why use a temporary file and rename when saving persistent state?

Writing directly to the existing file can be dangerous. If the process crashes halfway through writing, the file could become corrupted.

Instead, I:
1. Write the new state to a temporary file.
2. Call `Sync()` to ensure the file's contents are flushed to storage.
3. Close the temporary file.
4. Rename it over the old file.

Rename is atomic on the same filesystem. This helps ensure that a crash leaves either the old complete file or the new complete file, rather than a partially written state.

## 3. What would `TestNoDoubleVoteAfterRestart` show without `persist()` in `RequestVote`?

Suppose a node grants a vote to candidate A in term 5.

If it crashes and restarts without saving that vote, `votedFor` becomes `-1`.

When candidate B requests a vote in term 5, the restarted node may grant another vote.

That means the node has voted twice in the same term, violating Raft's voting rule.

`TestNoDoubleVoteAfterRestart` catches this bug.

## Bugs I hit and how I fixed them

### 1. Double voting after becoming a follower

I had a bug involving `votedFor` when a server stepped down to follower. Resetting the vote incorrectly could allow another vote in the same term.

**Fix:** I corrected the term and vote-reset logic so that a vote is reset only when moving to a genuinely higher term, not simply whenever the server becomes a follower.

### 2. Holding a lock while dialing an RPC connection

The client connection code held a mutex while establishing a network connection. A slow dial could block other operations that needed the same lock.

**Fix:** I improved the connection handling so the lock is not held unnecessarily during network operations.

### 3. Client timeout was shorter than server processing time

The client originally had a timeout of around 500 ms, while a server operation could take around 2 seconds.

The client could give up before the server had enough time to finish processing.

**Fix:** I increased the timeout and improved the client's redial and retry behavior.

---

## Key takeaway

Raft persistence is necessary to preserve safety across crashes and restarts. Volatile state can be reconstructed, but information such as the current term and the vote already cast must survive a restart.