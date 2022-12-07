#ifndef raft_protocol_h
#define raft_protocol_h

#include "rpc.h"
#include "raft_state_machine.h"

#define MAX_BUF_SIZE 8192

enum raft_rpc_opcodes {
  op_request_vote = 0x1212,
  op_append_entries = 0x3434,
  op_install_snapshot = 0x5656
};

enum raft_rpc_status {
  OK,
  RETRY,
  RPCERR,
  NOENT,
  IOERR
};

class request_vote_args {
 public:
  // Lab3: Your code here

  // candidate's term
  int term_;

  // candidate requesting vote
  int candidate_id_;

  // index of candidate's last log entry
  int last_log_index_;

  // term of candidate's last log entry
  int last_log_term_;

  request_vote_args() {}
  request_vote_args(int term, int candidate_id, int last_log_index, int last_log_term)
      : term_(term), candidate_id_(candidate_id), last_log_index_(last_log_index), last_log_term_(last_log_term) {}

};

marshall &operator<<(marshall &m, const request_vote_args &args);
unmarshall &operator>>(unmarshall &u, request_vote_args &args);

class request_vote_reply {
 public:

  // current term
  int term_;

  // true means candidate received vote
  bool vote_granted_;

  request_vote_reply() {}
  request_vote_reply(int term, bool vote_granted) : term_(term), vote_granted_(vote_granted) {}

  // Lab3: Your code here
};

marshall &operator<<(marshall &m, const request_vote_reply &reply);
unmarshall &operator>>(unmarshall &u, request_vote_reply &reply);

template<typename command>
class log_entry {
 public:
  // Lab3: Your code here

  // command for state machine
  command command_;

  // term when entry was received by leader
  // (first index is 1)
  int term_;

  int index_;

  log_entry() {}
  log_entry(int term, int index) : term_(term), index_(index) {}
  log_entry(int term, int index, const command &cmd) : term_(term), index_(index), command_(cmd) {}

};

template<typename command>
marshall &operator<<(marshall &m, const log_entry<command> &entry) {
  // Lab3: Your code here
  int size = entry.command_.size();
  char buf[MAX_BUF_SIZE];
  entry.command_.serialize(buf, size);
  std::string str(buf, size);
  m << entry.term_ << entry.index_ << size << str;
  return m;
}

template<typename command>
unmarshall &operator>>(unmarshall &u, log_entry<command> &entry) {
  // Lab3: Your code here
  u >> entry.term_ >> entry.index_;
  int size;
  std::string str;
  u >> size >> str;
  entry.command_.deserialize(str.c_str(), size);
  return u;
}

template<typename command>
class append_entries_args {
 public:
  // Your code here

  // leader's term
  int term_;

  // follower can redirect clients
  int leader_id_;

  // index of log entry immediately preceding new ones
  int prev_log_index_;

  // term of prev_log_index_ entry
  int prev_log_term_;

  // log entries to store
  // empty for heartbeat; may send more than one for efficiency
  std::vector<log_entry<command>> entries_;

  // leader's commit index
  int leader_commit_;

  // is heartbeat
  bool heartbeat_;

  append_entries_args() {}

  append_entries_args(int term, bool heartbeat, int leader_commit)
      : term_(term), heartbeat_(heartbeat), leader_commit_(leader_commit) {}

  append_entries_args(int term,
                      int leader_id,
                      int prev_log_index,
                      int prev_log_term,
                      std::vector<log_entry<command>> entries,
                      int leader_commit, bool heartbeat)
      : term_(term),
        leader_id_(leader_id),
        prev_log_index_(prev_log_index),
        prev_log_term_(prev_log_term),
        leader_commit_(leader_commit),
        entries_(entries),
        heartbeat_(heartbeat) {}

  append_entries_args(int term,
                      int leader_id,
                      int prev_log_index,
                      int prev_log_term,
                      int leader_commit,
                      bool heartbeat)
      : term_(term),
        leader_id_(leader_id),
        prev_log_index_(prev_log_index),
        prev_log_term_(prev_log_term),
        leader_commit_(leader_commit),
        heartbeat_(heartbeat) {}
};

template<typename command>
marshall &operator<<(marshall &m, const append_entries_args<command> &args) {
  // Lab3: Your code here
  m << args.term_ << args.leader_id_ << args.prev_log_index_ << args.prev_log_term_ << args.entries_
    << args.leader_commit_ << args.heartbeat_;
  return m;
}

template<typename command>
unmarshall &operator>>(unmarshall &u, append_entries_args<command> &args) {
  // Lab3: Your code here
  u >> args.term_ >> args.leader_id_ >> args.prev_log_index_ >> args.prev_log_term_ >> args.entries_
    >> args.leader_commit_ >> args.heartbeat_;
  return u;
}

class append_entries_reply {
 public:
  // Lab3: Your code here

  // current term, for leader to update itself
  int term_;

  // true if follower contained entry matching
  // prev_Log_index and prev_log_term
  bool success_;

  // tell leader index of highest log entry has been
  // replicated, add for snapshot case
  int match_index_;

  append_entries_reply() {}
  append_entries_reply(int term, bool success, int match_index)
      : term_(term), success_(success), match_index_(match_index) {}
};

marshall &operator<<(marshall &m, const append_entries_reply &reply);
unmarshall &operator>>(unmarshall &m, append_entries_reply &reply);

class install_snapshot_args {
 public:
  // Lab3: Your code here

  // leader's term
  int term_;

  // so follower can redirect clients
  int leader_id_;

  // the snapshot replaces all entries up through and including this index
  int last_included_index_;

  // term of last_included_index_
  int last_included_term_;

  // raw bytes of the snapshot chunk (starting at offset, but here is all)
  std::vector<char> data_;

  //  since we do not partition the snapshot,
  //  just send the whole snapshot in a single RPC.
  //  no need for below paras

  //  // byte offset where chunk is positioned in the snapshot file
  //  int offset;
  //
  //  // true if this is the last chunk
  //  bool done;

  install_snapshot_args() {}
  install_snapshot_args(int term,
                        int leader_id,
                        int last_included_index,
                        int last_included_term,
                        const std::vector<char> &data)
      : term_(term),
        leader_id_(leader_id),
        last_included_term_(last_included_term),
        last_included_index_(last_included_index),
        data_(data) {

  }
};

marshall &operator<<(marshall &m, const install_snapshot_args &args);
unmarshall &operator>>(unmarshall &m, install_snapshot_args &args);

class install_snapshot_reply {
 public:
  // Lab3: Your code here
  // current term, for leader to update itself
  int term_;

  install_snapshot_reply() {}
  install_snapshot_reply(int term) : term_(term) {}
};

marshall &operator<<(marshall &m, const install_snapshot_reply &reply);
unmarshall &operator>>(unmarshall &m, install_snapshot_reply &reply);

#endif // raft_protocol_h