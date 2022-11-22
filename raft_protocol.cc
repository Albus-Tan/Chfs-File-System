#include "raft_protocol.h"

marshall &operator<<(marshall &m, const request_vote_args &args) {
  // Lab3: Your code here
  m << args.term_ << args.candidate_id_ << args.last_log_index_ << args.last_log_term_;
  return m;
}
unmarshall &operator>>(unmarshall &u, request_vote_args &args) {
  // Lab3: Your code here
  u >> args.term_ >> args.candidate_id_ >> args.last_log_index_ >> args.last_log_term_;
  return u;
}

marshall &operator<<(marshall &m, const request_vote_reply &reply) {
  // Lab3: Your code here
  m << reply.term_ << reply.vote_granted_;
  return m;
}

unmarshall &operator>>(unmarshall &u, request_vote_reply &reply) {
  // Lab3: Your code here
  u >> reply.term_ >> reply.vote_granted_;
  return u;
}

marshall &operator<<(marshall &m, const append_entries_reply &args) {
  // Lab3: Your code here
  m << args.term_ << args.success_;
  return m;
}

unmarshall &operator>>(unmarshall &m, append_entries_reply &args) {
  // Lab3: Your code here
  m >> args.term_ >> args.success_;
  return m;
}

marshall &operator<<(marshall &m, const install_snapshot_args &args) {
  // Lab3: Your code here
  return m;
}

unmarshall &operator>>(unmarshall &u, install_snapshot_args &args) {
  // Lab3: Your code here
  return u;
}

marshall &operator<<(marshall &m, const install_snapshot_reply &reply) {
  // Lab3: Your code here
  return m;
}

unmarshall &operator>>(unmarshall &u, install_snapshot_reply &reply) {
  // Lab3: Your code here
  return u;
}