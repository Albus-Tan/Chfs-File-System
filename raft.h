#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>
#include <numeric>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

template<typename state_machine, typename command>
class raft {
  static_assert(std::is_base_of<raft_state_machine, state_machine>(),
  "state_machine must inherit from raft_state_machine");
  static_assert(std::is_base_of<raft_command, command>(),
  "command must inherit from raft_command");

  friend class thread_pool;

//#define LOG_TO_FILE

#define RAFT_LOG(fmt, args...) \
    do {                       \
    } while (0);

//#define RAFT_LOG(fmt, args...)                                                                                   \
//     do {                                                                                                         \
//         auto now =                                                                                               \
//             std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
//                 std::chrono::system_clock::now().time_since_epoch())                                             \
//                 .count();                                                                                        \
//         printf("[%ld][%s:%d:%s][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, __FUNCTION__ ,my_id, current_term, ##args); \
//     } while (0);

#define PRINT_ALL_LOG(args...) \
    do {                       \
    } while (0);

//#define PRINT_ALL_LOG(logs) \
//    do {  \
//        RAFT_LOG("print log entries")  \
//        for(auto log : logs) {  \
//          RAFT_LOG("index %d, term %d", log.index_, log.term_); \
//        } \
//    } while (0);

 public:
  raft(
      rpcs *rpc_server,
      std::vector<rpcc *> rpc_clients,
      int idx,
      raft_storage<command> *storage,
      state_machine *state);
  ~raft();

  // start the raft node.
  // Please make sure all of the rpc request handlers have been registered before this method.
  void start();

  // stop the raft node.
  // Please make sure all of the background threads are joined in this method.
  // Notice: you should check whether is server should be stopped by calling is_stopped().
  //         Once it returns true, you should break all of your long-running loops in the background threads.
  void stop();

  // send a new command to the raft nodes.
  // This method returns true if this raft node is the leader that successfully appends the log.
  // If this node is not the leader, returns false.
  bool new_command(command cmd, int &term, int &index);

  // returns whether this node is the leader, you should also set the current term;
  bool is_leader(int &term);

  // save a snapshot of the state machine and compact the log.
  bool save_snapshot();

 private:
  std::mutex mtx; // A big lock to protect the whole data structure
  ThrPool *thread_pool;
  raft_storage<command> *storage; // To persist the raft log
  state_machine *state;           // The state machine that applies the raft log, e.g. a kv store

  rpcs *rpc_server;                // RPC server to recieve and handle the RPC requests
  std::vector<rpcc *> rpc_clients; // RPC clients of all raft nodes including this node
  int my_id;                       // The index of this node in rpc_clients, start from 0

  std::atomic_bool stopped;

  enum raft_role {
    follower,
    candidate,
    leader
  };
  raft_role role;

  // Current terms are exchanged whenever servers communicate;
  // if one server’s current term is smaller than the other’s,
  // then it updates its current term to the larger value.
  // If a candidate or leader discovers that its term is out of date,
  // it immediately reverts to follower state.

  // latest term server has seen
  // (initialized to 0 on first boot, increases monotonically)
  int current_term;

  int leader_id;

  std::thread *background_election;
  std::thread *background_ping;
  std::thread *background_commit;
  std::thread *background_apply;

  // Your code here:

  // time related const in milliseconds
  const int heartbeat_time_interval = 100;
  const int commit_time_interval = 100;
  const int apply_time_interval = 10;
  const int sleep_time = 10;
  const int follower_election_timeout_lower = 700;
  const int follower_election_timeout_upper = 1300;
  const int candidate_election_timeout_lower = 1000;
  const int candidate_election_timeout_upper = 1500;
  std::chrono::milliseconds follower_election_timeout;
  std::chrono::milliseconds candidate_election_timeout;

  // time related
  std::chrono::system_clock::time_point last_election_start_time;
  std::chrono::system_clock::time_point last_received_RPC_time;


  /* ----Persistent state on all server----  */
  // updated on stable storage before responding to RPCs

  // current_term (has defined before)

  // candidateId that received vote in current term (or -1 if none)
  int voted_for;

  // log entries; each entry contains command for state machine,
  // and term when entry was received by leader (first index is 1)
  std::vector<log_entry<command> > log;

  /* ---- Volatile state on all server----  */

  // index of highest log entry known to be committed (initialized to 0)
  int commit_index;

  // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
  int last_applied;

  /* ---- Volatile state on leader----  */
  // reinitialized after election

  // for each server, index of the next log entry to send to that server
  // (initialized to leader last log index + 1)
  std::vector<int> next_index;

  // for each server, index of highest log entry known to be replicated on server
  // (initialized to 0, increases monotonically)
  std::vector<int> match_index;

  /* ---- Volatile state on candidate----  */
  // reinitialized before election

  // store the votes of followers
  // use std::vector<int> instead of std::vector<bool> since latter
  // may cause problem due to std::vector<bool> own optimization
  // elements are only 0 and 1
  std::vector<int> votes_get;

  // snapshot related

  // the index of the last entry in the log that the snapshot replaces
  // (the last entry the state machine had applied)
  int last_included_index;

  // the term of this entry
  int last_included_term;

 private:
  // RPC handlers
  int request_vote(request_vote_args arg, request_vote_reply &reply);

  int append_entries(append_entries_args<command> arg, append_entries_reply &reply);

  int install_snapshot(install_snapshot_args arg, install_snapshot_reply &reply);

  // RPC helpers
  void send_request_vote(int target, request_vote_args arg);
  void handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply);

  void send_append_entries(int target, append_entries_args<command> arg);
  void handle_append_entries_reply(int target,
                                   const append_entries_args<command> &arg,
                                   const append_entries_reply &reply);

  void send_install_snapshot(int target, install_snapshot_args arg);
  void handle_install_snapshot_reply(int target, const install_snapshot_args &arg, const install_snapshot_reply &reply);

 private:
  bool is_stopped();
  int num_nodes() {
    return rpc_clients.size();
  }

  // background workers
  void run_background_ping();
  void run_background_election();
  void run_background_commit();
  void run_background_apply();

  // Your code here:

  // two concepts for the log index:
  // physical index (e.g. the index of the std::vector)
  // and logical index (e.g. physical index + snapshot index)

  // convert logical_index to physical index
  int to_physical_index(int logical_index) {
    int physical_index = logical_index - last_included_index - 1;
    assert(physical_index >= 0 && physical_index < log.size());
    return physical_index;
  }

  // convert physical index to logical_index
  int to_logical_index(int physical_index) {
    return physical_index + last_included_index + 1;
  }

  int get_last_log_index() {
    if (log.empty()) {
      return last_included_index;
    } else {
      return to_logical_index(log.size() - 1);
    }
  }

  int get_last_log_term() {
    if (log.empty()) {
      return last_included_term;
    } else {
      return log[log.size() - 1].term_;
    }
  }

  log_entry<command> get_log_by_logical_index(int logical_index) {
    int physical_index = logical_index - last_included_index - 1;
    if (physical_index >= 0 && physical_index < log.size()) {
      return log[physical_index];
    } else {
      if (physical_index == -1) {
        return log_entry<command>(last_included_term, last_included_index);
      } else {
        assert(0);
      }
    }
  }

};

template<typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server,
                                   std::vector<rpcc *> clients,
                                   int idx,
                                   raft_storage<command> *storage,
                                   state_machine *state) :
    stopped(false),
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    storage(storage),
    state(state),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr),
    current_term(0),
    role(follower) {
  thread_pool = new ThrPool(32);

  // Register the rpcs.
  rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
  rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
  rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

  // Your code here:
  // Do the initialization
  voted_for = -1;
  commit_index = 0;
  last_applied = 0;

  last_included_index = -1;
  last_included_term = -1;

  log = std::vector<log_entry<command> >();
  next_index = std::vector<int>(num_nodes(), 1);
  match_index = std::vector<int>(num_nodes(), 0);
  votes_get = std::vector<int>(num_nodes(), false);

  last_election_start_time = std::chrono::system_clock::now();
  last_received_RPC_time = std::chrono::system_clock::now();

  candidate_election_timeout =
      std::chrono::milliseconds(rand() % (candidate_election_timeout_upper - candidate_election_timeout_lower)
                                    + candidate_election_timeout_lower);
  follower_election_timeout =
      std::chrono::milliseconds(rand() % (follower_election_timeout_upper - follower_election_timeout_lower)
                                    + follower_election_timeout_lower);

#ifdef LOG_TO_FILE
  freopen("raft.log", "w", stdout);
#endif

}

template<typename state_machine, typename command>
raft<state_machine, command>::~raft() {
  if (background_ping) {
    delete background_ping;
  }
  if (background_election) {
    delete background_election;
  }
  if (background_commit) {
    delete background_commit;
  }
  if (background_apply) {
    delete background_apply;
  }
  delete thread_pool;
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::stop() {
  stopped.store(true);
  background_ping->join();
  background_election->join();
  background_commit->join();
  background_apply->join();
  thread_pool->destroy();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
  return stopped.load();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
  term = current_term;
  return role == leader;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start() {
  // Lab3: Your code here

  RAFT_LOG("start");

  // the first log index is 1 instead of 0
  // To simplify the programming, you can append an empty log entry
  // to the logs at the very beginning. And since the 'lastApplied'
  // index starts from 0, the first empty log entry will never be
  // applied to the state machine.
  command cmd;
  // append a null log as the first log
  log.push_back(log_entry<command>(0, 0, cmd));

  // restore metadata and log
  storage->restore_metadata(current_term, voted_for);
  storage->restore_log(log);

  // restore snapshot
  std::vector<char> data;
  storage->restore_snapshot(last_included_index, last_included_term, data);
  if(!data.empty()){
    state->apply_snapshot(data);
    commit_index = last_included_index;
    last_applied = last_included_index;
  }

  // create 4 background threads
  this->background_election = new std::thread(&raft::run_background_election, this);
  this->background_ping = new std::thread(&raft::run_background_ping, this);
  this->background_commit = new std::thread(&raft::run_background_commit, this);
  this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
  // Lab3: Your code here
  // when the user calls raft::new_command to append a new command to the leader's log,
  // the leader should return the new_command function immediately
  // And the log should be replicated to the follower asynchronously in another background thread

  std::unique_lock<std::mutex> lock(mtx);
  if (is_leader(term)) {

    RAFT_LOG("new command");

    index = to_logical_index(log.size());

    log_entry<command> new_log(term, index, cmd);

    // persist log
    storage->persist_log(new_log);

    log.push_back(new_log);

    next_index[my_id] = index + 1;
    match_index[my_id] = index;

    return true;
  } else {
    term = current_term;
    return false;
  }
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
  // Lab3: Your code here

  std::unique_lock<std::mutex> lock(mtx);

  if (is_leader(current_term)) {
    RAFT_LOG("save_snapshot START");

    if (last_applied == 0) return false;

    auto log_last_applied = get_log_by_logical_index(last_applied);

    // do snapshot
    std::vector<char> snapshot_data = state->snapshot();

    // clear log before last_applied (including last_applied)
    log.erase(log.begin(), log.begin() + to_physical_index(last_applied) + 1);

    // update last_included
    last_included_index = log_last_applied.index_;
    last_included_term = log_last_applied.term_;

    // persist log
    storage->persist_logs(log);

    // persist snapshot
    storage->persist_snapshot(last_included_index, last_included_term, snapshot_data);

    //  send InstallSnapshot rpc
    for (int id = 0; id < num_nodes(); ++id) {
      if (id == my_id) continue;
      install_snapshot_args arg(current_term, my_id, last_included_index, last_included_term, snapshot_data);
      thread_pool->addObjJob(this, &raft::send_install_snapshot, id, arg);
    }
    RAFT_LOG("install_snapshot rpc sent");

    return true;
  } else {
    return false;
  }
}

/******************************************************************

                         RPC Related

*******************************************************************/

//  To implement an asynchronous RPC call,
//  use thread pool to handle asynchronous events
//  thread_pool->addObjJob(this, &raft::your_method, arg1, arg2);

template<typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply &reply) {
  // Lab3: Your code here
  std::unique_lock<std::mutex> lock(mtx);

  // should not update here
  // since rpc sender is not leader yet
  // last_received_RPC_time = std::chrono::system_clock::now();

  RAFT_LOG("request_vote start");

  if (args.term_ < current_term) {
    // reply false if term < currentTerm
    RAFT_LOG("term < currentTerm, reply vote_granted_ FALSE");
    reply.term_ = current_term;
    reply.vote_granted_ = false;
  } else {
    // args.term_ >= current_term
    if (args.term_ > current_term) {

      // If term > currentTerm, currentTerm ← term
      current_term = args.term_;
      // step down if leader or candidate
      role = follower;
      voted_for = -1;

      // persist metadata
      storage->persist_metadata(current_term, voted_for);

      RAFT_LOG("args.term_ > current_term, reverts to FOLLOWER");
    }

    // args.term_ == current_term
    // If term == currentTerm, votedFor is null or candidateId,
    // and candidate's log is at least as complete as local log,
    if (voted_for == -1 || voted_for == args.candidate_id_) {
      // voting server denies vote if its log is more complete
      if ((args.last_log_term_ < get_last_log_term())
          || ((args.last_log_term_ == get_last_log_term()) && (args.last_log_index_ < get_last_log_index()))) {
        RAFT_LOG("term == currentTerm, reply vote_granted_ FALSE");
        // do not grant vote
        reply.term_ = current_term;
        reply.vote_granted_ = false;
      } else {
        // grant vote
        reply.term_ = current_term;
        reply.vote_granted_ = true;

        RAFT_LOG("term == currentTerm, reply vote_granted_ TRUE");

        // modify voted for
        voted_for = args.candidate_id_;

        // reset election timeout

        // persist metadata
        storage->persist_metadata(current_term, voted_for);
      }
    } else {
      RAFT_LOG("term == currentTerm, reply vote_granted_ FALSE");
      // do not grant vote
      reply.term_ = current_term;
      reply.vote_granted_ = false;
    }
  }
  return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target,
                                                             const request_vote_args &arg,
                                                             const request_vote_reply &reply) {
  // Lab3: Your code here
  std::unique_lock<std::mutex> lock(mtx);

  RAFT_LOG("handle_request_vote_reply start");

  // if one server’s current term is smaller than the other’s,
  // then it updates its current term to the larger value.
  // If a candidate or leader discovers that its term is out of date,
  // it immediately reverts to follower state.
  if (reply.term_ > current_term) {
    current_term = reply.term_;
    role = follower;
    voted_for = -1;

    // persist metadata
    storage->persist_metadata(current_term, voted_for);

    RAFT_LOG("reply.term_ > current_term, reverts to FOLLOWER");
  } else {
    // if reply.vote_granted_ == true and still in candidate role
    if (reply.vote_granted_ && role == candidate) {
      votes_get[target] = reply.vote_granted_;

      // check votes num
      int votes_num = std::accumulate(votes_get.begin(), votes_get.end(), 0);
      RAFT_LOG("votes num get %d", votes_num);
      if (votes_num > (int) num_nodes() / 2) {

        // for each server, index of the next log entry to send to that server
        // initialized to leader last log index + 1 (log.size())
        next_index.assign(num_nodes(), to_logical_index(log.size()));

        // for each server, index of highest log entry known to be replicated on server
        // (initialized to 0, increases monotonically)
        match_index.assign(num_nodes(), 0);

        RAFT_LOG("become LEADER");
        // change role to leader
        role = leader;

        // immediately send heartbeat
        if (is_leader(current_term)) {
          for (int id = 0; id < num_nodes(); ++id) {
            if (id == my_id) continue;
            log_entry<command> prev_log = get_log_by_logical_index(next_index[id] - 1);
            append_entries_args<command> arg(current_term, my_id, prev_log.index_, prev_log.term_, commit_index, true);
            thread_pool->addObjJob(this, &raft::send_append_entries, id, arg);
          }
          RAFT_LOG("heartbeat sent");
        }


//        // append empty log of current term
//        command cmd;
//        log_entry<command> empty_log(current_term, to_logical_index(log.size()), cmd);
//        // persist log
//        storage->persist_log(empty_log);
//        log.push_back(empty_log);
      }

    }
  }

  return;
}

template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply) {
  // Lab3: Your code here

  std::unique_lock<std::mutex> lock(mtx);

  RAFT_LOG("append_entries start");
  last_received_RPC_time = std::chrono::system_clock::now();

  if (arg.heartbeat_) {
    // heartbeat
    RAFT_LOG("heartbeat received");
    if (arg.term_ < current_term) {
      // reply false if term < current_term
      reply.success_ = false;
      reply.term_ = current_term;
    } else {
      // arg.term >= current_term
      role = follower;
      if (arg.term_ > current_term) voted_for = -1;
      current_term = arg.term_;

      RAFT_LOG("leader_commit %d, commit_index %d", arg.leader_commit_, commit_index);
      // If leaderCommit > commitIndex, set commitIndex =
      // min(leaderCommit, index of last new entry)
      if (arg.leader_commit_ > commit_index && arg.prev_log_term_ == get_last_log_term()
          && arg.prev_log_index_ == get_last_log_index() && to_logical_index(log.size()) > arg.leader_commit_) {
        commit_index = std::min(arg.leader_commit_, to_logical_index(log.size()) - 1);
      }

      reply.term_ = current_term;
      reply.success_ = true;

      // persist metadata
      storage->persist_metadata(current_term, voted_for);
    }
  } else {
    // append_entries

    if (arg.term_ > current_term) {
      current_term = arg.term_;
      role = follower;
      voted_for = -1;

      // persist metadata
      storage->persist_metadata(current_term, voted_for);
    }

    if (arg.term_ < current_term) {
      // reply false if term < current_term
      reply.success_ = false;
      reply.term_ = current_term;
      RAFT_LOG("term < current_term, append_entries FAILED");
    } else if (to_logical_index(log.size()) <= arg.prev_log_index_
        || get_log_by_logical_index(arg.prev_log_index_).term_ != arg.prev_log_term_) {
      // reply false if log doesn't contain an entry at
      // prev_log_index whose term matches prev_log_term
      reply.success_ = false;
      reply.term_ = current_term;
      RAFT_LOG("arg.prev_log_term_ %d MISMATCH log.prev_log_term_ at index %d, append_entries FAILED",
               arg.prev_log_term_, arg.prev_log_index_);
    } else {

      // TODO
      // if an existing entry conflicts with a new one
      // (same index but different terms), delete the
      // existing entry and all that follow it
      auto start = log.begin() + to_physical_index(arg.prev_log_index_) + 1;
      auto end = log.end();

      RAFT_LOG("append_entries::log size before erase: %d", log.size());
      PRINT_ALL_LOG(log);

      // delete [arg.prev_log_index_+1, end]
      log.erase(start, end);

      RAFT_LOG("append_entries::log size after erase: %d", log.size());
      PRINT_ALL_LOG(log);
      RAFT_LOG("append_entries::arg.entries_ size: %d", arg.entries_.size());
      PRINT_ALL_LOG(arg.entries_);

      // persist logs
      storage->persist_logs(arg.entries_);

      // Append any new entries not already in the log
      log.insert(log.end(), arg.entries_.begin(), arg.entries_.end());

      RAFT_LOG("append_entries::log size after insert: %d", log.size());
      PRINT_ALL_LOG(log);

      // If leaderCommit > commitIndex, set commitIndex =
      // min(leaderCommit, index of last new entry)
      if (arg.leader_commit_ > commit_index) {
        commit_index = std::min(arg.leader_commit_, to_logical_index((int) log.size() - 1));
      }

      reply.success_ = true;
      reply.term_ = current_term;

      RAFT_LOG("append_entries SUCCESS");

    }
  }

  return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int node,
                                                               const append_entries_args<command> &arg,
                                                               const append_entries_reply &reply) {
  // Lab3: Your code here

  std::unique_lock<std::mutex> lock(mtx);

  RAFT_LOG("handle_append_entries_reply start");

  if (reply.term_ > current_term) {
    current_term = reply.term_;
    role = follower;
    voted_for = -1;

    // persist metadata
    storage->persist_metadata(current_term, voted_for);

    RAFT_LOG("reply.term_ > current_term, reverts to FOLLOWER");
  } else {
    if (arg.heartbeat_) {}
    else {
      if (reply.success_) {
        // append_entries success
        RAFT_LOG("handle_append_entries_reply: handle SUCCESS");

        // update index of highest log entry known to be replicated on server
        match_index[node] = std::max(match_index[node], arg.prev_log_index_ + (int) arg.entries_.size());

        // update index of the next log entry to send to that server
        next_index[node] = match_index[node] + 1;

        // update commit_index
        std::vector<int> temp = match_index;
        std::sort(temp.begin(), temp.end());
        // elements in temp are increasing
        // index before middle must be replicated on majority servers
        int new_commit_index = temp[(temp.size() - 1) / 2];
        if (new_commit_index > commit_index) {
          commit_index = new_commit_index;
        }
        RAFT_LOG("new_commit_index %d, commit_index %d", new_commit_index, commit_index);

        // TODO
      } else {
        // append_entries failed

        RAFT_LOG("handle_append_entries_reply: handle FAILURE");

//        // update next_index to smaller
//        int old_next_index = next_index[node];
//        next_index[node] = std::max(old_next_index - 1, 1);

        // trick: directly update next_index to 1
        next_index[node] = 1;

      }
    }
  }

  return;
}

template<typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply) {
  // Lab3: Your code here

  std::unique_lock<std::mutex> lock(mtx);

  RAFT_LOG("install_snapshot start");
  last_received_RPC_time = std::chrono::system_clock::now();

  if (args.term_ < current_term) {
    // reply immediately if term < current_term
    reply.term_ = current_term;
    RAFT_LOG("term < current_term, install_snapshot FAILED");
  } else {
    // args.term_ >= current_term
    if (args.term_ > current_term) {
      current_term = args.term_;
      role = follower;
      voted_for = -1;

      // persist metadata
      storage->persist_metadata(current_term, voted_for);
    }

    // erase log before last_included_index (including last_included_index)
    auto itr = log.begin();
    for (; itr != log.end(); ++itr) {
      if ((*itr).index_ > args.last_included_index_) {
        break;
      }
    }

    // if existing log entry has same index and term as snapshot's last included entry,
    // retain log entries following it and reply

    // discard the entire log if snapshot contain new information
    // not already in the recipient’s log
    // If instead, the follower receives a snapshot that describes a prefix of its log
    // (due to retransmission or by mistake), then log entries covered by the snapshot
    // are deleted but entries following the snapshot are still valid and must be retained.

    log.erase(log.begin(), itr);

    // persist logs
    storage->persist_logs(log);

    last_included_index = args.last_included_index_;
    last_included_term = args.last_included_term_;
    commit_index = last_included_index;
    last_applied = last_included_index;


    // reset state machine using snapshot contents
    // (and load snapshot's cluster configuration)
    state->apply_snapshot(args.data_);
    storage->persist_snapshot(last_included_index, last_included_term, args.data_);

    // reply
    reply.term_ = current_term;

  }

  return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int node,
                                                                 const install_snapshot_args &arg,
                                                                 const install_snapshot_reply &reply) {
  // Lab3: Your code here
  std::unique_lock<std::mutex> lock(mtx);

  RAFT_LOG("handle_install_snapshot_reply start");

  if (reply.term_ > current_term) {
    current_term = reply.term_;
    role = follower;
    voted_for = -1;

    // persist metadata
    storage->persist_metadata(current_term, voted_for);

    RAFT_LOG("reply.term_ > current_term, reverts to FOLLOWER");
  } else {
    next_index[node] = arg.last_included_index_ + 1;
    match_index[node] = arg.last_included_index_;
  }

  return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
  request_vote_reply reply;
  if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
    handle_request_vote_reply(target, arg, reply);
  } else {
    // RPC fails
  }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
  append_entries_reply reply;
  if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
    handle_append_entries_reply(target, arg, reply);
  } else {
    // RPC fails
  }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
  install_snapshot_reply reply;
  if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
    handle_install_snapshot_reply(target, arg, reply);
  } else {
    // RPC fails
  }
}

/******************************************************************

                        Background Workers

*******************************************************************/

// leader election protocol and heartbeat mechanism
template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
  // Periodly check the liveness of the leader (start an election)

  // election process
  // 1. A follower transitions to candidate state
  // 2. It increments its current term
  // 3. It then votes for itself, and issues raft::request_vote RPCs in parallel to each
  //    of the other servers. A candidate continues in this state until one of three
  //    things happens:
  //      · It receives votes from a majority of the servers and wins the election
  //      · Another server establishes itself as leader
  //      · A period of time goes by with no winner

  // Once a candidate wins an election, it becomes leader.
  // It then sends heartbeat messages to all of the other servers
  // to establish its authority and prevent new elections(raft::run_background_ping).

  srand(time(NULL));

  while (true) {
    if (is_stopped()) return;
    // Lab3: Your code here
    {
      std::unique_lock<std::mutex> lock(mtx);
      if (role != leader) {
        // Work for followers and candidates.
        // A server begins an election if it receives no communication over a period of time
        std::chrono::system_clock::time_point current_time = std::chrono::system_clock::now();
        if (role == candidate && current_time - last_election_start_time > candidate_election_timeout) {
          candidate_election_timeout = std::chrono::milliseconds(
              rand() % (candidate_election_timeout_upper - candidate_election_timeout_lower)
                  + candidate_election_timeout_lower);
          // For candidate: A period of time goes by with no winner
          // start election
          RAFT_LOG("role == candidate, restart election");
          // update last_election_start_time
          last_election_start_time = std::chrono::system_clock::now();

          // It increments its current term
          ++current_term;

          // It then votes for itself
          voted_for = my_id;

          // persist metadata
          storage->persist_metadata(current_term, voted_for);

          votes_get.assign(num_nodes(), false);
          votes_get[my_id] = true;

          // issues raft::request_vote RPCs in parallel to each
          // of the other servers
          request_vote_args arg(current_term, my_id, get_last_log_index(), get_last_log_term());
          for (int id = 0; id < num_nodes(); ++id) {
            if (id == my_id) continue;
            thread_pool->addObjJob(this, &raft::send_request_vote, id, arg);
          }
          RAFT_LOG("request_vote RPCs sent");

        } else if (role == follower) {
          if (current_time - last_received_RPC_time > follower_election_timeout) {
            follower_election_timeout =
                std::chrono::milliseconds(rand() % (follower_election_timeout_upper - follower_election_timeout_lower)
                                              + follower_election_timeout_lower);
            // start election
            RAFT_LOG("role == follower, start election");
            // update last_election_start_time
            last_election_start_time = std::chrono::system_clock::now();

            // A follower transitions to candidate state
            role = candidate;

            // It increments its current term
            ++current_term;

            // It then votes for itself
            voted_for = my_id;

            // persist metadata
            storage->persist_metadata(current_term, voted_for);

            votes_get.assign(num_nodes(), false);
            votes_get[my_id] = true;

            // issues raft::request_vote RPCs in parallel to each
            // of the other servers
            request_vote_args arg(current_term, my_id, get_last_log_index(), get_last_log_term());
            for (int id = 0; id < num_nodes(); ++id) {
              if (id == my_id) continue;
              thread_pool->addObjJob(this, &raft::send_request_vote, id, arg);
            }
            RAFT_LOG("request_vote RPCs sent");

          }
        }

      }
    }
    // unlock and sleep for a while
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
  }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
  // Periodly send logs to the follower.

  // Only work for the leader.

  // try append_entries using next_index (until match_index)

  while (true) {
    if (is_stopped()) return;
    {
      std::unique_lock<std::mutex> lock(mtx);
      if (is_leader(current_term)) {
        match_index[my_id] = to_logical_index(log.size() - 1);
        for (int id = 0; id < num_nodes(); ++id) {
          if (id == my_id) continue;
          if (next_index[id] >= to_logical_index(log.size())) continue;
          log_entry<command> prev_log = get_log_by_logical_index(next_index[id] - 1);
          std::vector<log_entry<command>> entries(log.begin() + to_physical_index(next_index[id]), log.end());
          append_entries_args<command>
              arg(current_term, my_id, prev_log.index_, prev_log.term_, entries, commit_index, false);
          RAFT_LOG("background_commit, role: %d, prev_log.index_: %d, prev_log.term_: %d",
                   role,
                   prev_log.index_,
                   prev_log.term_);
          thread_pool->addObjJob(this, &raft::send_append_entries, id, arg);
        }
      }
    }
    // unlock and sleep for a while
    std::this_thread::sleep_for(std::chrono::milliseconds(commit_time_interval));
  }

}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
  // Periodly apply committed logs the state machine

  // Work for all the nodes.
  while (true) {
    if (is_stopped()) return;
    // Lab3: Your code here:
    {
      std::unique_lock<std::mutex> lock(mtx);
      if (commit_index > last_applied) {
        RAFT_LOG("start background_apply, role: %d, commit_index: %d, last_applied: %d",
                 role,
                 commit_index,
                 last_applied);
        for (int idx = last_applied + 1; idx <= commit_index; idx++) {
          state->apply_log(log[to_physical_index(idx)].command_);
        };
        last_applied = commit_index;
        RAFT_LOG("finish background_apply, role: %d, commit_index: %d, last_applied: %d",
                 role,
                 commit_index,
                 last_applied);
      };
    }
    // unlock and sleep for a while
    std::this_thread::sleep_for(std::chrono::milliseconds(apply_time_interval));
  }

}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {

  // Leader sends heartbeat messages to all of the other servers
  // to establish its authority and prevent new elections(raft::run_background_ping).

  // Periodly send empty append_entries RPC (heartbeats) to the followers.

  // Only work for the leader.

  while (true) {
    if (is_stopped()) return;
    // Lab3: Your code here:
    {
      std::unique_lock<std::mutex> lock(mtx);
      if (is_leader(current_term)) {
        for (int id = 0; id < num_nodes(); ++id) {
          if (id == my_id) continue;
          log_entry<command> prev_log = get_log_by_logical_index(next_index[id] - 1);
          append_entries_args<command> arg(current_term, my_id, prev_log.index_, prev_log.term_, commit_index, true);
          thread_pool->addObjJob(this, &raft::send_append_entries, id, arg);
        }
        RAFT_LOG("heartbeat sent");
      }
    }
    // send heartbeat periodically
    std::this_thread::sleep_for(std::chrono::milliseconds(heartbeat_time_interval));
  }

}

/******************************************************************

                        Other functions

*******************************************************************/

#endif // raft_h