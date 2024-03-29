#include "extent_server_dist.h"

chfs_raft *extent_server_dist::leader() const {
  EXTENT_SERVER_DIST_LOG("step in");
  int leader = this->raft_group->check_exact_one_leader();
  if (leader < 0) {
    EXTENT_SERVER_DIST_LOG("leader < 0");
    return this->raft_group->nodes[0];
  } else {
    EXTENT_SERVER_DIST_LOG("leader >= 0");
    return this->raft_group->nodes[leader];
  }
}

int extent_server_dist::create(uint32_t type, extent_protocol::extentid_t &id) {

  EXTENT_SERVER_DIST_LOG("step in");

  // Lab3: your code here
  int term, index;

  // construct command
  chfs_command_raft cmd(chfs_command_raft::command_type::CMD_CRT, type, id, std::string(""));

  EXTENT_SERVER_DIST_LOG("chfs_command_raft constructed");

  bool new_command_sent = false;
  do {
    new_command_sent = leader()->new_command(cmd, term, index);
    mssleep(sleep_interval_milliseconds);
  } while (!new_command_sent);

  EXTENT_SERVER_DIST_LOG("new_command successfully sent to leader");

  if (new_command_sent) {
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    if (!cmd.res->done) {
      ASSERT(
          cmd.res->cv.wait_until(lock,
                                 std::chrono::system_clock::now() + std::chrono::milliseconds(cv_timeout_milliseconds))
              == std::cv_status::no_timeout,
          "extent_server_dist::create command timeout");
    }

    // get return result
    id = cmd.res->id;

    return extent_protocol::OK;
  }
  return extent_protocol::xxstatus::RPCERR;

}

int extent_server_dist::put(extent_protocol::extentid_t id, std::string buf, int &) {

  EXTENT_SERVER_DIST_LOG("step in");

  // Lab3: your code here
  int term, index;
  uint32_t type;

  // construct command
  chfs_command_raft cmd(chfs_command_raft::command_type::CMD_PUT, type, id, buf);

  EXTENT_SERVER_DIST_LOG("chfs_command_raft constructed");

  bool new_command_sent = false;
  do {
    EXTENT_SERVER_DIST_LOG("prepare to send new_command to leader");
    new_command_sent = leader()->new_command(cmd, term, index);
    mssleep(sleep_interval_milliseconds);
  } while (!new_command_sent);

  EXTENT_SERVER_DIST_LOG("new_command successfully sent to leader, id %llu, buf size %llu", cmd.id, buf.size());

  if (new_command_sent) {
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    if (!cmd.res->done) {
      EXTENT_SERVER_DIST_LOG("cv.wait_until start");
      ASSERT(
          cmd.res->cv.wait_until(lock,
                                 std::chrono::system_clock::now() + std::chrono::milliseconds(cv_timeout_milliseconds))
              == std::cv_status::no_timeout,
          "extent_server_dist::put command timeout");
    }

    // get return result
    return extent_protocol::OK;
  }
  return extent_protocol::xxstatus::RPCERR;

}

int extent_server_dist::get(extent_protocol::extentid_t id, std::string &buf) {

  EXTENT_SERVER_DIST_LOG("step in");

  // Lab3: your code here
  int term, index;
  uint32_t type;
  std::string buf_para;

  // construct command
  chfs_command_raft cmd(chfs_command_raft::command_type::CMD_GET, type, id, buf_para);

  bool new_command_sent = false;
  do {
    new_command_sent = leader()->new_command(cmd, term, index);
    mssleep(sleep_interval_milliseconds);
  } while (!new_command_sent);
  if (new_command_sent) {
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    EXTENT_SERVER_DIST_LOG("new_command successfully sent to leader, id %llu", cmd.id);
    if (!cmd.res->done) {
      ASSERT(
          cmd.res->cv.wait_until(lock,
                                 std::chrono::system_clock::now() + std::chrono::milliseconds(cv_timeout_milliseconds))
              == std::cv_status::no_timeout,
          "extent_server_dist::get command timeout");
    }

    // get return result
    buf = cmd.res->buf;

    return extent_protocol::OK;
  }
  return extent_protocol::xxstatus::RPCERR;
}

int extent_server_dist::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a) {

  EXTENT_SERVER_DIST_LOG("step in");

  // Lab3: your code here

  int term, index;
  uint32_t type;
  std::string buf_para;

  // construct command
  chfs_command_raft cmd(chfs_command_raft::command_type::CMD_GETA, type, id, buf_para);

  bool new_command_sent = false;
  do {
    new_command_sent = leader()->new_command(cmd, term, index);
    mssleep(sleep_interval_milliseconds);
  } while (!new_command_sent);
  if (new_command_sent) {
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    EXTENT_SERVER_DIST_LOG("new_command successfully sent to leader, id %llu", cmd.id);
    if (!cmd.res->done) {
      ASSERT(
          cmd.res->cv.wait_until(lock,
                                 std::chrono::system_clock::now() + std::chrono::milliseconds(cv_timeout_milliseconds))
              == std::cv_status::no_timeout,
          "extent_server_dist::getattr command timeout");
    }

    // get return result
    a = cmd.res->attr;

    return extent_protocol::OK;
  }
  return extent_protocol::xxstatus::RPCERR;
}

int extent_server_dist::remove(extent_protocol::extentid_t id, int &) {

  EXTENT_SERVER_DIST_LOG("step in");

  // Lab3: your code here
  int term, index;
  uint32_t type;
  std::string buf_para;

  // construct command
  chfs_command_raft cmd(chfs_command_raft::command_type::CMD_RMV, type, id, buf_para);

  bool new_command_sent = false;
  do {
    new_command_sent = leader()->new_command(cmd, term, index);
    mssleep(sleep_interval_milliseconds);
  } while (!new_command_sent);
  if (new_command_sent) {
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    EXTENT_SERVER_DIST_LOG("new_command successfully sent to leader, id %llu", cmd.id);
    if (!cmd.res->done) {
      ASSERT(
          cmd.res->cv.wait_until(lock,
                                 std::chrono::system_clock::now() + std::chrono::milliseconds(cv_timeout_milliseconds))
              == std::cv_status::no_timeout,
          "extent_server_dist::remove command timeout");
    }

    // get return result
    return extent_protocol::OK;
  }
  return extent_protocol::xxstatus::RPCERR;
}

extent_server_dist::~extent_server_dist() {
  delete this->raft_group;
}