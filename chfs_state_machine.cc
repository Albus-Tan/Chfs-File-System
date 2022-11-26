#include "chfs_state_machine.h"

chfs_command_raft::chfs_command_raft() : cmd_tp(CMD_NONE) {
  // Lab3: Your code here
}

chfs_command_raft::chfs_command_raft(const chfs_command_raft &cmd) :
    cmd_tp(cmd.cmd_tp), type(cmd.type), id(cmd.id), buf(cmd.buf), res(cmd.res) {
  // Lab3: Your code here
}

chfs_command_raft::chfs_command_raft(command_type cmd_tp,
                                     uint32_t type,
                                     extent_protocol::extentid_t id,
                                     const std::string &buf) :
    cmd_tp(cmd_tp), type(type), id(id), buf(buf) {
  res = std::make_shared<result>();
  res->tp = cmd_tp;
  res->start = std::chrono::system_clock::now();
}

chfs_command_raft::~chfs_command_raft() {
  // Lab3: Your code here

}

int chfs_command_raft::size() const {
  // Lab3: Your code here
  return sizeof(command_type) + sizeof(uint32_t) + sizeof(extent_protocol::extentid_t)
      + buf.size() + sizeof(int);  // int refer to the size of buf
}

void chfs_command_raft::serialize(char *buf_out, int size) const {
  // Lab3: Your code here

  // check the size
  if (size != this->size()) return;

  int pos = 0;
  // command_type cmd_tp
  memcpy(buf_out + pos, reinterpret_cast<char *>(const_cast<command_type *>(&cmd_tp)), sizeof(command_type));
  pos += sizeof(command_type);
  // uint32_t type
  memcpy(buf_out + pos, reinterpret_cast<char *>(const_cast<uint32_t *>(&type)), sizeof(uint32_t));
  pos += sizeof(uint32_t);
  // extent_protocol::extentid_t id
  memcpy(buf_out + pos, reinterpret_cast<char *>(const_cast<extent_protocol::extentid_t *>(&id)), sizeof(extent_protocol::extentid_t));
  pos += sizeof(extent_protocol::extentid_t);
  // size of std::string buf
  int buf_size = buf.size();
  memcpy(buf_out + pos, reinterpret_cast<char *>(&buf_size), sizeof(int));
  pos += sizeof(int);
  // std::string buf
  memcpy(buf_out + pos, const_cast<char *>(buf.c_str()), buf_size);
  pos += buf_size;

  return;
}

void chfs_command_raft::deserialize(const char *buf_in, int size) {
  // Lab3: Your code here

  int pos = 0;
  // command_type cmd_tp
  memcpy(reinterpret_cast<char *>(&cmd_tp), buf_in + pos, sizeof(command_type));
  pos += sizeof(command_type);
  // uint32_t type
  memcpy(reinterpret_cast<char *>(&type), buf_in + pos, sizeof(uint32_t));
  pos += sizeof(uint32_t);
  // extent_protocol::extentid_t id
  memcpy(reinterpret_cast<char *>(&id), buf_in + pos, sizeof(extent_protocol::extentid_t));
  pos += sizeof(extent_protocol::extentid_t);
  // size of std::string buf
  int buf_size;
  memcpy(reinterpret_cast<char *>(&buf_size), buf_in + pos, sizeof(int));
  pos += sizeof(int);
  // std::string buf
  buf = std::string(buf_in + pos, buf_size);
  pos += buf_size;

  return;
}

marshall &operator<<(marshall &m, const chfs_command_raft &cmd) {
  // Lab3: Your code here
  m << (int)cmd.cmd_tp << cmd.type << cmd.id << cmd.buf;
  return m;
}

unmarshall &operator>>(unmarshall &u, chfs_command_raft &cmd) {
  // Lab3: Your code here
  int cmd_tp;
  u >> cmd_tp >> cmd.type >> cmd.id >> cmd.buf;
  cmd.cmd_tp = chfs_command_raft::command_type(cmd_tp);
  return u;
}

void chfs_state_machine::apply_log(raft_command &cmd) {
  chfs_command_raft &chfs_cmd = dynamic_cast<chfs_command_raft &>(cmd);

  extent_protocol::extentid_t res_id;
  std::string res_buf;
  extent_protocol::attr res_attr;
  int r;

  switch (chfs_cmd.cmd_tp) {
    case chfs_command_raft::CMD_NONE: {
      break;
    }
    case chfs_command_raft::CMD_CRT: {
      std::unique_lock<std::mutex> lock(mtx); // you must use the lock to avoid contention.
      es.create(chfs_cmd.type, res_id);
      break;
    }
    case chfs_command_raft::CMD_PUT: {
      std::unique_lock<std::mutex> lock(mtx); // you must use the lock to avoid contention.
      es.put(chfs_cmd.id, chfs_cmd.buf, r);
      break;
    }
    case chfs_command_raft::CMD_GET: {
      std::unique_lock<std::mutex> lock(mtx); // you must use the lock to avoid contention.
      es.get(chfs_cmd.id, res_buf);
      break;
    }
    case chfs_command_raft::CMD_GETA: {
      std::unique_lock<std::mutex> lock(mtx); // you must use the lock to avoid contention.
      es.getattr(chfs_cmd.id, res_attr);
      break;
    }
    case chfs_command_raft::CMD_RMV: {
      std::unique_lock<std::mutex> lock(mtx); // you must use the lock to avoid contention.
      es.remove(chfs_cmd.id, r);
      break;
    }
  }

  {
    // fill result
    std::unique_lock<std::mutex> lock(chfs_cmd.res->mtx);
    chfs_cmd.res->tp = chfs_cmd.cmd_tp;
    chfs_cmd.res->id = res_id;
    chfs_cmd.res->buf = res_buf;
    chfs_cmd.res->attr = res_attr;

    chfs_cmd.res->done = true;  // set done true
    chfs_cmd.res->cv.notify_all();  // notify the caller
  }
  return;
}


