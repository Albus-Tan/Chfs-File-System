// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include <set>
#include "extent_protocol.h"

#include "inode_manager.h"
#include "persister.h"

#define EXTENT_SERVER_DEBUG 0

class extent_server {
 protected:
#if 0
  typedef struct extent {
    std::string data;
    struct extent_protocol::attr attr;
  } extent_t;
  std::map <extent_protocol::extentid_t, extent_t> extents;
#endif
  inode_manager *im;
  chfs_persister *_persister;

 private:
  void redo_log_commands();
  bool on_redoing_logs = false;

  // next transaction id
  chfs_command::txid_t next_txid = 1;

 public:
  extent_server();

  int create(uint32_t type, extent_protocol::extentid_t &id, chfs_command::txid_t txid = 0);
  int put(extent_protocol::extentid_t id, std::string, int &, chfs_command::txid_t txid = 0);
  int get(extent_protocol::extentid_t id, std::string &, chfs_command::txid_t txid = 0);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &, chfs_command::txid_t txid = 0);
  int remove(extent_protocol::extentid_t id, int &, chfs_command::txid_t txid = 0);

  // Your code here for lab2A: add logging APIs

  // log
  void log_create(uint32_t type, extent_protocol::extentid_t &id, chfs_command::txid_t txid);
  void log_put(extent_protocol::extentid_t id, std::string, chfs_command::txid_t txid);
  void log_get(extent_protocol::extentid_t id, chfs_command::txid_t txid);
  void log_getattr(extent_protocol::extentid_t id, chfs_command::txid_t txid);
  void log_remove(extent_protocol::extentid_t id, chfs_command::txid_t txid);

  // redo
  void redo_create(char* params_buf);
  void redo_put(char* params_buf, uint64_t params_size);
  void redo_get(char* params_buf);
  void redo_getattr(char* params_buf);
  void redo_remove(char* params_buf);

  // transaction
  void commit_transaction(chfs_command::txid_t txid);
  chfs_command::txid_t begin_transaction();

};

#endif 







