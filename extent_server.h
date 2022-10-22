// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include "extent_protocol.h"

#include "inode_manager.h"
#include "persister.h"

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

 public:
  extent_server();

  int create(uint32_t type, extent_protocol::extentid_t &id);
  int put(extent_protocol::extentid_t id, std::string, int &);
  int get(extent_protocol::extentid_t id, std::string &);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int remove(extent_protocol::extentid_t id, int &);

  // Your code here for lab2A: add logging APIs
  void log_create(uint32_t type, extent_protocol::extentid_t &id);
  void log_put(extent_protocol::extentid_t id, std::string);
  void log_get(extent_protocol::extentid_t id);
  void log_getattr(extent_protocol::extentid_t id);
  void log_remove(extent_protocol::extentid_t id);

  void redo_create(char* params_buf);
  void redo_put(char* params_buf, uint64_t params_size);
  void redo_get(char* params_buf);
  void redo_getattr(char* params_buf);
  void redo_remove(char* params_buf);

};

#endif 







