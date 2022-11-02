// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"
#include <map>

class lock_server {

 protected:
  enum lock_state {
    FREE = 0,  // no clients own the lock
    LOCKED  // some client owns the lock
  };
  int nacquire;
  std::map<lock_protocol::lockid_t, lock_state> lock_state_table;

  // a single pthreads mutex for all of lock_server
  static pthread_mutex_t mutex;  // protect lock_state_table
  static pthread_cond_t cond;

 public:
  lock_server();
  ~lock_server() {};
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif 