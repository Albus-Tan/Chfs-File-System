// this is the extent server

#ifndef extent_server_dist_h
#define extent_server_dist_h

#include "extent_protocol.h"
#include <map>
#include <string>
#include "raft.h"
#include "extent_server.h"
#include "raft_test_utils.h"
#include "chfs_state_machine.h"

using chfs_raft = raft<chfs_state_machine, chfs_command_raft>;
using chfs_raft_group = raft_group<chfs_state_machine, chfs_command_raft>;

class extent_server_dist {
#define EXTENT_SERVER_DIST_LOG(fmt, args...) \
    do {                       \
    } while (0);

//#define EXTENT_SERVER_DIST_LOG(fmt, args...)                                                                                   \
//     do {                                                                                                                   \
//         printf("[extent_server_dist][%s:%d:%s] " fmt "\n", __FILE__, __LINE__, __FUNCTION__ , ##args); \
//     } while (0);

 private:
  const int cv_timeout_milliseconds = 1000;
  const int sleep_interval_milliseconds = 20;
 public:
  chfs_raft_group *raft_group;
  extent_server_dist(const int num_raft_nodes = 3) {
    EXTENT_SERVER_DIST_LOG("step in");
    raft_group = new chfs_raft_group(num_raft_nodes);
    EXTENT_SERVER_DIST_LOG("chfs_raft_group successfully created");
  };

  chfs_raft *leader() const;

  int create(uint32_t type, extent_protocol::extentid_t &id);
  int put(extent_protocol::extentid_t id, std::string, int &);
  int get(extent_protocol::extentid_t id, std::string &);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int remove(extent_protocol::extentid_t id, int &);

  ~extent_server_dist();
};

#endif
