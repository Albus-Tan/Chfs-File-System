// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include "extent_protocol.h"
#include "extent_server.h"

class extent_client {
 private:
  extent_server *es;

 public:
  extent_client();

  extent_protocol::status create(uint32_t type, extent_protocol::extentid_t &eid, chfs_command::txid_t txid = 0);
  extent_protocol::status get(extent_protocol::extentid_t eid, 
			                        std::string &buf, chfs_command::txid_t txid = 0);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, 
				                          extent_protocol::attr &a, chfs_command::txid_t txid = 0);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf, chfs_command::txid_t txid = 0);
  extent_protocol::status remove(extent_protocol::extentid_t eid, chfs_command::txid_t txid = 0);

  // transaction log
  void commit_transaction(chfs_command::txid_t txid);
  chfs_command::txid_t begin_transaction();
};

#endif 

