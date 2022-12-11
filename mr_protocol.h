#ifndef mr_protocol_h_
#define mr_protocol_h_

#include <string>
#include <vector>

#include "rpc.h"

using namespace std;

#define REDUCER_COUNT 4

enum mr_tasktype {
  NONE = 0, // this flag means no task needs to be performed at this point
  MAP,
  REDUCE
};

class mr_protocol {
 public:
  typedef int status;
  enum xxstatus { OK, RPCERR, NOENT, IOERR };
  enum rpc_numbers {
    asktask = 0xa001,
    submittask,
  };

  struct AskTaskResponse {
    // Lab4: Your definition here.
    int tasktype;
    int index;
    std::string filename;
  };

  struct AskTaskRequest {
    // Lab4: Your definition here.
  };

  struct SubmitTaskResponse {
    // Lab4: Your definition here.
  };

  struct SubmitTaskRequest {
    // Lab4: Your definition here.
  };

};

inline unmarshall &
operator>>(unmarshall &u, mr_protocol::AskTaskResponse &a)
{
  u >> a.filename;
  u >> a.tasktype;
  u >> a.index;
  return u;
}

inline marshall &
operator<<(marshall &m, mr_protocol::AskTaskResponse a)
{
  m << a.filename;
  m << a.tasktype;
  m << a.index;
  return m;
}

#endif

