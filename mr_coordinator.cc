#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <string>
#include <vector>
#include <mutex>

#include "mr_protocol.h"
#include "rpc.h"

using namespace std;

#define COO_LOG(fmt, args...) \
    do {                       \
    } while (0);

//#define COO_LOG(fmt, args...)                                                                                   \
//     do {                                                                                                         \
//         printf("[MR_COORDINATOR_LOG][%s:%d:%s] " fmt "\n", __FILE__, __LINE__, __FUNCTION__ , ##args);                     \
//     } while (0);

struct Task {
  int taskType;     // should be either Mapper or Reducer
  bool isAssigned;  // has been assigned to a worker
  bool isCompleted; // has been finised by a worker
  int index;        // index to the file
};

class Coordinator {
 public:
  Coordinator(const vector<string> &files, int nReduce);
  mr_protocol::status askTask(int, mr_protocol::AskTaskResponse &reply);
  mr_protocol::status submitTask(int taskType, int index, bool &success);
  bool isFinishedMap();
  bool isFinishedReduce();
  bool Done();

 private:
  vector<string> files;
  vector<Task> mapTasks;
  vector<Task> reduceTasks;

  mutex mtx; // A big lock to protect the whole data structure

  long completedMapCount;
  long completedReduceCount;
  bool isFinished;

  string getFile(int index);

};


// Your code here -- RPC handlers for the worker to call.

mr_protocol::status Coordinator::askTask(int, mr_protocol::AskTaskResponse &reply) {
  // Lab4 : Your code goes here.
  if (!isFinishedMap()) {
    mtx.lock();
    reply.tasktype = mr_tasktype::NONE;
    // map not finished
    for (auto &task : mapTasks) {
      if(!task.isAssigned){
        reply.tasktype = mr_tasktype::MAP;
        reply.index = task.index;
        reply.filename = files[task.index];
        task.isAssigned = true;
        COO_LOG("assign mapTask index %d", task.index)
        mtx.unlock();
        return mr_protocol::OK;
      }
    }
    mtx.unlock();
  } else if (!isFinishedReduce()) {
    mtx.lock();
    reply.tasktype = mr_tasktype::NONE;
    // map finished, reduce not finished
    for (auto &task : reduceTasks) {
      if(!task.isAssigned){
        reply.tasktype = mr_tasktype::REDUCE;
        reply.index = task.index;
        task.isAssigned = true;
        COO_LOG("assign reduceTask index %d", task.index)
        mtx.unlock();
        return mr_protocol::OK;
      }
    }
    mtx.unlock();
  } else {
    // all finished
    reply.tasktype = mr_tasktype::NONE;
  }
  return mr_protocol::OK;
}

mr_protocol::status Coordinator::submitTask(int taskType, int index, bool &success) {
  // Lab4 : Your code goes here.
  mtx.lock();
  switch (taskType) {
    case mr_tasktype::MAP: {
      ++completedMapCount;
      mapTasks[index].isCompleted = true;
      success = true;
      break;
    }
    case mr_tasktype::REDUCE: {
      ++completedReduceCount;
      reduceTasks[index].isCompleted = true;
      success = true;
      if (this->completedReduceCount >= long(this->reduceTasks.size())) {
        isFinished = true;
      }
      break;
    }
    case mr_tasktype::NONE: {
      success = true;
      break;
    }
  }
  mtx.unlock();
  return mr_protocol::OK;
}

string Coordinator::getFile(int index) {
  this->mtx.lock();
  string file = this->files[index];
  this->mtx.unlock();
  return file;
}

bool Coordinator::isFinishedMap() {
  bool isFinished = false;
  this->mtx.lock();
  if (this->completedMapCount >= long(this->mapTasks.size())) {
    isFinished = true;
  }
  this->mtx.unlock();
  return isFinished;
}

bool Coordinator::isFinishedReduce() {
  bool isFinished = false;
  this->mtx.lock();
  if (this->completedReduceCount >= long(this->reduceTasks.size())) {
    isFinished = true;
  }
  this->mtx.unlock();
  return isFinished;
}

//
// mr_coordinator calls Done() periodically to find out
// if the entire job has finished.
//
bool Coordinator::Done() {
  bool r = false;
  this->mtx.lock();
  r = this->isFinished;
  this->mtx.unlock();
  return r;
}

//
// create a Coordinator.
// nReduce is the number of reduce tasks to use.
//
Coordinator::Coordinator(const vector<string> &files, int nReduce) {
  this->files = files;
  this->isFinished = false;
  this->completedMapCount = 0;
  this->completedReduceCount = 0;

  int filesize = files.size();
  COO_LOG("filesize %d", filesize)
  for (int i = 0; i < filesize; i++) {
    COO_LOG("mapTask %d init", i)
    this->mapTasks.push_back(Task{mr_tasktype::MAP, false, false, i});
  }
  for (int i = 0; i < nReduce; i++) {
    COO_LOG("reduceTask %d init", i)
    this->reduceTasks.push_back(Task{mr_tasktype::REDUCE, false, false, i});
  }
}

int main(int argc, char *argv[]) {
  int count = 0;

  if (argc < 3) {
    fprintf(stderr, "Usage: %s <port-listen> <inputfiles>...\n", argv[0]);
    exit(1);
  }
  char *port_listen = argv[1];

  setvbuf(stdout, NULL, _IONBF, 0);

  char *count_env = getenv("RPC_COUNT");
  if (count_env != NULL) {
    count = atoi(count_env);
  }

  vector<string> files;
  char **p = &argv[2];
  while (*p) {
    files.push_back(string(*p));
    ++p;
  }

  rpcs server(atoi(port_listen), count);

  Coordinator c(files, REDUCER_COUNT);

  //
  // Lab4: Your code here.
  // Hints: Register "askTask" and "submitTask" as RPC handlers here
  //
  printf("mr coordinator started at port %d\n", atoi(port_listen));
  server.reg(mr_protocol::rpc_numbers::asktask, &c, &Coordinator::askTask);
  server.reg(mr_protocol::rpc_numbers::submittask, &c, &Coordinator::submitTask);

  while (!c.Done()) {
    sleep(1);
  }

  return 0;
}


