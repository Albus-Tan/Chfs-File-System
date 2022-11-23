#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <fstream>

// persist the Raft log and metadata
template<typename command>
class raft_storage {
 public:
  raft_storage(const std::string &file_dir);
  ~raft_storage();
  // Lab3: Your code here
  void persist_log(const log_entry<command> &log);
  void persist_logs(const std::vector<log_entry<command> > &logs);
  void restore_log(std::vector<log_entry<command> > &log);

  void persist_metadata(int current_term, int voted_for);
  void restore_metadata(int &current_term, int &voted_for);

 private:
  std::mutex mtx;
  // Lab3: Your code here
  std::string dir_;
  std::string log_file_path;
  std::string metadata_file_path;
};

template<typename command>
raft_storage<command>::raft_storage(const std::string &dir) {
  // Lab3: Your code here
  dir_ = dir;
  log_file_path = dir + "/log_file.log";
  metadata_file_path = dir + "/metadata_file.log";
}

template<typename command>
raft_storage<command>::~raft_storage() {
  // Lab3: Your code here
}

template<typename command>
void raft_storage<command>::persist_log(const log_entry<command> &log) {
  // Lab3: Your code here
  std::unique_lock<std::mutex> lock(mtx);
  std::ofstream ofs;
  ofs.open(log_file_path, std::ios::out | std::ios::app | std::ios::binary);
  if (ofs.is_open()) {
    ofs.write(reinterpret_cast<char *>(const_cast<int *>(&(log.term_))), sizeof(int));
    ofs.write(reinterpret_cast<char *>(const_cast<int *>(&(log.index_))), sizeof(int));

    int size = log.command_.size();
    ofs.write(reinterpret_cast<char *>(&size), sizeof(int));

    char *buf = new char[size];
    (log.command_).serialize(buf, size);
    ofs.write(buf, size);

    delete[] buf;

    ofs.close();
  }
}


template<typename command>
void raft_storage<command>::persist_logs(const std::vector<log_entry<command> > &logs) {
  // Lab3: Your code here
  std::unique_lock<std::mutex> lock(mtx);
  std::ofstream ofs;
  ofs.open(log_file_path, std::ios::out | std::ios::app | std::ios::binary);
  if (ofs.is_open()) {
    for(log_entry<command> log : logs){
      ofs.write(reinterpret_cast<char *>(&(log.term_)), sizeof(int));
      ofs.write(reinterpret_cast<char *>(&(log.index_)), sizeof(int));

      int size = log.command_.size();
      ofs.write(reinterpret_cast<char *>(&size), sizeof(int));

      char *buf = new char[size];
      (log.command_).serialize(buf, size);
      ofs.write(buf, size);

      delete[] buf;

      ofs.close();
    }
  }
}

template<typename command>
void raft_storage<command>::restore_log(std::vector<log_entry<command>> &log) {
  // Lab3: Your code here
  std::unique_lock<std::mutex> lock(mtx);
  std::ifstream ifs;
  ifs.open(log_file_path, std::ios::in | std::ios::binary);
  if (ifs.is_open()) {
    int term;
    int index;
    command cmd;

    while (ifs.read(reinterpret_cast<char *>(&term), sizeof(int))
        && ifs.read(reinterpret_cast<char *>(&index), sizeof(int))) {
      int size;
      char *buf;
      ifs.read(reinterpret_cast<char *>(&size), sizeof(int));

      buf = new char[size];

      ifs.read(buf, size);
      cmd.deserialize(buf, size);

      delete[] buf;

      // pushback until index
      while (index >= log.size()) log.push_back(log_entry<command>());
      log[index] = log_entry<command>(term, index, cmd);

    }

    ifs.close();
  }
}

template<typename command>
void raft_storage<command>::persist_metadata(int current_term, int voted_for) {
  std::unique_lock<std::mutex> lock(mtx);
  std::ofstream ofs;
  ofs.open(metadata_file_path, std::ios::out | std::ios::app | std::ios::binary);
  if (ofs.is_open()) {
    ofs.write(reinterpret_cast<char *>(&(current_term)), sizeof(int));
    ofs.write(reinterpret_cast<char *>(&(voted_for)), sizeof(int));
    ofs.close();
  }

}
template<typename command>
void raft_storage<command>::restore_metadata(int &current_term, int &voted_for) {
  std::unique_lock<std::mutex> lock(mtx);
  std::ifstream ifs;
  ifs.open(metadata_file_path, std::ios::in | std::ios::binary);
  if (ifs.is_open()) {

    // until get the latest metadata
    while (ifs.read(reinterpret_cast<char *>(&current_term), sizeof(int))
        && ifs.read(reinterpret_cast<char *>(&voted_for), sizeof(int))) {
    }

    ifs.close();
  }
}

#endif // raft_storage_h