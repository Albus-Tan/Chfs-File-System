#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <fstream>

// persist the Raft log and metadata
template<typename command>
class raft_storage {

#define STORAGE_LOG(fmt, args...) \
    do {                       \
    } while (0);

//#define STORAGE_LOG(fmt, args...)                                                                                   \
//     do {                                                                                                         \
//         auto now =                                                                                               \
//             std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
//                 std::chrono::system_clock::now().time_since_epoch())                                             \
//                 .count();                                                                                        \
//         printf("[%ld][%s:%d:%s][STORAGE_LOG] " fmt "\n", now, __FILE__, __LINE__, __FUNCTION__ , ##args); \
//     } while (0);

 public:
  raft_storage(const std::string &file_dir);
  ~raft_storage();
  // Lab3: Your code here
  void persist_log(const log_entry<command> &log);
  void persist_logs(const std::vector<log_entry<command>> &logs);
  void restore_log(std::vector<log_entry<command>> &log);

  void persist_metadata(int current_term, int voted_for);
  void restore_metadata(int &current_term, int &voted_for);

  void persist_snapshot(int last_included_index, int last_included_term, const std::vector<char> &data);
  void restore_snapshot(int &last_included_index, int &last_included_term, std::vector<char> &data);

 private:
  std::mutex mtx;
  std::mutex snapshot_mtx;
  // Lab3: Your code here
  std::string dir_;
  std::string log_file_path;
  std::string metadata_file_path;
  std::string snapshot_file_path;
};

template<typename command>
raft_storage<command>::raft_storage(const std::string &dir) {
  // Lab3: Your code here
  dir_ = dir;
  log_file_path = dir + "/log_file.log";
  metadata_file_path = dir + "/metadata_file.log";
  snapshot_file_path = dir + "/snapshot";
}

template<typename command>
raft_storage<command>::~raft_storage() {
  // Lab3: Your code here
}

template<typename command>
void raft_storage<command>::persist_log(const log_entry<command> &log) {
  // Lab3: Your code here
//  std::unique_lock<std::mutex> lock(mtx);
//  std::ofstream ofs;
//  ofs.open(log_file_path, std::ios::out | std::ios::app | std::ios::binary);
//  if (ofs.is_open()) {
//
//    ofs.write(reinterpret_cast<char *>(const_cast<int *>(&(log.term_))), sizeof(int));
//    ofs.write(reinterpret_cast<char *>(const_cast<int *>(&(log.index_))), sizeof(int));
//
//    int size = log.command_.size();
//    ofs.write(reinterpret_cast<char *>(&size), sizeof(int));
//
//    char *buf = new char[size];
//    (log.command_).serialize(buf, size);
//    ofs.write(buf, size);
//
//    delete[] buf;
//
//    ofs.close();
//  }
//  STORAGE_LOG("log persist success, term %d, index %d", log.term_, log.index_);
}

template<typename command>
void raft_storage<command>::persist_logs(const std::vector<log_entry<command>> &logs) {
  // Lab3: Your code here
//  std::unique_lock<std::mutex> lock(mtx);
//  std::ofstream ofs;
//  ofs.open(log_file_path, std::ios::out | std::ios::app | std::ios::binary);
//  if (ofs.is_open()) {
//    for (log_entry<command> log : logs) {
//      ofs.write(reinterpret_cast<char *>(&(log.term_)), sizeof(int));
//      ofs.write(reinterpret_cast<char *>(&(log.index_)), sizeof(int));
//
//      int size = log.command_.size();
//      ofs.write(reinterpret_cast<char *>(&size), sizeof(int));
//
//      char *buf = new char[size];
//      (log.command_).serialize(buf, size);
//      ofs.write(buf, size);
//
//      delete[] buf;
//
//      STORAGE_LOG("log persist success, term %d, index %d", log.term_, log.index_);
//    }
//    ofs.close();
//  }
}

template<typename command>
void raft_storage<command>::restore_log(std::vector<log_entry<command>> &log) {
//  // Lab3: Your code here
//  std::unique_lock<std::mutex> lock(mtx);
//  std::ifstream ifs;
//  ifs.open(log_file_path, std::ios::in | std::ios::binary);
//  if (ifs.is_open()) {
//    int term;
//    int index;
//    command cmd;
//
//    while (ifs.read(reinterpret_cast<char *>(&term), sizeof(int))
//        && ifs.read(reinterpret_cast<char *>(&index), sizeof(int))) {
//      int size;
//      char *buf;
//      ifs.read(reinterpret_cast<char *>(&size), sizeof(int));
//
//      buf = new char[size];
//
//      ifs.read(buf, size);
//      cmd.deserialize(buf, size);
//
//      delete[] buf;
//
//      // pushback until index
//      while (index >= log.size()) log.push_back(log_entry<command>());
//      log[index] = log_entry<command>(term, index, cmd);
//
//      STORAGE_LOG("log restore success, term %d, index %d", term, index);
//
//    }
//
//    // pop back for deleted logs
//    int last_index = log.back().index_;
//    STORAGE_LOG("log restore last_index %d, log.size() %d", last_index, log.size());
//    while (log.size() > last_index + 1) log.pop_back();
//
//    ifs.close();
//  }
}

template<typename command>
void raft_storage<command>::persist_metadata(int current_term, int voted_for) {
//  std::unique_lock<std::mutex> lock(mtx);
//  std::ofstream ofs;
//  ofs.open(metadata_file_path, std::ios::out | std::ios::app | std::ios::binary);
//  if (ofs.is_open()) {
//    ofs.write(reinterpret_cast<char *>(&(current_term)), sizeof(int));
//    ofs.write(reinterpret_cast<char *>(&(voted_for)), sizeof(int));
//    ofs.close();
//  }

}
template<typename command>
void raft_storage<command>::restore_metadata(int &current_term, int &voted_for) {
//  std::unique_lock<std::mutex> lock(mtx);
//  std::ifstream ifs;
//  ifs.open(metadata_file_path, std::ios::in | std::ios::binary);
//  if (ifs.is_open()) {
//
//    // until get the latest metadata
//    while (ifs.read(reinterpret_cast<char *>(&current_term), sizeof(int))
//        && ifs.read(reinterpret_cast<char *>(&voted_for), sizeof(int))) {
//    }
//
//    ifs.close();
//  }
}

template<typename command>
void raft_storage<command>::persist_snapshot(int last_included_index,
                                             int last_included_term,
                                             const std::vector<char> &data) {
//  std::unique_lock<std::mutex> lock(snapshot_mtx);
//  std::ofstream ofs;
//  ofs.open(snapshot_file_path, std::ios::out | std::ios::binary);
//  if(ofs.is_open()){
//    ofs.write(reinterpret_cast<char *>(&(last_included_index)), sizeof(int));
//    ofs.write(reinterpret_cast<char *>(&(last_included_term)), sizeof(int));
//    int size = data.size();
//    ofs.write(reinterpret_cast<char *>(&(size)), sizeof(int));
//    std::string buf(data.begin(), data.end());
//    ofs.write(const_cast<char *>(buf.c_str()), size);
//    ofs.close();
//  }

}

template<typename command>
void raft_storage<command>::restore_snapshot(int &last_included_index,
                                             int &last_included_term,
                                             std::vector<char> &data) {
//  std::unique_lock<std::mutex> lock(snapshot_mtx);
//  std::ifstream ifs;
//  ifs.open(snapshot_file_path, std::ios::in | std::ios::binary);
//  if (ifs.is_open()) {
//
//    ifs.read(reinterpret_cast<char *>(&last_included_index), sizeof(int));
//    ifs.read(reinterpret_cast<char *>(&last_included_term), sizeof(int));
//    int size;
//    ifs.read(reinterpret_cast<char *>(&size), sizeof(int));
//    char *buf = new char[size];
//    ifs.read(buf, size);
//    std::string string_buf(buf, size);
//    delete[] buf;
//    std::vector<char> data_(string_buf.begin(), string_buf.end());
//    data.swap(data_);
//    ifs.close();
//  }
}

#endif // raft_storage_h