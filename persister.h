#ifndef persister_h
#define persister_h

#include <fcntl.h>
#include <mutex>
#include <iostream>
#include <fstream>
#include "rpc.h"
#include "utils.h"

#define MAX_LOG_SZ 1024

#define PRINT_DEBUG_INFO 0

/*
 * Your code here for Lab2A:
 * Implement class chfs_command, you may need to add command types such as
 * 'create', 'put' here to represent different commands a transaction requires. 
 * 
 * Here are some tips:
 * 1. each transaction in ChFS consists of several chfs_commands.
 * 2. each transaction in ChFS MUST contain a BEGIN command and a COMMIT command.
 * 3. each chfs_commands contains transaction ID, command type, and other information.
 * 4. you can treat a chfs_command as a log entry.
 */


class chfs_command {
public:
    typedef unsigned long long txid_t;
    enum cmd_type {
        CMD_BEGIN = 0,
        CMD_COMMIT,
        CMD_CREATE,
        CMD_PUT,
        CMD_GET,
        CMD_GETATTR,
        CMD_REMOVE,
    };

    // command type
    cmd_type type = CMD_BEGIN;

    // transaction id
    // start from 1
    // 0 represent belong to no transaction
    txid_t id = 0;

    // size of all params
    uint64_t params_size = 0;

    // params content
    char* params_buf = nullptr;

    // constructor
    chfs_command() {}

    uint64_t size() const {
      uint64_t s = sizeof(cmd_type) + sizeof(txid_t) + sizeof(uint64_t) + params_size;
        return s;
    }


    // LOG format
    // cmd_type txid_t chfs_command* params_size params_buf
    void format_log(char *buf) {
      uint64_t copied_size = 0;

      memcpy(buf + copied_size, reinterpret_cast<char *>(&type), sizeof(cmd_type));
      copied_size += sizeof(cmd_type);
      memcpy(buf + copied_size, reinterpret_cast<char *>(&id), sizeof(txid_t));
      copied_size += sizeof(txid_t);
      memcpy(buf + copied_size, reinterpret_cast<char *>(&params_size), sizeof(uint64_t));
      copied_size += sizeof(uint64_t);
      memcpy(buf + copied_size, params_buf, params_size);
    }

    void restore_log(std::ifstream &istrm){
      istrm.read(reinterpret_cast<char *>(&type), sizeof(cmd_type));
      istrm.read(reinterpret_cast<char *>(&id), sizeof(txid_t));
      istrm.read(reinterpret_cast<char *>(&params_size), sizeof(uint64_t));
      params_buf = (char *)malloc(params_size);
      istrm.read(params_buf, params_size);
#if PRINT_DEBUG_INFO
      print_cmd_info();
#endif
    }

    void print_cmd_info(){
      std::cout << "TYPE " << type << ", "
                << "TXID " << id << ", "
                << "PARA_SIZE " << params_size;
      if(type == CMD_PUT){

        extent_protocol::extentid_t inum;
        char *buf_ptr = (char *)malloc(params_size - sizeof(extent_protocol::extentid_t) + 1);

        uint64_t copied_size = 0;
        memcpy(reinterpret_cast<char *>(&inum), params_buf + copied_size, sizeof(extent_protocol::extentid_t));
        copied_size += sizeof(extent_protocol::extentid_t);
        memcpy(buf_ptr, params_buf + copied_size, params_size - sizeof(extent_protocol::extentid_t));
        memcpy(buf_ptr + params_size - sizeof(extent_protocol::extentid_t), "\0", 1);

        std::cout << ", INUM " << inum << ", " << std::endl << "BUF " << buf_ptr << std::endl;

        free(buf_ptr);
      } else {
        std::cout << std::endl;
      }
    }
};



/*
 * Your code here for Lab2A:
 * Implement class persister. A persister directly interacts with log files.
 * Remember it should not contain any transaction logic, its only job is to 
 * persist and recover data.
 * 
 * P.S. When and how to do checkpoint is up to you. Just keep your logfile size
 *      under MAX_LOG_SZ and checkpoint file size under DISK_SIZE.
 */
template<typename command>
class persister {

public:
    persister(const std::string& file_dir);
    ~persister();

    // persist data into solid binary file
    // You may modify parameters in these functions
    void append_log(command& log);
    void checkpoint();

    // restore data from solid binary file
    // You may modify parameters in these functions
    void restore_logdata();
    void restore_checkpoint();

    std::vector<command> get_restored_log_entries(){
      return log_entries;
    }

    void clear_restored_log_entries(){
      log_entries.clear();
    }

private:
    std::mutex mtx;
    std::string file_dir;
    std::string file_path_checkpoint;
    std::string file_path_logfile;

    // restored log data
    std::vector<command> log_entries;
};

template<typename command>
persister<command>::persister(const std::string& dir){

    // check if dir exists
    if(!utils::dirExists(dir)) {
      utils::mkdir(dir.c_str());
    }

    // DO NOT change the file names here
    file_dir = dir;
    file_path_checkpoint = file_dir + "/checkpoint.bin";
    file_path_logfile = file_dir + "/logdata.bin";
}

template<typename command>
persister<command>::~persister() {
    // Your code here for lab2A

}

template<typename command>
void persister<command>::append_log(command& log) {
    // Your code here for lab2A
    char* buf = (char *) malloc(log.size());
    log.format_log(buf);

    if(utils::get_file_size(file_path_logfile.c_str()) + log.size() + 1 >= MAX_LOG_SZ){
      checkpoint();
    }

    std::ofstream ostrm(file_path_logfile, std::ios::app | std::ios::binary);
    ostrm.write(buf, log.size());

    ostrm.write("\n",1);

    ostrm.flush();

    ostrm.close();

    free(buf);
}

template<typename command>
void persister<command>::checkpoint() {
    // Your code here for lab2A

    std::ifstream istrm(file_path_logfile, std::ios::binary);
    std::ofstream ostrm(file_path_checkpoint, std::ios::app | std::ios::binary);

    istrm.seekg(0, std::ios::end);
    long long length = istrm.tellg();  // C++ 支持的最大索引位置
    istrm.seekg(0);
    char buf[MAX_LOG_SZ + 1];
    while (length > 0)
    {
      int bufSize = length >= MAX_LOG_SZ ? MAX_LOG_SZ : length;
      istrm.read(buf, bufSize);
      ostrm.write(buf, bufSize);
      length -= bufSize;
    }

    ostrm.flush();
    ostrm.close();
    istrm.close();

    // clear log file
    utils::rmfile(file_path_logfile.c_str());
}

template<typename command>
void persister<command>::restore_logdata() {
    // Your code here for lab2A

    if(!utils::dirExists(file_dir)) {
      return;
    }

    std::ifstream istrm(file_path_logfile, std::ios::binary);
    // check if file exists
    if(!istrm.is_open()){
#if PRINT_DEBUG_INFO
      std::cout << __PRETTY_FUNCTION__ << "restore_logdata: file_path_logfile doesn't exist" << std::endl;
#endif
      return;
    }

    char c[1];
    while(!istrm.eof()){
      command log;
      log.restore_log(istrm);
      log_entries.push_back(log);
      istrm.read(c,1);
    }

    istrm.close();

};

template<typename command>
void persister<command>::restore_checkpoint() {
    // Your code here for lab2A

  if(!utils::dirExists(file_dir)) {
    return;
  }

  std::ifstream istrm(file_path_checkpoint, std::ios::binary);
  // check if file exists
  if(!istrm.is_open()){
#if PRINT_DEBUG_INFO
    std::cout << __PRETTY_FUNCTION__ << " restore_checkpoint: file_path_checkpoint doesn't exist" << std::endl;
#endif
    return;
  }

  char c[1];
  while(!istrm.eof()){
    command log;
    log.restore_log(istrm);
    log_entries.push_back(log);
    istrm.read(c,1);
  }

  istrm.close();

};

using chfs_persister = persister<chfs_command>;

#endif // persister_h