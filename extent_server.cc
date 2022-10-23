// the extent server implementation

#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "extent_server.h"
#include "persister.h"

extent_server::extent_server() 
{
  im = new inode_manager();
  _persister = new chfs_persister("log"); // DO NOT change the dir name here

  // Your code here for Lab2A: recover data on startup
  _persister->restore_logdata();
  redo_log_commands();
}

void extent_server::redo_log_commands()
{
  on_redoing_logs = true;
  std::vector<chfs_command> log_entries = _persister->get_restored_log_entries();
  for(chfs_command log_entry : log_entries){
    char *params_buf = log_entry.params_buf;
    uint64_t params_size = log_entry.params_size;
    assert(params_buf);
    switch (log_entry.type) {
      case chfs_command::CMD_BEGIN:
        break;
      case chfs_command::CMD_COMMIT:
        break;
      case chfs_command::CMD_CREATE:
        redo_create(params_buf);
        break;
      case chfs_command::CMD_PUT:
        redo_put(params_buf, params_size);
        break;
      case chfs_command::CMD_GET:
        redo_get(params_buf);
        break;
      case chfs_command::CMD_GETATTR:
        redo_getattr(params_buf);
        break;
      case chfs_command::CMD_REMOVE:
        redo_remove(params_buf);
        break;
      default:
        assert(0);
    }
  }
  on_redoing_logs = false;
}

int extent_server::create(uint32_t type, extent_protocol::extentid_t &id)
{

  // alloc a new inode and return inum
  printf("extent_server: create inode\n");
  id = im->alloc_inode(type);

  if(!on_redoing_logs) log_create(type, id);

  return extent_protocol::OK;
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  if(!on_redoing_logs) log_put(id, buf);

  id &= 0x7fffffff;
  
  const char * cbuf = buf.c_str();
  int size = buf.size();
  im->write_file(id, cbuf, size);
  
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  if(!on_redoing_logs) log_get(id);
  printf("extent_server: get %lld\n", id);

  id &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;

  im->read_file(id, &cbuf, &size);
  if (size == 0)
    buf = "";
  else {
    buf.assign(cbuf, size);
    free(cbuf);
  }

  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  if(!on_redoing_logs) log_getattr(id);
  printf("extent_server: getattr %lld\n", id);

  id &= 0x7fffffff;
  
  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->get_attr(id, attr);
  a = attr;

  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  if(!on_redoing_logs) log_remove(id);
  printf("extent_server: write %lld\n", id);

  id &= 0x7fffffff;
  im->remove_file(id);
 
  return extent_protocol::OK;
}

void extent_server::log_create(uint32_t type, extent_protocol::extentid_t &id)
{
  chfs_command command;
  command.type = chfs_command::CMD_CREATE;
  command.params_size = sizeof(extent_protocol::extentid_t)+ sizeof(uint32_t);
  command.params_buf = (char *)malloc(command.params_size);

  uint64_t copied_size = 0;
  memcpy((command.params_buf) + copied_size, reinterpret_cast<char *>(&type), sizeof(uint32_t));
  copied_size += sizeof(uint32_t);
  memcpy((command.params_buf) + copied_size, reinterpret_cast<char *>(&id), sizeof(extent_protocol::extentid_t));

  _persister->append_log(command);

  free((command.params_buf));
}

void extent_server::log_put(extent_protocol::extentid_t id, std::string buf)
{

#if EXTENT_SERVER_DEBUG
  std::cout << "EXTENT_SERVER log_put: id " << id << " buf \n" << buf << std::endl;
#endif

  chfs_command command;
  command.type = chfs_command::CMD_PUT;
  command.params_size = sizeof(extent_protocol::extentid_t) + buf.size();
  command.params_buf = (char *)malloc(command.params_size);

  uint64_t copied_size = 0;
  memcpy((command.params_buf) + copied_size, reinterpret_cast<char *>(&id), sizeof(extent_protocol::extentid_t));
  copied_size += sizeof(extent_protocol::extentid_t);
  memcpy((command.params_buf) + copied_size, buf.c_str(), buf.size());

  _persister->append_log(command);

  free((command.params_buf));
}

void extent_server::log_get(extent_protocol::extentid_t id)
{
  chfs_command command;
  command.type = chfs_command::CMD_GET;
  command.params_size = sizeof(extent_protocol::extentid_t);
  command.params_buf = (char *)malloc(command.params_size);

  memcpy((command.params_buf), reinterpret_cast<char *>(&id), sizeof(extent_protocol::extentid_t));

  _persister->append_log(command);

  free((command.params_buf));
}

void extent_server::log_getattr(extent_protocol::extentid_t id)
{
  chfs_command command;
  command.type = chfs_command::CMD_GETATTR;
  command.params_size = sizeof(extent_protocol::extentid_t);
  command.params_buf = (char *)malloc(command.params_size);

  memcpy((command.params_buf), reinterpret_cast<char *>(&id), sizeof(extent_protocol::extentid_t));

  _persister->append_log(command);

  free((command.params_buf));
}

void extent_server::log_remove(extent_protocol::extentid_t id)
{
  chfs_command command;
  command.type = chfs_command::CMD_REMOVE;
  command.params_size = sizeof(extent_protocol::extentid_t);
  command.params_buf = (char *)malloc(command.params_size);

  memcpy((command.params_buf), reinterpret_cast<char *>(&id), sizeof(extent_protocol::extentid_t));

  _persister->append_log(command);

  free((command.params_buf));
}

void extent_server::redo_create(char* params_buf)
{

  uint32_t type;
  extent_protocol::extentid_t id;

  uint64_t copied_size = 0;
  memcpy(reinterpret_cast<char *>(&type), params_buf + copied_size, sizeof(uint32_t));
  copied_size += sizeof(uint32_t);
  memcpy(reinterpret_cast<char *>(&id), params_buf + copied_size,  sizeof(extent_protocol::extentid_t));

  // im->alloc_inode_appointed(type, id);
  // im->alloc_inode(type);
  create(type, id);

  free(params_buf);
}

void extent_server::redo_put(char* params_buf, uint64_t params_size)
{
  extent_protocol::extentid_t id;
  int i;
  // char *buf_ptr = (char *)malloc(params_size - sizeof(extent_protocol::extentid_t) + 1);
  std::string buf;

  uint64_t copied_size = 0;
  memcpy(reinterpret_cast<char *>(&id), params_buf + copied_size, sizeof(extent_protocol::extentid_t));
  copied_size += sizeof(extent_protocol::extentid_t);
  // memcpy(buf_ptr,params_buf + copied_size, params_size - sizeof(extent_protocol::extentid_t));
  // memcpy(buf_ptr + params_size - sizeof(extent_protocol::extentid_t), "\0", 1);

  buf.resize(params_size - sizeof(extent_protocol::extentid_t));
  memcpy(reinterpret_cast<char *>(&buf[0]), params_buf + copied_size, buf.size());

  // redo
  // do not directly use char* to construct string!
  // may have \0 in the middle and end str early!
  // need to check the size
  // std::string buf(buf_ptr);
  // free(buf_ptr);
  put(id, buf, i);

  free(params_buf);
}

void extent_server::redo_get(char* params_buf)
{
  // do not need to redo
  free(params_buf);
}

void extent_server::redo_getattr(char* params_buf)
{
  // do not need to redo
  free(params_buf);
}

void extent_server::redo_remove(char* params_buf)
{
  extent_protocol::extentid_t id;
  int i;

  memcpy(reinterpret_cast<char *>(&id), params_buf, sizeof(extent_protocol::extentid_t));

  remove(id, i);

  free(params_buf);
}


