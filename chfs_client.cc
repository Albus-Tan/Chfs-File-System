// chfs client.  implements FS operations using extent and lock server
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "chfs_client.h"
#include "extent_client.h"

/* 
 * Your code here for Lab2A:
 * Here we treat each ChFS operation(especially write operation such as 'create', 
 * 'write' and 'symlink') as a transaction, your job is to use write ahead log 
 * to achive all-or-nothing for these transactions.
 */

chfs_client::chfs_client()
{
    ec = new extent_client();

}

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a file\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
chfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    // return ! isfile(inum);
  extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
      printf("error getting attr\n");
      return false;
    }

    if (a.type == extent_protocol::T_DIR) {
      printf("isdir: %lld is a dir\n", inum);
      return true;
    }
    printf("isdir: %lld is not a dir\n", inum);
    return false;
}

bool
chfs_client::issymlink(inum inum)
{
  extent_protocol::attr a;

  if (ec->getattr(inum, a) != extent_protocol::OK) {
    printf("error getting attr\n");
    return false;
  }

  if (a.type == extent_protocol::T_SYMLINK) {
    printf("issymlink: %lld is a symlink\n", inum);
    return true;
  }
  printf("issymlink: %lld is not a symlink\n", inum);
  return false;
}

int
chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    std::string buf;
    // get the content of inode ino
    ec->get(ino, buf);
    // modify its content according to the size (<, =, or >) content length.
    buf.resize(size);
    ec->put(ino, buf);

    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found = false;
    inum inum_fd;
    lookup(parent, name, found, inum_fd);
    std::string buf = "";
    if(found) {
      // file exist
      return EXIST;
    } else {
      // create file
      ec->create(extent_protocol::T_FILE, ino_out);

      // modify the parent infomation to add entry
      // format for directory: name/inum/name/inum/name/inum/
      // as / can not be part of file name
      // read directory
      ec->get(parent, buf);
      if(CHFS_CLIENT_LOG) printf("chfs_client::create: create file, get parent dir content:\n %s\n" , buf.c_str());
      buf += (std::string(name) + "/" + filename(ino_out) + "/");
      ec->put(parent, buf);
    }

    if(CHFS_CLIENT_LOG) printf("chfs_client::create: create file, put parent dir content:\n %s\n" , buf.c_str());

    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    bool found = false;
    inum inum_fd;
    lookup(parent, name, found, inum_fd);
    if(found) {
      // dir exist
      return EXIST;
    } else {
      // create dir
      ec->create(extent_protocol::T_DIR, ino_out);

      // modify the parent infomation to add entry
      // format for directory: name/inum/name/inum/name/inum/
      // as / can not be part of file name
      std::string buf = "";
      // read directory
      ec->get(parent, buf);
      buf += (std::string(name) + "/" + filename(ino_out) + "/");
      ec->put(parent, buf);
    }

    return r;
}

int
chfs_client::symlink(inum parent, const char *name, const char *link, inum &ino_out)
{
  int r = OK;

  bool found = false;
  inum inum_fd;
  lookup(parent, name, found, inum_fd);
  if(found) {
    // symlink exist
    return EXIST;
  } else {
    // create symlink ( store it just under path of name )
    // pick an inum to store
    ec->create(extent_protocol::T_SYMLINK, ino_out);
    // add symlink content ( link str )
    ec->put(ino_out, std::string(link));

    // modify the parent infomation to add entry
    // format for directory: name/inum/name/inum/name/inum/
    // as / can not be part of file name
    std::string buf = "";
    // read directory
    ec->get(parent, buf);
    buf += (std::string(name) + "/" + filename(ino_out) + "/");
    ec->put(parent, buf);
  }

  return r;
}

int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */

    if(CHFS_CLIENT_LOG) printf("chfs_client::lookup: step in\n");

    // read parent dir
    std::list<dirent> list;
    readdir(parent, list);

    if(CHFS_CLIENT_LOG) printf("chfs_client::lookup: parent directory std::list<dirent> content\n");

    // check file name in list
    for(dirent d : list){
      if(CHFS_CLIENT_LOG) {
        printf("chfs_client::lookup: file name: %s, inum %llu\n", d.name.c_str(), d.inum);
      }
      // const char *name, string d.name
      // !!! use == to judge equal char * does not mean strcmp
      //if(d.name.c_str() == name){
      if(strcmp(d.name.c_str(), name) == 0){
        // found
        found = true;
        ino_out = d.inum;
        return r;
      }
    }

    // not found
    found = false;
    return r;
}

int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */

    // format for directory: name/inum/name/inum/name/inum/
    // as / can not be part of file name

    if(CHFS_CLIENT_LOG) printf("chfs_client::readdir: step in\n");

    std::string buf = "";
    // read directory
    ec->get(dir, buf);

    if(CHFS_CLIENT_LOG) printf("chfs_client::readdir: buf content\n%s\n", buf.c_str());

    // parse content of dir
    size_t name_begin = 0;
    size_t name_end = buf.find('/', name_begin);
    size_t inum_begin, inum_end;
    while(name_end != std::string::npos){
      inum_begin = name_end + 1;
      inum_end = buf.find('/', inum_begin);
      struct dirent d;
      std::string tmp = buf.substr(name_begin, name_end - name_begin);
      d.name = tmp;
      if(CHFS_CLIENT_LOG) printf("chfs_client::readdir: file name from %zu to %zu is %s\n", name_begin, name_end, tmp.c_str());
      d.inum = n2i(buf.substr(inum_begin, inum_end - inum_begin));
      // add name inum pair to list
      list.push_back(d);
      name_begin = inum_end + 1;
      name_end = buf.find('/', name_begin);
    }
    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    std::string buf;
    ec->get(ino, buf);
    size_t buf_size = buf.size();

    // offset 超出长度范围
    if(off > buf_size){
      printf("chfs_client: FAILED, read offset larger than file size\n");
      data = "";
      return r;
    }
    // offset + size 超出长度范围
    if((off + size) > buf_size){
      printf("chfs_client: read size + offset larger than file size\n");
      data = buf.substr(off);
      return r;
    }
    // offset + size 在长度范围之内
    data = buf.substr(off, size);
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    std::string buf;
    ec->get(ino, buf);
    size_t buf_size = buf.size();

    // when off > length of original file, fill the holes with '\0'.
    if(off > buf_size){
      // make file buf larger
      buf.resize(off + size);
      // fill the holes with '\0'
      for(size_t i = buf_size; i < off; ++i){
        buf[i] = '\0';
      }
      // fill with data
      for(size_t i = off; i < off + size; ++i){
        buf[i] = data[i - off];
      }
      bytes_written = size;
      ec->put(ino, buf);
      return r;
    }
    // off + size > buf_size, off <= buf_size
    if(off + size > buf_size){
      // make file buf larger
      buf.resize(off + size);
      // fill with data
      for(size_t i = off; i < off + size; ++i){
        buf[i] = data[i - off];
      }
      bytes_written = size;
      ec->put(ino, buf);
      return r;
    }
    // off + size <= buf_size
    // fill with data
    for(size_t i = off; i < off + size; ++i){
      buf[i] = data[i - off];
    }
    bytes_written = size;
    ec->put(ino, buf);

    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    bool found = false;
    inum inum_fd;
    lookup(parent, name, found, inum_fd);
    if(found){

      // remove the file using ec->remove
      ec->remove(inum_fd);

      // update parent directory content
      std::string buf;
      ec->get(parent, buf);
      size_t entry_start = buf.find(name);
      size_t name_end = buf.find('/', entry_start);
      // !!! find from name_end + 1
      size_t entry_end = buf.find('/', name_end + 1);
      // also delete the / at end
      buf.erase(entry_start, entry_end - entry_start + 1);
      ec->put(parent, buf);
    } else {
      // not found
      printf("chfs_client::unlink: file %s not found\n", name);
    }

    return r;
}

int chfs_client::readlink(inum inode,std::string &buf)
{
  // read from inode, fill link into buf
  ec->get(inode, buf);
  return OK;
}


