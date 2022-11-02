// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

pthread_mutex_t lock_server::mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t lock_server::cond = PTHREAD_COND_INITIALIZER;

lock_server::lock_server():
  nacquire (0)
{
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  // Your lab2B part2 code goes here

  pthread_mutex_lock(&mutex); // lock to use lock_state_table
  std::map<lock_protocol::lockid_t, lock_state>::iterator it = lock_state_table.find(lid);
  if(it != lock_state_table.end()){
    // first check if the lock is locked
    // Threads should wait on a condition variable inside a loop that checks the boolean condition on which the thread is waiting
    while(lock_state_table[lid] == lock_state::LOCKED){  // need to check again state of lock after leaving condition
      // locked, handler should block until the lock is free
      pthread_cond_wait(&cond, &mutex);  // wait for condition change
    }
    // free, modify to locked
    lock_state_table[lid] = lock_state::LOCKED;
  } else {
    //  client asks for a lock that the server has never seen before
    //  create the lock and grant it to the client
    lock_state_table.insert(std::pair<lock_protocol::lockid_t, lock_state>(lid, lock_state::LOCKED));
  }
  pthread_mutex_unlock(&mutex);  // unlock mutex
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  // Your lab2B part2 code goes here
  pthread_mutex_lock(&mutex); // lock to use lock_state_table
  std::map<lock_protocol::lockid_t, lock_state>::iterator it = lock_state_table.find(lid);
  if(it == lock_state_table.end()){
    // lock not found
    ret = lock_protocol::NOENT;
  } else {
    // change the lock state to free
    lock_state_table[lid] = lock_state::FREE;
    // notify any threads that are waiting for the lock
    pthread_cond_signal(&cond);
  }
  pthread_mutex_unlock(&mutex);  // unlock mutex
  return ret;
}