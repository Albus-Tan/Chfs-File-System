#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  // copy content in block to buf
  memcpy(buf,blocks[id],BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  // copy content in buf to block
  memcpy(blocks[id],buf,BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  // start to allocate from data part
  // |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
  // |  1 1 |   (nblocks)/BPB + 1 |(INODE_NUM)/IPB|
  uint32_t i = IBLOCK(INODE_NUM, sb.nblocks);
  for(; i < BLOCK_NUM; ++i){
    if(using_blocks[i] == 0) {
      // mark the corresponding bit in block bitmap
      using_blocks[i] = 1;
      return i;
    }
  }
  printf("\tim: ERROR alloc_block failed!\n");
  return 0; // alloc fail
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  // unmark the corresponding bit in the block bitmap
  using_blocks[id] = 0;
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // mark free block bitmap for |<-sb->|<-free block bitmap->|<-inode table->|
  for(uint32_t i = 0; i < IBLOCK(INODE_NUM, sb.nblocks); ++i){
    using_blocks[i] = 1;
  }

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  if(PRINT_LOG) printf("\tim: alloc_inode %d\n", type);

//  // check from where last alloc
//  static uint32_t idx = 0;
  uint32_t idx = last_alloc_idx;
  // check every inode once
  for(int i = 0; i < INODE_NUM; i++){
    idx = (idx + 1) % INODE_NUM;
    inode_t  *inode = get_inode(idx);
    if(inode != NULL) {free(inode); continue;}
    inode = (inode_t *)malloc(sizeof(inode_t));
    bzero(inode, sizeof(inode_t));
    inode->type = type;
    inode->atime = time(NULL);
    inode->mtime = time(NULL);
    inode->ctime = time(NULL);
    put_inode(idx, inode);
    last_alloc_idx = idx;
    free(inode);
    return idx;
  }

  printf("\tim: ERROR alloc_inode %d failed!\n", type);
  return -1;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */

  if(PRINT_LOG) printf("\tim: free_inode %d\n", inum);
  inode_t *inode = get_inode(inum);
  if(inode != NULL){
    inode->type = 0;
    put_inode(inum,inode);  // write back to disk
    free(inode);  // free its space
  }
  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];
  /* 
   * your code goes here.
   */
  if(PRINT_LOG) printf("\tim: get_inode %d\n", inum);

  if(inum < 0 || inum >= INODE_NUM){
    printf("\tim: ERROR inum out of range %d\n", inum);
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    if(PRINT_LOG) printf("\tim: inode not exist\n");
    return NULL;
  }

  /* Malloc inode and caller should release the memory. */
  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  if(PRINT_LOG) printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

uint32_t
inode_manager::get_block_id(uint32_t i, inode_t *inode)
{
  char buf[BLOCK_SIZE];
  // find block number according to data in inode
  if(i < NDIRECT){
    return inode->blocks[i];
  } else {
    if (i < MAXFILE){
      // check id in INDIRECT block pointed
      bm->read_block(inode->blocks[NDIRECT], buf);
      // return id according to pos in INDIRECT block (get directly by [] as all id are uint)
      return ((blockid_t *)buf)[i - NDIRECT];
    } else{
      printf("\tim: ERROR block id out of range\n");
      assert(0);
    }
  }
}

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  char buf[BLOCK_SIZE];

  if(PRINT_LOG) printf("\tim: read_file %d\n", inum);

  // read blocks related to inode number inum
  inode_t *inode = get_inode(inum);
  if(inode != NULL){

    *size = inode->size;
    // calculate block num and remain size
    uint32_t num_blocks = (inode->size)/BLOCK_SIZE;
    uint32_t size_remain = (inode->size)%BLOCK_SIZE;
    // copy them to buf_out
    // malloc buf
    *buf_out = (char *)malloc(inode->size);

    // read blocks
    for(uint32_t i = 0; i < num_blocks; ++i){
      // find block number according to data in inode inum
      bm->read_block(get_block_id(i, inode), buf);
      memcpy(*buf_out + i*BLOCK_SIZE, buf, BLOCK_SIZE);
    }

    if(size_remain != 0){
      // read remain
      bm->read_block(get_block_id(num_blocks, inode), buf);
      memcpy(*buf_out + num_blocks*BLOCK_SIZE, buf, size_remain);
    }

    free(inode);
  }

  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  if(PRINT_LOG) printf("\tim: write_file %d\n", inum);
  inode_t *inode = get_inode(inum);
  if(inode != NULL){
    // write buf to blocks of inode inum

    // calculate num of blocks have and blocks need
    uint32_t num_blocks_now = (inode->size % BLOCK_SIZE) ? inode->size/BLOCK_SIZE + 1 : inode->size/BLOCK_SIZE;
    uint32_t num_blocks_need = (size % BLOCK_SIZE) ? size/BLOCK_SIZE + 1 : size/BLOCK_SIZE;

    // adjust and malloc
    if(num_blocks_need > num_blocks_now){
      // malloc
      for(uint32_t i = num_blocks_now; i < num_blocks_need; ++i){
        if(i < NDIRECT){
          inode->blocks[i] = bm->alloc_block();
        } else {
          if(i < MAXFILE){
            // malloc block and record id in block pointed by idx NDIRECT
            if (!inode->blocks[NDIRECT]) {
              if(PRINT_LOG) printf("\tim: alloc new NDIRECT block\n");
              inode->blocks[NDIRECT] = bm->alloc_block();
            };
            char buf[BLOCK_SIZE];
            bm->read_block(inode->blocks[NDIRECT], buf);
            // write new block id to pos i - NDIRECT in block pointed by idx NDIRECT
            ((blockid_t*)buf)[i - NDIRECT] = bm->alloc_block();
            bm->write_block(inode->blocks[NDIRECT], buf);
          } else {
            printf("\tim: ERROR file too large\n");
            assert(0);
          }
        }
      }
    } else {
      if(num_blocks_need < num_blocks_now){
        // free
        for(uint32_t i = num_blocks_need; i < num_blocks_now; ++i){
          bm->free_block(get_block_id(i, inode));
        }
      }
    }

    // calculate block num and remain size
    uint32_t num_blocks = size/BLOCK_SIZE;
    uint32_t size_remain = size%BLOCK_SIZE;

    // write blocks
    for(uint32_t i = 0; i < num_blocks; ++i){
      // find block number according to data in inode inum
      bm->write_block(get_block_id(i, inode), buf + i*BLOCK_SIZE);
    }

    if(size_remain != 0){
      // read remain
      char remain_buf[BLOCK_SIZE];
      memcpy(remain_buf, buf + num_blocks*BLOCK_SIZE, size_remain);
      bm->write_block(get_block_id(num_blocks, inode), remain_buf);
    }

    inode->size = size;
    inode->atime = time(NULL);
    inode->mtime = time(NULL);
    inode->ctime = time(NULL);
    put_inode(inum,inode);
    free(inode);
  }
  return;
}

void
inode_manager::get_attr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  if(PRINT_LOG) printf("\tim: get_attr %d\n", inum);
  inode_t * inode = get_inode(inum);
  if(inode != NULL){
    a.type = inode->type;
    a.mtime = inode->mtime;
    a.ctime = inode->ctime;
    a.atime = inode->atime;
    a.size = inode->size;
    free(inode);
  }
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */

  if(PRINT_LOG) printf("\tim: remove_file %d\n", inum);

  inode_t *inode = get_inode(inum);
  if(inode != NULL){

    // calculate block num
    uint32_t num_blocks = (inode->size)%BLOCK_SIZE ? (inode->size)/BLOCK_SIZE + 1 : (inode->size)/BLOCK_SIZE;

    // remove data blocks
    for(uint32_t i = 0; i < num_blocks; ++i){
      bm->free_block(get_block_id(i, inode));
    }
    if(num_blocks > NDIRECT)
      bm->free_block(inode->blocks[NDIRECT]);
    // since need to check if blocks[NDIRECT] = NULL when malloc
    // clear that part data
    bzero(inode, sizeof(inode_t));

    // remove inode of the file
    free_inode(inum);
    free(inode);
  }

  return;
}
