#ifndef XFS2_FSAPI_INTERNAL_H
#define XFS2_FSAPI_INTERNAL_H

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <stdbool.h>

#include <sys/stat.h>

typedef long xfs2_fs_config_t;
typedef long xfs2_fs_t;
typedef long xfs2_dir_t;
typedef long xfs2_file_t;

#define IS_DIRECTORY(t) (t) == 1
#define IS_REGULAR(t) (t) == 2
#define IS_SYMLINK(t) (t) == 3
#define IS_FIFO(t) (t) == 4
#define IS_IRREGULAR(t) (t) == 99

void set_errno(int err);

int64_t macro_UTIME_NOW();
int64_t macro_UTIME_OMIT();

struct xfs2_stat {
  uint8_t st_type;
  uint64_t st_size;
  struct timespec st_atim;
  struct timespec st_mtim;
  struct timespec st_ctim;
  uint64_t st_ino;
  uint64_t st_nlink;
  const char name[256];
};


struct xfs2_dirent {
  const char name[256];
};

#endif /* XFS2_FSAPI_INTERNAL_H */
