/*
 * CDDL HEADER START
 *
 * The contents of this file are subject to the terms of the
 * Common Development and Distribution License (the "License").
 * You may not use this file except in compliance with the License.
 *
 * You can obtain a copy of the license at usr/src/OPENSOLARIS.LICENSE
 * or http://www.opensolaris.org/os/licensing.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL HEADER in each
 * file and include the License file at usr/src/OPENSOLARIS.LICENSE.
 * If applicable, add the following below this CDDL HEADER, with the
 * fields enclosed by brackets "[]" replaced with your own identifying
 * information: Portions Copyright [yyyy] [name of copyright owner]
 *
 * CDDL HEADER END
 */
/*
 * Copyright 2021 Basebit, Inc.  All rights reserved.
 * Use is subject to license terms.
 *
 * Portions Copyright 2021 Gangfeng Huang
 */

#include "config.h"
#include "filebench.h"
#include "flowop.h"
#include "threadflow.h" /* For aiolist definition */
#include "xfs2/libxfs2_fsapi.h"

#include <dirent.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <libgen.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/param.h>
#include <sys/resource.h>
#include <strings.h>

#include "filebench.h"
#include "fsplug.h"

#ifdef HAVE_AIO
#include <aio.h>
#endif /* HAVE_AIO */

/*
 * These routines implement local file access. They are placed into a
 * vector of functions that are called by all I/O operations in fileset.c
 * and flowop_library.c. This represents the default file system plug-in,
 * and may be replaced by vectors for other file system plug-ins.
 */

#define XFS_DEBUG 1 

#define XFS2_DEBUG(fmt, ...) \
            do { if (XFS_DEBUG) filebench_log(LOG_INFO, fmt, ##__VA_ARGS__); } while (0)

#define XFS2_ERROR(fmt, ...) \
	{ filebench_log(LOG_ERROR, fmt, ##__VA_ARGS__);}

static int fb_xfs2_freemem(fb_fdesc_t *fd, off64_t size);
static int fb_xfs2_open(fb_fdesc_t *, char *, int, int);
static int fb_xfs2_pread(fb_fdesc_t *, caddr_t, fbint_t, off64_t);
static int fb_xfs2_read(fb_fdesc_t *, caddr_t, fbint_t);
static int fb_xfs2_pwrite(fb_fdesc_t *, caddr_t, fbint_t, off64_t);
static int fb_xfs2_write(fb_fdesc_t *, caddr_t, fbint_t);
static int fb_xfs2_lseek(fb_fdesc_t *, off64_t, int);
static int fb_xfs2_truncate(fb_fdesc_t *, off64_t);
static int fb_xfs2_rename(const char *, const char *);
static int fb_xfs2_close(fb_fdesc_t *);
static int fb_xfs2_link(const char *, const char *);
static int fb_xfs2_symlink(const char *, const char *);
static int fb_xfs2_unlink(char *);
static ssize_t fb_xfs2_readlink(const char *, char *, size_t);
static int fb_xfs2_mkdir(char *, int);
static int fb_xfs2_rmdir(char *);
static fb_dir_t *fb_xfs2_opendir(char *);
static struct dirent *fb_xfs2_readdir(fb_dir_t *);
static int fb_xfs2_closedir(fb_dir_t *);
static int fb_xfs2_fsync(fb_fdesc_t *);
static int fb_xfs2_stat(char *, struct stat64 *);
static int fb_xfs2_fstat(fb_fdesc_t *, struct stat64 *);
static int fb_xfs2_access(const char *, int);
static void fb_xfs2_recur_rm(char *);

static fsplug_func_t fb_xfs2_funcs =
{
	"locfs",
	fb_xfs2_freemem,		/* flush page cache */
	fb_xfs2_open,		/* open */
	fb_xfs2_pread,		/* pread */
	fb_xfs2_read,		/* read */
	fb_xfs2_pwrite,		/* pwrite */
	fb_xfs2_write,		/* write */
	fb_xfs2_lseek,		/* lseek */
	fb_xfs2_truncate,	/* ftruncate */
	fb_xfs2_rename,		/* rename */
	fb_xfs2_close,		/* close */
	fb_xfs2_link,		/* link */
	fb_xfs2_symlink,		/* symlink */
	fb_xfs2_unlink,		/* unlink */
	fb_xfs2_readlink,	/* readlink */
	fb_xfs2_mkdir,		/* mkdir */
	fb_xfs2_rmdir,		/* rmdir */
	fb_xfs2_opendir,		/* opendir */
	fb_xfs2_readdir,		/* readdir */
	fb_xfs2_closedir,	/* closedir */
	fb_xfs2_fsync,		/* fsync */
	fb_xfs2_stat,		/* stat */
	fb_xfs2_fstat,		/* fstat */
	fb_xfs2_access,		/* access */
	fb_xfs2_recur_rm		/* recursive rm */
};

struct xfs2_c_client {
	char *mount_info;
	char *id;
	char *key;
} xfs2_client_t;

#ifdef HAVE_AIO
/*
 * Local file system asynchronous IO flowops are in this module, as
 * they have a number of local file system specific features.
 */
static int fb_xfs2flow_aiowrite(threadflow_t *threadflow, flowop_t *flowop);
static int fb_xfs2flow_aiowait(threadflow_t *threadflow, flowop_t *flowop);

static flowop_proto_t fb_xfs2flow_funcs[] = {
	{FLOW_TYPE_AIO, FLOW_ATTR_WRITE, "aiowrite", flowop_init_generic,
	fb_xfs2flow_aiowrite, flowop_destruct_generic},
	{FLOW_TYPE_AIO, 0, "aiowait", flowop_init_generic,
	fb_xfs2flow_aiowait, flowop_destruct_generic}
};

#endif /* HAVE_AIO */

const char* mount_id = "43a7719d-51d6-4bec-a5c0-6c5ba18285bb";
const char* xfs_server_addr = "10.0.1.211";
static xfs2_fs_t xfs2 = -1;

/*
 * Initialize file system functions vector to point to the vector of local file
 * system functions. This function will be called for the master process and
 * every created worker process.
 */
void
fb_xfs2_funcvecinit(void)
{
	filebench_log(LOG_INFO, "fb_xfs2_funcvecinit");
	fs_functions_vec = &fb_xfs2_funcs;
}

/*
 * Initialize those flowops which implementation is file system specific. It is
 * called only once in the master process.
 */
void
fb_xfs2_newflowops(void)
{
	filebench_log(LOG_INFO, "fb_xfs2_newflowops");

    xfs2_fs_config_t config = xfs2_new_fs_config((char*)mount_id, (char *)xfs_server_addr, 22501, 0, "", "");
    xfs2_fs_config_set_dial_timeout(config, 5000);
    xfs2_fs_config_set_write_timeout(config, 5000);
    xfs2_fs_config_set_read_timeout(config, 5000);
    xfs2_fs_config_set_max_retry_on_rpc_timeout(config, 3);
    xfs2 = xfs2_openfs(config);
    xfs2_destroy_fs_config(config);
    if (xfs2 == -1) {
		filebench_log(LOG_ERROR, "open filesystem error");
    }

#ifdef HAVE_AIO
	int nops;
	nops = sizeof (fb_xfs2flow_funcs) / sizeof (flowop_proto_t);
	flowop_add_from_proto(fb_xfs2flow_funcs, nops);
#endif /* HAVE_AIO */
}

/*
 * Frees up memory mapped file region of supplied size. The
 * file descriptor "fd" indicates which memory mapped file.
 * If successful, returns 0. Otherwise returns -1 if "size"
 * is zero, or -1 times the number of times msync() failed.
 */
static int
fb_xfs2_freemem(fb_fdesc_t *fd, off64_t size)
{
	off64_t left;
	int ret = 0;

	for (left = size; left > 0; left -= MMAP_SIZE) {
		off64_t thismapsize;
		caddr_t addr;

		thismapsize = MIN(MMAP_SIZE, left);
		addr = mmap64(0, thismapsize, PROT_READ|PROT_WRITE,
		    MAP_SHARED, fd->fd_num, size - left);
		ret += msync(addr, thismapsize, MS_INVALIDATE);
		(void) munmap(addr, thismapsize);
	}
	return (ret);
}

/*
 * Does a posix pread. Returns what the pread() returns.
 */
static int
fb_xfs2_pread(fb_fdesc_t *fd, caddr_t iobuf, fbint_t iosize, off64_t fileoffset)
{
	int ret = 0;
	if (fd == NULL) {
		XFS2_ERROR("fb_xfs2_read, invalid fd");
	}

	ret = xfs2_preadfile(fd->fd_num, (char *)iobuf, (ssize_t)iosize, (off_t)fileoffset);
	if (ret == -1) {
		XFS2_ERROR("fb_xfs2_pread, err : %d, path: %s", errno, fd->path);
	}
	return ret;
}

/*
 * Does a posix read. Returns what the read() returns.
 */
static int
fb_xfs2_read(fb_fdesc_t *fd, caddr_t iobuf, fbint_t iosize)
{
	int ret = 0;
	if (fd == NULL) {
		XFS2_ERROR("fb_xfs2_read, invalid fd");
	}

	ret = xfs2_readfile(fd->fd_num, (char *)iobuf, (ssize_t)iosize);
	if (ret == -1) {
		XFS2_ERROR("fb_xfs2_read, err : %d, path: %s", errno, fd->path);
	}
	return ret;
}

#ifdef HAVE_AIO

/*
 * Asynchronous write section. An Asynchronous IO element
 * (aiolist_t) is used to associate the asynchronous write request with
 * its subsequent completion. This element includes a aiocb64 struct
 * that is used by posix aio_xxx calls to track the asynchronous writes.
 * The flowops aiowrite and aiowait result in calls to these posix
 * aio_xxx system routines to do the actual asynchronous write IO
 * operations.
 */


/*
 * Allocates an asynchronous I/O list (aio, of type
 * aiolist_t) element. Adds it to the flowop thread's
 * threadflow aio list. Returns a pointer to the element.
 */
static aiolist_t *
aio_allocate(flowop_t *flowop)
{
	aiolist_t *aiolist;

	if ((aiolist = malloc(sizeof (aiolist_t))) == NULL) {
		filebench_log(LOG_ERROR, "malloc aiolist failed");
		filebench_shutdown(1);
	}

	bzero(aiolist, sizeof(*aiolist));

	/* Add to list */
	if (flowop->fo_thread->tf_aiolist == NULL) {
		flowop->fo_thread->tf_aiolist = aiolist;
		aiolist->al_next = NULL;
	} else {
		aiolist->al_next = flowop->fo_thread->tf_aiolist;
		flowop->fo_thread->tf_aiolist = aiolist;
	}
	return (aiolist);
}

/*
 * Searches for the aiolist element that has a matching
 * completion block, aiocb. If none found returns FILEBENCH_ERROR. If
 * found, removes the aiolist element from flowop thread's
 * list and returns FILEBENCH_OK.
 */
static int
aio_deallocate(flowop_t *flowop, struct aiocb64 *aiocb)
{
	aiolist_t *aiolist = flowop->fo_thread->tf_aiolist;
	aiolist_t *previous = NULL;
	aiolist_t *match = NULL;

	if (aiocb == NULL) {
		filebench_log(LOG_ERROR, "null aiocb deallocate");
		return (FILEBENCH_OK);
	}

	while (aiolist) {
		if (aiocb == &(aiolist->al_aiocb)) {
			match = aiolist;
			break;
		}
		previous = aiolist;
		aiolist = aiolist->al_next;
	}

	if (match == NULL)
		return (FILEBENCH_ERROR);

	/* Remove from the list */
	if (previous)
		previous->al_next = match->al_next;
	else
		flowop->fo_thread->tf_aiolist = match->al_next;

	return (FILEBENCH_OK);
}

/*
 * Emulate posix aiowrite(). Determines which file to use,
 * either one file of a fileset, or the file associated
 * with a fileobj, allocates and fills an aiolist_t element
 * for the write, and issues the asynchronous write. This
 * operation is only valid for random IO, and returns an
 * error if the flowop is set for sequential IO. Returns
 * FILEBENCH_OK on success, FILEBENCH_NORSC if iosetup can't
 * obtain a file to open, and FILEBENCH_ERROR on any
 * encountered error.
 */
static int
fb_xfs2flow_aiowrite(threadflow_t *threadflow, flowop_t *flowop)
{
	caddr_t iobuf;
	fbint_t wss;
	fbint_t iosize;
	fb_fdesc_t *fdesc;
	int ret;

	iosize = avd_get_int(flowop->fo_iosize);

	if ((ret = flowoplib_iosetup(threadflow, flowop, &wss, &iobuf,
	    &fdesc, iosize)) != FILEBENCH_OK)
		return (ret);

	if (avd_get_bool(flowop->fo_random)) {
		uint64_t fileoffset;
		struct aiocb64 *aiocb;
		aiolist_t *aiolist;

		if (wss < iosize) {
			filebench_log(LOG_ERROR,
			    "file size smaller than IO size for thread %s",
			    flowop->fo_name);
			return (FILEBENCH_ERROR);
		}

		fb_random64(&fileoffset, wss, iosize, NULL);

		aiolist = aio_allocate(flowop);
		aiolist->al_type = AL_WRITE;
		aiocb = &aiolist->al_aiocb;

		aiocb->aio_fildes = fdesc->fd_num;
		aiocb->aio_buf = iobuf;
		aiocb->aio_nbytes = (size_t)iosize;
		aiocb->aio_offset = (off64_t)fileoffset;
		aiocb->aio_reqprio = 0;

		filebench_log(LOG_DEBUG_IMPL,
		    "aio fd=%d, bytes=%llu, offset=%llu",
		    fdesc->fd_num, (u_longlong_t)iosize,
		    (u_longlong_t)fileoffset);

		flowop_beginop(threadflow, flowop);
		if (aio_write64(aiocb) < 0) {
			filebench_log(LOG_ERROR, "aiowrite failed: %s",
			    strerror(errno));
			filebench_shutdown(1);
		}
		flowop_endop(threadflow, flowop, iosize);
	} else {
		return (FILEBENCH_ERROR);
	}

	return (FILEBENCH_OK);
}



#define	MAXREAP 4096

/*
 * Emulate posix aiowait(). Waits for the completion of half the
 * outstanding asynchronous IOs, or a single IO, which ever is
 * larger. The routine will return after a sufficient number of
 * completed calls issued by any thread in the procflow have
 * completed, or a 1 second timout elapses. All completed
 * IO operations are deleted from the thread's aiolist.
 */
static int
fb_xfs2flow_aiowait(threadflow_t *threadflow, flowop_t *flowop)
{
	struct aiocb64 **worklist;
	aiolist_t *aio = flowop->fo_thread->tf_aiolist;
	int uncompleted = 0;
#ifdef HAVE_AIOWAITN
	int i;
#endif

	worklist = calloc(MAXREAP, sizeof (struct aiocb64 *));

	/* Count the list of pending aios */
	while (aio) {
		uncompleted++;
		aio = aio->al_next;
	}

	do {
		uint_t ncompleted = 0;
		uint_t todo;
		int inprogress;
#ifdef HAVE_AIOWAITN
		struct timespec timeout;

		/* Wait for half of the outstanding requests */
		timeout.tv_sec = 1;
		timeout.tv_nsec = 0;
#endif

		if (uncompleted > MAXREAP)
			todo = MAXREAP;
		else
			todo = uncompleted / 2;

		if (todo == 0)
			todo = 1;

		flowop_beginop(threadflow, flowop);

#ifdef HAVE_AIOWAITN
		if (((aio_waitn64((struct aiocb64 **)worklist,
		    MAXREAP, &todo, &timeout)) == -1) &&
		    errno && (errno != ETIME)) {
			filebench_log(LOG_ERROR,
			    "aiowait failed: %s, outstanding = %d, "
			    "ncompleted = %d ",
			    strerror(errno), uncompleted, todo);
		}

		ncompleted = todo;
		/* Take the  completed I/Os from the list */
		inprogress = 0;
		for (i = 0; i < ncompleted; i++) {
			if ((aio_return64(worklist[i]) == -1) &&
			    (errno == EINPROGRESS)) {
				inprogress++;
				continue;
			}
			if (aio_deallocate(flowop, worklist[i])
			    == FILEBENCH_ERROR) {
				filebench_log(LOG_ERROR, "Could not remove "
				    "aio from list ");
				flowop_endop(threadflow, flowop, 0);
				return (FILEBENCH_ERROR);
			}
		}

		uncompleted -= ncompleted;
		uncompleted += inprogress;

#else

		for (ncompleted = 0, inprogress = 0,
		    aio = flowop->fo_thread->tf_aiolist;
		    ncompleted < todo && aio != NULL; aio = aio->al_next) {
			int result = aio_error64(&aio->al_aiocb);

			if (result == EINPROGRESS) {
				inprogress++;
				continue;
			}

			if ((aio_return64(&aio->al_aiocb) == -1) || result) {
				filebench_log(LOG_ERROR, "aio failed: %s",
				    strerror(result));
				continue;
			}

			ncompleted++;

			if (aio_deallocate(flowop, &aio->al_aiocb) < 0) {
				filebench_log(LOG_ERROR, "Could not remove "
				    "aio from list ");
				flowop_endop(threadflow, flowop, 0);
				return (FILEBENCH_ERROR);
			}
		}

		uncompleted -= ncompleted;

#endif
		filebench_log(LOG_DEBUG_SCRIPT,
		    "aio2 completed %d ios, uncompleted = %d, inprogress = %d",
		    ncompleted, uncompleted, inprogress);

	} while (uncompleted > MAXREAP);

	flowop_endop(threadflow, flowop, 0);

	free(worklist);

	return (FILEBENCH_OK);
}

#endif /* HAVE_AIO */

/*
 * Does an open64 of a file. Inserts the file descriptor number returned
 * by open() into the supplied filebench fd. Returns FILEBENCH_OK on
 * successs, and FILEBENCH_ERROR on failure.
 */

static int
fb_xfs2_open(fb_fdesc_t *fd, char *path, int flags, int perms)
{
	xfs2_fs_t f = 0;
	XFS2_DEBUG("fb_xfs2_open: path: %s, flags: %d, perms: %d", path, flags, perms);
	f = xfs2_openfile(xfs2, path, flags|perms);
	if (f == - 1) {
		XFS2_ERROR("fs_xfs2_open, err: %d, path: %p", errno, path);
		return FILEBENCH_ERROR;
	}

	fd->fd_num = f;
	fd->path  = path;
	return FILEBENCH_OK;
}

/*
 * Does an unlink (delete) of a file.
 */
static int
fb_xfs2_unlink(char *path)
{
	int err = 0;
	XFS2_DEBUG("fb_xfs2_unlink, path: %s", path);
	err = xfs2_remove(xfs2, path);
	if (err == -1) {
		XFS2_ERROR("fb_xfs2_unlink, err:%d, path: %s", err, path);
	}
	return err;
}

/*
 * Does a readlink of a symbolic link.
 */
static ssize_t
fb_xfs2_readlink(const char *path, char *buf, size_t buf_size)
{
	ssize_t ret = 0;
	if (path == NULL || buf == NULL) {
		XFS2_ERROR("fb_xfs2_readlink, invalid argument");
	}

	XFS2_DEBUG("fb_xfs2_readlink, path: %s", path);
	ret = xfs2_readlink(xfs2, (char *)path, buf, buf_size);
	if (ret == -1) {
		XFS2_ERROR("fb_xfs2_readlink, err:%s, path: %s", errno, path);
	}

	return ret;
}

/*
 * Does fsync of a file. Returns with fsync return info.
 */
static int
fb_xfs2_fsync(fb_fdesc_t *fd)
{
	int ret = 0;
	if (fd == NULL) {
		XFS2_ERROR("fb_xfs2_lseek, fd is NULL");
	}

	ret = xfs2_syncfile(fd->fd_num);
	if (ret == -1) {
		XFS2_ERROR("fb_xfs2_fsync, err: %d", errno);
	}
	return ret;
}

/*
 * Do a posix lseek of a file. Return what lseek() returns.
 */
static int
fb_xfs2_lseek(fb_fdesc_t *fd, off64_t offset, int whence)
{
	int ret = 0;
	if (fd == NULL) {
		XFS2_ERROR("fb_xfs2_lseek, fd is NULL");
	}
	ret = xfs2_seekfile(fd->fd_num, offset, whence);
	if (ret == -1) {
		XFS2_ERROR("fb_xfs2_lseek, err: %d", errno);
	}
	return ret;
}

/*
 * Do a posix rename of a file. Return what rename() returns.
 */
static int
fb_xfs2_rename(const char *old, const char *new)
{
	int ret = 0;
	if (old == NULL || new == NULL) {
		XFS2_ERROR("fb_xfs2_rename, invalid argument");
	}

	ret = xfs2_rename(xfs2, (char *)old, (char *)new);
	if (ret == -1) {
		XFS2_ERROR("fb_xfs2_rename, err: %d, old: %s, new: %s", errno, old, new);
	}
	return ret;
}


/*
 * Do a posix close of a file. Return what close() returns.
 */
static int
fb_xfs2_close(fb_fdesc_t *fd)
{
	int ret = 0;
	if (fd == NULL) {
		XFS2_ERROR("fb_xfs2_close, fd is NULL");
	}
	ret = xfs2_closefile(fd->fd_num);
	if (ret == -1) {
		XFS2_ERROR("fb_xfs2_close, err: %d, fd %d, path: %s", errno, fd->fd_num, fd->path);
	}
	return ret;
}

/*
 * Use mkdir to create a directory.
 */
static int
fb_xfs2_mkdir(char *path, int perm)
{
	// TODO
	int err = 0;
	if (path == NULL) {
		XFS2_ERROR("fb_xfs2_mkdir, invalid argument");
	}
	XFS2_DEBUG("fb_xfs2_mkdir, path: %s, perm: %d", path, perm);
	err = xfs2_mkdir_all(xfs2, path);
	if (err == -1) {
		filebench_log(LOG_ERROR, "fb_xfs2_mkdir error, err: %d, path: %s", errno, path);
	}
	return err;
}

/*
 * Use rmdir to delete a directory. Returns what rmdir() returns.
 */
static int
fb_xfs2_rmdir(char *path)
{
	int err = 0;
	if (path == NULL) {
		XFS2_ERROR("fb_xfs2_rmdir, invalid argument");
	}
	XFS2_DEBUG("fb_xfs2_rmdir, path: %s", path);
	err = xfs2_remove(xfs2, path);
	if (err == -1) {
		filebench_log(LOG_ERROR,"fb_xfs2_mkdir error, %d, path: %s", err, path);
	}
	return err;
}

/*
 * does a recursive rm to remove an entire directory tree (i.e. a fileset).
 * Supplied with the path to the root of the tree.
 */
static void
fb_xfs2_recur_rm(char *path)
{
	int err = 0;
	if (path == NULL) {
		XFS2_ERROR("fb_xfs2_recur_rm, invalid argument");
	}
	XFS2_DEBUG("fb_xfs2_rmdir, path: %s", path);
	err = xfs2_remove_all(xfs2, path);
	if (err == -1) {
		filebench_log(LOG_ERROR,"fb_xfs2_mkdir error, %d, path: %s", err, path);
	}
}

/*
 * Does a posix opendir(), Returns a directory handle on success,
 * NULL on failure.
 */
static fb_dir_t *
fb_xfs2_opendir(char *path)
{
	fb_dir_t *dir = NULL;
	int d = 0;
	if (path == NULL) {
		XFS2_ERROR("fb_xfs2_opendir, invalid argument");
	}
	XFS2_DEBUG("fb_xfs2_opendir, path: %s", path);
	d = xfs2_opendir(xfs2, path);
	if (d == -1) {
		XFS2_ERROR("fb_xfs2_opendir,err: %d, path: %s", errno, path);
	}
	dir = (fb_dir_t*)malloc(sizeof(fb_dir_t));
	if (dir != NULL) {
		memset(dir, 0, sizeof(fb_dir_t));
		dir->dir_handler = d;;
		dir->offset = 0;
	}
	return dir;
}

/*
 * Does a readdir() call. Returns a pointer to a table of directory
 * information on success, NULL on failure.
 */
static struct dirent *
fb_xfs2_readdir(fb_dir_t *dirp)
{
	struct xfs2_stat stat[16];
	struct dirent *dirs = NULL;
	off_t offset = dirp->offset;
	int n = xfs2_readdir_plus(dirp->dir_handler, stat, 16, &offset);
	if (n == -1) {
		XFS2_ERROR("fb_xfs2_readdir, err: %d", errno);
	}
	if (n >= 0) {
		// TODO :fullfill struct dirents
		dirs = (struct dirent *)malloc(sizeof(struct dirent) * n);
		if (dirs == NULL) {
			XFS2_ERROR("fb_xfs2_readdir, out of memory");
		}
		memset(dirs, 0, sizeof(struct dirent) * n);
		for (int i = 0; i < n; i++) {
			dirs[i].d_ino = stat[i].st_ino;
			memcpy(dirs[i].d_name, stat[i].name, strlen(stat[i].name) + 1);
			dirs[i].d_type = stat[i].st_type;
		}
		dirp->offset += n;
	}
	return dirs;
}

/*
 * Does a closedir() call.
 */
static int
fb_xfs2_closedir(fb_dir_t *dirp)
{
	int ret = 0;
	if (dirp == NULL) {
		XFS2_ERROR("fb_xfs2_closedir, invalid argument");
	}
	XFS2_DEBUG("fb_xfs2_closedir, dir: %ld", dirp->dir_handler);
	ret = xfs2_closedir(dirp->dir_handler);
	if (ret == -1) {
		XFS2_ERROR("fb_xfs2_closedir, err: %d", errno);
	}
	free(dirp);
	dirp = NULL;
	return ret;
}

/*
 * Does an fstat of a file.
 */
static int
fb_xfs2_fstat(fb_fdesc_t *fd, struct stat64 *statbufp)
{
	return (fstat64(fd->fd_num, statbufp));
}

/*
 * Does a stat of a file.
 */
static int
fb_xfs2_stat(char *path, struct stat64 *statbufp)
{
	return (stat64(path, statbufp));
}

/*
 * Do a pwrite64 to a file.
 */
static int
fb_xfs2_pwrite(fb_fdesc_t *fd, caddr_t iobuf, fbint_t iosize, off64_t offset)
{
	XFS2_DEBUG("fb_xfs2_pwrite");
	return	xfs2_pwritefile(xfs2, (char *)iobuf, (size_t)iosize, offset);
}

/*
 * Do a write to a file.
 */
static int
fb_xfs2_write(fb_fdesc_t *fd, caddr_t iobuf, fbint_t iosize)
{
	XFS2_DEBUG("fb_xfs2_write: fd: %d, iosize: %d", fd->fd_num, iosize);
	return xfs2_writefile(xfs2, (char *)iobuf, (size_t)iosize);
}

/*
 * Does a truncate operation and returns the result
 */
static int
fb_xfs2_truncate(fb_fdesc_t *fd, off64_t fse_size)
{
#ifdef HAVE_FTRUNCATE64
	return (ftruncate64(fd->fd_num, fse_size));
#else
	filebench_log(LOG_ERROR, "Converting off64_t to off_t in ftruncate,"
			" might be a possible problem");
	return (ftruncate(fd->fd_num, (off_t)fse_size));
#endif
}

/*
 * Does a link operation and returns the result
 */
static int
fb_xfs2_link(const char *existing, const char *new)
{
	return (link(existing, new));
}

/*
 * Does a symlink operation and returns the result
 */
static int
fb_xfs2_symlink(const char *existing, const char *new)
{
	return (symlink(existing, new));
}

/*
 * Does an access() check on a file.
 */
static int
fb_xfs2_access(const char *path, int amode)
{
	return (access(path, amode));
}
