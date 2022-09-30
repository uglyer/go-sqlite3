#include "sqlite3vfs.h"
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <stdbool.h>
#include <string.h>

#ifdef SQLITE3VFS_LOADABLE_EXT
SQLITE_EXTENSION_INIT1
#endif

/* Create a new vfs object. */
static struct vfs *vfsCreate(void)
{
	struct vfs *v;

	v = sqlite3_malloc(sizeof *v);
	if (v == NULL) {
		return NULL;
	}

	v->databases = NULL;
	v->n_databases = 0;

	return v;
}

/* Release all resources used by a shared memory mapping. */
static void vfsShmClose(struct vfsShm *s)
{
	void *region;
	unsigned i;

	assert(s != NULL);

	/* Free all regions. */
	for (i = 0; i < s->n_regions; i++) {
		region = *(s->regions + i);
		assert(region != NULL);
		sqlite3_free(region);
	}

	/* Free the shared memory region array. */
	if (s->regions != NULL) {
		sqlite3_free(s->regions);
	}
}

/* Release all memory used by a database object. */
static void vfsDatabaseClose(struct vfsDatabase *d)
{
	unsigned i;
	for (i = 0; i < d->n_pages; i++) {
		sqlite3_free(d->pages[i]);
	}
	if (d->pages != NULL) {
		sqlite3_free(d->pages);
	}
	vfsShmClose(&d->shm);
//	vfsWalClose(&d->wal);
}

/* Destroy the content of a database object. */
static void vfsDatabaseDestroy(struct vfsDatabase *d)
{
	assert(d != NULL);

	sqlite3_free(d->name);

	vfsDatabaseClose(d);
	sqlite3_free(d);
}

/* Release the memory used internally by the VFS object.
 *
 * All file content will be de-allocated, so dangling open FDs against
 * those files will be broken.
 */
static void vfsDestroy(struct vfs *r)
{
	unsigned i;

	assert(r != NULL);

	for (i = 0; i < r->n_databases; i++) {
		struct vfsDatabase *database = r->databases[i];
		vfsDatabaseDestroy(database);
	}

	if (r->databases != NULL) {
		sqlite3_free(r->databases);
	}
}

static bool vfsFilenameEndsWith(const char *filename, const char *suffix)
{
	size_t n_filename = strlen(filename);
	size_t n_suffix = strlen(suffix);
	if (n_suffix > n_filename) {
		return false;
	}
	return strncmp(filename + n_filename - n_suffix, suffix, n_suffix) == 0;
}

/* Find the database object associated with the given filename. */
static struct vfsDatabase *vfsDatabaseLookup(struct vfs *v,
					     const char *filename)
{
	size_t n = strlen(filename);
	unsigned i;

	assert(v != NULL);
	assert(filename != NULL);

	if (vfsFilenameEndsWith(filename, "-wal")) {
		n -= strlen("-wal");
	}
	if (vfsFilenameEndsWith(filename, "-journal")) {
		n -= strlen("-journal");
	}

	for (i = 0; i < v->n_databases; i++) {
		struct vfsDatabase *database = v->databases[i];
		if (strncmp(database->name, filename, n) == 0) {
			// Found matching file.
			return database;
		}
	}

	return NULL;
}

static int vfsDeleteDatabase(struct vfs *r, const char *name)
{
	unsigned i;

	for (i = 0; i < r->n_databases; i++) {
		struct vfsDatabase *database = r->databases[i];
		unsigned j;

		if (strcmp(database->name, name) != 0) {
			continue;
		}

		/* Free all memory allocated for this file. */
		vfsDatabaseDestroy(database);

		/* Shift all other contents objects. */
		for (j = i + 1; j < r->n_databases; j++) {
			r->databases[j - 1] = r->databases[j];
		}
		r->n_databases--;

		return SQLITE_OK;
	}

//	r->error = ENOENT;
	return SQLITE_IOERR_DELETE_NOENT;
}

/* Initialize the shared memory mapping of a database file. */
static void vfsShmInit(struct vfsShm *s)
{
	int i;

	s->regions = NULL;
	s->n_regions = 0;
	s->refcount = 0;

	for (i = 0; i < SQLITE_SHM_NLOCK; i++) {
		s->shared[i] = 0;
		s->exclusive[i] = 0;
	}
}

/* Revert the shared mamory to its initial state. */
static void vfsShmReset(struct vfsShm *s)
{
	vfsShmClose(s);
	vfsShmInit(s);
}

/* Initialize a new WAL object. */
//static void vfsWalInit(struct vfsWal *w)
//{
//	memset(w->hdr, 0, VFS__WAL_HEADER_SIZE);
//	w->frames = NULL;
//	w->n_frames = 0;
//	w->tx = NULL;
//	w->n_tx = 0;
//}

/* Initialize a new database object. */
static void vfsDatabaseInit(struct vfsDatabase *d)
{
	d->pages = NULL;
	d->n_pages = 0;
	vfsShmInit(&d->shm);
//	vfsWalInit(&d->wal);
}

extern int goVFSOpen(sqlite3_vfs* vfs, const char * name, sqlite3_file* file, int flags, int *outFlags);
extern int goVFSDelete(sqlite3_vfs*, const char *zName, int syncDir);
extern int goVFSAccess(sqlite3_vfs*, const char *zName, int flags, int* pResOut);
extern int goVFSFullPathname(sqlite3_vfs*, const char *zName, int nOut, char* zOut);
extern int goVFSRandomness(sqlite3_vfs*, int nByte, char *zOut);
extern int goVFSSleep(sqlite3_vfs*, int microseconds);
extern int goVFSCurrentTimeInt64(sqlite3_vfs*, sqlite3_int64* piNow);

extern int goVFSClose(sqlite3_file* file);
extern int goVFSRead(sqlite3_file* file, void* buf, int iAmt, sqlite3_int64 iOfst);
extern int goVFSWrite(sqlite3_file* file ,const void* buf, int iAmt, sqlite3_int64 iOfst);
extern int goVFSTruncate(sqlite3_file* file, sqlite3_int64 size);
extern int goVFSSync(sqlite3_file* file, int flags);
extern int goVFSFileSize(sqlite3_file* file, sqlite3_int64 *pSize);
extern int goVFSLock(sqlite3_file* file, int eLock);
extern int goVFSUnlock(sqlite3_file*, int eLock);
extern int goVFSCheckReservedLock(sqlite3_file* file, int *pResOut);
extern int goVFSSectorSize(sqlite3_file* file);
extern int goVFSDeviceCharacteristics(sqlite3_file* file);


int s3vfsNew(char* name, int maxPathName) {
  sqlite3_vfs* vfs;
  sqlite3_vfs* delegate;
  vfs = calloc(1, sizeof(sqlite3_vfs));
  if (vfs == NULL) {
    return SQLITE_ERROR;
  }

  delegate = sqlite3_vfs_find(0);

  vfs->pAppData = vfsCreate();

  vfs->iVersion = 2;
  vfs->szOsFile = sizeof(s3vfsFile);
  vfs->mxPathname = maxPathName;
  vfs->zName = name;
  vfs->xOpen = s3vfsOpen;
  vfs->xDelete = s3vfsDelete;
  vfs->xAccess = s3vfsAccess;
  vfs->xFullPathname = s3vfsFullPathname;
  vfs->xDlOpen = delegate->xDlOpen;
  vfs->xDlError = delegate->xDlError;
  vfs->xDlSym = delegate->xDlSym;
  vfs->xDlClose = delegate->xDlClose;
  vfs->xRandomness = s3vfsRandomness;
  vfs->xSleep = s3vfsSleep;
  vfs->xCurrentTime = s3vfsCurrentTime;
  vfs->xGetLastError = delegate->xGetLastError;
  vfs->xCurrentTimeInt64 = s3vfsCurrentTimeInt64;

  return sqlite3_vfs_register(vfs, 0);
}


/* Create a database object and add it to the databases array. */
static struct vfsDatabase *vfsCreateDatabase(struct vfs *v, const char *name)
{
	unsigned n = v->n_databases + 1;
	struct vfsDatabase **databases;
	struct vfsDatabase *d;

	assert(name != NULL);

	/* Create a new entry. */
	databases = sqlite3_realloc64(v->databases, sizeof *databases * n);
	if (databases == NULL) {
		goto oom;
	}
	v->databases = databases;

	d = sqlite3_malloc(sizeof *d);
	if (d == NULL) {
		goto oom;
	}

	d->name = sqlite3_malloc64(strlen(name) + 1);
	if (d->name == NULL) {
		goto oom_after_database_malloc;
	}
	strcpy(d->name, name);

	vfsDatabaseInit(d);

	v->databases[n - 1] = d;
	v->n_databases = n;

	return d;

oom_after_database_malloc:
	sqlite3_free(d);
oom:
	return NULL;
}

int s3vfsOpen(sqlite3_vfs* vfs, const char * name, sqlite3_file* file, int flags, int *outFlags) {
  int ret = goVFSOpen(vfs, name, file, flags, outFlags);
  /* Search if the database object exists already. */

	int rc;
	struct vfs *v;
    enum vfsFileType type;
    struct vfsDatabase *database;
    bool exists;
	int create = flags & SQLITE_OPEN_CREATE;
    v = (struct vfs *)(vfs->pAppData);
    database = vfsDatabaseLookup(v, name);
    exists = database != NULL;
    if (flags & SQLITE_OPEN_MAIN_DB) {
        type = VFS__DATABASE;
    } else if (flags & SQLITE_OPEN_MAIN_JOURNAL) {
        type = VFS__JOURNAL;
    } else if (flags & SQLITE_OPEN_WAL) {
        type = VFS__WAL;
    } else {
//        v->error = ENOENT;
        return SQLITE_CANTOPEN;
    }
/* todo If file exists, and the exclusive flag is on, return an error. */
	if (!exists) {
        /* When opening a WAL or journal file we expect the main
         * database file to have already been created. */
        if (type == VFS__WAL || type == VFS__JOURNAL) {
//            v->error = ENOENT;
			rc = SQLITE_CANTOPEN;
            goto err;
        }

        assert(type == VFS__DATABASE);

        /* Check the create flag. */
        if (!create) {
//            v->error = ENOENT;
			rc = SQLITE_CANTOPEN;
            goto err;
        }

        database = vfsCreateDatabase(v, name);
        if (database == NULL) {
//            v->error = ENOMEM;
			rc = SQLITE_CANTOPEN;
            goto err;
        }
    }

    file->pMethods = &s3vfs_io_methods;
	struct s3vfsFile *f;
    f = (struct s3vfsFile *)file;
    f->vfs = v;
    f->type = type;
    f->database = database;
  return ret;
err:
	assert(rc != SQLITE_OK);
	return rc;
}

int s3vfsDelete(sqlite3_vfs* vfs, const char *zName, int syncDir) {
  int ret = goVFSDelete(vfs, zName, syncDir);
  return ret;
}

int s3vfsAccess(sqlite3_vfs* vfs, const char *zName, int flags, int* pResOut) {
  return goVFSAccess(vfs, zName, flags, pResOut);
}

int s3vfsFullPathname(sqlite3_vfs* vfs, const char *zName, int nOut, char* zOut) {
  return goVFSFullPathname(vfs, zName, nOut, zOut);
}

int s3vfsRandomness(sqlite3_vfs* vfs, int nByte, char *zOut) {
  return goVFSRandomness(vfs, nByte, zOut);
}

int s3vfsSleep(sqlite3_vfs* vfs, int microseconds) {
  return goVFSSleep(vfs, microseconds);
}

int s3vfsCurrentTime(sqlite3_vfs* vfs, double* prNow) {
  sqlite3_int64 i = 0;
  int rc;
  rc = s3vfsCurrentTimeInt64(0, &i);
  *prNow = i/86400000.0;
  return rc;
}

int s3vfsCurrentTimeInt64(sqlite3_vfs* vfs, sqlite3_int64* piNow) {
  return goVFSCurrentTimeInt64(vfs, piNow);
}

int s3vfsClose(sqlite3_file* file) {
  return goVFSClose(file);
}

int s3vfsRead(sqlite3_file* file, void* zBuf, int iAmt, sqlite3_int64 iOfst) {
  return goVFSRead(file, zBuf, iAmt, iOfst);
}

int s3vfsWrite(sqlite3_file* file, const void* zBuf, int iAmt, sqlite3_int64 iOfst) {
  return goVFSWrite(file, zBuf, iAmt, iOfst);
}

int s3vfsTruncate(sqlite3_file* file, sqlite3_int64 size) {
  return goVFSTruncate(file, size);
}

int s3vfsSync(sqlite3_file* file, int flags) {
  return goVFSSync(file, flags);
}

int s3vfsFileSize(sqlite3_file* file, sqlite3_int64 *pSize) {
  return goVFSFileSize(file, pSize);
}

int s3vfsLock(sqlite3_file* file, int eLock) {
  return goVFSLock(file, eLock);
}

int s3vfsUnlock(sqlite3_file* file , int eLock) {
  return goVFSUnlock(file, eLock);
}

int s3vfsCheckReservedLock(sqlite3_file* file, int *pResOut) {
  return goVFSCheckReservedLock(file, pResOut);
}

int s3vfsSectorSize(sqlite3_file* file) {
  return goVFSSectorSize(file);

}

int s3vfsDeviceCharacteristics(sqlite3_file* file) {
  return goVFSDeviceCharacteristics(file);
}

int s3vfsFileControl(sqlite3_file *pFile, int op, void *pArg){
  return SQLITE_NOTFOUND;
}

/**(from dqlite)**/
int xVfsShmMap(struct vfsShm *s,
		     unsigned region_index,
		     unsigned region_size,
		     bool extend,
		     void volatile **out)
{
//    printf("xVfsShmMap\n");
	void *region;
	int rv;
//    printf("xVfsShmMap #0:%s\n",s != NULL ? " s->regions != NULL ": "s->regions is NULL");
//    printf("xVfsShmMap #1:%s\n",s->regions != NULL ? " s->regions != NULL ": "s->regions is NULL");
//    s->regions != NULL;
//    printf("xVfsShmMap #2:%d\n",region_index);
//    region_index < s->n_regions;
//    printf("xVfsShmMap #4\n");

	if (s->regions != NULL && region_index < s->n_regions) {
    printf("xVfsShmMap s->regions != NULL && region_index < s->n_regions\n");
		/* The region was already allocated. */
		region = s->regions[region_index];
		assert(region != NULL);
	} else {
//    printf("xVfsShmMap s->regions != NULL && region_index < s->n_regions else\n");
		if (extend) {
//    printf("xVfsShmMap extend\n");
			void **regions;

			/* We should grow the map one region at a time. */
			assert(region_size == VFS__WAL_INDEX_REGION_SIZE);
			assert(region_index == s->n_regions);
			region = sqlite3_malloc64(region_size);
			if (region == NULL) {
				rv = SQLITE_NOMEM;
				goto err;
			}

			memset(region, 0, region_size);

			regions = sqlite3_realloc64(
			    s->regions,
			    sizeof *s->regions * (s->n_regions + 1));

			if (regions == NULL) {
				rv = SQLITE_NOMEM;
				goto err_after_region_malloc;
			}

			s->regions = regions;
			s->regions[region_index] = region;
			s->n_regions++;

		} else {
//    printf("xVfsShmMap no extend\n");
			/* The region was not allocated and we don't have to
			 * extend the map. */
			region = NULL;
		}
	}

	*out = region;

	if (region_index == 0 && region != NULL) {
		s->refcount++;
	}
//    printf("SQLITE_OK\n");
	return SQLITE_OK;

err_after_region_malloc:
//    printf("xVfsShmMap err_after_region_malloc\n");
	sqlite3_free(region);
err:
//    printf("xVfsShmMap err\n");
	assert(rv != SQLITE_OK);
	*out = NULL;
	return rv;
}

/* Simulate shared memory by allocating on the C heap.(from dqlite) */
int xVfsFileShmMap(sqlite3_file *file, /* Handle open on database file */
			 int region_index,   /* Region to retrieve */
			 int region_size,    /* Size of regions */
			 int extend, /* True to extend file if necessary */
			 void volatile **out /* OUT: Mapped memory */
)
{
//    printf("xVfsFileShmMap\n");
	struct s3vfsFile *f = (struct s3vfsFile *)file;

	assert(f->type == VFS__DATABASE);

	return xVfsShmMap(&f->database->shm, (unsigned)region_index,
			 (unsigned)region_size, extend != 0, out);
}

static int vfsShmLock(struct vfsShm *s, int ofst, int n, int flags)
{
	int i;

	if (flags & SQLITE_SHM_EXCLUSIVE) {
		/* No shared or exclusive lock must be held in the region. */
		for (i = ofst; i < ofst + n; i++) {
			if (s->shared[i] > 0 || s->exclusive[i] > 0) {
//			        tracef("EXCLUSIVE lock contention ofst:%d n:%d exclusive[%d]=%d shared[%d]=%d",
//			                ofst, n, i, s->exclusive[i], i, s->shared[i]);
				return SQLITE_BUSY;
			}
		}

		for (i = ofst; i < ofst + n; i++) {
			assert(s->exclusive[i] == 0);
			s->exclusive[i] = 1;
		}
	} else {
		/* No exclusive lock must be held in the region. */
		for (i = ofst; i < ofst + n; i++) {
			if (s->exclusive[i] > 0) {
//			        tracef("SHARED lock contention ofst:%d n:%d exclusive[%d]=%d shared[%d]=%d",
//			                ofst, n, i, s->exclusive[i], i, s->shared[i]);
				return SQLITE_BUSY;
			}
		}

		for (i = ofst; i < ofst + n; i++) {
			s->shared[i]++;
		}
	}

	return SQLITE_OK;
}

static int vfsShmUnlock(struct vfsShm *s, int ofst, int n, int flags)
{
	unsigned *these_locks;
	unsigned *other_locks;
	int i;

	if (flags & SQLITE_SHM_SHARED) {
		these_locks = s->shared;
		other_locks = s->exclusive;
	} else {
		these_locks = s->exclusive;
		other_locks = s->shared;
	}

	for (i = ofst; i < ofst + n; i++) {
		/* Coherence check that no lock of the other type is held in this
		 * region. */
		assert(other_locks[i] == 0);

		/* Only decrease the lock count if it's positive. In other words
		 * releasing a never acquired lock is legal and idemponent. */
		if (these_locks[i] > 0) {
			these_locks[i]--;
		}
	}

	return SQLITE_OK;
}

static int vfsFileShmLock(sqlite3_file *file, int ofst, int n, int flags)
{
//  printf("vfsFileShmLock flag:%d\n",flags);
	struct s3vfsFile *f;
	struct vfsShm *shm;
//	struct vfsWal *wal;
	int rv;

	assert(file != NULL);
	assert(ofst >= 0);
	assert(n >= 0);

	/* Legal values for the offset and the range */
	assert(ofst >= 0 && ofst + n <= SQLITE_SHM_NLOCK);
	assert(n >= 1);
	assert(n == 1 || (flags & SQLITE_SHM_EXCLUSIVE) != 0);

	/* Legal values for the flags.
	 *
	 * See https://sqlite.org/c3ref/c_shm_exclusive.html. */
	assert(flags == (SQLITE_SHM_LOCK | SQLITE_SHM_SHARED) ||
	       flags == (SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE) ||
	       flags == (SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED) ||
	       flags == (SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE));

	/* This is a no-op since shared-memory locking is relevant only for
	 * inter-process concurrency. See also the unix-excl branch from
	 * upstream (git commit cda6b3249167a54a0cf892f949d52760ee557129). */

	f = (struct s3vfsFile *)file;

	assert(f->type == VFS__DATABASE);
	assert(f->database != NULL);

	shm = &f->database->shm;
	if (flags & SQLITE_SHM_UNLOCK) {
		rv = vfsShmUnlock(shm, ofst, n, flags);
	} else {
		rv = vfsShmLock(shm, ofst, n, flags);
	}
//  printf("vfsFileShmLock:%d\n",rv);
//	wal = &f->database->wal;
//	if (rv == SQLITE_OK && ofst == VFS__WAL_WRITE_LOCK) {
//		assert(n == 1);
//		/* When acquiring the write lock, make sure there's no
//		 * transaction that hasn't been rolled back or polled. */
//		if (flags == (SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)) {
//			assert(wal->n_tx == 0);
//		}
//		/* When releasing the write lock, if we find a pending
//		 * uncommitted transaction then a rollback must have occurred.
//		 * In that case we delete the pending transaction. */
//		if (flags == (SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE)) {
//			vfsWalRollbackIfUncommitted(wal);
//		}
//	}

	return rv;
}


static void vfsFileShmBarrier(sqlite3_file *file)
{
	(void)file;
	/* This is a no-op since we expect SQLite to be compiled with mutex
	 * support (i.e. SQLITE_MUTEX_OMIT or SQLITE_MUTEX_NOOP are *not*
	 * defined, see sqliteInt.h). */
}

static void vfsShmUnmap(struct vfsShm *s)
{
	s->refcount--;
	if (s->refcount == 0) {
		vfsShmReset(s);
	}
}

static int vfsFileShmUnmap(sqlite3_file *file, int delete_flag)
{
	struct s3vfsFile *f = (struct s3vfsFile *)file;
	(void)delete_flag;
	vfsShmUnmap(&f->database->shm);
	return SQLITE_OK;
}

const sqlite3_io_methods s3vfs_io_methods = {
  2,                               /* iVersion */
  s3vfsClose,                      /* xClose */
  s3vfsRead,                       /* xRead */
  s3vfsWrite,                      /* xWrite */
  s3vfsTruncate,                   /* xTruncate */
  s3vfsSync,                       /* xSync */
  s3vfsFileSize,                   /* xFileSize */
  s3vfsLock,                       /* xLock */
  s3vfsUnlock,                     /* xUnlock */
  s3vfsCheckReservedLock,          /* xCheckReservedLock */
  s3vfsFileControl,                /* xFileControl */
  s3vfsSectorSize,                 /* xSectorSize */
  s3vfsDeviceCharacteristics,      /* xDeviceCharacteristics */

  xVfsFileShmMap,                      /* xShmMap */
  vfsFileShmLock,                     /* xShmLock */
  vfsFileShmBarrier,                  /* xShmBarrier */
  vfsFileShmUnmap,                    /* xShmUnmap */
  0,                       /* xFetch */
  0                      /* xUnfetch */
};
