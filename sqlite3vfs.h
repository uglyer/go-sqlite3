#ifndef SQLITE3VFS_H
#define SQLITE3VFS_H

#ifdef SQLITE3VFS_LOADABLE_EXT
#include "sqlite3ext.h"
#else
#include "sqlite3-binding.h"
#endif


/* Maximum pathname length supported by this VFS. */
#define VFS__MAX_PATHNAME 512

/* WAL magic value. Either this value, or the same value with the least
 * significant bit also set (FORMAT__WAL_MAGIC | 0x00000001) is stored in 32-bit
 * big-endian format in the first 4 bytes of a WAL file.
 *
 * If the LSB is set, then the checksums for each frame within the WAL file are
 * calculated by treating all data as an array of 32-bit big-endian
 * words. Otherwise, they are calculated by interpreting all data as 32-bit
 * little-endian words. */
#define VFS__WAL_MAGIC 0x377f0682

/* WAL format version (same for WAL index). */
#define VFS__WAL_VERSION 3007000

/* Index of the write lock in the WAL-index header locks area. */
#define VFS__WAL_WRITE_LOCK 0

/* Write ahead log header size. */
#define VFS__WAL_HEADER_SIZE 32

/* Write ahead log frame header size. */
#define VFS__FRAME_HEADER_SIZE 24

/* Size of the first part of the WAL index header. */
#define VFS__WAL_INDEX_HEADER_SIZE 48

/* Size of a single memory-mapped WAL index region. */
#define VFS__WAL_INDEX_REGION_SIZE 32768

enum vfsFileType {
	VFS__DATABASE, /* Main database file */
	VFS__JOURNAL,  /* Default SQLite journal file */
	VFS__WAL       /* Write-Ahead Log */
};

/* Hold content for a shared memory mapping. */
struct vfsShm
{
	void **regions;     /* Pointers to shared memory regions. */
	unsigned n_regions; /* Number of shared memory regions. */
	unsigned refcount;  /* Number of outstanding mappings. */
	unsigned shared[SQLITE_SHM_NLOCK];    /* Count of shared locks */
	unsigned exclusive[SQLITE_SHM_NLOCK]; /* Count of exclusive locks */
};

/* Hold the content of a single WAL frame. */
//struct vfsFrame
//{
//	uint8_t header[VFS__FRAME_HEADER_SIZE];
//	uint8_t *page; /* Content of the page. */
//};
//
///* WAL-specific content.
// * Watch out when changing the members of this struct, see
// * comment in `formatWalChecksumBytes`. */
//struct vfsWal
//{
//	uint8_t hdr[VFS__WAL_HEADER_SIZE]; /* Header. */
//	struct vfsFrame **frames;          /* All frames committed. */
//	unsigned n_frames;                 /* Number of committed frames. */
//	struct vfsFrame **tx;              /* Frames added by a transaction. */
//	unsigned n_tx;                     /* Number of added frames. */
//};

/* Database-specific content */
struct vfsDatabase
{
	char *name;        /* Database name. */
	void **pages;      /* All database. */
	unsigned n_pages;  /* Number of pages. */
	struct vfsShm shm; /* Shared memory. */
//	struct vfsWal wal; /* Associated WAL. */
};

/* Custom dqlite VFS. Contains pointers to all databases that were created. */
struct vfs
{
	struct vfsDatabase **databases; /* Database objects */
	unsigned n_databases;           /* Number of databases */
	int error;                      /* Last error occurred. */
};

typedef struct s3vfsFile {
  sqlite3_file base; /* IO methods */
  sqlite3_uint64 id; /* Go object id  */
  struct vfs *vfs;
  enum vfsFileType type;        /* Associated file (main db or WAL). */
  struct vfsDatabase *database; /* Underlying database content. */
} s3vfsFile;

int s3vfsNew(char* name, int maxPathName);

int s3vfsClose(sqlite3_file*);
int s3vfsRead(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);
int s3vfsWrite(sqlite3_file*,const void*,int iAmt, sqlite3_int64 iOfst);
int s3vfsTruncate(sqlite3_file*, sqlite3_int64 size);
int s3vfsSync(sqlite3_file*, int flags);
int s3vfsFileSize(sqlite3_file*, sqlite3_int64 *pSize);
int s3vfsLock(sqlite3_file*, int);
int s3vfsUnlock(sqlite3_file*, int);
int s3vfsCheckReservedLock(sqlite3_file*, int *pResOut);
int s3vfsFileControl(sqlite3_file*, int op, void *pArg);
int s3vfsSectorSize(sqlite3_file*);
int s3vfsDeviceCharacteristics(sqlite3_file*);
int s3vfsShmMap(sqlite3_file*, int iPg, int pgsz, int, void volatile**);
int s3vfsShmLock(sqlite3_file*, int offset, int n, int flags);
void s3vfsShmBarrier(sqlite3_file*);
int s3vfsShmUnmap(sqlite3_file*, int deleteFlag);
int s3vfsFetch(sqlite3_file*, sqlite3_int64 iOfst, int iAmt, void **pp);
int s3vfsUnfetch(sqlite3_file*, sqlite3_int64 iOfst, void *p);


int s3vfsOpen(sqlite3_vfs*, const char *, sqlite3_file*, int , int *);
int s3vfsDelete(sqlite3_vfs*, const char *, int);
int s3vfsAccess(sqlite3_vfs*, const char *, int, int *);
int s3vfsFullPathname(sqlite3_vfs*, const char *zName, int, char *zOut);
void *s3vfsDlOpen(sqlite3_vfs*, const char *zFilename);
void s3vfsDlError(sqlite3_vfs*, int nByte, char *zErrMsg);
void (*s3vfsDlSym(sqlite3_vfs *pVfs, void *p, const char*zSym))(void);
void s3vfsDlClose(sqlite3_vfs*, void*);
int s3vfsRandomness(sqlite3_vfs*, int nByte, char *zOut);
int s3vfsSleep(sqlite3_vfs*, int microseconds);
int s3vfsCurrentTime(sqlite3_vfs*, double*);
int s3vfsGetLastError(sqlite3_vfs*, int, char *);
int s3vfsCurrentTimeInt64(sqlite3_vfs*, sqlite3_int64*);

const extern sqlite3_io_methods s3vfs_io_methods;

#endif /* SQLITE3_VFS */
