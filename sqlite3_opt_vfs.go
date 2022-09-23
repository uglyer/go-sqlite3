// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
// https://github.com/k2wanko/go-sqlite3-gcs-vfs

package sqlite3

/*
#ifndef USE_LIBSQLITE3
#include <sqlite3-binding.h>
#else
#include <sqlite3.h>
#endif
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

typedef struct GoFile GoFile;
struct GoFile {
  sqlite3_file base;
};

typedef uintptr_t (*vfs_xCloseFunc) (void *file);
static uintptr_t call_vfs_xCloseFunc(void *file, vfs_xCloseFunc f) { return f(file); }
uintptr_t goVFSClose(void *file);

static int cVFSClose(sqlite3_file *pFile)
{
	return (int)(goVFSClose(pFile));
}

typedef uintptr_t (*vfs_xReadFunc) (void *file, void *zBuf, int iAmt, sqlite_int64 iOfst);
static uintptr_t call_vfs_xReadFunc(void *file, vfs_xReadFunc f, void *zBuf, int iAmt, sqlite_int64 iOfst) { return f(file, zBuf, iAmt, iOfst); }
uintptr_t goVFSRead(void *file, void *zBuf, int iAmt, sqlite_int64 iOfst);

static int cVFSRead(
  sqlite3_file *pFile,
  void *zBuf,
  int iAmt,
  sqlite_int64 iOfst
){
	return (int)(goVFSRead(pFile, zBuf, iAmt, iOfst));
}

typedef uintptr_t (*vfs_xWriteFunc) (void *file, void *zBuf, int iAmt, sqlite_int64 iOfst);
static uintptr_t call_vfs_xWriteFunc(void *file, vfs_xWriteFunc f, void *zBuf, int iAmt, sqlite_int64 iOfst) { return f(file, zBuf, iAmt, iOfst); }
uintptr_t goVFSWrite(void *file, void *zBuf, int iAmt, sqlite_int64 iOfst);

static int cVFSWrite(
  sqlite3_file *pFile,
  const void *zBuf,
  int iAmt,
  sqlite_int64 iOfst
){
	return (int)(goVFSWrite(pFile, (void*)zBuf, iAmt, iOfst));
}

typedef uintptr_t (*vfs_xSyncFunc) (void *file, int flags);
static uintptr_t call_vfs_xSyncFunc(void *file, vfs_xSyncFunc f, int flags) { return f(file, flags); }
uintptr_t goVFSSync(void *file, int flags);

static int cVFSSync(sqlite3_file *pFile, int flags)
{
	return (int)(goVFSSync(pFile, flags));
}

typedef uintptr_t (*vfs_xFileSizeFunc) (void *file, sqlite_int64 *pSize);
static uintptr_t call_vfs_xFileSizeFunc(void *file, vfs_xFileSizeFunc f, sqlite_int64 *pSize) { return f(file, pSize); }
uintptr_t goVFSFileSize(void *file, sqlite_int64 *pSize);

static int cVFSFileSize(sqlite3_file *pFile, sqlite_int64 *pSize)
{
	return (int)(goVFSFileSize(pFile, pSize));
}

typedef uintptr_t (*vfs_xLockFunc) (void *file, int eLock);
static uintptr_t call_vfs_xLockFunc(void *file, vfs_xLockFunc f, int eLock) { return f(file, eLock); }
uintptr_t goVFSLock(void *file, int eLock);

static int cVFSLock(sqlite3_file *pFile, int eLock){
  return (int)(goVFSLock(pFile, eLock));
}

typedef uintptr_t (*vfs_xUnlockFunc) (void *file, int eLock);
static uintptr_t call_vfs_xUnlockFunc(void *file, vfs_xUnlockFunc f, int eLock) { return f(file, eLock); }
uintptr_t goVFSUnlock(void *file, int eLock);

static int cVFSUnlock(sqlite3_file *pFile, int eLock){
  return (int)(goVFSUnlock(pFile, eLock));
}

typedef uintptr_t (*vfs_xCheckReservedLockFunc) (void *file, int *pResOut);
static uintptr_t call_vfs_xCheckReservedLockFunc(void *file, vfs_xCheckReservedLockFunc f, int *pResOut) { return f(file, pResOut); }
uintptr_t goVFSCheckReservedLock(void *file, int *pResOut);

static int cVFSCheckReservedLock(sqlite3_file *pFile, int *pResOut) {
  return (int)(goVFSCheckReservedLock(pFile, pResOut));
}

typedef uintptr_t (*vfs_xFileControlFunc) (void *file, int op, void *pArg);
static uintptr_t call_vfs_xFileControlFunc(void *file, vfs_xFileControlFunc f, int op, void *pArg) { return f(file, op, pArg); }
uintptr_t goVFSFileControl(void *file, int op, void *pArg);

static int cVFSFileControl(sqlite3_file *pFile, int op, void *pArg) {
  return (int)(goVFSFileControl(pFile, op, pArg));
}

typedef uintptr_t (*vfs_xSectorSizeFunc) (void *file);
static uintptr_t call_vfs_xSectorSizeFunc(void *file, vfs_xSectorSizeFunc f) { return f(file); }
uintptr_t goVFSSectorSize(void *file);

static int cVFSSectorSize(sqlite3_file *pFile){
  return (int)(goVFSSectorSize(pFile));
}

typedef uintptr_t (*vfs_xDeviceCharacteristicsFunc) (void *file);
static uintptr_t call_vfs_xDeviceCharacteristicsFunc(void *file, vfs_xDeviceCharacteristicsFunc f) { return f(file); }
uintptr_t goVFSDeviceCharacteristics(void *file);

static int cVFSDeviceCharacteristics(sqlite3_file *pFile){
  return (int)(goVFSDeviceCharacteristics(pFile));
}

typedef uintptr_t (*vfs_xOpenFunc) (void *vfs, char *zName, sqlite3_file *pFile, int flags, int *pOutFlags);
static uintptr_t call_vfs_xOpenFunc(void *vfs, vfs_xOpenFunc f, char *zName, sqlite3_file *pFile, int flags, int *pOutFlags) { return f(vfs, zName, pFile, flags, pOutFlags); }
uintptr_t goVFSOpen(void *vfs, char *zName, sqlite3_file *pFile, int flags, int *pOutFlags);

static int
cVFSOpen(
	sqlite3_vfs *pVfs,
	const char *zName,
	sqlite3_file *pFile,
	int flags,
	int *pOutFlags
)
{
	int rc = goVFSOpen(pVfs, (char*)zName, pFile, flags, pOutFlags);
	if (rc != SQLITE_OK) {
		return rc;
	}

	sqlite3_io_methods *goio = (sqlite3_io_methods *)sqlite3_malloc(sizeof(sqlite3_io_methods));
	memset(goio, 0, sizeof(sqlite3_io_methods));

	goio->iVersion = 3;
	goio->xClose = cVFSClose;
	goio->xRead = cVFSRead;
	goio->xWrite = cVFSWrite;
	goio->xSync = cVFSSync;
	goio->xFileSize = cVFSFileSize;
	goio->xLock = cVFSLock;
	goio->xUnlock = cVFSUnlock;
	goio->xCheckReservedLock = cVFSCheckReservedLock;
	goio->xFileControl = cVFSFileControl;
	goio->xSectorSize = cVFSSectorSize;
	goio->xDeviceCharacteristics = cVFSDeviceCharacteristics;

	GoFile *p = (GoFile*)pFile;
	p->base.pMethods = goio;

	return rc;
}

typedef uintptr_t (*vfs_xDeleteFunc) (void *vfs, char *zPath, int dirSync);
static uintptr_t call_vfs_xDeleteFunc(void *vfs, vfs_xDeleteFunc f, char *zPath, int dirSync) { return f(vfs, zPath, dirSync); }
uintptr_t goVFSDelete(void *vfs, char *zPath, int dirSync);

static int
cVFSDelete(
	sqlite3_vfs *pVfs,
	const char *zPath,
	int dirSync
)
{
	return (int)(goVFSDelete(pVfs, (char*)zPath, dirSync));
}

typedef uintptr_t (*vfs_xAccessFunc) (void *vfs, char *zPath, int flags, int *pResOut);
static uintptr_t call_vfs_xAccessFunc(void *vfs, vfs_xAccessFunc f, char *zPath, int flags, int *pResOut) { return f(vfs, zPath, flags, pResOut); }
uintptr_t goVFSAccess(void *vfs, char *zPath, int flags, int *pResOut);

static int cVFSAccess(
  sqlite3_vfs *pVfs,
  const char *zPath,
  int flags,
  int *pResOut
){
	return goVFSAccess(pVfs, (char*)zPath, flags, pResOut);
}

typedef uintptr_t (*vfs_xRandomnessFunc) (void *vfs, int nBytes, char *zByte);
static uintptr_t call_vfs_xRandomnessFunc(void *vfs, vfs_xRandomnessFunc f, int nBytes, char *zByte) { return f(vfs, nBytes, zByte); }
uintptr_t goVFSRandomness(void *vfs, int nBytes, char *zByte);

static int cVFSRandomness(sqlite3_vfs *pVfs, int nByte, char *zByte)
{
  return goVFSRandomness(pVfs, nByte, zByte);
}

typedef uintptr_t (*vfs_xFullPathnameFunc) (void *vfs, char *zPath, int nOut, char *zOut);
static uintptr_t call_vfs_xFullPathnameFunc(void *vfs, vfs_xFullPathnameFunc f, char *zPath, int nOut, char *zOut) { return f(vfs, zPath, nOut, zOut); }
uintptr_t goVFSFullPathname(void *vfs, char *zPath, int nOut, char *zOut);

static int cVFSFullPathname(
  sqlite3_vfs *pVfs,
  const char *zPath,
  int nPathOut,
  char *zPathOut
){
	return goVFSFullPathname(pVfs, (char*)zPath, nPathOut, zPathOut);
}

typedef uintptr_t (*vfs_xGetLastErrorFunc) (void *vfs, int nByte, char *zErrMsg);
static uintptr_t call_vfs_xGetLastErrorFunc(void *vfs, vfs_xGetLastErrorFunc f, int nByte, char *zErrMsg) { return f(vfs, nByte, zErrMsg); }
uintptr_t goVFSGetLastError(void *vfs, int nByte, char *zErrMsg);

static int cVFSGetLastError(
  sqlite3_vfs *pVfs,
  int nByte,
  char *zErrMsg
){
	return goVFSGetLastError(pVfs, nByte, zErrMsg);
}

typedef uintptr_t (*vfs_xSleepFunc) (void *vfs, int nMicro);
static uintptr_t call_vfs_xSleepFunc(void *vfs, vfs_xSleepFunc f, int nMicro) { return f(vfs, nMicro); }
uintptr_t goVFSSleep(void *vfs, int nMicro);

static int cVFSSleep(
  sqlite3_vfs *pVfs,
  int nMicro
){
	return goVFSSleep(pVfs, nMicro);
}

typedef uintptr_t (*vfs_xCurrentTimeFunc) (void *vfs, double *pTime);
static uintptr_t call_vfs_xCurrentTimeFunc(void *vfs, vfs_xCurrentTimeFunc f, double *pTime) { return f(vfs, pTime); }
uintptr_t goVFSCurrentTime(void *vfs, double *pTime);

static int cVFSCurrentTime(
  sqlite3_vfs *pVfs,
  double *pTime
){
	return goVFSCurrentTime(pVfs, pTime);
}

typedef void *(*vfs_xDlOpenFunc) (void *vfs, char *zPath);
static void *call_vfs_xDlOpenFunc(void *vfs, vfs_xDlOpenFunc f, char *zPath) { return f(vfs, zPath); }
void *goVFSDlOpen(void *vfs, char *zPath);

static void *cVFSDlOpen(
  sqlite3_vfs *pVfs,
  const char *zPath
){
	return goVFSDlOpen(pVfs, (char*)zPath);
}

typedef void (*vfs_xDlErrorFunc) (void *vfs, int nByte, char *zErrMsg);
static void call_vfs_xDlErrorFunc(void *vfs, vfs_xDlErrorFunc f, int nByte, char *zErrMsg) { f(vfs, nByte, zErrMsg); return; }
void goVFSDlError(void *vfs, int nByte, char *zErrMsg);

static void cVFSDlError(
  sqlite3_vfs *pVfs,
  int nByte,
  char *zErrMsg
){
	goVFSDlError(pVfs, nByte, zErrMsg);
  return;
}

typedef void *(*vfs_xDlSymFunc) (void *vfs, void *pH, char *z);
static void *call_vfs_xDlSymFunc(void *vfs, vfs_xDlSymFunc f, void *pH, char *z) { return f(vfs, pH, z); }
void* goVFSDlSym(void *vfs, void *pH, char *z);

static void (*cVFSDlSym(
  sqlite3_vfs *pVfs,
  void *pH,
  const char *z
))(void){
	return goVFSDlSym(pVfs, pH, (char*)z);
}

typedef void (*vfs_xDlCloseFunc) (void *vfs, void *pHandle);
static void call_vfs_xDlCloseFunc(void *vfs, vfs_xDlCloseFunc f, void *pHandle) { f(vfs, pHandle); return; }
void goVFSDlClose(void *vfs, void *pHandle);

static void cVFSDlClose(
  sqlite3_vfs *pVfs,
  void *pHandle
){
	goVFSDlClose(pVfs, pHandle);
  return;
}

static sqlite3_vfs
*_sqlite3_vfs(
	char *name,
	int maxPathname
)
{
	sqlite3_vfs *vfs = (sqlite3_vfs *)sqlite3_malloc(sizeof(sqlite3_vfs));
	memset(vfs, 0, sizeof(sqlite3_vfs));
	vfs->iVersion = 3;
	vfs->szOsFile = sizeof(GoFile);
	vfs->mxPathname = maxPathname;
	vfs->zName = name;

	vfs->xFullPathname = cVFSFullPathname;
	vfs->xOpen = cVFSOpen;
	vfs->xDelete = cVFSDelete;
	vfs->xAccess = cVFSAccess;
	vfs->xRandomness = cVFSRandomness;
  vfs->xGetLastError = cVFSGetLastError;
  vfs->xSleep = cVFSSleep;
  vfs->xCurrentTime = cVFSCurrentTime;
  vfs->xDlOpen = cVFSDlOpen;
  vfs->xDlError = cVFSDlError;
  vfs->xDlSym = cVFSDlSym;
  vfs->xDlClose = cVFSDlClose;

	return vfs;
}
*/
import "C"
import (
	"io"
	"os"
	"reflect"
	"runtime"
	"sync"
	"unsafe"
)

const VFSDefaultMaxPathnameSize = 4096

type sqliteVFS struct {
	vfs  VFS
	pvfs unsafe.Pointer
	base *C.sqlite3_vfs
}

type sqliteVFSFile struct {
	vf    interface{}
	pf    unsafe.Pointer
	vfs   *sqliteVFS
	cf    *C.sqlite3_file
	base  *C.sqlite3_file
	pBase unsafe.Pointer
}

type VFSFullPathnamer interface {
	FullPathname(path string) (fullpath string, err error)
}

type VFS interface {
	Open(name string, flags int) (interface{}, error)
}

type VFSDeleter interface {
	Delete(path string, dirSync int) error
}

type VFSAccesser interface {
	// https://sqlite.org/c3ref/c_access_exists.html
	Access(path string, flags int) (int, error)
}

type VFSRandomnesser interface {
	Randomness(size int) ([]byte, error)
}

type VFileDeviceCharacteristicser interface {
	// DeviceCharacteristics returns I/O capabilities.
	//
	// <https://sqlite.org/c3ref/c_iocap_atomic.html>
	DeviceCharacteristics() int
}

type VFileSectorSizer interface {
	// SectorSize returns the native underlying sector size.
	SectorSize() int
}

type VFileFileControler interface {
	FileControl(op int) bool
}

type VFileFileSizer interface {
	FileSize() (int64, error)
}

type VFileSyncer interface {
	// https://sqlite.org/c3ref/c_sync_dataonly.html
	Sync(flags int) error
}

// https://sqlite.org/c3ref/c_lock_exclusive.html
type VFileLocker interface {
	Lock(lock int) error
	Unlock(lock int) error
	CheckReservedLock() (int, error)
}

var (
	vfssLock sync.Mutex
	vfss     = make(map[unsafe.Pointer]*sqliteVFS)

	vfilesLock sync.Mutex
	vfiles     = make(map[unsafe.Pointer]*sqliteVFSFile)
)

//export goVFSFullPathname
func goVFSFullPathname(vfs unsafe.Pointer, zPath *C.char, nOut C.int, zOut *C.char) C.uintptr_t {
	return lookupVFS(vfs).fullPathname(zPath, nOut, zOut)
}

func (vfs *sqliteVFS) fullPathname(zPath *C.char, nOut C.int, zOut *C.char) C.uintptr_t {
	v, ok := vfs.vfs.(VFSFullPathnamer)
	if !ok {
		f := C.vfs_xFullPathnameFunc(vfs.base.xFullPathname)
		return C.call_vfs_xFullPathnameFunc(unsafe.Pointer(vfs.base), f, zPath, nOut, zOut)
	}
	path := C.GoString(zPath)
	fp, err := v.FullPathname(path)
	if err != nil {
		return C.SQLITE_IOERR
	}

	cp := C.CString(fp)
	defer C.free(unsafe.Pointer(cp))

	C.strncpy(zOut, cp, C.ulonglong(nOut))

	return C.SQLITE_OK
}

//export goVFSOpen
func goVFSOpen(vfs unsafe.Pointer, zName *C.char, pFile *C.sqlite3_file, flags C.int, pOutFlags *C.int) C.uintptr_t {
	return lookupVFS(vfs).open(zName, pFile, flags, pOutFlags)
}

func (vfs *sqliteVFS) open(zName *C.char, pFile *C.sqlite3_file, flags C.int, pOutFlags *C.int) (rc C.uintptr_t) {
	name := C.GoString(zName)
	goFlags := int(flags)
	oflags := 0
	if goFlags&int(C.SQLITE_OPEN_EXCLUSIVE) != 0 {
		oflags |= os.O_EXCL
	}
	if goFlags&int(C.SQLITE_OPEN_CREATE) != 0 {
		oflags |= os.O_CREATE
	}
	if goFlags&int(C.SQLITE_OPEN_READONLY) != 0 {
		oflags |= os.O_RDONLY
	}
	if goFlags&int(C.SQLITE_OPEN_READWRITE) != 0 {
		oflags |= os.O_RDWR
	}
	vf, err := vfs.vfs.Open(name, oflags)
	rc = pickErrCode(err)

	if rc != C.SQLITE_OK {
		return
	}

	pBase := C.sqlite3_malloc(vfs.base.szOsFile)
	base := (*C.sqlite3_file)(pBase)

	res := C.call_vfs_xOpenFunc(unsafe.Pointer(vfs.base), vfs.base.xOpen, zName, base, flags, pOutFlags)
	if res != C.SQLITE_OK {
		C.sqlite3_free(unsafe.Pointer(&pBase))
		return
	}

	file := unsafe.Pointer(pFile)
	f := &sqliteVFSFile{
		vf:    vf,
		pf:    file,
		vfs:   vfs,
		cf:    pFile,
		base:  base,
		pBase: pBase,
	}
	runtime.SetFinalizer(f, (*sqliteVFSFile).close)
	vfilesLock.Lock()
	vfiles[file] = f
	vfilesLock.Unlock()

	if pOutFlags != nil {
		*pOutFlags = flags
	}

	return
}

//export goVFSDelete
func goVFSDelete(vfs unsafe.Pointer, zPath *C.char, dirSync C.int) C.uintptr_t {
	return lookupVFS(vfs).delete(zPath, dirSync)
}

func (vfs *sqliteVFS) delete(zPath *C.char, dirSync C.int) C.uintptr_t {
	if v, ok := vfs.vfs.(VFSDeleter); ok {
		path := C.GoString(zPath)
		err := v.Delete(path, int(dirSync))
		return pickErrCode(err)
	}
	f := C.vfs_xDeleteFunc(vfs.base.xDelete)
	return C.call_vfs_xDeleteFunc(unsafe.Pointer(vfs.base), f, zPath, dirSync)
}

//export goVFSAccess
func goVFSAccess(vfs unsafe.Pointer, zPath *C.char, flags C.int, pResOut *C.int) C.uintptr_t {
	return lookupVFS(vfs).access(zPath, flags, pResOut)
}

func (vfs *sqliteVFS) access(zPath *C.char, flags C.int, pResOut *C.int) C.uintptr_t {
	if v, ok := vfs.vfs.(VFSAccesser); ok {
		path := C.GoString(zPath)
		outFlags, err := v.Access(path, int(flags))
		rc := pickErrCode(err)
		if rc == C.SQLITE_OK {
			*pResOut = C.int(outFlags)
		}
		return rc
	}
	f := C.vfs_xAccessFunc(vfs.base.xAccess)
	return C.call_vfs_xAccessFunc(unsafe.Pointer(vfs.base), f, zPath, flags, pResOut)
}

//export goVFSRandomness
func goVFSRandomness(vfs unsafe.Pointer, nBytes C.int, zByte *C.char) C.uintptr_t {
	return lookupVFS(vfs).randomness(nBytes, zByte)
}

func (vfs *sqliteVFS) randomness(nBytes C.int, zByte *C.char) C.uintptr_t {
	if v, ok := vfs.vfs.(VFSRandomnesser); ok {
		res, err := v.Randomness(int(nBytes))
		rc := pickErrCode(err)

		if rc == C.SQLITE_OK {
			resc := C.CString(*(*string)(unsafe.Pointer(&res)))
			defer C.free(unsafe.Pointer(resc))

			C.strncpy(zByte, resc, C.ulonglong(nBytes))
		}

		return rc
	}

	f := C.vfs_xRandomnessFunc(vfs.base.xRandomness)
	return C.call_vfs_xRandomnessFunc(unsafe.Pointer(vfs.base), f, nBytes, zByte)
}

//export goVFSGetLastError
func goVFSGetLastError(vfs unsafe.Pointer, nByte C.int, zErrMsg *C.char) C.uintptr_t {
	return lookupVFS(vfs).getLastError(nByte, zErrMsg)
}

func (vfs *sqliteVFS) getLastError(nByte C.int, zErrMsg *C.char) C.uintptr_t {
	f := C.vfs_xGetLastErrorFunc(vfs.base.xGetLastError)
	return C.call_vfs_xGetLastErrorFunc(unsafe.Pointer(vfs.base), f, nByte, zErrMsg)
}

//export goVFSSleep
func goVFSSleep(vfs unsafe.Pointer, nMicro C.int) C.uintptr_t {
	return lookupVFS(vfs).sleep(nMicro)
}

func (vfs *sqliteVFS) sleep(nMicro C.int) C.uintptr_t {
	f := C.vfs_xSleepFunc(vfs.base.xSleep)
	return C.call_vfs_xSleepFunc(unsafe.Pointer(vfs.base), f, nMicro)
}

//export goVFSCurrentTime
func goVFSCurrentTime(vfs unsafe.Pointer, pTime *C.double) C.uintptr_t {
	return lookupVFS(vfs).currentTime(pTime)
}

func (vfs *sqliteVFS) currentTime(pTime *C.double) C.uintptr_t {
	f := C.vfs_xCurrentTimeFunc(vfs.base.xCurrentTime)
	return C.call_vfs_xCurrentTimeFunc(unsafe.Pointer(vfs.base), f, pTime)
}

//export goVFSDlOpen
func goVFSDlOpen(vfs unsafe.Pointer, zPath *C.char) unsafe.Pointer {
	return lookupVFS(vfs).dlOpen(zPath)
}

func (vfs *sqliteVFS) dlOpen(zPath *C.char) unsafe.Pointer {
	f := C.vfs_xDlOpenFunc(vfs.base.xDlOpen)
	return C.call_vfs_xDlOpenFunc(unsafe.Pointer(vfs.base), f, zPath)
}

//export goVFSDlError
func goVFSDlError(vfs unsafe.Pointer, nByte C.int, zErrMsg *C.char) {
	lookupVFS(vfs).dlError(nByte, zErrMsg)
	return
}

func (vfs *sqliteVFS) dlError(nByte C.int, zErrMsg *C.char) {
	f := C.vfs_xDlErrorFunc(vfs.base.xDlError)
	C.call_vfs_xDlErrorFunc(unsafe.Pointer(vfs.base), f, nByte, zErrMsg)
	return
}

//export goVFSDlSym
func goVFSDlSym(vfs unsafe.Pointer, ph unsafe.Pointer, z *C.char) unsafe.Pointer {
	return lookupVFS(vfs).dlSym(ph, z)
}

func (vfs *sqliteVFS) dlSym(ph unsafe.Pointer, z *C.char) unsafe.Pointer {
	f := C.vfs_xDlSymFunc(vfs.base.xDlSym)
	return C.call_vfs_xDlSymFunc(unsafe.Pointer(vfs.base), f, ph, z)
}

//export goVFSDlClose
func goVFSDlClose(vfs unsafe.Pointer, pHandle unsafe.Pointer) {
	lookupVFS(vfs).dlClose(pHandle)
	return
}

func (vfs *sqliteVFS) dlClose(pHandle unsafe.Pointer) {
	f := C.vfs_xDlCloseFunc(vfs.base.xDlClose)
	C.call_vfs_xDlCloseFunc(unsafe.Pointer(vfs.base), f, pHandle)
}

//export goVFSClose
func goVFSClose(file unsafe.Pointer) C.uintptr_t {
	return lookupVFSFile(file).close()
}

func (file *sqliteVFSFile) close() (rc C.uintptr_t) {
	defer deleteVFSFile(file.pf)
	if v, ok := file.vf.(io.Closer); ok {
		rc = pickErrCode(v.Close())
	}

	if file.base != nil {
		rc = C.call_vfs_xCloseFunc(unsafe.Pointer(file.base), file.base.pMethods.xClose)
		C.sqlite3_free(file.pBase)
		file.pBase = nil
		file.base = nil
	}

	return
}

//export goVFSDeviceCharacteristics
func goVFSDeviceCharacteristics(file unsafe.Pointer) C.uintptr_t {
	return lookupVFSFile(file).deviceCharacteristics()
}

func (file *sqliteVFSFile) deviceCharacteristics() C.uintptr_t {
	if v, ok := file.vf.(VFileDeviceCharacteristicser); ok {
		return C.uintptr_t(v.DeviceCharacteristics())
	}

	f := C.vfs_xDeviceCharacteristicsFunc(file.base.pMethods.xDeviceCharacteristics)
	return C.call_vfs_xDeviceCharacteristicsFunc(unsafe.Pointer(file.base), f)
}

//export goVFSSectorSize
func goVFSSectorSize(file unsafe.Pointer) C.uintptr_t {
	return lookupVFSFile(file).sectorSize()
}

func (file *sqliteVFSFile) sectorSize() C.uintptr_t {
	if v, ok := file.vf.(VFileSectorSizer); ok {
		return C.uintptr_t(v.SectorSize())
	}

	f := C.vfs_xSectorSizeFunc(file.base.pMethods.xSectorSize)
	return C.call_vfs_xSectorSizeFunc(unsafe.Pointer(file.base), f)
}

//export goVFSLock
func goVFSLock(file unsafe.Pointer, eLock C.int) C.uintptr_t {
	return lookupVFSFile(file).lock(eLock)
}

func (file *sqliteVFSFile) lock(eLock C.int) C.uintptr_t {
	if v, ok := file.vf.(VFileLocker); ok {
		return pickErrCode(v.Lock(int(eLock)))
	}

	f := C.vfs_xLockFunc(file.base.pMethods.xLock)
	return C.call_vfs_xLockFunc(unsafe.Pointer(file.base), f, eLock)
}

//export goVFSUnlock
func goVFSUnlock(file unsafe.Pointer, eLock C.int) C.uintptr_t {
	return lookupVFSFile(file).unlock(eLock)
}

func (file *sqliteVFSFile) unlock(eLock C.int) C.uintptr_t {
	if v, ok := file.vf.(VFileLocker); ok {
		return pickErrCode(v.Unlock(int(eLock)))
	}

	f := C.vfs_xUnlockFunc(file.base.pMethods.xUnlock)
	return C.call_vfs_xUnlockFunc(unsafe.Pointer(file.base), f, eLock)
}

//export goVFSCheckReservedLock
func goVFSCheckReservedLock(file unsafe.Pointer, pResOut *C.int) C.uintptr_t {
	return lookupVFSFile(file).checkReservedLock(pResOut)
}

func (file *sqliteVFSFile) checkReservedLock(pResOut *C.int) C.uintptr_t {
	if v, ok := file.vf.(VFileLocker); ok {
		lock, err := v.CheckReservedLock()
		rc := pickErrCode(err)

		if rc == C.SQLITE_OK {
			*pResOut = C.int(lock)
		}
		return rc
	}

	f := C.vfs_xCheckReservedLockFunc(file.base.pMethods.xCheckReservedLock)
	return C.call_vfs_xCheckReservedLockFunc(unsafe.Pointer(file.base), f, pResOut)
}

//export goVFSFileControl
func goVFSFileControl(file unsafe.Pointer, op C.int, pArg unsafe.Pointer) C.uintptr_t {
	return lookupVFSFile(file).fileControl(op, pArg)
}

func (file *sqliteVFSFile) fileControl(op C.int, pArg unsafe.Pointer) C.uintptr_t {
	if v, ok := file.vf.(VFileFileControler); ok {
		res := v.FileControl(int(op))
		if res {
			return C.SQLITE_OK
		} else {
			return C.SQLITE_NOTFOUND
		}
	}

	f := C.vfs_xFileControlFunc(file.base.pMethods.xFileControl)
	return C.call_vfs_xFileControlFunc(unsafe.Pointer(file.base), f, op, pArg)
}

//export goVFSFileSize
func goVFSFileSize(file unsafe.Pointer, pSize *C.sqlite_int64) C.uintptr_t {
	return lookupVFSFile(file).fileSize(pSize)
}

func (file *sqliteVFSFile) fileSize(pSize *C.sqlite_int64) C.uintptr_t {
	if v, ok := file.vf.(VFileFileSizer); ok {
		size, err := v.FileSize()
		rc := pickErrCode(err)
		if rc == C.SQLITE_OK {
			*pSize = C.sqlite_int64(size)
		}

		return rc
	}

	f := C.vfs_xFileSizeFunc(file.base.pMethods.xFileSize)
	return C.call_vfs_xFileSizeFunc(unsafe.Pointer(file.base), f, pSize)
}

//export goVFSSync
func goVFSSync(file unsafe.Pointer, flags C.int) C.uintptr_t {
	return lookupVFSFile(file).sync(flags)
}

func (file *sqliteVFSFile) sync(flags C.int) C.uintptr_t {
	if v, ok := file.vf.(VFileSyncer); ok {
		return pickErrCode(v.Sync(int(flags)))
	}

	f := C.vfs_xSyncFunc(file.base.pMethods.xSync)
	return C.call_vfs_xSyncFunc(unsafe.Pointer(file.base), f, flags)
}

//export goVFSRead
func goVFSRead(file, zBuf unsafe.Pointer, iAmt C.int, iOfst C.sqlite_int64) C.uintptr_t {
	return lookupVFSFile(file).read(zBuf, iAmt, iOfst)
}

func (file *sqliteVFSFile) read(zBuf unsafe.Pointer, iAmt C.int, iOfst C.sqlite_int64) C.uintptr_t {
	v, ok := file.vf.(io.ReaderAt)
	if !ok {
		f := C.vfs_xReadFunc(file.base.pMethods.xRead)
		return C.call_vfs_xReadFunc(unsafe.Pointer(file.base), f, zBuf, iAmt, iOfst)
	}
	b := C.GoBytes(zBuf, iAmt)
	n, err := v.ReadAt(b, int64(iOfst))

	if serr, ok := err.(Error); ok {
		return C.uintptr_t(serr.Code)
	}

	if err != io.EOF && err != nil {
		return C.SQLITE_IOERR_READ
	}

	if int(iAmt) == n {
		copyBuf(zBuf, b)
		return C.SQLITE_OK
	} else if int(iAmt) > n {
		for i := range b[n : int(iAmt)-n] {
			b[i] = 0
		}
		copyBuf(zBuf, b)
		return C.SQLITE_IOERR_SHORT_READ
	}

	return C.SQLITE_IOERR_READ
}

//export goVFSWrite
func goVFSWrite(file, zBuf unsafe.Pointer, iAmt C.int, iOfst C.sqlite_int64) C.uintptr_t {
	return lookupVFSFile(file).write(zBuf, iAmt, iOfst)
}

func (file *sqliteVFSFile) write(zBuf unsafe.Pointer, iAmt C.int, iOfst C.sqlite_int64) C.uintptr_t {
	v, ok := file.vf.(io.WriterAt)
	if !ok {
		f := C.vfs_xWriteFunc(file.base.pMethods.xWrite)
		return C.call_vfs_xWriteFunc(unsafe.Pointer(file.base), f, zBuf, iAmt, iOfst)
	}
	b := C.GoBytes(zBuf, iAmt)
	n, err := v.WriteAt(b, int64(iOfst))

	if serr, ok := err.(Error); ok {
		return C.uintptr_t(serr.Code)
	}
	if err != nil {
		return C.SQLITE_IOERR_WRITE
	}

	if int(iAmt) != n {
		return C.SQLITE_IOERR_WRITE
	}

	return C.SQLITE_OK
}

type vfsRegisterOptions struct {
	makeDflt        int
	maxPathnameSize int
}

type vfsRegisterOption func(*vfsRegisterOptions)

func MakeDefault(f int) vfsRegisterOption {
	return func(o *vfsRegisterOptions) {
		o.makeDflt = f
	}
}

func MaxFilePathSize(size int) vfsRegisterOption {
	return func(o *vfsRegisterOptions) {
		o.maxPathnameSize = size
	}
}

func (vfs *sqliteVFS) Close() error {
	if vfs.pvfs != nil {
		C.sqlite3_free(vfs.pvfs)
		vfs.pvfs = nil
	}
	return nil
}

func VFSRegister(name string, vfs VFS, options ...vfsRegisterOption) error {
	o := &vfsRegisterOptions{
		maxPathnameSize: VFSDefaultMaxPathnameSize,
	}
	for _, f := range options {
		f(o)
	}

	vfssLock.Lock()
	defer vfssLock.Unlock()
	cname := C.CString(name)
	// 不能销毁, 销毁后 c vfsList zName 会变为 null, 导致 sqlite3_vfs_find 无法查找到正确的 vfs
	// defer C.free(unsafe.Pointer(cname))

	if o.maxPathnameSize <= 0 {
		o.maxPathnameSize = VFSDefaultMaxPathnameSize
	}
	cmxps := C.int(o.maxPathnameSize)

	cvfs := C._sqlite3_vfs(cname, cmxps)
	pvfs := unsafe.Pointer(cvfs)
	base := C.sqlite3_vfs_find(nil)
	_vfs := &sqliteVFS{vfs, pvfs, base}
	vfss[pvfs] = _vfs
	runtime.SetFinalizer(_vfs, (*sqliteVFS).Close)

	rc := int(C.sqlite3_vfs_register(cvfs, C.int(o.makeDflt)))
	if rc != C.SQLITE_OK {
		return Error{Code: ErrNo(rc)}
	}

	return nil
}

func copyBuf(b unsafe.Pointer, s []byte) int {
	var sh reflect.SliceHeader
	sh.Data = uintptr(b)
	sh.Len = len(s)
	sh.Cap = cap(s)
	rb := *(*[]byte)(unsafe.Pointer(&sh))
	return copy(rb, s)
}

func lookupVFS(vfs unsafe.Pointer) *sqliteVFS {
	vfssLock.Lock()
	defer vfssLock.Unlock()
	return vfss[vfs]
}

func deleteVFS(vfs unsafe.Pointer) {
	vfssLock.Lock()
	defer vfssLock.Unlock()
	delete(vfss, vfs)
}

func lookupVFSFile(file unsafe.Pointer) *sqliteVFSFile {
	vfilesLock.Lock()
	defer vfilesLock.Unlock()
	return vfiles[file]
}

func deleteVFSFile(file unsafe.Pointer) {
	vfilesLock.Lock()
	defer vfilesLock.Unlock()
	delete(vfiles, file)
}

func pickErrCode(err error) C.uintptr_t {
	if serr, ok := err.(Error); ok {
		return C.uintptr_t(serr.Code)
	}
	if err != nil {
		return C.SQLITE_ERROR
	}
	return C.SQLITE_OK
}
