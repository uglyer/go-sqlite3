// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package sqlite3

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
)

type testVFS struct {
	t *testing.T
}

func (vfs *testVFS) Open(name string, flags int) (interface{}, error) {
	return &testVFile{vfs.t}, nil
}

type testVFile struct {
	t *testing.T
}

func TestVFSRegister(t *testing.T) {
	t.Helper()
	tempFilename := TempFilename(t)
	defer os.Remove(tempFilename)

	name := fmt.Sprintf("test_%s", t.Name())
	err := VFSRegister(name, &testVFS{t})
	if err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?vfs=%s", tempFilename, name))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name TEXT);")
	if err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare("INSERT INTO t(id, name) values(?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for i := 0; i < 10; i++ {
		_, err = stmt.Exec(i, fmt.Sprintf("test_%d", i))
		if err != nil {
			t.Fatal(err)
		}
	}
	tx.Commit()

	rows, err := db.Query("SELECT id, name FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	i := 0
	for rows.Next() {
		var (
			id   int
			name string
		)
		err = rows.Scan(&id, &name)
		if err != nil {
			t.Fatal(err)
		}
		if id != i {
			t.Errorf("id = %d, want = %d", id, i)
		}
		if name != fmt.Sprintf("test_%d", i) {
			t.Errorf("name = %s, want = test_%d", name, i)
		}
		i++
	}
}
