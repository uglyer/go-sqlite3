package sqlite3

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSqlite3vfs(t *testing.T) {

	vfs := newTempVFS()

	vfsName := "tmpfs"
	err := RegisterVFS(vfsName, vfs)
	if err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite3", fmt.Sprintf("foo.db?vfs=%s", vfsName))
	if err != nil {
		t.Fatal(err)
	}
	//_, err = db.Exec("PRAGMA locking_mode=EXCLUSIVE;")
	//if err != nil {
	//	t.Fatalf("set locking_mode = EXCLUSIVE error:%v", err)
	//}
	_, err = db.Exec("PRAGMA synchronous = ON")
	if err != nil {
		t.Fatalf("set synchronous = OFF error:%v", err)
	}
	_, err = db.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		t.Fatalf("set journal_mode = WAL error:%v", err)
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS foo (
id text NOT NULL PRIMARY KEY,
title text
)`)
	if err != nil {
		t.Fatal(err)
	}

	rows := []FooRow{
		{
			ID:    "415",
			Title: "romantic-swell",
		},
		{
			ID:    "610",
			Title: "ironically-gnarl",
		},
		{
			ID:    "768",
			Title: "biophysicist-straddled",
		},
	}
	cacheEmptyRowByte, err := ioutil.ReadAll(vfs.walFile.f)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		_, err = db.Exec(`INSERT INTO foo (id, title) values (?, ?)`, row.ID, row.Title)
		if err != nil {
			t.Fatal(err)
		}
	}

	rowIter, err := db.Query(`SELECT id, title from foo order by id`)
	if err != nil {
		t.Fatal(err)
	}

	var gotRows []FooRow

	for rowIter.Next() {
		var row FooRow
		err = rowIter.Scan(&row.ID, &row.Title)
		if err != nil {
			t.Fatal(err)
		}
		gotRows = append(gotRows, row)
	}
	err = rowIter.Close()
	if err != nil {
		t.Fatal(err)
	}

	if !cmp.Equal(rows, gotRows) {
		t.Fatal(cmp.Diff(rows, gotRows))
	}
	var row [1]int
	if err := db.QueryRow(`SELECT count(*) from foo`).Scan(&row[0]); err != nil {
		t.Fatal(err)
	} else if row[0] != len(rows) {
		t.Fatalf("count result error:%v", row[0])
	}
	// 恢复缓存状态
	err = ioutil.WriteFile(vfs.walFile.f.Name(), cacheEmptyRowByte, 0600)
	if err != nil {
		t.Fatal(err)
	}
	vfs.walFile.invalidateWalIndexHeader()
	if err := db.QueryRow(`SELECT count(*) from foo`).Scan(&row[0]); err != nil {
		t.Fatal(err)
	} else if row[0] != 0 {
		t.Fatalf("count result error:%v", row[0])
	}

	// 重新插入记录
	for _, row := range rows {
		_, err = db.Exec(`INSERT INTO foo (id, title) values (?, ?)`, row.ID, row.Title)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

	// reopen db
	db, err = sql.Open("sqlite3", fmt.Sprintf("foo.db?vfs=%s", vfsName))
	if err != nil {
		t.Fatal(err)
	}

	rowIter, err = db.Query(`SELECT id, title from foo order by id`)
	if err != nil {
		t.Fatal(err)
	}

	gotRows = gotRows[:0]

	for rowIter.Next() {
		var row FooRow
		err = rowIter.Scan(&row.ID, &row.Title)
		if err != nil {
			t.Fatal(err)
		}
		gotRows = append(gotRows, row)
	}
	err = rowIter.Close()
	if err != nil {
		t.Fatal(err)
	}

	if !cmp.Equal(rows, gotRows) {
		t.Fatal(cmp.Diff(rows, gotRows))
	}

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}

type FooRow struct {
	ID    string
	Title string
}
