package sqlite3

import (
	"testing"
)

func TestSqlite3Randomness(t *testing.T) {
	count := 8
	offset := 4
	buffer := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	Sqlite3Randomness(8, buffer, 4)
	for i := 0; i < len(buffer); i++ {
		if i < (count+offset) && i >= offset {
			if buffer[i] == 0 {
				t.Fatalf("i:(%d)期望值不为0,目标值:%d", i, buffer[i])
			}
		} else if buffer[i] != 0 {
			t.Fatalf("i:(%d)期望值为0,目标值:%d", i, buffer[i])
		}
	}
}
