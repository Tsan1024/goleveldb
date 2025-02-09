package memdb

import (
	"testing"

	"github.com/Tsan1024/goleveldb/leveldb/testutil"
)

func TestMemDB(t *testing.T) {
	testutil.RunSuite(t, "MemDB Suite")
}
