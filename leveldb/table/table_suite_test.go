package table

import (
	"testing"

	"github.com/Tsan1024/goleveldb/leveldb/testutil"
)

func TestTable(t *testing.T) {
	testutil.RunSuite(t, "Table Suite")
}
