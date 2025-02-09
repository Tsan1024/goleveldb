package leveldb

import (
	"testing"

	"github.com/Tsan1024/goleveldb/leveldb/testutil"
)

func TestLevelDB(t *testing.T) {
	testutil.RunSuite(t, "LevelDB Suite")
}
