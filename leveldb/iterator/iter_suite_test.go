package iterator_test

import (
	"testing"

	"github.com/Tsan1024/goleveldb/leveldb/testutil"
)

func TestIterator(t *testing.T) {
	testutil.RunSuite(t, "Iterator Suite")
}
