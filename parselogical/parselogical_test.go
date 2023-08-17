package parselogical

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTruncateNoFlags(t *testing.T) {
	pr := NewParseResult("table public.customers: TRUNCATE: (no-flags)")
	assert.NoError(t, pr.Parse())

	assert.Equal(t, "public.customers", pr.Relation)
	assert.Equal(t, "TRUNCATE", pr.Operation)
}

func TestTruncateWithFlags(t *testing.T) {
	pr := NewParseResult("table public.customers: TRUNCATE: restart_seqs")
	assert.NoError(t, pr.Parse())

	assert.Equal(t, "public.customers", pr.Relation)
	assert.Equal(t, "TRUNCATE", pr.Operation)
}

func TestTruncateCascade(t *testing.T) {
	pr := NewParseResult("table public.customers, public.orders: TRUNCATE: cascade")
	assert.NoError(t, pr.Parse())

	assert.Equal(t, "public.customers, public.orders", pr.Relation)
	assert.Equal(t, "TRUNCATE", pr.Operation)
}
