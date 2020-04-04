package memory

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMemStorage(t *testing.T) {
	s := NewMemStorage()
	err := s.AppendLog("{}")
	assert.NoError(t, err)
	looked, err := s.Lookup()
	assert.NoError(t, err)
	assert.Contains(t, looked, "{}")
}
