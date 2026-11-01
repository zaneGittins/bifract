package auth

import (
	"os"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func TestMain(m *testing.M) {
	// Test users are hashed at MinCost; the dummy matches them, as it matches
	// real hashes in production.
	dummyHashCost = bcrypt.MinCost
	os.Exit(m.Run())
}
