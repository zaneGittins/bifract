package chat

import (
	"context"
	"errors"
	"testing"

	"bifract/internal/fakedb"
	"bifract/pkg/instructions"
	"bifract/pkg/storage/storagetest"
)

// A conversation asked to start with another scope's library is not created at all.
func TestCreateConversationRejectsOutOfScopeLibrary(t *testing.T) {
	pg, fake := storagetest.Postgres()
	t.Cleanup(func() { pg.Close() })
	m := &Manager{pg: pg, instructionManager: instructions.NewManager(pg)}
	fake.On("SELECT COUNT(*) FROM instruction_libraries", fakedb.Row(int64(0)))

	_, err := m.CreateConversation(context.Background(), "f1", "", "t", "alice", []string{"other-fractal-lib"})
	if !errors.Is(err, instructions.ErrLibraryOutOfScope) {
		t.Fatalf("err = %v, want ErrLibraryOutOfScope", err)
	}
	if n := len(fake.Matching("INSERT INTO chat_conversations")); n != 0 {
		t.Errorf("conversation inserted %d times", n)
	}
}
