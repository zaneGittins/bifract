package queryhistory

import "testing"

func TestRelativeArgFitsColumn(t *testing.T) {
	ok, big, neg := int64(90), int64(1<<31), int64(-1)
	if got := relativeArg(&ok); got != ok {
		t.Errorf("relativeArg(90) = %v", got)
	}
	for _, n := range []*int64{nil, &big, &neg} {
		if got := relativeArg(n); got != nil {
			t.Errorf("relative_n is a positive INTEGER column, got %v", got)
		}
	}
}
