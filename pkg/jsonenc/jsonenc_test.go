package jsonenc

import "testing"

func TestMarshalKeepsHTMLCharacters(t *testing.T) {
	got, err := Marshal(map[string]any{"cmd": "a<b && c>d", "args": []any{"x>y"}})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"args":["x>y"],"cmd":"a<b && c>d"}`; string(got) != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestMarshalStillEscapesJSONSpecials(t *testing.T) {
	got, err := Marshal("q\"\\\n\u2028")
	if err != nil {
		t.Fatal(err)
	}
	if want := `"q\"\\\n\u2028"`; string(got) != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestMarshalError(t *testing.T) {
	if _, err := Marshal(make(chan int)); err == nil {
		t.Fatal("want error for unsupported type")
	}
}
