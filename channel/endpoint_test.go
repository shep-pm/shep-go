package channel

import (
	"errors"
	"strings"
	"testing"
)

// fakeEnv is a lookup over a map. No test mutates the process
// environment, which would race every other test.
func fakeEnv(pairs map[string]string) lookup {
	return func(name string) (string, bool) {
		value, set := pairs[name]
		return value, set
	}
}

func TestNeitherVariableMeansNoChannel(t *testing.T) {
	found, err := discover(fakeEnv(map[string]string{nameVar: "web"}))
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if found.Kind != EndpointAbsent {
		t.Fatalf("kind is %v, want absent", found.Kind)
	}
}

func TestADescriptorIsTakenFromTheEnvironment(t *testing.T) {
	found, err := discover(fakeEnv(map[string]string{FDVar: " 3 "}))
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if found.Kind != EndpointDescriptor || found.FD != 3 {
		t.Fatalf("found %+v, want descriptor 3", found)
	}
}

func TestAPipePathIsTakenFromTheEnvironment(t *testing.T) {
	path := `\\.\pipe\shep-channel-1234-0-0123456789abcdef`
	found, err := discover(fakeEnv(map[string]string{PipeVar: path}))
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if found.Kind != EndpointPipe || found.Pipe != path {
		t.Fatalf("found %+v, want pipe %s", found, path)
	}
}

// Taking 1 would give this module the app's stdout. It would write JSON
// into it and close it on exit. Worse than a merely wrong number.
func TestADescriptorBelowThreeIsRefused(t *testing.T) {
	for _, raw := range []string{"0", "1", "2", "-1"} {
		_, err := discover(fakeEnv(map[string]string{FDVar: raw}))
		if !errors.Is(err, ErrUnusable) {
			t.Fatalf("%s=%s reported %v, want ErrUnusable", FDVar, raw, err)
		}
		if !strings.Contains(err.Error(), FDVar) || !strings.Contains(err.Error(), raw) {
			t.Fatalf("the refusal names neither the variable nor the value: %v", err)
		}
	}
}

func TestADescriptorThatIsNotANumberIsRefused(t *testing.T) {
	_, err := discover(fakeEnv(map[string]string{FDVar: "three"}))
	if !errors.Is(err, ErrUnusable) {
		t.Fatalf("%s=three reported %v, want ErrUnusable", FDVar, err)
	}
	if !strings.Contains(err.Error(), "three") {
		t.Fatalf("the refusal does not name the value: %v", err)
	}
}

func TestAnEmptyPipePathIsRefusedRatherThanOpened(t *testing.T) {
	if _, err := discover(fakeEnv(map[string]string{PipeVar: "   "})); !errors.Is(err, ErrUnusable) {
		t.Fatalf("%s= reported %v, want ErrUnusable", PipeVar, err)
	}
}

// The shepherd sets exactly one. This pins what happens if that stops
// being true, rather than leaving it to chance.
func TestTheDescriptorWinsWhenBothVariablesAreSet(t *testing.T) {
	found, err := discover(fakeEnv(map[string]string{FDVar: "3", PipeVar: `\\.\pipe\x`}))
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if found.Kind != EndpointDescriptor {
		t.Fatalf("kind is %v, want descriptor", found.Kind)
	}
}

// The exported wrapper reads the process environment, unlike every test
// above. It only looks, so nothing here opens or claims a channel.
func TestDiscoverReadsTheProcessEnvironment(t *testing.T) {
	t.Setenv(FDVar, " 3 ")
	found, err := Discover()
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if found.Kind != EndpointDescriptor || found.FD != 3 {
		t.Fatalf("Discover found %+v, want descriptor 3", found)
	}
}

func TestEveryEndpointKindPrintsItsName(t *testing.T) {
	names := map[EndpointKind]string{
		EndpointAbsent:     "absent",
		EndpointDescriptor: "descriptor",
		EndpointPipe:       "pipe",
	}
	for kind, want := range names {
		if got := kind.String(); got != want {
			t.Fatalf("kind %d prints %q, want %q", int(kind), got, want)
		}
	}
}
