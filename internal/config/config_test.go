package config

import "testing"

// Development endpoints are opt-in and the default listen address is loopback.
func TestSafeDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("HTTP_ADDR", "")
	t.Setenv("DEV_ENDPOINTS", "")

	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != "127.0.0.1:8080" {
		t.Errorf("default HTTP_ADDR = %q, want 127.0.0.1:8080", c.HTTPAddr)
	}
	if c.DevEndpoints {
		t.Error("DEV_ENDPOINTS must default to false")
	}
}

func TestExplicitOptIn(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("HTTP_ADDR", ":9090")

	for _, v := range []string{"true", "1", "TRUE"} {
		t.Setenv("DEV_ENDPOINTS", v)
		c, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if !c.DevEndpoints {
			t.Errorf("DEV_ENDPOINTS=%q should enable development endpoints", v)
		}
		if c.HTTPAddr != ":9090" {
			t.Errorf("explicit HTTP_ADDR ignored: %q", c.HTTPAddr)
		}
	}
	for _, v := range []string{"false", "0", "", "banana"} {
		t.Setenv("DEV_ENDPOINTS", v)
		c, _ := Load()
		if c.DevEndpoints {
			t.Errorf("DEV_ENDPOINTS=%q must not enable development endpoints", v)
		}
	}
}
