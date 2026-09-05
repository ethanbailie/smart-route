package fake

import (
	"testing"

	"github.com/ethan/smart-route/internal/sandbox"
	"github.com/ethan/smart-route/internal/sandbox/providertest"
)

func TestProviderContract(t *testing.T) {
	providertest.Run(t, func(*testing.T) sandbox.Provider { return New() })
}
