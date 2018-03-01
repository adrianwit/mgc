package mgc_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"testing"
)

func TestNewConnection(t *testing.T) {
	var params = map[string]interface{}{
		"host":       "127.3.0.1",
		"port":       "1111",
		"dbname":     "mydb",
		"timeoutSec": "1",
	}
	config, err := dsc.NewConfigWithParameters("mgc", "", "", params)
	if !assert.Nil(t, err) {
		return
	}
	factory := dsc.NewManagerFactory()
	manager, _ := factory.Create(config)
	provider := manager.ConnectionProvider()
	_, err = provider.NewConnection()
	assert.NotNil(t, err)
}
