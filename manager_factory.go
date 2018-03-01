package mgc

import (
	"errors"
	"github.com/viant/dsc"
)

type managerFactory struct{}

func (f *managerFactory) Create(config *dsc.Config) (dsc.Manager, error) {
	var connectionProvider = newConnectionProvider(config)
	manager := &manager{}
	var self dsc.Manager = manager
	super := dsc.NewAbstractManager(config, connectionProvider, self)
	manager.AbstractManager = super
	var err error
	dbname := config.Get(dbnameKey)
	if dbname == "" {
		return nil, errors.New("dbname was empty")
	}
	manager.config, err = newConfig(config)
	manager.config.dbName = dbname
	return self, err
}

func (f managerFactory) CreateFromURL(URL string) (dsc.Manager, error) {
	config, err := dsc.NewConfigFromURL(URL)
	if err != nil {
		return nil, err
	}
	return f.Create(config)
}

func newManagerFactory() dsc.ManagerFactory {
	var result dsc.ManagerFactory = &managerFactory{}
	return result
}
