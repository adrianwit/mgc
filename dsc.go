package mgc

import (
	"github.com/viant/dsc"
)

func register() {
	dsc.RegisterManagerFactory("mgc", newManagerFactory())
	dsc.RegisterDatastoreDialect("mgc", newDialect())
}

func init() {
	register()
}
