package mgc

import (
	"github.com/viant/dsc"
)

var maxRecordColumnScan = 20

type dialect struct{ dsc.DatastoreDialect }

//GetKeyName returns a name of column name that is a key, or coma separated list if complex key
func (d *dialect) GetKeyName(manager dsc.Manager, datastore, table string) string {
	var key = manager.Config().GetString(pkColumnNameKey, mongoIDKey)
	return key
}

func (d *dialect) GetColumns(manager dsc.Manager, datastore, table string) []string {
	var result = make([]string, 0)
	connection, err := manager.ConnectionProvider().Get()
	if err != nil {
		return result
	}
	defer connection.Close()
	db, err := asDatabase(connection)
	if err != nil {
		return result
	}

	inter := db.C(table).Find(nil).Iter()
	var keys = make(map[string]bool)
	record := make(map[string]interface{})
	var i = 0
	//bit hacky, TODO change for map reduce and cache all update/inserts
	for inter.Next(&result) && i < maxRecordColumnScan {
		for k := range record {
			keys[k] = true
		}
		i++
	}
	for k := range keys {
		result = append(result, k)
	}
	return result
}

func (d *dialect) DropTable(manager dsc.Manager, datastore string, table string) error {
	connection, err := manager.ConnectionProvider().Get()
	if err != nil {
		return err
	}
	defer connection.Close()
	session, err := asSession(connection)
	if err != nil {
		return err
	}
	return session.DB(datastore).C(table).DropCollection()
}

func (d *dialect) GetDatastores(manager dsc.Manager) ([]string, error) {
	connection, err := manager.ConnectionProvider().Get()
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	session, err := asSession(connection)
	if err != nil {
		return nil, err
	}
	return session.DatabaseNames()
}

func (d *dialect) GetCurrentDatastore(manager dsc.Manager) (string, error) {
	config := manager.Config()
	return config.Get(dbnameKey), nil
}

func (d *dialect) GetTables(manager dsc.Manager, datastore string) ([]string, error) {
	connection, err := manager.ConnectionProvider().Get()
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	db, err := asDatabase(connection)
	if err != nil {
		return nil, err
	}
	return db.CollectionNames()
}

func (d *dialect) CanPersistBatch() bool {
	return true
}

func newDialect() dsc.DatastoreDialect {
	var resut dsc.DatastoreDialect = &dialect{dsc.NewDefaultDialect()}
	return resut
}
