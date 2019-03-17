package mgc

import (
	"database/sql"
	"fmt"
	mgo "github.com/globalsign/mgo"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
)

const (
	pkColumnKey = "keyColumn"
	mongoIDKey  = "_id"
)

type config struct {
	*dsc.Config
	keyColumn string
	dbName    string
}

type manager struct {
	*dsc.AbstractManager
	config *config
}

func (m *manager) getKeyColumn(table string) string {
	if keyColumn := m.config.GetString(table+"."+pkColumnKey, ""); keyColumn != "" {
		return keyColumn
	}
	return m.config.keyColumn
}

func (m *manager) updatePKIfNeeded(table string, record map[string]interface{}, replace bool) {
	if _, has := record[mongoIDKey]; has {
		return
	}
	keyColumn := m.getKeyColumn(table)
	if id, has := record[keyColumn]; has {
		record[mongoIDKey] = id
		if replace {
			delete(record, keyColumn)
		}
	}
	return
}

func (m *manager) runInsert(db *mgo.Database, statement *dsc.DmlStatement, sqlParameters []interface{}) (err error) {
	parameters := toolbox.NewSliceIterator(sqlParameters)
	var record map[string]interface{}
	if record, err = statement.ColumnValueMap(parameters); err != nil {
		return err
	}
	m.updatePKIfNeeded(statement.Table, record, false)
	collection := db.C(statement.Table)
	return collection.Insert(record)
}

func (m *manager) runUpdate(db *mgo.Database, statement *dsc.DmlStatement, sqlParameters []interface{}) (err error) {
	parameters := toolbox.NewSliceIterator(sqlParameters)
	var record map[string]interface{}
	if record, err = statement.ColumnValueMap(parameters); err != nil {
		return err
	}
	criteria, err := m.criteria(statement.BaseStatement, parameters)
	if err != nil {
		return err
	}
	m.updatePKIfNeeded(statement.Table, criteria, true)
	collection := db.C(statement.Table)
	query := collection.Find(criteria)
	previous := map[string]interface{}{}
	if query.Iter().Next(&previous) {
		for k, v := range previous {
			if _, has := record[k]; has {
				continue
			}
			record[k] = v
		}
	}
	return collection.Update(criteria, record)
}

func (m *manager) criteria(statement *dsc.BaseStatement, parameters toolbox.Iterator) (map[string]interface{}, error) {
	criteriaValues, err := statement.CriteriaValues(parameters)
	if err != nil {
		return nil, err
	}
	return AsMongoCriteria(statement.SQLCriteria, criteriaValues)

}

func (m *manager) runDelete(db *mgo.Database, statement *dsc.DmlStatement, sqlParameters []interface{}) (affected int, err error) {
	collection := db.C(statement.Table)
	if len(statement.Criteria) == 0 {

		var count, _ = collection.Count()
		if count == 0 {
			return 0, nil
		}
		return count, db.C(statement.Table).DropCollection()
	}
	parameters := toolbox.NewSliceIterator(sqlParameters)
	criteria, err := m.criteria(statement.BaseStatement, parameters)
	if err != nil {
		return 0, err
	}
	m.updatePKIfNeeded(statement.Table, criteria, true)
	info, err := collection.RemoveAll(criteria)
	if err != nil {
		return 0, err
	}
	return info.Removed, nil
}

func (m *manager) ExecuteOnConnection(connection dsc.Connection, sql string, sqlParameters []interface{}) (result sql.Result, err error) {
	dsc.Logf("[%v]:%v, %v\n", m.config.dbName, sql, sqlParameters)
	db, err := asDatabase(connection)
	if err != nil {
		return nil, err
	}
	parser := dsc.NewDmlParser()
	statement, err := parser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %v due to %v", sql, err)
	}
	var affectedRecords = 1
	switch statement.Type {
	case "INSERT":
		err = m.runInsert(db, statement, sqlParameters)
	case "UPDATE":
		err = m.runUpdate(db, statement, sqlParameters)
	case "DELETE":
		affectedRecords, err = m.runDelete(db, statement, sqlParameters)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to modify %v, %v", statement.Table, err)
	}
	return dsc.NewSQLResult(int64(affectedRecords), 0), nil
}

func (m *manager) enrichRecordIfNeeded(statement *dsc.QueryStatement, record map[string]interface{}) []string {
	var columns = make([]string, 0)
	for _, column := range statement.Columns {
		var name = column.Name
		if column.Alias != "" {
			if value, ok := record[name]; ok {
				delete(record, name)
				record[column.Alias] = value
			}
			name = column.Alias
		}
		columns = append(columns, name)
	}

	return columns
}

func (m *manager) ReadAllOnWithHandlerOnConnection(connection dsc.Connection, SQL string, SQLParameters []interface{}, readingHandler func(scanner dsc.Scanner) (toContinue bool, err error)) error {
	dsc.Logf("[%v]:%v, %v\n", m.config.dbName, SQL, SQLParameters)
	db, err := asDatabase(connection)
	if err != nil {
		return err
	}
	parser := dsc.NewQueryParser()
	statement, err := parser.Parse(SQL)
	if err != nil {
		return fmt.Errorf("failed to parse statement %v, %v", SQL, err)
	}
	parameters := toolbox.NewSliceIterator(SQLParameters)
	criteria, err := m.criteria(statement.BaseStatement, parameters)
	if err != nil {
		return err
	}
	m.updatePKIfNeeded(statement.Table, criteria, true)
	collection := db.C(statement.Table)

	if len(criteria) == 0 {
		criteria = nil
	}
	var count = 0
	iter := collection.Find(criteria).Iter()
	scanner := dsc.NewSQLScanner(statement, m.Config(), nil)
	for iter.Next(&scanner.Values) {
		count++
		toContinue, err := readingHandler(scanner)
		if err != nil {
			return err
		}
		if !toContinue {
			break
		}
		scanner.Values = make(map[string]interface{})
	}
	return nil
}

func newConfig(conf *dsc.Config) (*config, error) {
	var keyColumnName = conf.GetString(pkColumnKey, mongoIDKey)
	return &config{
		Config:    conf,
		keyColumn: keyColumnName,
	}, nil
}
