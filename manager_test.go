package mgc_test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/dsc"
	"testing"
)

type User struct {
	Id   int    `column:"id"`
	Name string `column:"name"`
}

func TestManager(t *testing.T) {

	//dsc.Logf = dsc.StdoutLogger

	config, err := dsc.NewConfigWithParameters("mgc", "", "", map[string]interface{}{
		"host":      "127.0.0.1",
		"dbname":    "mydb",
		"keyColumn": "id",
	})
	if !assert.Nil(t, err) {
		return
	}
	factory := dsc.NewManagerFactory()
	manager, err := factory.Create(config)
	if err != nil {
		fmt.Printf("make sure mongodb is runnig on localhost")
		return
	}

	//Test insert
	dialect := dsc.GetDatastoreDialect("mgc")
	dialect.DropTable(manager, "mydb", "users")
	for i := 0; i < 3; i++ {
		sqlResult, err := manager.Execute("INSERT INTO users(id, name) VALUES(?, ?)", i, fmt.Sprintf("Name %d", i))
		assert.Nil(t, err)
		affected, _ := sqlResult.RowsAffected()
		assert.EqualValues(t, 1, affected)
	}

	queryCases := []struct {
		Description string
		SQL         string
		Parameters  []interface{}
		Expected    interface{}
	}{
		{
			Description: "Read records ",
			SQL:         "SELECT id, name FROM users",
			Expected: []*User{
				{
					Id:   0,
					Name: "Name 0",
				},
				{
					Id:   1,
					Name: "Name 1",
				},
				{
					Id:   2,
					Name: "Name 2",
				},
			},
		},
		{
			Description: "Read single with placeholder",
			SQL:         "SELECT id, name FROM users WHERE id = ?",
			Parameters:  []interface{}{2},
			Expected: []*User{
				{
					Id:   2,
					Name: "Name 2",
				},
			},
		},
		{
			Description: "Read single with number constant",
			SQL:         "SELECT id, name FROM users WHERE id = 2",
			Expected: []*User{
				{
					Id:   2,
					Name: "Name 2",
				},
			},
		},
		{
			Description: "Read empty with text constant",
			SQL:         "SELECT id, name FROM users WHERE id = '2'",
			Expected:    []*User{},
		},
		{
			Description: "Read records  with in operator",
			SQL:         "SELECT id, name FROM users WHERE id IN(?, ?)",
			Parameters:  []interface{}{1, 2},

			Expected: []*User{
				{
					Id:   1,
					Name: "Name 1",
				},
				{
					Id:   2,
					Name: "Name 2",
				},
			},
		},
		{
			Description: "Read records  with not in",
			SQL:         "SELECT id, name FROM users WHERE id NOT IN(?, ?)",
			Parameters:  []interface{}{0, 4},

			Expected: []*User{
				{
					Id:   1,
					Name: "Name 1",
				},
				{
					Id:   2,
					Name: "Name 2",
				},
			},
		},
		{
			Description: "Read records  with !=",
			SQL:         "SELECT id, name FROM users WHERE id != 0",
			Parameters:  []interface{}{0, 4},

			Expected: []*User{
				{
					Id:   1,
					Name: "Name 1",
				},
				{
					Id:   2,
					Name: "Name 2",
				},
			},
		},
		{
			Description: "Read records with >",
			SQL:         "SELECT id, name FROM users WHERE id > 0",
			Parameters:  []interface{}{0, 4},

			Expected: []*User{
				{
					Id:   1,
					Name: "Name 1",
				},
				{
					Id:   2,
					Name: "Name 2",
				},
			},
		},
	}

	for _, useCase := range queryCases {

		var records = make([]*User, 0)
		err = manager.ReadAll(&records, useCase.SQL, useCase.Parameters, nil)
		if !assert.Nil(t, err) {
			return
		}
		assertly.AssertValues(t, useCase.Expected, records, useCase.Description)
	}

	{
		var SQL = " SELECT  id FROM users WHERE id IN (?,?,?)"
		var records = make([]map[string]interface{}, 0)
		err = manager.ReadAll(&records, SQL, []interface{}{1, 2, 5}, nil)
		if !assert.Nil(t, err) {
			return
		}
		assert.EqualValues(t, 2, len(records))

	}

	{
		var SQL = " SELECT  id FROM users WHERE id = ? OR  id = ? "
		var records = make([]map[string]interface{}, 0)
		err = manager.ReadAll(&records, SQL, []interface{}{1, 2}, nil)
		if !assert.Nil(t, err) {
			return
		}
		assert.EqualValues(t, 2, len(records))

	}

	{ //Test persist

		var records = []*User{
			{
				Id:   1,
				Name: "Name 1",
			},
			{
				Id:   2,
				Name: "Name 22",
			},

			{
				Id:   5,
				Name: "Name 5",
			},
		}

		inserted, updated, err := manager.PersistAll(&records, "users", nil)
		assert.Nil(t, err)
		assert.EqualValues(t, 1, inserted)
		assert.EqualValues(t, 2, updated)
	}

}
