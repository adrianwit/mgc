package mgc

import (
	"fmt"
	mgo "github.com/globalsign/mgo"
	"github.com/pkg/errors"
	"github.com/viant/dsc"
	"github.com/viant/toolbox/url"
	"time"
)

const (
	hostKey    = "host"
	portKey    = "port"
	dbnameKey  = "dbname"
	timeoutKey = "timeoutSec"
)

var SessionPointer = (*mgo.Session)(nil)
var DbPointer = (*mgo.Database)(nil)

func asDatabase(connection dsc.Connection) (*mgo.Database, error) {
	db := connection.Unwrap(DbPointer).(*mgo.Database)
	return db, nil
}

func asSession(connection dsc.Connection) (*mgo.Session, error) {
	session := connection.Unwrap(SessionPointer).(*mgo.Session)
	return session, nil
}

type connection struct {
	*dsc.AbstractConnection
	session *mgo.Session
	dbName  string
}

func (c *connection) CloseNow() error {
	session := c.session
	session.Close()
	return nil
}

func (c *connection) Unwrap(targetType interface{}) interface{} {
	if targetType == SessionPointer {
		return c.session
	}
	if targetType == DbPointer {
		return c.session.DB(c.dbName)
	}
	panic(fmt.Sprintf("unsupported targetType type %v", targetType))
}

type connectionProvider struct {
	*dsc.AbstractConnectionProvider
}

func (p *connectionProvider) NewConnection() (dsc.Connection, error) {
	config := p.ConnectionProvider.Config()

	dbname := config.Get(dbnameKey)
	if dbname == "" {
		return nil, errors.New("dbname was empty")
	}
	host := config.Get(hostKey)
	if host == "" {
		return nil, errors.New("host was empty")
	}
	port := config.GetInt(portKey, 27017)
	var err error
	hostname := fmt.Sprintf("%v:%d", host, port)
	var session *mgo.Session
	if config.Has(timeoutKey) {
		var timeout = config.GetDuration(timeoutKey, time.Second, 5*time.Second)
		session, err = mgo.DialWithTimeout(hostname, timeout)
	} else {
		session, err = mgo.Dial(hostname)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %v, %v", hostname, err)
	}
	if p.Config().Credentials != "" {

		var credential = &mgo.Credential{}
		resource := url.NewResource(p.Config().Credentials)
		if err := resource.Decode(credential); err != nil {
			return nil, err
		}
		if err = session.Login(credential); err != nil {
			return nil, err
		}
	}
	var mgoConnection = &connection{session: session, dbName: dbname}
	var super = dsc.NewAbstractConnection(config, p.ConnectionProvider.ConnectionPool(), mgoConnection)
	mgoConnection.AbstractConnection = super
	return mgoConnection, nil
}

func newConnectionProvider(config *dsc.Config) dsc.ConnectionProvider {
	if config.MaxPoolSize == 0 {
		config.MaxPoolSize = 1
	}
	aerospikeConnectionProvider := &connectionProvider{}
	var connectionProvider dsc.ConnectionProvider = aerospikeConnectionProvider
	var super = dsc.NewAbstractConnectionProvider(config, make(chan dsc.Connection, config.MaxPoolSize), connectionProvider)
	aerospikeConnectionProvider.AbstractConnectionProvider = super
	aerospikeConnectionProvider.AbstractConnectionProvider.ConnectionProvider = connectionProvider
	return aerospikeConnectionProvider
}
