package postgrestest

import (
	"crypto/rand"
	"database/sql"
	"fmt"
	mathrand "math/rand"
	"net/url"
	"os"
	"strings"

	_ "github.com/jackc/pgx/v4/stdlib" // postgres driver
	"github.com/stretchr/testify/require"
)

// ConnectFunction is the signature of a function used to open database connections.
type ConnectFunction func(address string) (*sql.DB, error)

// CreateDatabaseFunction is the signature of function used to create the database.
type CreateDatabaseFunction func(db *sql.DB, database string) error

// DeleteDatabaseFunction is the signature of function used to delete the database.
type DeleteDatabaseFunction func(db *sql.DB, database string) error

// DefaultConnectFunction is the default function used to open database connections.
func DefaultConnectFunction(address string) (*sql.DB, error) {
	return sql.Open("pgx", address)
}

// DefaultCreateDatabaseFunction is the default function used to create instances.
func DefaultCreateDatabaseFunction(db *sql.DB, database string) error {
	_, err := db.Exec(`CREATE DATABASE ` + database)
	return err
}

// DefaultDeleteDatabaseFunction is the default function used to delete instances.
func DefaultDeleteDatabaseFunction(db *sql.DB, database string) error {
	_, err := db.Exec(`DROP DATABASE ` + database)
	return err
}

// ForceDeleteDatabaseFunction is a function used to delete instances with force.
func ForceDeleteDatabaseFunction(db *sql.DB, database string) error {
	_, err := db.Exec(`DROP DATABASE ` + database + ` WITH (FORCE);`)
	return err
}

// Option is the signature of options that can be provided to NewPostgresTest.
type Option func(opts *options)

// WithBaseAddress is an option that allows providing the address
// for the base database.
func WithBaseAddress(address string) Option {
	return func(opts *options) {
		opts.baseAddress = address
	}
}

// WithConnectFunction is an option that allows providing the connection
// function to be used with NewPostgresTest.
func WithConnectFunction(connectFunction ConnectFunction) Option {
	return func(opts *options) {
		opts.connectFunction = connectFunction
	}
}

// WithCreateDatabaseFunction is an option that allows providing the create
// database function to be used with NewPostgresTest.
func WithCreateDatabaseFunction(createDatabaseFunction CreateDatabaseFunction) Option {
	return func(opts *options) {
		opts.createDatabaseFunction = createDatabaseFunction
	}
}

// WithDeleteDatabaseFunction is an option that allows providing the create
// database function to be used with NewPostgresTest.
func WithDeleteDatabaseFunction(deleteDatabaseFunction DeleteDatabaseFunction) Option {
	return func(opts *options) {
		opts.deleteDatabaseFunction = deleteDatabaseFunction
	}
}

// options holds references for all the options we allow proving on NewPostgresTest.
type options struct {
	baseAddress            string
	connectFunction        ConnectFunction
	createDatabaseFunction CreateDatabaseFunction
	deleteDatabaseFunction DeleteDatabaseFunction
}

type TestingT interface {
	Errorf(format string, args ...interface{})
	FailNow()
	Cleanup(func())
}

// NewPostgresTest returns a database DSN for connecting to a test database.
// It wil create a database on the base testing Postgres server.
// It's possible to provide a DSN on the TESTING_POSTGRES_TEST environmental variable.
// If no value is present on the TESTING_POSTGRES_TEST envrioment variable
// we try to use the default postgres://postgres:root@localhost:65432 as the base
// Postgres server.
func NewPostgresTest(t TestingT, opts ...Option) string {
	if h, ok := t.(interface {
		Helper()
	}); ok {
		h.Helper()
	}
	defaultOpts := &options{
		baseAddress:            os.Getenv("TESTING_POSTGRES_TEST"),
		connectFunction:        DefaultConnectFunction,
		createDatabaseFunction: DefaultCreateDatabaseFunction,
		deleteDatabaseFunction: DefaultDeleteDatabaseFunction,
	}
	for _, opt := range opts {
		opt(defaultOpts)
	}
	if defaultOpts.baseAddress == "" {
		defaultOpts.baseAddress = "postgres://postgres:root@localhost:55432"
	}
	// connect to the base database and create the test database
	globalDB, err := sql.Open("pgx", defaultOpts.baseAddress)
	require.NoError(t, err)
	databaseName := createTestingDatabase(t, defaultOpts.createDatabaseFunction, globalDB, defaultOpts.baseAddress)
	_ = globalDB.Close()
	t.Cleanup(func() {
		if defaultOpts.deleteDatabaseFunction == nil {
			return
		}
		globalDB, err := sql.Open("pgx", defaultOpts.baseAddress)
		require.NoError(t, err)
		deleteDatabase(t, defaultOpts.deleteDatabaseFunction, globalDB, databaseName)
		_ = globalDB.Close()
	})
	u, err := url.Parse(defaultOpts.baseAddress)
	require.NoError(t, err)
	u.Path = databaseName
	return u.String()
}

// AlterTableSequences alters the table sequences to random numbers.
// This can be used to help find cases where a bug is introduced
// because integration tests uses fresh database and sequences numbers are
// very close to each other in all tables.
func AlterTableSequences(t TestingT, db *sql.DB) {
	if h, ok := t.(interface {
		Helper()
	}); ok {
		h.Helper()
	}
	rows, err := db.Query(`SELECT c.relname FROM pg_class c WHERE c.relkind = 'S';`)
	require.NoError(t, err)
	defer rows.Close()
	var sequences []string
	for rows.Next() {
		var sequence string
		err := rows.Scan(&sequence)
		require.NoError(t, err)
		sequences = append(sequences, sequence)
	}
	for _, seq := range sequences {
		_, err := db.Exec(fmt.Sprintf("ALTER SEQUENCE %s RESTART WITH %d;", seq, mathrand.Intn(100000)+100)) //nolint:gosec
		require.NoError(t, err)
	}
}

func createTestingDatabase(t TestingT, createDatabase CreateDatabaseFunction, db *sql.DB, addr string) string {
	if h, ok := t.(interface {
		Helper()
	}); ok {
		h.Helper()
	}
	b := make([]byte, 8)
	_, err := rand.Read(b) //nolint:gosec
	require.NoError(t, err)
	database := strings.ToLower(fmt.Sprintf("testing_db_%x", b))
	err = createDatabase(db, database)
	require.NoError(t, err)
	return database
}

func deleteDatabase(t TestingT, deleteDatabase DeleteDatabaseFunction, db *sql.DB, databaseName string) {
	if h, ok := t.(interface {
		Helper()
	}); ok {
		h.Helper()
	}
	err := deleteDatabase(db, databaseName)
	require.NoError(t, err)
}
