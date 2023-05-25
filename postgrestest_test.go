package postgrestest

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewPostgresTest(t *testing.T) {
	t.Parallel()
	testDB := NewPostgresTest(t)
	db, err := sql.Open("pgx", testDB)
	require.NoError(t, err)
	defer db.Close()
	rows, err := db.Query(`SELECT 42`)
	require.NoError(t, err)
	for rows.Next() {
		var r int
		err = rows.Scan(&r)
		require.NoError(t, err)
		require.Equal(t, 42, r)
	}
	defer rows.Close()
}

func TestAlterTableSequences(t *testing.T) {
	t.Parallel()
	testDB := NewPostgresTest(t)
	db, err := sql.Open("pgx", testDB)
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE table_a (id serial PRIMARY KEY);`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE SEQUENCE seq_a INCREMENT 1 START 1;`)
	require.NoError(t, err)
	currentSequenceValue := func(t *testing.T, db *sql.DB, seqName string) int {
		t.Helper()
		rows, err := db.Query(`SELECT last_value FROM ` + seqName + `;`)
		require.NoError(t, err)
		defer rows.Close()
		require.True(t, rows.Next())
		var curVal int
		err = rows.Scan(&curVal)
		require.NoError(t, err)
		return curVal
	}
	require.Equal(t, 1, currentSequenceValue(t, db, "table_a_id_seq"))
	require.Equal(t, 1, currentSequenceValue(t, db, "seq_a"))
	AlterTableSequences(t, db)
	require.NotEqual(t, 1, currentSequenceValue(t, db, "table_a_id_seq"))
	require.NotEqual(t, 1, currentSequenceValue(t, db, "seq_a"))
}
