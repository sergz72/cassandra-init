package main

import (
	"database/sql"
)

type postgresDriver struct {
	db *sql.DB
}

func (d *postgresDriver) Connect() error {
	psqlInfo := "host=127.0.0.1 port=5432 user=postgres sslmode=disable"
	var err error
	d.db, err = sql.Open("postgres", psqlInfo)
	return err
}

func (d *postgresDriver) Disconnect() {
	_ = d.db.Close()
}

func (d *postgresDriver) Exec(sql string) error {
	_, err := d.db.Exec(sql)
	return err
}

func (d *postgresDriver) CreateDatabase(dbName string, dbUser string, dbPass string) error {
	err := d.Exec(preprocess(dbName, dbUser, dbPass, "CREATE USER ${DB_USER} WITH ENCRYPTED PASSWORD '${DB_PASS}'"))
	if err != nil {
		return err
	}
	return d.Exec(preprocess(dbName, dbUser, dbPass, "CREATE DATABASE ${DB_NAME} WITH OWNER = postgres ENCODING = \"UTF8\" CONNECTION LIMIT = -1"))
}
