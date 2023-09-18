package main

import (
	"database/sql"
	"fmt"
)

type postgresDriver struct {
	host          string
	port          int
	adminUser     string
	adminPassword *string
	adminDB       *string
	db            *sql.DB
}

func newPostgresDriver(host string, port int, adminUser, adminPassword, adminDB *string) *postgresDriver {
	admin := "postgres"
	if adminUser != nil {
		admin = *adminUser
	}
	return &postgresDriver{host: host, port: port, adminUser: admin, adminPassword: adminPassword, adminDB: adminDB}
}

func (d *postgresDriver) connect(psqlInfo string) error {
	var err error
	d.db, err = sql.Open("postgres", psqlInfo)
	return err
}

func (d *postgresDriver) Connect() error {
	return d.connectTo(d.adminDB)
}

func (d *postgresDriver) connectTo(dbName *string) error {
	connectString := fmt.Sprintf("host=%v port=%v user=%v sslmode=disable", d.host, d.port, d.adminUser)
	if dbName != nil {
		connectString += fmt.Sprintf(" dbname=%v", *dbName)
	}
	if d.adminPassword != nil {
		connectString += fmt.Sprintf(" password=%v", *d.adminPassword)
	}
	return d.connect(connectString)
}

func (d *postgresDriver) Disconnect() {
	_ = d.db.Close()
}

func (d *postgresDriver) Exec(sql string) error {
	_, err := d.db.Exec(sql)
	return err
}

func (d *postgresDriver) Query(sql string) (*sql.Rows, error) {
	return d.db.Query(sql)
}

func (d *postgresDriver) CreateDatabase(dbName string, dbUser string, dbPass string) error {
	rows, err := d.Query(preprocess(dbName, dbUser, dbPass, "SELECT 1 FROM pg_roles WHERE rolname='${DB_USER}'"))
	if err != nil {
		return err
	}
	if !rows.Next() {
		err = d.Exec(preprocess(dbName, dbUser, dbPass, "CREATE USER ${DB_USER} WITH ENCRYPTED PASSWORD '${DB_PASS}'"))
		if err != nil {
			return err
		}
	}
	err = d.Exec(preprocess(dbName, dbUser, dbPass, "DROP DATABASE IF EXISTS ${DB_NAME}"))
	if err != nil {
		return err
	}
	err = d.Exec(preprocess(dbName, dbUser, dbPass, "CREATE DATABASE ${DB_NAME} ENCODING = \"UTF8\" CONNECTION LIMIT = -1"))
	if err != nil {
		return err
	}
	err = d.Exec(preprocess(dbName, dbUser, dbPass, "GRANT CONNECT, TEMP on database ${DB_NAME} to ${DB_USER}"))
	if err != nil {
		return err
	}
	d.Disconnect()
	return d.connectTo(&dbName)
}
