package main

import (
	"bufio"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"os"
	"strings"
)

type dbDriver interface {
	Connect() error
	Disconnect()
	Exec(sql string) error
	CreateDatabase(dbName string, dbUser string, dbPass string) error
}

func preprocess(dbName, dbUser, dbPass string, sql string) string {
	text := strings.Replace(sql, "${DB_NAME}", dbName, -1)
	text = strings.Replace(text, "${DB_USER}", dbUser, -1)
	text = strings.Replace(text, "${DB_PASS}", dbPass, -1)
	fmt.Println(text)
	return text
}

func usage() {
	fmt.Println("Usage: database-init [postgres|cassandra|dryrun] db_name init_scripts_folder [db_user db_pass]")
}

func main() {
	l := len(os.Args)
	if l != 4 && l != 6 {
		usage()
		return
	}

	var driver dbDriver

	switch os.Args[1] {
	case "cassandra":
		fmt.Println("Using Cassandra db driver...")
		driver = &cassandraDriver{}
	case "postgres":
		fmt.Println("Using Postgres db driver...")
		driver = &postgresDriver{}
	case "dryrun":
		fmt.Println("Dryrun...")
	default:
		usage()
		return
	}

	dbName := os.Args[2]
	initScriptsFolder := os.Args[3]

	var dbUser, dbPass string
	if l == 6 {
		dbUser = os.Args[4]
		dbPass = os.Args[5]
	}

	if driver != nil {
		err := driver.Connect()
		if err != nil {
			log.Fatal(err)
		}
		defer driver.Disconnect()

		err = driver.CreateDatabase(dbName, dbUser, dbPass)
		if err != nil {
			log.Fatal(err)
		}
	}

	files, err := buildFileList(initScriptsFolder, nil)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		fmt.Printf("Executing statements from %v...\n", file)
		statements, err := extractStatements(file)
		if err != nil {
			log.Fatal(err)
		}
		for _, statement := range statements {
			sql := preprocess(dbName, dbUser, dbPass, statement)
			if driver != nil {
				if err = driver.Exec(sql); err != nil {
					log.Fatal(err)
				}
			}
		}
	}
}

func extractStatements(fileName string) ([]string, error) {
	f, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var result []string

	scanner := bufio.NewScanner(f)
	var sb strings.Builder
	functionMode := false
	for scanner.Scan() {
		scanned := scanner.Text()
		text := strings.TrimSpace(scanned)
		if strings.Contains(text, "$$") {
			functionMode = !functionMode
		}
		if functionMode {
			sb.WriteString(scanned)
			sb.WriteRune('\n')
		} else {
			if len(text) == 0 || strings.HasPrefix(text, "--") {
				continue
			}
			if strings.Contains(text, ";") {
				scanned = strings.Replace(scanned, ";", "", -1)
				sb.WriteString(scanned)
				result = append(result, sb.String())
				sb.Reset()
			} else {
				sb.WriteString(scanned)
				sb.WriteRune('\n')
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	last := sb.String()
	if len(last) > 0 {
		result = append(result, last)
	}

	return result, nil
}
