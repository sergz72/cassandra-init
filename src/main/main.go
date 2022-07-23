package main

import (
	"bufio"
	"fmt"
	"github.com/gocql/gocql"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func preprocess(dbName string, sql string) string {
	text := strings.Replace(sql, "${DB_NAME}", dbName, -1)
	fmt.Println(text)
	return text
}

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: cassandra-init db_name init_scripts_folder")
		return
	}
	dbName := os.Args[1]
	initScriptsFolder := os.Args[2]

	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Timeout = time.Minute
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	if err := session.Query(preprocess(dbName, "DROP KEYSPACE IF EXISTS ${DB_NAME}")).Exec(); err != nil {
		log.Fatal(err)
	}

	if err = session.Query(preprocess(dbName, "CREATE KEYSPACE ${DB_NAME} WITH REPLICATION = { 'class': 'SimpleStrategy','replication_factor': 1} AND DURABLE_WRITES =  true")).Exec(); err != nil {
		log.Fatal(err)
	}

	files, err := ioutil.ReadDir(initScriptsFolder)
	if err != nil {
		log.Fatal(err)
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})

	for _, file := range files {
		fmt.Printf("Executing statements from %v...\n", file.Name())
		statements, err := extractStatements(filepath.Join(initScriptsFolder, file.Name()))
		if err != nil {
			log.Fatal(err)
		}
		for _, statement := range statements {
			if err = session.Query(preprocess(dbName, statement)).Exec(); err != nil {
				log.Fatal(err)
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
	for scanner.Scan() {
		text := strings.TrimSpace(scanner.Text())
		if len(text) == 0 || strings.HasPrefix(text, "--") {
			continue
		}
		if strings.Contains(text, ";") {
			text = strings.Replace(text, ";", "", -1)
			sb.WriteString(text)
			result = append(result, sb.String())
			sb.Reset()
		} else {
			sb.WriteString(text)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return result, nil
}
