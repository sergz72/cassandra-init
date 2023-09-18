package main

import (
	"bufio"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"os"
	"strconv"
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
	fmt.Println("Usage: database-init [/adminUser adminUserName][/adminPassword adminPassword][postgres|cassandra|dryrun] host db_name init_scripts_folder [db_user db_pass]")
}

func main() {
	l := len(os.Args)
	if l < 5 {
		usage()
		return
	}
	var adminUser *string
	var adminPassword *string
	argIdx := 1
forLabel:
	for {
		if len(os.Args) < argIdx+2 {
			usage()
			return
		}
		switch os.Args[argIdx] {
		case "/adminUser":
			adminUser = &os.Args[argIdx+1]
			argIdx += 2
			l -= 2
		case "/adminPassword":
			adminPassword = &os.Args[argIdx+1]
			argIdx += 2
			l -= 2
		default:
			break forLabel
		}
	}

	if l != 5 && l != 7 {
		usage()
		return
	}

	var driver dbDriver

	switch os.Args[argIdx] {
	case "cassandra":
		fmt.Println("Using Cassandra db driver...")
		driver = newCassandraDriver(os.Args[argIdx+1])
	case "postgres":
		fmt.Println("Using Postgres db driver...")
		host, port, err := parseHostPort(os.Args[argIdx+1], 5432)
		if err != nil {
			log.Fatal(err)
		}
		driver = newPostgresDriver(host, port, adminUser, adminPassword)
	case "dryrun":
		fmt.Println("Dryrun...")
	default:
		usage()
		return
	}

	dbName := os.Args[argIdx+2]
	initScriptsFolder := os.Args[argIdx+3]

	var dbUser, dbPass string
	if l == 7 {
		dbUser = os.Args[argIdx+4]
		dbPass = os.Args[argIdx+5]
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

func parseHostPort(hostPort string, defaultPort int) (string, int, error) {
	parts := strings.Split(hostPort, ":")
	switch len(parts) {
	case 1:
		return hostPort, defaultPort, nil
	case 2:
		port, err := parsePort(parts[1])
		return parts[0], port, err
	default:
		return "", defaultPort, errors.New("invalid host name")
	}
}

func parsePort(port string) (int, error) {
	p, err := strconv.ParseInt(port, 10, 64)
	if err != nil {
		return 0, err
	}
	if p <= 0 || p > 65535 {
		return 0, errors.New("port value is out of range")
	}
	return int(p), nil
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
