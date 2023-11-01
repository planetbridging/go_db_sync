package main

//rm -rf /usr/local/go && tar -C /usr/local -xzf go1.21.3.linux-amd64.tar.gz
//append below to ~./bashrc
//export PATH=$PATH:/usr/local/go/bin
import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)


type DBConfig struct {
	Username string
	Password string
	Hostname string
	DBName   string
}

const (
	chunkSize = 1000 // number of rows to fetch and insert at once
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	//masterDSN := os.Getenv("MASTER_DSN")
	//slaveDSN := os.Getenv("SLAVE_DSN")

	configMaster := DBConfig{
		Username: os.Getenv("DB_USERNAME1"),
		Password: os.Getenv("DB_PASSWORD1"),
		Hostname: os.Getenv("DB_HOST1"),
		DBName:   os.Getenv("DB_NAME1"),
	}

	masterDb, err := ConnectToDB(configMaster)
	if err != nil {
		log.Fatalf("Failed to connect to master: %s", err)
	}
	defer masterDb.Close()



	configSlave := DBConfig{
		Username: os.Getenv("DB_USERNAME2"),
		Password: os.Getenv("DB_PASSWORD2"),
		Hostname: os.Getenv("DB_HOST2"),
		DBName:   os.Getenv("DB_NAME2"),
	}

	slaveDb, err := ConnectToDB(configSlave)
	if err != nil {
		log.Fatalf("Failed to connect to slave: %s", err)
	}
	defer slaveDb.Close()

	tables := getTables(masterDb)
	for _, table := range tables {
		syncTable(masterDb, slaveDb, table)
	}
}

func ConnectToDB(config DBConfig) (*sql.DB, error) {
	mysql.RegisterTLSConfig("custom", &tls.Config{
		InsecureSkipVerify: true,
	})

	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?parseTime=true&tls=custom", config.Username, config.Password, config.Hostname)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

func getTables(db *sql.DB) []string {
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		log.Fatalf("Failed to get tables: %s", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			log.Fatalf("Failed to scan table name: %s", err)
		}
		tables = append(tables, table)
	}
	return tables
}

func ensureTableExists(master, slave *sql.DB, table string) {
	// Get the table creation query from the master
	query := fmt.Sprintf("SHOW CREATE TABLE %s", table)
	row := master.QueryRow(query)

	var tableName, createTable string
	if err := row.Scan(&tableName, &createTable); err != nil {
		log.Fatalf("Failed to get create table query from master for table %s: %s", table, err)
		return
	}

	// Execute the table creation query on the slave
	if _, err := slave.Exec(createTable); err != nil {
		log.Printf("Could not create table %s on slave (it might already exist): %s", table, err)
	}
}

func syncTable(master, slave *sql.DB, table string) {
	ensureTableExists(master, slave, table)
	offset := 0
	for {
		query := fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", table, chunkSize, offset)
		rows, err := master.Query(query)
		if err != nil {
			log.Fatalf("Failed to query master: %s", err)
		}

		columns, err := rows.Columns()
		if err != nil {
			log.Fatalf("Failed to get columns: %s", err)
		}
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))

		batchInsert := make([]string, 0, chunkSize)
		for rows.Next() {
			for i := 0; i < len(columns); i++ {
				valuePtrs[i] = &values[i]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				log.Fatalf("Failed to scan row: %s", err)
			}

			valueStrings := make([]string, len(columns))
			for i, col := range values {
				if b, ok := col.([]byte); ok {
					// If the value is of type []byte, convert it to string
					valueStrings[i] = fmt.Sprintf("'%s'", string(b))
				} else {
					valueStrings[i] = fmt.Sprintf("'%v'", col)
				}
			}

			batchInsert = append(batchInsert, fmt.Sprintf("(%s)", join(valueStrings, ",")))
		}
		rows.Close()

		if len(batchInsert) == 0 {
			break
		}

		updateClause := []string{}
		for _, col := range columns {
			updateClause = append(updateClause, fmt.Sprintf("%s=VALUES(%s)", col, col))
		}

		insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s", table, join(columns, ","), join(batchInsert, ","), join(updateClause, ","))
		if _, err := slave.Exec(insertQuery); err != nil {
			log.Fatalf("Failed to insert into slave: %s", err)
		}

		if len(batchInsert) < chunkSize {
			break
		}
		offset += chunkSize
	}
}

func join(items []string, sep string) string {
	return strings.Join(items, sep)
}

