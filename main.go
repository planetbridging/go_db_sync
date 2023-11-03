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

	var masterDb *sql.DB
	var slaveDb *sql.DB


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

	// Check if SSL is required
	if os.Getenv("DB_SSL1") != "" {
		// Connect to the DB with TLS
		masterDb, err = ConnectToDB(configMaster)
		if err != nil {
			log.Fatalf("Failed to connect to master: %s", err)
		}
	} else {
		
		// Connect to the DB without TLS
		masterDb, err = normalConnectToDB(configMaster)
		if err != nil {
			log.Fatalf("Failed to connect to master: %s", err)
		}
	}

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

	//fmt.Println(configSlave)

	// Check if SSL is required
	if os.Getenv("DB_SSL2") != "" {
		// Connect to the DB with TLS
		slaveDb, err = ConnectToDB(configSlave)
		if err != nil {
			log.Fatalf("Failed to connect to slave: %s", err)
		}
	} else {
		
		// Connect to the DB without TLS
		slaveDb, err = normalConnectToDB(configSlave)
		if err != nil {
			log.Fatalf("Failed to connect to slave: %s", err)
		}
	}


	
	defer slaveDb.Close()

	tables := getTables(masterDb)
	for _, table := range tables {
		isView, err := isView(masterDb, os.Getenv("DB_NAME1"), table)
		if err != nil {
			log.Printf("Failed to check if %s is a view: %s", table, err)
			continue // Skip to the next table instead of stopping the entire process
		}
		if isView {
			log.Printf("%s is a view. Skipping...", table)
			continue // Skip to the next table
		}

		syncTable(masterDb, slaveDb, table)
	}

}

func isView(db *sql.DB, dbName, tableName string) (bool, error) {
    query := `
    SELECT TABLE_NAME 
    FROM information_schema.VIEWS 
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?;
    `
    var result string
    err := db.QueryRow(query, dbName, tableName).Scan(&result)
    if err != nil {
        if err == sql.ErrNoRows {
            return false, nil // Not a view
        }
        return false, err
    }
    return true, nil // Is a view
}


func ConnectToDB(config DBConfig) (*sql.DB, error) {
    // Register a custom TLS config that skips server's certificate verification.
    mysql.RegisterTLSConfig("custom", &tls.Config{
        InsecureSkipVerify: true,
    })

    // Include the DBName in the DSN
    dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&tls=custom", config.Username, config.Password, config.Hostname, config.DBName)

    db, err := sql.Open("mysql", dsn)
    if err != nil {
        return nil, err
    }

    err = db.Ping()
    if err != nil {
        return nil, err
    }

    return db, nil
}



func normalConnectToDB(config DBConfig) (*sql.DB, error) {
	// Create the Data Source Name (DSN), without custom TLS config
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", config.Username, config.Password, config.Hostname, config.DBName)

	// Open a new connection to the database
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	// Ping the database to verify connection establishment
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

