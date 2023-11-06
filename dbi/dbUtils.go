package dbi

import (
	"database/sql"
	"log"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

type DbCtx struct {
	DbName   string
	dbHandle *sql.DB
}

func (db *DbCtx) OpenDB(dbName string) {
	if db != nil {
		if len(dbName) > 0 {
			var err error
			db.dbHandle, err = sql.Open("sqlite3", dbName)

			if err != nil {
				log.Fatal(err)
			}

		}
	}
}

func (db *DbCtx) CloseDB() {
	if db != nil {
		db.dbHandle.Close()
	}

}

/*func (db *DbCtx) SetDBContext(dbc *sql.DB) {
	db.dbHandle = dbc
}*/

func (db *DbCtx) GetRows(qStr string) int {
	count := 0
	if db != nil {
		if db.dbHandle != nil {
			rows, err := db.dbHandle.Query(qStr)
			if err != nil {
				log.Fatal(err)
			}
			defer rows.Close()
			for rows.Next() {
				err = rows.Scan(&count)
				if err != nil {
					log.Fatal(err)
				}
				return count
			}
		}
	}
	return count
}

// onus of closing rows is on caller
func (db *DbCtx) ExecuteQuery(qStr string) *sql.Rows {

	if db != nil && len(qStr) > 0 {
		if db.dbHandle != nil {
			rows, err := db.dbHandle.Query(qStr)
			if err != nil {
				log.Fatal(err)
			}
			return rows
		}
	}
	return nil
}

func GetColumnNames(rows *sql.Rows) []string {
	var colNames []string
	if rows != nil {
		columns, _ := rows.Columns()
		for _, cn := range columns {
			colNames = append(colNames, strings.ToUpper(cn))
		}
	}
	return colNames
}

/*
Donot use this function in production. This is for debug only
*/
func GetColNameValuePair(rows *sql.Rows) map[string]interface{} {
	outMap := make(map[string]interface{})

	if rows != nil {
		columns, _ := rows.Columns()
		noOfCols := len(columns)
		Values := make([]interface{}, noOfCols)
		ValuesPtr := make([]interface{}, noOfCols)

		for i := range columns {
			ValuesPtr[i] = &Values[i]
		}

		for i, col := range columns {
			outMap[col] = Values[i]
			/*	val := Values[i]
				b, ok := val.([]byte)
				var v interface{}
				if ok {
					v = string(b)
				} else {
					v = val
				} */
			//fmt.Printf("ColName:%s, ColValue:%#v",Values[i])

		}
	}
	return outMap
}
