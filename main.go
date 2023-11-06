package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"trysql/cfa"
	"trysql/dbi"

	_ "github.com/mattn/go-sqlite3"
)

var DBContext dbi.DbCtx

func main() {

	/*
		db, err := sql.Open("sqlite3", ":memory:")

		if err != nil {
			log.Fatal(err)
		}

		defer db.Close()

		var version string
		err = db.QueryRow("SELECT SQLITE_VERSION()").Scan(&version)

		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(version) */

	DBContext.OpenDB("RBBNGH.db")

	//writeCFAConfig()
	var chainQ cfa.ChainQuery
	readCFAConfig(&chainQ)
	fmt.Printf("Read data:%v\n", chainQ)

	/* trial code
	   built_query := buildQuery(&chainQ)
	   readData(built_query)
	   trial sample
	*/

	buildRecursiveQuery(&chainQ)
}

func buildRecursiveQuery(rchainQ *cfa.ChainQuery) {

	hdrString := ""
	recString := ""

	if nil != rchainQ {
		//	fmt.Printf("^^^^^^^^^^^%v\n", rchainQ.GetDisplayFileds())
		initQuery := "select " + rchainQ.ParentQuery.DisplayFields + " from " + rchainQ.ParentQuery.InputTable + " where " + rchainQ.ParentQuery.Condition
		rchainQ.ParentQuery.SqlQueryStr = initQuery
		no_of_rows_query := "select count(*) " + " from " + rchainQ.ParentQuery.InputTable + " where " + rchainQ.ParentQuery.Condition
		fmt.Printf("%s\n", strings.Repeat("=", 80))
		fmt.Println("Parent query:", initQuery)
		NoOfRows := DBContext.GetRows(no_of_rows_query)
		fmt.Printf("No of Rows from Parent:%d\n", NoOfRows)
		fmt.Printf("%s\n", strings.Repeat("=", 80))

		runningQ := 0

		rows := DBContext.ExecuteQuery(initQuery)
		if rows == nil {
			log.Fatal("Execution failed")
		}
		defer rows.Close()

		columns, _ := rows.Columns()
		//columnTypes, _ := rows.ColumnTypes()

		count := len(columns)
		Values := make([]interface{}, count)
		ValuesPtr := make([]interface{}, count)
		rowCount := 0
		var colNames []string
		if len(colNames) == 0 {
			colNames = dbi.GetColumnNames(rows)
			//	fmt.Printf("Columns:%s\n", strings.Join(colNames, ","))
		}
		colStartCount := 0
		EmptyEntries := ""
		parentRecStr := ""
		for rows.Next() {
			rowCount++

			for i := range columns {
				ValuesPtr[i] = &Values[i]
			}
			//err = rows.Scan(&imsi, &call_id)
			err := rows.Scan(ValuesPtr...)

			if err != nil {
				fmt.Printf("Scan Error:%s\n", err)
				continue
			}
			imsiVal := ""

			for i, col := range columns {
				if rowCount == 1 {
					hdrString = hdrString + col + ","
				}
				//fmt.Printf("col:%s value:%v childlink:%s\n", col, Values[i], rchainQ.ParentQuery.ChildLink)
				//	fmt.Printf("col:%s value:%v \n", col, Values[i])
				//	recString = recString + convertToString(Values[i]) + ","
				parentRecStr = parentRecStr + convertToString(Values[i]) + ","
				if strings.ToUpper(strings.TrimSpace(col)) == strings.ToUpper(strings.TrimSpace(rchainQ.ParentQuery.ChildLink)) {
					/*if str, ok := Values[i].(string); ok {parentRecStr
						fmt.Printf("IMSI Matched\n")
						imsiVal = str
						break
					} else {
						fmt.Printf("string did not match:%T\n", Values[i])
					} */
					//imsiVal = strconv.FormatFloat(float64(Values[i]), 'f', 2, 64)

					imsiVal = convertToString(Values[i])

				}
			}
			//parentRecStr = parentRecStr
			recString = parentRecStr + "\n"
			//	fmt.Printf("=====>Record String:%s\n", recString)

			if rowCount == 1 {
				//	fmt.Printf("**********HeaderString:%s\n", hdrString)
				//emptyEntries = len(columns)
				colStartCount += len(columns)
				//fmt.Printf("col start:%d col end:%d\n", 0, colStartCount)
			}

			fmt.Printf("imsi:%s Row:%d/%d\n", imsiVal, rowCount, NoOfRows)

			if len(imsiVal) > 0 {
				EmptyEntries = imsiVal + strings.Repeat(",", colStartCount)
				//	fmt.Printf("^^^^^^^ %s\n", EmptyEntries)
				for _, genQ := range rchainQ.ChildQuery {
					nxtrowCount := 0
					runningQ++
					nextQuery := "select " + genQ.DisplayFields + " from " + genQ.InputTable + " where " + genQ.Condition + " and " + rchainQ.ParentQuery.ChildLink + " = " + imsiVal
					nextQrows := DBContext.ExecuteQuery(nextQuery)
					fmt.Println("		Child query:", nextQuery)

					if nextQrows == nil {
						log.Fatal("Query Execution failed")
					}

					nxtColumns, _ := nextQrows.Columns()
					nxtValues := make([]interface{}, len(nxtColumns))
					nxtValuesPtr := make([]interface{}, len(nxtColumns))

					for i := range nxtColumns {
						nxtValuesPtr[i] = &nxtValues[i]
					}
					var nextColNames []string
					if len(nextColNames) == 0 {
						nextColNames = dbi.GetColumnNames(nextQrows)
						//		fmt.Printf("		Next Columns:%s\n", strings.Join(nextColNames, ","))
					}
					nxtRowRecord := ""
					for nextQrows.Next() {
						nxtrowCount++
						nxtRowRecord = ""

						err := nextQrows.Scan(nxtValuesPtr...)

						if err != nil {
							fmt.Printf("Scan Error:%s\n", err)
							continue
						}

						for nxti, nxtCol := range nextColNames {
							//fmt.Printf("col:%s value:%v \n", nxtCol, nxtValues[nxti])
							if nxtrowCount == 1 {
								hdrString = hdrString + nxtCol + ","
							}
							//recString = recString + convertToString(nxtValues[nxti]) + ","
							nxtRowRecord = nxtRowRecord + convertToString(nxtValues[nxti]) + ","
							//recString = recString + EmptyEntries + convertToString(nxtValues[nxti]) + ","

						}
						//fmt.Printf("  =====>Next RecString:%s\n", EmptyEntries+nxtRowRecord)
						recString = recString + EmptyEntries + nxtRowRecord + "\n"
						if nxtrowCount == 1 {
							//	fmt.Printf("**********HeaderString:%s\n", hdrString)
							//	fmt.Printf("col start:%d col end:%d\n", colStartCount, colStartCount+len(nextColNames))
							colStartCount += len(nextColNames)
						}

					}
					EmptyEntries = imsiVal + strings.Repeat(",", colStartCount)
					fmt.Printf("%s\n", strings.Repeat("-", 80))
					nextQrows.Close()

				}
			}
		}

		fmt.Printf("%s\n", hdrString)
		//fmt.Printf("%s\n", recString)
		outFileCSV := "output.csv"
		outF, err := os.Create(outFileCSV)
		if err != nil {
			log.Fatal("Cannot open output file", outFileCSV)
		}
		defer outF.Close()
		hdrString = hdrString + "\n"
		recString = recString + "\n"
		_, _ = outF.WriteString(hdrString)
		_, _ = outF.WriteString(recString)

	}

}

func old_buildRecursiveQuery(rchainQ *cfa.ChainQuery) {

	if nil != rchainQ {
		initQuery := "select " + rchainQ.ParentQuery.DisplayFields + " from " + rchainQ.ParentQuery.InputTable + " where " + rchainQ.ParentQuery.Condition
		no_of_rows_query := "select count(*) " + " from " + rchainQ.ParentQuery.InputTable + " where " + rchainQ.ParentQuery.Condition

		fmt.Println("Parent query:", initQuery)

		fmt.Printf("No of Rows from Parent:%d\n", DBContext.GetRows(no_of_rows_query))
		//noChildQ := len(rchainQ.ChildQuery)
		runningQ := 0

		db, err := sql.Open("sqlite3", "RBBNGH.db")
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		rows, err := db.Query(initQuery)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		columns, _ := rows.Columns()
		//columnTypes, _ := rows.ColumnTypes()

		count := len(columns)
		Values := make([]interface{}, count)
		ValuesPtr := make([]interface{}, count)
		for rows.Next() {
			for i := range columns {
				ValuesPtr[i] = &Values[i]
			}
			//err = rows.Scan(&imsi, &call_id)
			err = rows.Scan(ValuesPtr...)

			if err != nil {
				fmt.Printf("Scan Error:%s\n", err)
				continue
			}
			imsiVal := ""
			for i, col := range columns {
				val := Values[i]

				b, ok := val.([]byte)
				var v interface{}
				if ok {
					v = string(b)
				} else {
					v = val
				}
				//fmt.Printf("col:%s value:%v\n", col, v)
				if col == "IMSI" {
					if str, ok := v.(string); ok {
						imsiVal = str
					}
				}
			}
			//fmt.Printf("imsi:%s callid:%s\n", imsi, call_id)

			for _, genQ := range rchainQ.ChildQuery {
				runningQ++

				nextQuery := "select " + genQ.DisplayFields + " from " + genQ.InputTable + " where " + genQ.Condition + " and " + rchainQ.ParentQuery.ChildLink + " = " + imsiVal
				fmt.Println("Child query:", nextQuery)

			}
		}

	}

}

func buildQuery(rchainQ *cfa.ChainQuery) string {

	initQuery := "select " + rchainQ.ParentQuery.DisplayFields + " from " + rchainQ.ParentQuery.InputTable + " where " + rchainQ.ParentQuery.Condition

	fmt.Printf("Query: %s\n", initQuery)
	return initQuery

}

func readCFAConfig(rchainQ *cfa.ChainQuery) {

	jsonQuery, err := ioutil.ReadFile("CfaConfig.json")
	if err != nil {
		log.Fatal(err)
	}

	json.Unmarshal(jsonQuery, &rchainQ)

}

func writeCFAConfig() {

	chainQ := &cfa.ChainQuery{}
	chainQ.ParentQuery.InputTable = "gh_volte_Call"
	chainQ.ParentQuery.DisplayFields = "imsi,erab_status,erab_release_cause"
	chainQ.ParentQuery.Condition = "erab_status=2"
	chainQ.ParentQuery.ChildLink = "imsi"

	gQ1 := cfa.GenQuery{}
	gQ1.InputTable = "rbbn_dce_volte"
	gQ1.DisplayFields = "DLMos, DLNoMedia,DroppedCall"
	gQ1.Condition = "DroppedCall=1"
	gQ1.Sequence = 1

	chainQ.ChildQuery = append(chainQ.ChildQuery, gQ1)

	gQ2 := cfa.GenQuery{}
	gQ2.InputTable = "rbbn_dce_s1mme_s1aperab"
	gQ2.DisplayFields = "CellLatitude,CellLongitude,ReleaseCause"
	gQ2.Condition = "ERABEstablished > 0"
	gQ2.Sequence = 2

	chainQ.ChildQuery = append(chainQ.ChildQuery, gQ2)

	//ChildQuery
	jsonQuery, err := json.MarshalIndent(chainQ, "", " ")
	if err != nil {
		log.Fatal(err)
	}

	err = ioutil.WriteFile("CfaConfig.json", jsonQuery, 0644)
	if err != nil {
		log.Fatal(err)
	}
}

func readData(sqlQuery string) {

	db, err := sql.Open("sqlite3", "RBBNGH.db")
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	//	rows, err := db.Query("SELECT imsi,call_id from gh_volte_Call")
	rows, err := db.Query(sqlQuery)
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	columns, _ := rows.Columns()
	columnTypes, _ := rows.ColumnTypes()
	fmt.Printf("Columns: %v\n Types:%#v\n", columns, columnTypes)

	for i := range columns {
		fmt.Printf("column:%s DBtype:%s DbName:%s\n", columns[i], columnTypes[i].DatabaseTypeName(), columnTypes[i].Name())
	}

	count := len(columns)
	Values := make([]interface{}, count)
	ValuesPtr := make([]interface{}, count)
	for rows.Next() {
		//fmt.Printf("Row")
		//var imsi, call_id string
		for i := range columns {
			ValuesPtr[i] = &Values[i]
		}
		//err = rows.Scan(&imsi, &call_id)
		err = rows.Scan(ValuesPtr...)

		if err != nil {
			fmt.Printf("Scan Error:%s\n", err)
			continue
		}
		for i, col := range columns {
			val := Values[i]

			b, ok := val.([]byte)
			var v interface{}
			if ok {
				v = string(b)
			} else {
				v = val
			}

			fmt.Printf("col:%s value:%v\n", col, v)
		}

		//fmt.Printf("imsi:%s callid:%s\n", imsi, call_id)
	}

}

func convertToString(data interface{}) string {
	switch data.(type) {
	case int:
		return strconv.Itoa(data.(int))
	case int64:
		return strconv.FormatInt(data.(int64), 10)
	case float32:
		return strconv.FormatFloat(float64(data.(float32)), 'f', 2, 32)
	case float64:
		return strconv.FormatFloat(data.(float64), 'f', 2, 64)
	case bool:
		return strconv.FormatBool(data.(bool))
	case string:
		return data.(string)
	default:
		fmt.Printf("%#v of data type:%T\n", data, data)
		/*case []interface{}:
		      var stringSlice []string
		      for _, item := range data.([]interface{}) {
		          stringSlice = append(stringSlice, convertToString(item))
		      }
		      return fmt.Sprintf("[%s]", fmt.Sprintf("%s, ", stringSlice...))
		  case map[interface{}]interface{}:
		      var stringMap []string
		      for key, value := range data.(map[interface{}]interface{}) {
		          stringMap = append(stringMap, fmt.Sprintf("%s:%s", convertToString(key), convertToString(value)))
		      }
		      return fmt.Sprintf("{%s}", fmt.Sprintf("%s, ", stringMap...))
		  default:
		      return fmt.Sprintf("%v", data) */
	}
	return ""
}
