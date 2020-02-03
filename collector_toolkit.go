package collector_toolkit

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

const (
	Today              = "today"
	Yesterday          = "yesterday"
	DayBeforeYesterday = "day-before-yesterday"
	WeekAgo            = "week-ago"
	TwoWeekAgo         = "two-week-ago"
	ThreeWeekAgo       = "three-week-ago"
	MonthAgo           = "month-ago"
	TwoMonthAgo        = "two-month-ago"
	QuarterAgo         = "quarter-ago"
	HalfYearAgo        = "half-year-ago"
	YearAgo            = "year-ago"
)

var (
	tkOnce sync.Once
	tk     *toolkit
)

type WebResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

type IRow interface {
	UpdateFields() []string
	UniqFields() []string
	Struct() interface{}
}

type toolkit struct {
	logrus          *logrus.Entry
	lockMutex       sync.Mutex
	lockMapMutex    map[string]bool
	queueMutex      sync.Mutex
	queueMapMutex   map[string]*sync.Mutex
	queueMapCounter map[string]int
}

func Toolkit() *toolkit {
	tkOnce.Do(func() {
		tk = &toolkit{
			lockMapMutex:    make(map[string]bool),
			queueMapMutex:   make(map[string]*sync.Mutex),
			queueMapCounter: make(map[string]int),
		}
	})
	return tk
}

func (tk toolkit) TryToStartTask(log *logrus.Entry, skipKey string, queueKey string) bool {
	if !tk.Lock(skipKey) {
		log.Infof("Skip run by key %s", skipKey)
		return false
	}

	log.Infof("Add task to queue by key: %s", queueKey)
	tk.QueueUpAndWait(queueKey)

	log.Info("Run task")
	return true
}

func (tk toolkit) FinishTask(log *logrus.Entry, skipKey string, queueKey string) {
	tk.Unlock(log, skipKey)
	tk.QuitQueue(log, queueKey)
}

// Get key by parameters
func (tk toolkit) ParamsToKey(params ...string) string {
	var key bytes.Buffer

	for _, param := range params {
		key.WriteString(param)
	}

	return key.String()
}

// Try to hang the lock by parameters key. Return true if lock isn't exist, else false
func (tk *toolkit) Lock(key string) bool {
	tk.lockMutex.Lock()
	defer tk.lockMutex.Unlock()

	if _, ok := tk.lockMapMutex[key]; ok {
		return false
	}
	tk.lockMapMutex[key] = true
	return true
}

//Unlock by parameters key after lock
func (tk *toolkit) Unlock(log *logrus.Entry, key string) {
	tk.lockMutex.Lock()
	defer tk.lockMutex.Unlock()

	if _, ok := tk.lockMapMutex[key]; !ok {
		log.Panic("Not exist key for unlock: " + key)
	}
	delete(tk.lockMapMutex, key)
}

func (tk toolkit) QueueUpAndWait(key string) {
	tk.queueMutex.Lock()
	if _, ok := tk.queueMapMutex[key]; !ok {
		tk.queueMapMutex[key] = &sync.Mutex{}
		tk.queueMapCounter[key] = 0
	}
	tk.queueMapCounter[key]++
	tk.queueMutex.Unlock()
	mutex := tk.queueMapMutex[key]
	mutex.Lock()
}

func (tk toolkit) QuitQueue(log *logrus.Entry, key string) {
	tk.queueMutex.Lock()
	defer tk.queueMutex.Unlock()

	if _, ok := tk.queueMapMutex[key]; !ok {
		log.Panic("Not exist key for queue:" + key)
	}
	mutex := tk.queueMapMutex[key]
	mutex.Unlock()

	tk.queueMapCounter[key]--
	if tk.queueMapCounter[key] == 0 {
		delete(tk.queueMapMutex, key)
	}
}

// Returning success json response
func (tk toolkit) SetSuccessResp(log *logrus.Entry, w http.ResponseWriter, r *http.Request) {
	log.Infof("Add task to queue: %s", r.URL.RequestURI())
	tk.SetJsonResp(log, w, true, "Add task to queue successfully!", false)
}

// Load source row by source name without case sensetive from db
func (tk toolkit) FindSourceByName(db orm.DB, sourceRow interface{}, sourceName string) error {
	return db.Model(sourceRow).Where("lower(name) = lower(?)", sourceName).Select()
}

// Load source row by source name from db. If an error occurs, returns json response.
func (tk toolkit) LoadSourceOrSetFailResp(log *logrus.Entry, w http.ResponseWriter, db orm.DB, sourceName string, sourceRow interface{}) (ok bool) {
	err := tk.FindSourceByName(db, sourceRow, sourceName)

	if err == nil {
		return true
	}

	if err == pg.ErrNoRows {
		tk.SetJsonResp(log, w, false, "Source not found: "+sourceName, false)
		return false
	}

	tk.SetJsonResp(log, w, false, err.Error(), true)
	log.Error(err)
	return false
}

// Checks the start and end date and their order and returns them as time.Time. If an error occurs, returns json response.
func (tk toolkit) DatesCheckOrSetFailResp(log *logrus.Entry, w http.ResponseWriter, dateFromStr, dateToStr string, dateFrom, dateTo *time.Time) (ok bool) {
	*dateFrom, ok = tk.parseDateOrSetFailResp(log, w, dateFromStr, "from")
	if !ok {
		return
	}

	*dateTo, ok = tk.parseDateOrSetFailResp(log, w, dateToStr, "to")
	if !ok {
		return
	}

	return tk.datesOrderCheckOrSetFailResp(log, w, *dateFrom, *dateTo)
}

func (tk toolkit) SetJsonResp(log *logrus.Entry, w http.ResponseWriter, success bool, message string, isServerError bool) {
	wr, err := json.Marshal(WebResponse{
		Success: success,
		Message: message,
	})

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Panic(err)
	}

	if success {
		w.WriteHeader(http.StatusOK)
	} else if isServerError {
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusBadRequest)
	}

	if _, err := fmt.Fprintf(w, string(wr)); err != nil {
		log.Panic(err)
	}
}

func (tk toolkit) parseDateOrSetFailResp(log *logrus.Entry, w http.ResponseWriter, dateStr string, dateType string) (date time.Time, ok bool) {
	date, err := tk.DateStrToTime(log, dateStr)

	if err != nil {
		tk.SetJsonResp(log, w, false, "{date-"+dateType+"} is incorrect: "+dateStr, false)
		return date, false
	}
	return date, true
}

func (tk toolkit) datesOrderCheckOrSetFailResp(log *logrus.Entry, w http.ResponseWriter, dateFrom, dateTo time.Time) (ok bool) {
	if dateFrom.After(dateTo) {
		tk.SetJsonResp(log, w, false, "{date-from} url param more than {date-to} url param", false)
		return false
	}
	return true
}

func (tk toolkit) genKeyByFieldsFromRow(row IRow, rowFields []string) string {
	vo := reflect.ValueOf(row.Struct())
	var buffer bytes.Buffer
	for _, field := range rowFields {
		buffer.WriteString("|")
		buffer.WriteString(fmt.Sprintf("%+v", tk.getValueFromStructField(vo, field)))
	}
	return buffer.String()
}

func (tk toolkit) getUpdateKeyFromRow(row IRow) string {
	return tk.genKeyByFieldsFromRow(row, row.UpdateFields())
}

func (tk toolkit) getUniqKeyFromRow(row IRow) string {
	return tk.genKeyByFieldsFromRow(row, row.UniqFields())
}

func (tk toolkit) getUniqDbColumnsFromRow(log *logrus.Entry, row IRow) []string {
	return tk.getDbColumnsFromRow(log, row, row.UniqFields())
}

func (tk toolkit) getUpdateDbColumnsFromRow(log *logrus.Entry, row IRow) []string {
	return tk.getDbColumnsFromRow(log, row, row.UpdateFields())
}

func (tk toolkit) getDbColumnsFromRow(log *logrus.Entry, row IRow, rowFields []string) (columns []string) {
	to := reflect.TypeOf(row.Struct())
	for _, field := range rowFields {
		columns = append(columns, tk.getDbColumnFromStructField(log, to, field))
	}
	return columns
}

func (tk toolkit) getUniqDbCol2ValFromRow(log *logrus.Entry, row IRow) map[string]interface{} {
	return tk.getDbCol2ValFromRow(log, row, row.UniqFields())
}

func (tk toolkit) getAllCol2ValFromRow(log *logrus.Entry, row IRow) map[string]interface{} {
	return tk.getDbCol2ValFromRow(log, row, append(row.UniqFields(), row.UpdateFields()...))
}

func (tk toolkit) getDbCol2ValFromRow(log *logrus.Entry, row IRow, rowFields []string) (dbCol2Val map[string]interface{}) {
	to := reflect.TypeOf(row.Struct())
	vo := reflect.ValueOf(row.Struct())
	dbCol2Val = make(map[string]interface{})
	for _, field := range rowFields {
		dbCol2Val[tk.getDbColumnFromStructField(log, to, field)] = tk.getValueFromStructField(vo, field)
	}
	return dbCol2Val
}

func (tk toolkit) getValueFromStructField(vo reflect.Value, field string) interface{} {
	return vo.FieldByName(field).Interface()
}

func (tk toolkit) getDbColumnFromStructField(log *logrus.Entry, to reflect.Type, field string) string {
	meta, ok := to.FieldByName(field)
	if !ok {
		log.Panic("Can not find feild: " + field)
	}
	sqlParts := strings.Split(meta.Tag.Get("sql"), ",")
	return sqlParts[0]
}

func (tk toolkit) ParseInt(log *logrus.Entry, strVal string) uint64 {
	if strVal == "" {
		return 0
	}

	intVal, err := strconv.ParseUint(strVal, 10, 64)
	if err != nil {
		log.Panic(err)
	}
	return intVal
}

func (tk toolkit) ParseFloat(log *logrus.Entry, strVal string) float64 {
	if strVal == "" {
		return 0
	}

	floarVal, err := strconv.ParseFloat(strVal, 64)
	if err != nil {
		log.Panic(err)
	}
	return floarVal
}

func (tk toolkit) ExpandLog(logrus *logrus.Entry, runFromApi bool, taskName string, source string, dateFrom, dateTo time.Time, otherParams string) *logrus.Entry {
	var buf bytes.Buffer
	if runFromApi {
		buf.WriteString("API:")
	} else {
		buf.WriteString("CRON:")
	}

	buf.WriteString(taskName)

	if source != "" {
		buf.WriteString(":")
		buf.WriteString(source)
	}

	zeroTime := time.Time{}
	if dateFrom != zeroTime {
		buf.WriteString(":")
		buf.WriteString(tk.ToDate(dateFrom))
	}
	if dateTo != zeroTime {
		buf.WriteString(":")
		buf.WriteString(tk.ToDate(dateTo))
	}

	if otherParams != "" {
		buf.WriteString(":")
		buf.WriteString(otherParams)
	}

	return logrus.WithField("task", buf.String())
}

// Compares rows received from api with rows in the database.
// If there are new or changed rows in api, then returns them to the first variable(insertRows).
// Old rows that are in the database, but not in api, are returned to the second variable (deleteRows)
func (tk toolkit) DetectInsertAndDeleteRows(apiRows []IRow, dbRows []IRow) (insertRows []IRow, deleteRows []IRow) {
	uniqKeyToApiRow := make(map[string]IRow)
	uniqKeyToDbRow := make(map[string]IRow)

	for _, row := range apiRows {
		uniqKeyToApiRow[tk.getUniqKeyFromRow(row)] = row
	}
	for _, row := range dbRows {
		uniqKeyToDbRow[tk.getUniqKeyFromRow(row)] = row
	}

	for key, apiRow := range uniqKeyToApiRow {
		if dbRow, ok := uniqKeyToDbRow[key]; ok {
			if tk.getUpdateKeyFromRow(dbRow) != tk.getUpdateKeyFromRow(apiRow) {
				insertRows = append(insertRows, apiRow)
			}
		} else {
			insertRows = append(insertRows, apiRow)
		}
	}

	for key, dbRow := range uniqKeyToDbRow {
		if _, ok := uniqKeyToApiRow[key]; !ok {
			deleteRows = append(deleteRows, dbRow)
		}
	}
	return insertRows, deleteRows
}

func (tk toolkit) GetValAndDel(key2val map[string]string, key string) string {
	val := key2val[key]
	if val == "N/A" {
		val = ""
	}
	delete(key2val, key)
	return val
}

//Insert rows by chunk with ON CONFLICT DO UPDATE by uniq key for structures implement of IRow
func (tk toolkit) InsertUpdateRowsByChunk(log *logrus.Entry, db orm.DB, insertRows []IRow, chunkSize int) int {
	var insertRowsChunk []IRow
	affectedNum := 0
	for _, row := range insertRows {
		if len(insertRowsChunk) == chunkSize {
			affectedNum += tk.insertUpdateRows(log, db, insertRowsChunk)
			insertRowsChunk = nil
		}
		insertRowsChunk = append(insertRowsChunk, row)
	}
	affectedNum += tk.insertUpdateRows(log, db, insertRowsChunk)
	return affectedNum
}

func (tk toolkit) insertUpdateRows(log *logrus.Entry, db orm.DB, insertRowsChunk []IRow) int {
	if len(insertRowsChunk) == 0 {
		return 0
	}

	var onConflictBuffer bytes.Buffer
	onConflictBuffer.WriteString("(")
	for i, column := range tk.getUniqDbColumnsFromRow(log, insertRowsChunk[0]) {
		if i > 0 {
			onConflictBuffer.WriteString(",")
		}
		onConflictBuffer.WriteString(column)
	}
	onConflictBuffer.WriteString(") DO UPDATE")
	query := db.Model(&insertRowsChunk).OnConflict(onConflictBuffer.String())
	for _, column := range tk.getUpdateDbColumnsFromRow(log, insertRowsChunk[0]) {
		var set bytes.Buffer
		set.WriteString(column)
		set.WriteString(" = EXCLUDED.")
		set.WriteString(column)
		query.Set(set.String())
	}
	res, err := query.Insert()
	if err != nil {
		log.Panic(err)
	}
	return res.RowsAffected()
}

// Insert rows by chunk
func (tk toolkit) InsertRowsByChunk(log *logrus.Entry, db orm.DB, insertRows []IRow, chunkSize int) int {
	var insertRowsChunk []IRow
	affectedNum := 0
	for _, row := range insertRows {
		if len(insertRowsChunk) == chunkSize {
			affectedNum += tk.insertRows(log, db, insertRowsChunk)
			insertRowsChunk = nil
		}
		insertRowsChunk = append(insertRowsChunk, row)
	}
	affectedNum += tk.insertRows(log, db, insertRowsChunk)
	return affectedNum
}

func (tk toolkit) insertRows(log *logrus.Entry, db orm.DB, insertRows []IRow) int {
	if len(insertRows) == 0 {
		return 0
	}
	query := db.Model(&insertRows)

	res, err := query.Insert()
	if err != nil {
		log.Panic(err)
	}
	return res.RowsAffected()
}

// Delete rows by uniq key for for structures implement of IRow
func (tk toolkit) DeleteRowsByChunk(log *logrus.Entry, db orm.DB, deleteRows []IRow, chunkSize int) int {
	var delRowsChunk []IRow
	affectedNum := 0
	for _, row := range deleteRows {
		if len(delRowsChunk) == chunkSize {
			affectedNum += tk.deleteRows(log, db, delRowsChunk)
			delRowsChunk = nil
		}
		delRowsChunk = append(delRowsChunk, row)
	}
	affectedNum += tk.deleteRows(log, db, delRowsChunk)
	return affectedNum
}

func (tk toolkit) deleteRows(log *logrus.Entry, db orm.DB, deleteRows []IRow) int {
	if len(deleteRows) == 0 {
		return 0
	}

	query := db.Model(deleteRows[0])

	condition, values := tk.buildGroupInCondition(log, deleteRows)

	query.Where(condition, values...)
	res, err := query.Delete()

	if err != nil {
		log.Panic(err)
	}

	return res.RowsAffected()
}

func (tk toolkit) buildGroupInCondition(log *logrus.Entry, deleteRows []IRow) (condition string, values []interface{}) {
	var columns []string
	var condBuffer bytes.Buffer
	for _, row := range deleteRows {
		col2val := tk.getUniqDbCol2ValFromRow(log, row)

		if condBuffer.Len() == 0 {
			condBuffer.WriteString("(")
			for column := range col2val {
				if len(columns) > 0 {
					condBuffer.WriteString(",")
				}
				columns = append(columns, column)
				condBuffer.WriteString(column)
			}
			condBuffer.WriteString(") IN (")
		} else {
			condBuffer.WriteString(",")
		}

		condBuffer.WriteString("(")
		for i, column := range columns {
			if i > 0 {
				condBuffer.WriteString(",")
			}
			condBuffer.WriteString("?")
			values = append(values, col2val[column])
		}
		condBuffer.WriteString(")")
	}
	condBuffer.WriteString(")")
	return condBuffer.String(), values
}

// Delete rows in Clickhouse by chunk with uniq key for for structures implement of IRow
func (tk toolkit) DeleteRowsByChunkInCh(log *logrus.Entry, db *sqlx.DB, table string, deleteRows []IRow, chunkSize int) {
	var delRowsChunk []IRow
	for _, row := range deleteRows {
		if len(delRowsChunk) == chunkSize {
			tk.deleteRowsInCh(log, db, table, delRowsChunk)
			delRowsChunk = nil
		}
		delRowsChunk = append(delRowsChunk, row)
	}
	tk.deleteRowsInCh(log, db, table, delRowsChunk)
}

func (tk toolkit) deleteRowsInCh(log *logrus.Entry, db *sqlx.DB, tableName string, deleteRows []IRow) {
	rowsNum := len(deleteRows)
	if rowsNum == 0 {
		return
	}

	condition, values := tk.buildGroupInCondition(log, deleteRows)

	_, err := db.Exec(
		`ALTER TABLE `+tableName+` DELETE WHERE `+condition,
		values...,
	)
	if err != nil {
		log.Panic(err)
	}
}

// Insert rows in Clickhouse by chunk with ON CONFLICT DO UPDATE by uniq key for structures implement of IRow
func (tk toolkit) InsertRowsByChunkInCh(log *logrus.Entry, db *sql.DB, table string, insertRows []IRow, chunkSize int) {
	var insertRowsChunk []IRow
	for _, row := range insertRows {
		if len(insertRowsChunk) == chunkSize {
			tk.insertRowsInCh(log, db, table, insertRowsChunk)
			insertRowsChunk = nil
		}
		insertRowsChunk = append(insertRowsChunk, row)
	}
	tk.insertRowsInCh(log, db, table, insertRowsChunk)
}

func (tk toolkit) insertRowsInCh(log *logrus.Entry, db *sql.DB, tableName string, insertRows []IRow) {
	if len(insertRows) == 0 {
		return
	}

	var query string
	var stmt *sql.Stmt
	var tx *sql.Tx
	var columns []string
	var err error
	for i, row := range insertRows {
		col2val := tk.getAllCol2ValFromRow(log, row)
		if i == 0 {
			var columnsPart bytes.Buffer
			var palceholdersPart bytes.Buffer
			for column := range col2val {
				if columnsPart.Len() != 0 {
					columnsPart.WriteString(",")
					palceholdersPart.WriteString(",")
				}
				columnsPart.WriteString(column)
				palceholdersPart.WriteString("?")
				columns = append(columns, column)
			}

			//vs: this is multi insert
			query = `INSERT INTO ` + tableName + ` (` + columnsPart.String() + `) VALUES (` + palceholdersPart.String() + `)`
			tx, err = db.Begin()
			if err != nil {
				log.Panic(err)
			}

			stmt, err = tx.Prepare(query)
			if err != nil {
				_ = tx.Rollback()
				log.Panic(err)
			}

		}

		var values []interface{}
		for _, column := range columns {
			values = append(values, col2val[column])
		}

		_, err := stmt.Exec(values...)
		if err != nil {
			log.Panic(err)
		}

	}

	if err = tx.Commit(); err != nil {
		log.Panic(err)
	}
}

// Converting time.Time to date string YYYY-MM-DD
func (tk toolkit) ToDate(date time.Time) string {
	return date.Format("2006-01-02")
}

//Convert datetime to start of day, for example 2019-02-20 12:12:12 -> 2019-02-20 00:00:00
func (tk toolkit) StartOfDay(log *logrus.Entry, date time.Time) time.Time {
	date, err := time.Parse("2006-01-02", tk.ToDate(date))
	if err != nil {
		log.Panic(err)
	}
	return date
}

func (tk toolkit) Today(log *logrus.Entry) time.Time {
	return tk.StartOfDay(log, time.Now())
}

func (tk toolkit) Yesterday(log *logrus.Entry) time.Time {
	return tk.StartOfDay(log, tk.Today(log).AddDate(0, 0, -1))
}

func (tk toolkit) DayBeforeYesterday(log *logrus.Entry) time.Time {
	return tk.StartOfDay(log, tk.Today(log).AddDate(0, 0, -2))
}

func (tk toolkit) WeekAgo(log *logrus.Entry) time.Time {
	return tk.StartOfDay(log, tk.Today(log).AddDate(0, 0, -7))
}

func (tk toolkit) TwoWeekAgo(log *logrus.Entry) time.Time {
	return tk.StartOfDay(log, tk.Today(log).AddDate(0, 0, -14))
}

func (tk toolkit) ThreeWeekAgo(log *logrus.Entry) time.Time {
	return tk.StartOfDay(log, tk.Today(log).AddDate(0, 0, -21))
}

func (tk toolkit) MonthAgo(log *logrus.Entry) time.Time {
	return tk.StartOfDay(log, tk.Today(log).AddDate(0, -1, 0))
}

func (tk toolkit) TwoMonthAgo(log *logrus.Entry) time.Time {
	return tk.StartOfDay(log, tk.Today(log).AddDate(0, -2, 0))
}

func (tk toolkit) QuarterAgo(log *logrus.Entry) time.Time {
	return tk.StartOfDay(log, tk.Today(log).AddDate(0, -3, 0))
}

func (tk toolkit) HalfYearAgo(log *logrus.Entry) time.Time {
	return tk.StartOfDay(log, tk.Today(log).AddDate(0, -6, 0))
}

func (tk toolkit) YearAgo(log *logrus.Entry) time.Time {
	return tk.StartOfDay(log, tk.Today(log).AddDate(0, -12, 0))
}

func (tk toolkit) DateConstToTime(log *logrus.Entry, dateConst string) (time.Time, error) {
	switch dateConst {
	case Today:
		return tk.Today(log), nil
	case Yesterday:
		return tk.Yesterday(log), nil
	case DayBeforeYesterday:
		return tk.DayBeforeYesterday(log), nil
	case WeekAgo:
		return tk.WeekAgo(log), nil
	case TwoWeekAgo:
		return tk.TwoWeekAgo(log), nil
	case ThreeWeekAgo:
		return tk.ThreeWeekAgo(log), nil
	case MonthAgo:
		return tk.MonthAgo(log), nil
	case TwoMonthAgo:
		return tk.TwoMonthAgo(log), nil
	case QuarterAgo:
		return tk.QuarterAgo(log), nil
	case HalfYearAgo:
		return tk.HalfYearAgo(log), nil
	case YearAgo:
		return tk.YearAgo(log), nil
	}
	return time.Time{}, errors.New("date const does not exist")
}

func (tk toolkit) DatePeriodStrToTime(log *logrus.Entry, dateFromStr, dateToStr string) (time.Time, time.Time) {
	dateFrom, err := tk.DateStrToTime(log, dateFromStr)
	if err != nil {
		log.Panic(err)
	}
	dateTo, err := tk.DateStrToTime(log, dateToStr)
	if err != nil {
		log.Panic(err)
	}

	if dateFrom.After(dateTo) {
		log.Panic(errors.New("{date-from} more than {date-to}"))
	}

	return dateFrom, dateTo
}

func (tk toolkit) DateStrToTime(log *logrus.Entry, dateStr string) (time.Time, error) {
	date, err := tk.DateConstToTime(log, dateStr)
	if err != nil {
		date, err = time.Parse("2006-01-02", dateStr)
	}
	return date, err
}

func (tk toolkit) DateToTime(log *logrus.Entry, dateStr string) time.Time {
	date, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		log.Panic(err)
	}
	return date
}

func (tk toolkit) Recover(log *logrus.Entry) {
	if r := recover(); r != nil {
		var msg string
		switch x := r.(type) {
		case string:
			msg = x
		case error:
			msg = x.Error()
		case *logrus.Entry:
			msg, _ = x.String()
		default:
			msg = fmt.Sprintf("unknown error '%s'", x)
		}
		log.WithField("stack", string(debug.Stack())).Errorf("recovered panic %s", msg)
	}
}
