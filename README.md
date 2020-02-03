# Collectors ToolKit

created by Viktor Safronov cafronoff@gmail.com

##Methods
* `Init(log *logrus.Entry)` - Assign the configured logger object
* `DetectInsertAndDeleteRows(apiRows []IRow, dbRows []IRow) (insertRows []IRow, deleteRows []IRow)` - Compares rows received from api with rows in the database. If there are new or changed rows in api, then returns them to the first variable(insertRows). Old rows that are in the database, but not in api, are returned to the second variable (deleteRows)
* `InsertRowsByChunk(log *logrus.Entry, db orm.DB, insertRows []IRow, chunkSize int) int` - Insert rows by chunk
* `InsertUpdateRowsByChunk(log *logrus.Entry, db orm.DB, insertRows []IRow, chunkSize int) int` - Insert rows by chunk with ON CONFLICT DO UPDATE by uniq key for structures implement of IRow
* `DeleteRowsByChunk(log *logrus.Entry, db orm.DB, deleteRows []IRow, chunkSize int) int` - Delete rows by chunk with uniq key for for structures implement of IRow
* `InsertRowsByChunkInCh(log *logrus.Entry, db *sql.DB, table string, insertRows []IRow, chunkSize int)` - Insert rows in Clickhouse by chunk with ON CONFLICT DO UPDATE by uniq key for structures implement of IRow
* `DeleteRowsByChunkInCh(log *logrus.Entry, db *sqlx.DB, table string, deleteRows []IRow, chunkSize int)` - Delete rows in Clickhouse by chunk with uniq key for for structures implement of IRow
* `DatesCheckOrSetFailResp(w http.ResponseWriter, dateFromStr string, dateToStr string) (dateFrom time.Time, dateTo time.Time, ok bool)` - Checks the start and end date and their order and returns them as time.Time. If an error occurs, returns json response.
* `SetSuccessResp(w http.ResponseWriter, r *http.Request)` - Returning success json response
* `SetFailedRespByDbResult(w http.ResponseWriter, err error, nowRowsMsg string)` - Returning server failed json response by db result
* `StartOfDay(date time.Time) time.Time` - Convert datetime to start of day, for example 2019-02-20 12:12:12 -> 2019-02-20 00:00:00
* `Today() time.Time` - Return today time
* `Yesterday() time.Time` - Return yesterday time
* `DayBeforeYesterday() time.Time` - Return the day before yesterday time
* `WeekAgo() time.Time` - Return week ago time
* `TwoWeekAgo() time.Time` - Return two week ago time
* `ThreeWeekAgo() time.Time` - Return three week ago time
* `MonthAgo() time.Time` - Return month ago time
* `TwoMonthAgo() time.Time` - Return two month ago time
* `QuarterAgo() time.Time` - Return quarter ago time
* `HalfYearAgo() time.Time` - Return half year ago time
* `YearAgo() time.Time` - Return year ago time
* `DatePeriodStrToTime(dateFromStr string, dateToStr string) (time.Time, time.Time)` - Converting dateFrom and dateTo strings [YYYY-MM-DD|today|yesterday|day-before-yesterday|week-ago|two-week-ago|three-week-ago|month-ago|two-month-ago|quarter-ago|half-year-ago|year-ago] to time.Time 
* `DateConstToTime(dateConst string) (time.Time, error)` - Converting date constant [today|yesterday|day-before-yesterday|week-ago|two-week-ago|three-week-ago|month-ago|two-month-ago|quarter-ago|half-year-ago|year-ago] to time.Time 
* `ToDate(date time.Time) string` - Converting time.Time to date string YYYY-MM-DD
* `DateStrToTime(dateStr string) (time.Time, error` - Converting date string [YYYY-MM-DD|today|yesterday|day-before-yesterday|week-ago|two-week-ago|three-week-ago|month-ago|two-month-ago|quarter-ago|half-year-ago|year-ago] to time.Time
* `DateToTime(dateStr string) (time.Time, error` - Converting date string [YYYY-MM-DD] to time.Time 
* `ExpandLog(logrus *logrus.Entry, runFromApi bool, taskName string, dateFrom time.Time, dateTo time.Time, otherParams string) *logrus.Entry` - Add info to log about task name and task params   
* `Recover()` - Catch panic and write to error log instead it
* `LockKey(params ...string) string` - Get key for lock by parameters   
* `Lock(params ...string) bool` - Try to hang the lock by prameters key. Return true if lock isn't exist, else false
* `Unlock(params ...string) error` - Unlock by prameters key after lock

##Usage
```go
package main
import (
    cltk "gitlab.mobio.ru/go-packages/collector-toolkit"
    "github.com/sirupsen/logrus"
)
logger := logrus.New()
cltk.Toolkit().Init(logger)
halfYearAgoTime, todayAgoTime := Toolkit().GetDatePeriodByConsts(cltk.HalfYearAgo, cltk.Today)
```
##Dependencies
* `github.com/go-pg/pg` - PostgresSQL ORM
* `github.com/sirupsen/logrus`  - Logger with hooks
