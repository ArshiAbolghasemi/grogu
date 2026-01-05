package call

import (
	"time"

	"github.com/goccy/go-json"
)

type TelCCallRecord struct {
	CallID        string          `gorm:"column:call_id"         json:"call_id"`
	AccessCode    string          `gorm:"column:access_code"     json:"access_code"`
	CityCode      string          `gorm:"column:city_code"       json:"city_code"`
	ServiceNo     string          `gorm:"column:service_no"      json:"service_no"`
	CallStartDate *time.Time      `gorm:"column:call_start_date" json:"call_start_date"`
	CallEndDate   *time.Time      `gorm:"column:call_end_date"   json:"call_end_date"`
	CallTo        string          `gorm:"column:call_to"         json:"call_to"`
	Duration      int             `gorm:"column:duration"        json:"duration"`
	Reasons       json.RawMessage `gorm:"column:reasons"         json:"reasons"`
	CreatedAt     *time.Time      `gorm:"column:created_at"      json:"created_at"`
}

func (TelCCallRecord) TableName() string {
	return "call_records"
}

const (
	FileWithIssueDurtion = -1
)

type Call struct {
	CallID    string     `gorm:"column:call_id"    json:"call_id"`
	Status    string     `gorm:"column:status"     json:"status"`
	CreatedAt *time.Time `gorm:"column:created_at" json:"created_at"`
}

func (Call) TableName() string {
	return "calls"
}

const (
	StatusQueued     = "queued"
	StatusInProgress = "in_progress"
	StatusSucceed    = "succeed"
	StatusFailed     = "failed"
)
