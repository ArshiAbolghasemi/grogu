package deadletter

import (
	"time"

	"gorm.io/datatypes"
)

type CallASRDeadLetter struct {
	CallID      string         `gorm:"column:call_id;type:varchar(255);primaryKey;not null"`
	Msg         datatypes.JSON `gorm:"column:msg;type:jsonb;not null"`
	Error       string         `gorm:"column:error;type:text;not null"`
	Status      string         `gorm:"column:status;type:varchar(20);default:'pending';not null"`
	RetryCount  int            `gorm:"column:retry_count;type:int;default:0;not null"`
	LastRetryAt *time.Time     `gorm:"column:last_retry_at;type:timestamp"`
	CreatedAt   time.Time      `gorm:"column:created_at;autoCreateTime"`
}

const (
	StatusPending    = "pending"
	StatusInProgress = "in_progress"
)

func (CallASRDeadLetter) TableName() string {
	return "call_asr_dl"
}
