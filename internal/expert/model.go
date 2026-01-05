package expert

import (
	"time"
)

type Expert struct {
	ID          int        `gorm:"column:id"           json:"id"`
	FirstName   *string    `gorm:"column:first_name"   json:"first_name"`
	LastName    *string    `gorm:"column:last_name"    json:"last_name"`
	UserCode    string     `gorm:"column:user_code"    json:"user_code"`
	PhoneNumber *time.Time `gorm:"column:phone_number" json:"phone_number"`
	CreatedAt   *time.Time `gorm:"column:created_at"   json:"created_at"`
	UpdatedAt   *time.Time `gorm:"column:updated_at"   json:"updated_at"`
}

func (Expert) TableName() string {
	return "experts"
}
