package expert

import (
	"context"
	"errors"
	"time"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/database"
	"github.com/sony/gobreaker/v2"
	"gorm.io/gorm"
)

type ExpertRepository struct {
	DBConn         *gorm.DB
	CircuitBreaker *gobreaker.CircuitBreaker[any]
}

func NewExpertRepository(dbConn *gorm.DB) *ExpertRepository {
	cbSettings := database.GetCircuitBreakerSettings()

	return &ExpertRepository{
		DBConn:         dbConn,
		CircuitBreaker: gobreaker.NewCircuitBreaker[any](cbSettings),
	}
}

var ErrInvalidExpertResult = errors.New("invalid result type, it should be pointer to Expert struct")

// GetExpertByUserCode retrieves a Expert by itsuserCode
func (expertRepository *ExpertRepository) GetExpertByUserCode(ctx context.Context, userCode string) (*Expert, error) {
	result, err := expertRepository.CircuitBreaker.Execute(func() (any, error) {
		var expert Expert

		err := expertRepository.DBConn.WithContext(ctx).
			Where("user_code = ?", userCode).
			First(&expert).Error
		if err != nil {
			return nil, err
		}

		return &expert, nil
	})
	if err != nil {
		return nil, err
	}

	expert, ok := result.(*Expert)
	if !ok {
		return nil, ErrInvalidExpertResult
	}

	return expert, nil
}

// CreateExpert inserts a new Expert into the database
func (expertRepository *ExpertRepository) CreateExpert(ctx context.Context, userCode string) (*Expert, error) {
	result, err := expertRepository.CircuitBreaker.Execute(func() (any, error) {
		now := time.Now()

		expert := &Expert{
			UserCode:  userCode,
			UpdatedAt: &now,
		}

		err := expertRepository.DBConn.
			WithContext(ctx).
			Omit("first_name", "last_name", "phone_number").
			Create(expert).Error
		if err != nil {
			return nil, err
		}

		return expert, err
	})
	if err != nil {
		return nil, err
	}

	expert, ok := result.(*Expert)
	if !ok {
		return nil, ErrInvalidExpertResult
	}

	return expert, nil
}
