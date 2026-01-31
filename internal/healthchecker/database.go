package healthchecker

import (
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/database"
)

func CheckDB() error {
	_, err := database.NewDatabase()
	return err
}
