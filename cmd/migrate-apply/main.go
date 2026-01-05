package main

import (
	"errors"
	"log"
	"os"
	"path/filepath"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/database"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const validArgsLen = 2

func main() {
	if len(os.Args) < validArgsLen {
		log.Fatal("usage: up | down")
	}

	dbURL := database.GetURL()

	projectRoot, err := filepath.Abs(filepath.Join("migrations"))
	if err != nil {
		log.Fatal(err)
	}

	migrationsPath := "file://" + filepath.ToSlash(projectRoot)

	migrator, err := migrate.New(
		migrationsPath,
		dbURL,
	)
	if err != nil {
		log.Fatal(err)
	}

	cmd := os.Args[1]

	switch cmd {
	case "up":
		err = migrator.Up()
	case "down":
		err = migrator.Steps(-1)
	default:
		log.Fatal("unknown command")
	}

	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		log.Fatal(err)
	}

	version, dirty, _ := migrator.Version()
	log.Printf("migration complete. version=%d dirty=%v", version, dirty)
}
