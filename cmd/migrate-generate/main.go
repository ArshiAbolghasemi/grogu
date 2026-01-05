package main

import (
	"log"
	"os"
	"os/exec"
	"path/filepath"

	"ariga.io/atlas-provider-gorm/gormschema"
	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/deadletter"
)

const minArgs = 2

func main() {
	if len(os.Args) < minArgs {
		log.Fatal("please provide a migration name")
	}

	migrationName := filepath.Base(os.Args[1])

	loader := gormschema.New("postgres")

	schema, err := loader.Load(&deadletter.CallASRDeadLetter{})
	if err != nil {
		log.Fatalf("failed to load gorm schema: %v", err)
	}

	tmp, err := os.CreateTemp("/tmp", "schema-*.sql")
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		err := os.Remove(tmp.Name())
		if err != nil {
			log.Printf("failed to remove temp file %s: %v", tmp.Name(), err)
		}
	}()

	_, err = tmp.WriteString(schema)
	if err != nil {
		log.Fatal(err)
	}

	err = tmp.Close()
	if err != nil {
		log.Printf("failed to close temp file: %v", err)
	}

	abs, err := filepath.Abs(tmp.Name())
	if err != nil {
		log.Fatal(err)
	}

	cmd := exec.Command(
		"atlas",
		"migrate", "diff",
		migrationName,
		"--to", "file://"+abs,
		"--dev-url", "docker+postgres://docker.mci.dev/postgres:14-alpine/dev?search_path=public",
		"--dir", "file://migrations?format=golang-migrate",
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatalf("atlas diff failed: %v\n%s", err, out)
	}

	log.Printf("migration generated successfully:\n%s", out)
}
