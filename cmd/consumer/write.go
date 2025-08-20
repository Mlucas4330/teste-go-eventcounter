package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

func createAndWriteFile(path, name string, content map[string]int) error {
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		log.Printf("can't create directory %s, err: %s", path, err)
		return err
	}

	file, err := os.Create(fmt.Sprintf("%s/%s.json", path, name))
	if err != nil {
		log.Printf("can't write file %s.json, err: %s", name, err)
		return err
	}
	defer file.Close()

	b, err := json.MarshalIndent(content, "", "\t")
	if err != nil {
		log.Printf("can't marshal data for file %s.json, err: %s", name, err)
		return err
	}

	if _, err := file.Write(b); err != nil {
		log.Printf("can't write data for file %s.json, err: %s", name, err)
		return err
	}

	log.Printf("%s json file created succesfully", name)

	return nil
}
