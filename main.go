package main

import (
	"fmt"
	"github.com/xuri/excelize/v2"
	"log"
	"os"
	"path/filepath"
	"time"
)

const (
	BASE_FILE_PATH = "/home/felipe/Área de Trabalho/Demandas CWS/Script Grade LEO/"
	BASE_FILE_NAME = "Carga de Grade 2024 05 10.xlsx"
	BASE_SHEET_INDEX = 1
	DEST_FILE_PATH = "/home/felipe/Área de Trabalho/Demandas CWS/Script Grade LEO/generated"
	DEST_FILE_NAME = "Generated"
	TABLE_GRID = "grid"
)

type GridScript struct {
	Table string
	Inserts []string
	Exists map[string]bool
}

func main() {
	// Open excel file
	f, err := excelize.OpenFile(BASE_FILE_PATH + BASE_FILE_NAME)
	if err != nil {
		log.Fatalf("Error open the file: %s \n", err.Error())
	}
	defer f.Close()

	// get sheet name
	sheetNames := f.GetSheetList()
	sheetName := sheetNames[BASE_SHEET_INDEX]

	// get all sheet rows
	rows, err := f.GetRows(sheetName)

	gridScript := []GridScript{}
	grid := GridScript{Table: TABLE_GRID, Inserts: []string{}, Exists: map[string]bool{}}

	for rowIndex, row := range rows {
		if rowIndex >= 2 {
			for colIndex, colCell := range row {
				insertGrid := generateInsertGrid(colIndex, colCell)
				if insertGrid != "" {
					exists := grid.Exists[insertGrid]
					if !exists {
						grid.Inserts = append(grid.Inserts, insertGrid)
					}
					grid.Exists[insertGrid] = true
				}
			}
		}
	}

	gridScript = append(gridScript, grid)

	if ok, err := generateDestFileWithInserts(gridScript); !ok || err != nil {
		log.Fatalf("Error to generate the Excel file. Error: %v", err)
	}

	fmt.Println("Processed with successfully!")
}

func generateInsertGrid(colIndex int, colValue string) string {
	if colIndex == 6 {
		query := `ExecRaw(db, "INSERT INTO grid (description, date_created, last_updated)
				  SELECT '%s', now(), now()
                    WHERE NOT EXISTS (
						SELECT 1 FROM grid WHERE description = '%s'
					);") &&
				`
		description := colValue
		query = fmt.Sprintf(query, description, description)
		return query
	}
	return ""
}

func generateDestFileWithInserts(gs []GridScript) (bool, error) {
	fileName := fmt.Sprintf("%s_%v.xlsx", DEST_FILE_NAME, time.Now().Format("20060102150405"))

	for _, script := range gs {
		if script.Table == TABLE_GRID {

			f := excelize.NewFile()

			// add a new sheet
			sheetName := TABLE_GRID
			gridTabIndex, err := f.NewSheet(sheetName)
			if err != nil {
				return false, err
			}

			for rowIndex, insert := range script.Inserts {
				cell := fmt.Sprintf("A%d", rowIndex+1)
				err = f.SetCellValue(sheetName, cell, insert)
				if err != nil {
					return false, err
				}
			}

			f.SetActiveSheet(gridTabIndex)

			dirPerm := os.FileMode(0755)
			err = os.MkdirAll(DEST_FILE_PATH, dirPerm)
			if err != nil {
				return false, err
			}

			newFilePath := filepath.Join(DEST_FILE_PATH, fileName)

			// save the Excel file
			if err := f.SaveAs(newFilePath); err != nil {
				return false, err
			}

			fmt.Println("Excel file was created successfully!")
		}
	}

	return true, nil
}
