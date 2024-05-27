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
	TABLE_GRID_TYPE = "grid_type"
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
	gridType := GridScript{Table: TABLE_GRID_TYPE, Inserts: []string{}, Exists: map[string]bool{}}

	for rowIndex, row := range rows {
		if rowIndex >= 2 {
			insertGrid := generateInsertGrid(row)
			if insertGrid != "" {
				exists := grid.Exists[insertGrid]
				if !exists {
					grid.Inserts = append(grid.Inserts, insertGrid)
				}
				grid.Exists[insertGrid] = true
			}

			insertGridType, key := generateInsertGridType(row, gridType)
			if insertGridType != "" {
				exists := gridType.Exists[key]
				if !exists {
					gridType.Inserts = append(gridType.Inserts, insertGridType)
				}
				gridType.Exists[key] = true
			}

		}
	}

	gridScript = append(gridScript, grid, gridType)

	if ok, err := generateDestFileWithInserts(gridScript); !ok || err != nil {
		log.Fatalf("Error to generate the Excel file. Error: %v", err)
	}

	fmt.Println("Processed with successfully!")
}

func generateInsertGrid(row []string) string {
	description := row[6]
	if description != "" {
		query := `ExecRaw(db, "INSERT INTO grid (description, date_created, last_updated)
				  SELECT '%s', now(), now()
                    WHERE NOT EXISTS (
						SELECT 1 FROM grid WHERE description = '%s'
					);") &&
				`
		query = fmt.Sprintf(query, description, description)
		return query
	}
	return ""
}

func generateInsertGridType(row []string, script GridScript) (string, string) {
	gridTypeDescription := row[1]
	gridTypeAlias := row[2]
	gridTypeViewType := row[4]

	switch gridTypeViewType {
	case "Image":
		gridTypeViewType = "I"
	case "Combobox":
		gridTypeViewType = "C"
	case "RadioButton":
		gridTypeViewType = "R"
	default:
		gridTypeViewType = ""
	}

	key := gridTypeDescription + gridTypeAlias + gridTypeViewType

	if gridTypeDescription != "" && gridTypeAlias != "" && gridTypeViewType != "" {
		query := `ExecRaw(db,
				"INSERT INTO (description, alias, view_type, date_created, last_updated)
				SELECT '%s', '%s', '%s', now(), now()
      			WHERE NOT EXISTS (
					SELECT 1 FROM grid_type WHERE description = '%s' AND view_type = '%s'
				);") &&
              `
		query = fmt.Sprintf(query, gridTypeDescription, gridTypeAlias, gridTypeViewType, gridTypeDescription, gridTypeViewType)
		return query, key
	}

	return "", key
}

func generateDestFileWithInserts(gs []GridScript) (bool, error) {
	fileName := fmt.Sprintf("%s_%v.xlsx", DEST_FILE_NAME, time.Now().Format("20060102150405"))
	f := excelize.NewFile()
	defer f.Close()

	for _, script := range gs {
		if err := buildSheetCells(script.Table, f, script.Inserts); err != nil {
			return false, err
		}
	}

	dirPerm := os.FileMode(0755)
	err := os.MkdirAll(DEST_FILE_PATH, dirPerm)
	if err != nil {
		return false, err
	}

	newFilePath := filepath.Join(DEST_FILE_PATH, fileName)

	// save the Excel file
	if err := f.SaveAs(newFilePath); err != nil {
		return false, err
	}

	fmt.Println("Excel file was created successfully!")

	return true, nil
}

func createSheetNewTab(tabName string, f *excelize.File) error {
	gridTypeTabIndex, err := f.NewSheet(tabName)
	if err != nil {
		return err
	}
	f.SetActiveSheet(gridTypeTabIndex)
	return nil
}

func fillSheetCells(sheetName string, f *excelize.File, inserts []string) error {
	for rowIndex, insert := range inserts {
		cell := fmt.Sprintf("A%d", rowIndex+1)
		err := f.SetCellValue(sheetName, cell, insert)
		if err != nil {
			return err
		}
	}
	return nil
}

func buildSheetCells(sheetName string, f *excelize.File, inserts []string) error {
	// add a new sheet
	if err := createSheetNewTab(sheetName, f); err != nil {
		return err
	}

	// filling sheet cells with queries
	if err := fillSheetCells(sheetName, f, inserts); err != nil {
		return err
	}

	return nil
}