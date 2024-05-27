package main

import (
	"fmt"
	"github.com/xuri/excelize/v2"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
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
	TABLE_GRID_TYPE_ITEM = "grid_type_item"
	TABLE_GRID_SKU = "grid_sku"
	TABLE_GRID_SKU_ITEM = "grid_sku_item"
	NUM_WORKERS = 5
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

	gridScripts := []*GridScript{}
	grid := &GridScript{Table: TABLE_GRID, Inserts: []string{}, Exists: map[string]bool{}}
	gridType := &GridScript{Table: TABLE_GRID_TYPE, Inserts: []string{}, Exists: map[string]bool{}}
	gridTypeItem := &GridScript{Table: TABLE_GRID_TYPE_ITEM, Inserts: []string{}, Exists: map[string]bool{}}
	gridSku := &GridScript{Table: TABLE_GRID_SKU, Inserts: []string{}, Exists: map[string]bool{}}
	gridSkuItem := &GridScript{Table: TABLE_GRID_SKU_ITEM, Inserts: []string{}, Exists: map[string]bool{}}

	for rowIndex, row := range rows {
		if rowIndex >= 2 {
			var wg sync.WaitGroup
			wg.Add(NUM_WORKERS)

			insertGrid := generateInsertGrid(row)
			go buildGridScript(insertGrid, insertGrid, grid, &wg)

			insertGridType, key := generateInsertGridType(row)
			go buildGridScript(insertGridType, key, gridType , &wg)

			insertGridTypeItem, key := generateInsertGridTypeItem(row)
			go buildGridScript(insertGridTypeItem, key, gridTypeItem , &wg)

			insertGridSku, key := generateInsertGridSku(row)
			go buildGridScript(insertGridSku, key, gridSku , &wg)

			insertGridSkuItem, key := generateInsertGridSkuItem(row)
			go buildGridScript(insertGridSkuItem, key, gridSkuItem, &wg)

			wg.Wait()
		}
	}

	gridScripts = append(gridScripts, grid, gridType, gridTypeItem, gridSku)

	if ok, err := generateDestFileWithInserts(gridScripts); !ok || err != nil {
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

func generateInsertGridType(row []string) (string, string) {
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
		query := `ExecRaw(db, "INSERT INTO grid_type (description, alias, view_type, date_created, last_updated)
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

func generateInsertGridTypeItem(row []string) (string, string) {
	gridTypeDescription := row[1]
	gridTypeItemDescription := row[3]

	key := gridTypeDescription + gridTypeItemDescription

	if gridTypeDescription != "" && gridTypeItemDescription != "" {
		query := `ExecRaw(db, "INSERT INTO grid_type_item (grid_type_id, order_item, description, date_created, last_updated) 
							  SELECT gt.id,
									 max(gti.order_item) + 1,
									 '%s',
									 now(),
									 now()
							   FROM grid_type gt
							   LEFT JOIN grid_type_item gti ON gt.id = gti.grid_type_item
							  WHERE gti.description = '%s'
								AND gt.description = '%s'
								AND NOT EXISTS (
									SELECT 1 FROM grid_type_item gti2
									WHERE gti.id = gti2.id
								);")
				`
		query = fmt.Sprintf(query, gridTypeItemDescription, gridTypeItemDescription, gridTypeDescription)
		return query, key
	}

	return "", key
}

func generateInsertGridSku(row []string) (string, string) {
	gridDescription := row[6]
	gridSku := row[7]
	skuMain := row[8]

	if skuMain != "" {
		skuMain = "1"
	} else {
		skuMain = "0"
	}

	key := gridDescription + gridSku
	if gridDescription != "" && gridSku != "" {
		skuId, _ := strconv.Atoi(gridSku)
		skuMainInt, _ := strconv.Atoi(skuMain)

		query := `ExecRaw(db, "INSERT INTO grid_sku (grid_id, sku_id, order_sku, main, date_created, last_updated) 
				SELECT
					g.id,
				    %d,
					max(gs.order_sku) + 1,
					%d,
					now(),
					now()
				FROM grid g
				LEFT JOIN grid_sku gs ON gs.grid_id = g.id AND gs.sku_id = %d
			    WHERE g.description = '%s' 
				AND NOT EXISTS (
					SELECT 1 FROM grid_sku gs2
					WHERE gs2.id = gs.id
				);") &&		
				`

		query = fmt.Sprintf(query, skuId, skuMainInt, skuId, gridDescription)
		return query, key
	}

	return "", key
}

func generateInsertGridSkuItem(row []string) (string, string) {
	gridTypeDescription := row[1]
	gridTypeItemDescription := row[3]
	gridDescription := row[6]
	gridSku := row[7]
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

	key := gridTypeDescription + gridTypeItemDescription + gridDescription + gridSku + gridTypeAlias + gridTypeViewType
	if gridTypeDescription != "" && gridSku != "" && gridTypeViewType != "" &&
		gridDescription != "" && gridTypeItemDescription != "" && gridTypeAlias != "" {
		skuId, _ := strconv.Atoi(gridSku)

		query := `ExecRaw(db, "INSERT INTO grid_sku_item (grid_sku_id, grid_type_item_id, date_created, last_updated)
						SELECT gs.id,
							   gti.id,
							   now(),
							   now()
						  FROM grid_grid_type ggt
							JOIN grid g ON ggt.grid_id = g.id
						    JOIN grid_type gt ON gt.id = ggt.grid_type_id
						    JOIN grid_type_item gti ON gti.grid_type_id = gt.id
							JOIN grid_sku gs ON gs.grid_id = g.id and gs.sku_id = %d
						 WHERE g.description = '%s'
						   AND gt.description = '%s' AND gt.alias = '%s' AND gt.view_type = '%s'
							AND gti.description = '%s'
							AND NOT EXISTS (
								SELECT 1 FROM grid_sku_item gsi 
								WHERE gsi.grid_sku_id = gs.id
								  AND gsi.grid_type_item_id = gti.id
							);") &&
				`
		query = fmt.Sprintf(query, skuId, gridDescription, gridTypeDescription, gridTypeAlias, gridTypeViewType, gridTypeItemDescription)
		return query, key
	}

	return "", key
}

func generateDestFileWithInserts(gs []*GridScript) (bool, error) {
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

func buildGridScript(insert, key string, script *GridScript, wg *sync.WaitGroup) {
	defer wg.Done()
	if insert != "" {
		exists := script.Exists[key]
		if !exists {
			script.Inserts = append(script.Inserts, insert)
		}
		script.Exists[key] = true
	}
}