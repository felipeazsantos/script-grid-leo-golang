package main

import (
	"fmt"
	"github.com/xuri/excelize/v2"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

const (
	BASE_FILE_PATH = "/home/felipe/Área de Trabalho/Demandas CWS/Script Grade LEO/"
	BASE_FILE_NAME = "Ajuste carga inicial de Grade com 3 variações 2024 04 29.xlsx"
	BASE_SHEET_INDEX = 1
	DEST_FILE_PATH = "/home/felipe/Área de Trabalho/Demandas CWS/Script Grade LEO/generated"
	DEST_FILE_NAME = "SCRIPT - "
	TABLE_IMAGE = "image"
	TABLE_GRID = "grid"
	TABLE_GRID_TYPE = "grid_type"
	TABLE_GRID_GRID_TYPE = "grid_grid_type"
	TABLE_GRID_TYPE_ITEM = "grid_type_item"
	TABLE_GRID_SKU = "grid_sku"
	TABLE_GRID_SKU_ITEM = "grid_sku_item"
	NUM_WORKERS = 7
	CRASIS = "`"
	INSERT_LIMIT_ON_CELL = 20
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
	sheetName := sheetNames[len(sheetNames) - 1]

	// get all sheet rows
	rows, err := f.GetRows(sheetName)

	gridScripts := []*GridScript{}
	grid := &GridScript{Table: TABLE_GRID, Inserts: []string{}, Exists: map[string]bool{}}
	gridType := &GridScript{Table: TABLE_GRID_TYPE, Inserts: []string{}, Exists: map[string]bool{}}
	gridGridType := &GridScript{Table: TABLE_GRID_GRID_TYPE, Inserts: []string{}, Exists: map[string]bool{}}
	gridTypeItem := &GridScript{Table: TABLE_GRID_TYPE_ITEM, Inserts: []string{}, Exists: map[string]bool{}}
	gridSku := &GridScript{Table: TABLE_GRID_SKU, Inserts: []string{}, Exists: map[string]bool{}}
	gridSkuItem := &GridScript{Table: TABLE_GRID_SKU_ITEM, Inserts: []string{}, Exists: map[string]bool{}}
	image := &GridScript{Table: TABLE_IMAGE, Inserts: []string{}, Exists: map[string]bool{}}

	for rowIndex, row := range rows {
		if rowIndex >= 2 {
			var wg sync.WaitGroup
			wg.Add(NUM_WORKERS)

			insertGrid := generateInsertGrid(row)
			go buildGridScript(insertGrid, insertGrid, grid, &wg)

			insertGridType, key := generateInsertGridType(row)
			go buildGridScript(insertGridType, key, gridType , &wg)

			insertGridGridType, key := generateInsertGridGridType(row)
			go buildGridScript(insertGridGridType, key, gridGridType , &wg)

			insertGridTypeItem, key := generateInsertGridTypeItem(row)
			go buildGridScript(insertGridTypeItem, key, gridTypeItem , &wg)

			insertGridSku, key := generateInsertGridSku(row)
			go buildGridScript(insertGridSku, key, gridSku , &wg)

			insertGridSkuItem, key := generateInsertGridSkuItem(row)
			go buildGridScript(insertGridSkuItem, key, gridSkuItem, &wg)

			insertImages, key := generateInsertImages(row)
			go buildGridScript(insertImages, key, image, &wg)

			wg.Wait()
		}
	}

	gridScripts = append(gridScripts, grid, gridType, gridGridType, gridTypeItem, gridSku, gridSkuItem, image)

	if ok, err := generateDestFileWithInserts(gridScripts); !ok || err != nil {
		log.Fatalf("Error to generate the Excel file. Error: %v", err)
	}

	fmt.Printf("Processed sheet %s with successfully! \n", DEST_FILE_NAME + BASE_FILE_NAME)
}

func generateInsertGrid(row []string) string {
	description := row[6]
	if description != "" {
		query := `ExecRaw(db, %sINSERT INTO grid (description, date_created, last_updated)
				  SELECT '%s', now(), now()
                    WHERE NOT EXISTS (
						SELECT 1 FROM grid WHERE description = '%s'
					);%s) &&
				`
		query = fmt.Sprintf(query, CRASIS, description, description, CRASIS)
		return query
	}
	return ""
}

func generateInsertGridType(row []string) (string, string) {
	gridTypeDescription := row[1]
	gridTypeAlias := row[2]
	gridTypeViewType := row[4]
	gridTypeViewType = getGridTypeViewType(gridTypeViewType)

	key := gridTypeDescription + gridTypeAlias + gridTypeViewType

	if gridTypeDescription != "" && gridTypeAlias != "" && gridTypeViewType != "" {
		query := `ExecRaw(db, %sINSERT INTO grid_type (description, alias, view_type, date_created, last_updated)
								SELECT '%s', '%s', '%s', now(), now()
								WHERE NOT EXISTS (
									SELECT 1 FROM grid_type WHERE description = '%s' AND view_type = '%s'
								);%s) &&
              `
		query = fmt.Sprintf(query, CRASIS, gridTypeDescription, gridTypeAlias, gridTypeViewType, gridTypeDescription, gridTypeViewType, CRASIS)
		return query, key
	}

	return "", key
}

func generateInsertGridGridType(row []string) (string, string) {
	gridTypeDescription := row[1]
	gridTypeAlias := row[2]
	gridTypeViewType := row[4]
	gridDescription := row[6]
	gridTypeViewType = getGridTypeViewType(gridTypeViewType)

	key := gridTypeDescription + gridTypeAlias + gridTypeViewType + gridDescription

	if gridTypeDescription != "" && gridTypeAlias != "" && gridTypeViewType != "" && gridDescription != "" {
		query := `ExecRaw(db, %sINSERT INTO grid_grid_type(grid_id, grid_type_id, order_Type, date_created, last_updated)
				  SELECT g.id,
						 gt.id,
						 COALESCE(max(ggt.order_type), 0) + 1,
						 now(),
					     now()
					FROM grid g
					 CROSS JOIN grid_type gt
					 LEFT JOIN grid_grid_type ggt ON ggt.grid_id = g.id and ggt.grid_type_id = gt.id
					WHERE g.description = '%s' 
					  AND gt.description = '%s' AND gt.alias = '%s' AND gt.view_type = '%s'
					  AND NOT EXISTS (
					 	SELECT 1 FROM grid_grid_type ggt2
						 WHERE ggt2.id = ggt.id
					  )
					 GROUP BY g.id, gt.id;%s) &&
				`
		query = fmt.Sprintf(query, CRASIS, gridDescription, gridTypeDescription, gridTypeAlias, gridTypeViewType, CRASIS)
		return query, key
	}

	return "", key
}

func generateInsertGridTypeItem(row []string) (string, string) {
	gridTypeDescription := row[1]
	gridTypeItemDescription := row[3]

	key := gridTypeDescription + gridTypeItemDescription

	if gridTypeDescription != "" && gridTypeItemDescription != "" {
		query := `ExecRaw(db, %sINSERT INTO grid_type_item (grid_type_id, order_item, description, date_created, last_updated) 
							  SELECT gt.id,
									 COALESCE(max(gti.order_item), 0) + 1,
									 '%s',
									 now(),
									 now()
							   FROM grid_type gt
							   LEFT JOIN grid_type_item gti ON gt.id = gti.grid_type_id AND gti.description = '%s'
							  WHERE gt.description = '%s'
								AND NOT EXISTS (
									SELECT 1 FROM grid_type_item gti2
									WHERE gti.id = gti2.id
								)
								GROUP BY gt.id;%s) &&
				`
		query = fmt.Sprintf(query, CRASIS, gridTypeItemDescription, gridTypeItemDescription, gridTypeDescription, CRASIS)
		return query, key
	}

	return "", key
}

func generateInsertGridSku(row []string) (string, string) {
	gridDescription := row[6]
	gridSku := row[7]
	var skuMain string
	if len(row) > 8 {
		skuMain = row[8]
	}

	if skuMain != "" {
		skuMain = "1"
	} else {
		skuMain = "0"
	}

	key := gridDescription + gridSku
	if gridDescription != "" && gridSku != "" {
		skuId, _ := strconv.Atoi(gridSku)
		skuMainInt, _ := strconv.Atoi(skuMain)

		query := `ExecRaw(db, %sINSERT INTO grid_sku (grid_id, sku_id, order_sku, main, date_created, last_updated) 
				SELECT
					g.id,
				    %d as sku_id,
					COALESCE(max(gs.order_sku), 0) + 1,
					%d,
					now(),
					now()
				FROM grid g
				LEFT JOIN grid_sku gs ON gs.grid_id = g.id AND gs.sku_id = %d
			    WHERE g.description = '%s' 
				AND NOT EXISTS (
					SELECT 1 FROM grid_sku gs2
					WHERE gs2.id = gs.id
				)
				GROUP BY g.id, sku_id;%s) &&		
				`

		query = fmt.Sprintf(query, CRASIS, skuId, skuMainInt, skuId, gridDescription, CRASIS)
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

	gridTypeViewType = getGridTypeViewType(gridTypeViewType)

	key := gridTypeDescription + gridTypeItemDescription + gridDescription + gridSku + gridTypeAlias + gridTypeViewType
	if gridTypeDescription != "" && gridSku != "" && gridTypeViewType != "" &&
		gridDescription != "" && gridTypeItemDescription != "" && gridTypeAlias != "" {
		skuId, _ := strconv.Atoi(gridSku)

		query := `ExecRaw(db, %sINSERT INTO grid_sku_item (grid_sku_id, grid_type_item_id, date_created, last_updated)
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
							);%s) &&
				`
		query = fmt.Sprintf(query, CRASIS, skuId, gridDescription, gridTypeDescription, gridTypeAlias, gridTypeViewType, gridTypeItemDescription, CRASIS)
		return query, key
	}

	return "", key
}

func generateInsertImages(row []string) (string, string) {
	imagePath := row[5]
	gridTypeDescription := row[1]
	gridTypeItemDescription := row[3]
	gridTypeAlias := row[2]
	gridTypeViewType := row[4]
	key := imagePath

	gridTypeViewType = getGridTypeViewType(gridTypeViewType)

	if imagePath != "" && gridTypeDescription != "" && gridTypeItemDescription != "" && gridTypeAlias != "" &&
	   gridTypeViewType != "" {
		imageName := filepath.Base(imagePath)

		query := `ExecRaw(db, %sINSERT INTO image (id_origin, type, name, path, source, priority, date_created, last_updated, tenant_store_id)
				  SELECT gti.id,
						 'gti',
					     '%s',
						 'grid-type-item/full/',
						 gti.id,
					     10,
						 now(),
						 now(),
						 1
					FROM grid_type_item gti
					JOIN grid_type gt ON gti.grid_type_id = gt.id
				   WHERE gti.description = '%s' 
					AND gt.description = '%s' AND gt.alias = '%s' AND gt.view_type = '%s'
				    AND NOT EXISTS (
						SELECT 1 FROM image i 
						WHERE i.id_origin = gti.id and i.type = 'gti' and i.priority = 10
					);%s) &&
				`
		query = fmt.Sprintf(query, CRASIS, imageName, gridTypeItemDescription, gridTypeDescription, gridTypeAlias, gridTypeViewType, CRASIS)
		return query, key
	}

	return "", key
}

func generateDestFileWithInserts(gs []*GridScript) (bool, error) {
	fileName := DEST_FILE_NAME + BASE_FILE_NAME
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
	var cellValue string
	cellRow := 1
	count := 1
	limit := INSERT_LIMIT_ON_CELL
	cell := fmt.Sprintf("A%d", cellRow)
	for _, insert := range inserts {
		if count == limit {
			err := f.SetCellValue(sheetName, cell, cellValue)
			if err != nil {
				return err
			}
			limit += INSERT_LIMIT_ON_CELL
			cellRow++
			cell = fmt.Sprintf("A%d", cellRow)
			cellValue = ""
		}

		cellValue += insert
		count++
	}

	if count < limit && cellValue != "" {
		err := f.SetCellValue(sheetName, cell, cellValue)
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

func getGridTypeViewType(value string) string {
	switch value {
	case "Image":
		return "I"
	case "Combobox":
		return "C"
	case "RadioButton":
		return "R"
	default:
		return ""
	}
}