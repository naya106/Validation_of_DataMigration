import os
from openpyxl import load_workbook


def assignVar():
    # file directory
    base_dir = "C:/Users/Your directory/ValidationOfDataMigration/Validation/venv/Lib/site-packages"
    fileNm = "Table_Info.xlsx"

    wb = load_workbook(os.path.join(base_dir, fileNm), data_only=True)
    ws = wb.active

    findId = int(input("Find ID : "))
    cell_num = 0

    for row in ws.rows:
        if row[cell_num].value == findId:
            row_value = []
            for cell in row:
                row_value.append(cell.value)
            return row_value


var = assignVar()

base_dir = var[1]
module = var[2]
srcSchema = var[3]
srcTableNm = var[4]
srcColumn = var[5]
tgtSchema = var[6]
tgtTableNm = var[7]
tgtColumn = var[8]
migDate = str(var[9])
srcSql = var[10]
tgtSql = var[11]
migIssue = var[12]
exportPath = var[13]
dateFlag = var[14]