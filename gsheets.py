import gspread
import numpy as np
import pandas as pd
import datetime

TYPECONVERTOR = {
    np.int64: int,
    np.float64: float,
    datetime.date: str,
}

def convert_type(value):
    for key, val in TYPECONVERTOR.items():
        if isinstance(value, key):
            return val(value)
    return value

# auth
GC = gspread.oauth()

class BaseSheet:
    "sheet with base functions"

    def __init__(self, sheet_name):
        self.sheet = GC.open(sheet_name)
        self.worksheet = self.change_sheet(self.sheet.get_worksheet(0).title)

    @property
    def worksheet(self):
        "the current worksheet"
        return self.__worksheet

    @worksheet.setter
    def worksheet(self, value):
        self.__worksheet = value

    def change_sheet(self, worksheet_name, headers_row=1):
        "change the current worksheet"
        self.worksheet = self.sheet.worksheet(worksheet_name)
        self.__get_headers(headers_row)

    def show(self):
        "show the sheet as dataframe"
        df = pd.DataFrame(self.worksheet.get_all_records())
        return df

    def __get_headers(self, headers_row):
        self.headers = [x.lower() for x in self.worksheet.row_values(headers_row)]


class DateSheet(BaseSheet):
    "the sheet based on date"

    def __init__(self, *args, date, **kwargs):
        self.date = date
        super().__init__(*args, **kwargs)

    def change_sheet(self, *args, **kwargs):
        super().change_sheet(*args, **kwargs)
        self.__find_date()

    def __find_date(self):
        # locate col date
        headers = self.headers
        date = self.date

        try:
            col_id = headers.index('日期')
        except ValueError:
            col_id = headers.index('date')
        col_id += 1

        # find the given date
        dates = self.worksheet.col_values(col_id)
        try:
            row_id = dates.index(date)
        except ValueError:
            raise ValueError(f'Given date {date} not found, min {dates[0]} max {dates[-1]}')
        row_id += 1
        self.row_id = row_id
        return row_id

    def update_cells(self, col_name, value):
        col_name = col_name.lower()
        col_idx = [i + 1 for i, v in enumerate(self.headers) if v == col_name]
        for col_id in col_idx:
            self._update_cell(col_id, value)
        return len(col_idx)

    def _update_cell(self, col_id, value):
        value = convert_type(value)
        return self.worksheet.update_cell(self.row_id, col_id, value)


if __name__ == '__main__':
    # for test
    sheet = DateSheet('[RP_CN] Revenue Report', date='2024-06-22')
    sheet.change_sheet('設備留存', 2)
    print(sheet.headers)