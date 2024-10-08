import warnings
import datetime
import asyncio
import gspread
import numpy as np
import pandas as pd


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
        self.change_sheet(self.sheet.get_worksheet(0).title)

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
        self.headers_row = headers_row
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
        headers = self.headers
        date = self.date

        # locate col date
        col_id = -1
        for key in ['日期', 'date']:
            try:
                col_id = [h.strip() for h in headers].index(key)
            except ValueError:
                continue
            else:
                col_id += 1
                break
        if col_id <= 0:
            warnings.warn("No columns named '日期' or 'date'.")
        self.date_col = col_id

        # find the given date
        row_id = -1
        if col_id > 0:
            dates = self.worksheet.col_values(col_id)
            try:
                row_id = dates.index(date)
            except ValueError:
                warnings.warn(f'Given date {date} not found, min {dates[self.headers_row]} max {dates[-1]}')
            else:
                row_id += 1
        else:
            dates = []
            row_id = -1

        self.row_id = row_id
        return row_id

    def update_cells(self, col_name, value):
        col_name = col_name.lower()
        col_idx = [i + 1 for i, v in enumerate(self.headers) if v == col_name]
        for col_id in col_idx:
            asyncio.create_task(self._update_cell(col_id, value))
        return len(col_idx)

    async def _update_cell(self, col_id, value):
        value = convert_type(value)
        return self.worksheet.update_cell(self.row_id, col_id, value)


def int_to_char(i):
    "convert int into char"
    step = ord("Z") - ord("A") + 1
    c = chr(i % step + 65)
    i = int(i / step)
    if i > 0:
        return int_to_char(i - 1) + c
    return c


if __name__ == '__main__':
    # for test
    sheet = DateSheet('[RP_CN] Revenue Report', date='2024-06-22')
    sheet.change_sheet('設備留存', 2)
    print(sheet.headers)
