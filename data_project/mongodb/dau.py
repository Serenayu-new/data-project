import warnings
import datetime
import pandas as pd
from data_project.gsheets import DateSheet
from pymongo.collection import Collection


WEEKDAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']


def update_dau(sheet: DateSheet, collection) -> None:
    yesterday = (datetime.date.today() - datetime.timedelta(days=1))
    dates = sheet.worksheet.col_values(sheet.date_col)[sheet.headers_row:]
    if yesterday.isoformat() in dates:
        warnings.warn(f'{yesterday} exists, the update is canceled.')
        return

    # get data from mongodb
    data = _get_data(collection, dates, yesterday)

    # re-order the cols
    for header in sheet.headers:
        if header not in data.columns:
            data.loc[:, header] = None
    data = data[sheet.headers]

    # sort by date and platforms
    data.sort_values(['日期', 'platform'], inplace=True)

    # show data
    print(data)

    # update to sheet
    sheet.worksheet.update(
        f'A{len(dates) + sheet.headers_row + 1}', data.values.tolist()
    )

def _get_data(collection: Collection, dates, yesterday) -> pd.DataFrame:
    pipeline = _build_pipeline(dates, yesterday)
    data = pd.DataFrame(collection.aggregate(pipeline))
    
    # add weekday
    data.loc[:, 'weekday'] = data['日期'].apply(lambda x: WEEKDAYS[x.weekday()])
    data['日期'] = data['日期'].apply(lambda x: x.isoformat().split('T')[0])
    return data


def _build_pipeline(
    dates: list[datetime.date], yesterday: datetime.date,
    timedelta: datetime.timedelta = datetime.timedelta(),
) -> list[dict]:
    # define time intervals
    start_time = datetime.datetime.fromisoformat(
        dates[-1]) + datetime.timedelta(days=1)
    stop_time = datetime.datetime(
        yesterday.year, yesterday.month, yesterday.day,
    ) + datetime.timedelta(days=1)

    pipeline = [
        # filter by time
        {"$match": {
            "createTime": {
                "$gte": start_time,
                "$lt": stop_time,
            }}},
        # convert createTime into date format
        {"$addFields": {
            "createDate": {
                "$dateTrunc": {"date": "$createTime", "unit": "day"}
            }}},
        # gropu by date and platform
        {"$group": {
            '_id': {
                'date': '$createDate',
                'platform': '$deviceData.platform',
            },
            'acid': {
                '$addToSet': '$userData.acid'
            },
            'userId': {
                '$addToSet': '$userData.userId'
            },
            'deviceId': {
                '$addToSet': '$deviceData.deviceId'
            },
            'total': {
                '$sum': 1,
            }
        }},
        # count the unique idx
        {'$project': {
            '_id': 0,
            '日期': '$_id.date',
            'platform': '$_id.platform',
            'distinct (acid)': {'$size': '$acid'},
            'distinct (uid)': {'$size': '$userId'},
            'distinct (deviceid)': {'$size': '$deviceId'},
            'count ( _id )': "$total",
        }}
    ]
    return pipeline
