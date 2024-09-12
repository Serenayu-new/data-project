import warnings
import datetime
import pandas as pd
from data_project.gsheets import DateSheet
import pymongo
from pymongo.collection import Collection


WEEKDAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']


def update_dau(sheet: DateSheet, collection: Collection, timezone: datetime.timedelta = datetime.timedelta()) -> None:
    yesterday = (datetime.date.today() - datetime.timedelta(days=1))
    dates = sheet.worksheet.col_values(sheet.date_col)[sheet.headers_row:]
    if yesterday.isoformat() in dates:
        warnings.warn(f'{yesterday} exists, the update is canceled.')
        return

    # get data from mongodb
    data = _get_data(collection, dates, yesterday, timezone)

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
        f'A{len(dates) + sheet.headers_row + 1}', data.values.tolist(), raw=False
    )
    
def _get_data(collection: Collection, dates, yesterday, timezone) -> pd.DataFrame:
    pipeline_pf, pipeline_to = _build_pipeline(dates, yesterday, timezone)

    with pymongo.timeout(120):
        data_pf = pd.DataFrame(collection.aggregate(pipeline_pf))
    print(data_pf)

    with pymongo.timeout(120):
        data_to = pd.DataFrame(collection.aggregate(pipeline_to))
    print(data_to)

    data = pd.concat([data_pf, data_to])

    # add weekday
    data.loc[:, 'weekday'] = data['日期'].apply(lambda x: WEEKDAYS[x.weekday()])
    data['日期'] = data['日期'].apply(lambda x: x.isoformat().split('T')[0])
    return data


def _build_pipeline(
    dates: list[datetime.date], yesterday: datetime.date, timezone: datetime.timedelta,
) -> list[dict]:
    # define time intervals
    start_time = datetime.datetime.fromisoformat(
        dates[-1]) + datetime.timedelta(days=1)
    stop_time = datetime.datetime(
        yesterday.year, yesterday.month, yesterday.day,
    ) + datetime.timedelta(days=1)

    stages_date = [
        # filter by time
        {"$match": {
            "createTime": {
                "$gte": start_time - timezone,
                "$lt": stop_time - timezone,
            }}},
        # convert createTime into date format
        {"$addFields": {
            "createDate": {
                "$dateTrunc": {"date": {
                    "$dateAdd": {
                        "startDate": "$createTime",
                        "unit": "hour",
                        "amount": int(timezone.total_seconds() // 3600),
                    }}, "unit": "day"}
            }}}
    ]
    pipeline_platform = stages_date + [
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

    pipeline_total = stages_date + [
        # gropu by date and platform
        {"$group": {
            '_id': '$createDate',
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
            '日期': '$_id',
            'platform': 'Total',
            'distinct (acid)': {'$size': '$acid'},
            'distinct (uid)': {'$size': '$userId'},
            'distinct (deviceid)': {'$size': '$deviceId'},
            'count ( _id )': "$total",
        }}
    ]

    return pipeline_platform, pipeline_total
