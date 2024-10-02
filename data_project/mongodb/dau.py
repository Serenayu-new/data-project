import warnings
import datetime
import pandas as pd
from data_project.gsheets import DateSheet
import pymongo
from pymongo.collection import Collection


WEEKDAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']


def update_dau(sheet: DateSheet, collection: Collection, timezone: datetime.timedelta = datetime.timedelta()) -> None:
    return _base_update_dau(sheet, collection, 'day', timezone)


def update_wau(sheet: DateSheet, collection: Collection, timezone: datetime.timedelta = datetime.timedelta()) -> None:
    return _base_update_dau(sheet, collection, 'week', timezone)


def update_mau(sheet: DateSheet, collection: Collection, timezone: datetime.timedelta = datetime.timedelta()) -> None:
    return _base_update_dau(sheet, collection, 'month', timezone)


def _base_update_dau(sheet: DateSheet, collection: Collection, unit: str,
                     timezone: datetime.timedelta) -> None:
    yesterday = (datetime.date.today() - datetime.timedelta(days=1))
    dates = sheet.worksheet.col_values(sheet.date_col)[sheet.headers_row:]
    if yesterday.isoformat() in dates:
        warnings.warn(f'{yesterday} exists, the update is canceled.')
        return

    # get data from mongodb
    data = _get_data(collection, dates, yesterday, timezone, unit=unit)

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


def _get_data(collection: Collection, dates, yesterday, timezone, unit) -> pd.DataFrame:
    pipeline_pf, pipeline_to = _build_pipeline(
        dates, yesterday, timezone, unit)

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


def _compute_period(last_time: datetime.datetime, yesterday: datetime.datetime, unit: str) -> datetime.datetime:
    # ensure type of yesterday
    yesterday = datetime.datetime(
        yesterday.year, yesterday.month, yesterday.day,
    )

    # get period function
    try:
        func = eval(f'_compute_period_by_{unit}')
    except BaseException as err:
        raise ValueError(
            "@unit should be one of 'day', 'week', 'month'") from err

    return func(last_time, yesterday,)


def _compute_period_by_day(
    last_time: datetime.datetime, yesterday: datetime.datetime,
) -> tuple[datetime.datetime, datetime.datetime]:
    start_time = last_time + datetime.timedelta(days=1)
    stop_time = yesterday + datetime.timedelta(days=1)
    return start_time, stop_time


def _compute_period_by_week(
    last_time: datetime.datetime, yesterday: datetime.datetime,
) -> tuple[datetime.datetime, datetime.datetime]:
    start_time = last_time + datetime.timedelta(days=7)
    days = (yesterday - start_time).days + 1
    stop_time = start_time + datetime.timedelta(weeks=days // 7)
    return start_time, stop_time


def _compute_period_by_month(
    last_time: datetime.datetime, yesterday: datetime.datetime | None = None,
) -> tuple[datetime.datetime, datetime.datetime]:
    # drop ignored arg
    del yesterday

    start_month = last_time.month + 1
    if start_month > 12:
        start_time = datetime.datetime(last_time.year + 1, 1, 1)

    today = datetime.datetime.today()
    stop_time = datetime.datetime(today.year, today.month, 1)
    return start_time, stop_time


def _build_pipeline(
    dates: list[datetime.date], yesterday: datetime.date, timezone: datetime.timedelta,
    unit: str,
) -> list[dict]:
    # compute period
    last_time = datetime.datetime.fromisoformat(dates[-1])
    start_time, stop_time = _compute_period(last_time, yesterday, unit)

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
                    }}, "unit": unit}
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
