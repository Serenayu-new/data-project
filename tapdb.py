import requests
import pandas as pd
from datetime import datetime, date, timedelta


class TapDBClient:

    urls = {
        'pay': 'https://www.tapdb.com/api/v1/ga-payment/income-data',
        'retain': 'https://www.tapdb.com/api/v1/ga-retention/device-retention',
        'active': 'https://www.tapdb.com/api/v1/ga-active/active-data',
        'source': 'https://www.tapdb.com/api/v1/ga-source/source-data',
    }

    def __init__(self, email, password, target_date=None):
        self.__login(email, password)
        self.__set_date(target_date)
        self.__get_project_idx()

    def __login(self, email, password):
        s = requests.Session()
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'}
        res = s.post(
            'https://www.tapdb.com/api/v1/auth/login',
            headers=headers,
            json={
                'email': email,
                'password': password,
                'remember': True,
            }
        )
        assert res.status_code == 200
        self.s = s

    def __set_date(self, target_date):
        if target_date is None:
            target_date  = date.today() - timedelta(days=1)
        if isinstance(target_date, str):
             target_date = date.fromisoformat(target_date)
        self.target_date = target_date

    def __get_project_idx(self):
        yesterday = self.target_date
        today = self.target_date + timedelta(days=1)

        res = self.s.get('https://www.tapdb.com/api/v1/ga-overviews/games-detail-web',
                    params={
                        'comparedDateBegin': yesterday,
                        'comparedDateEnd': yesterday,
                        'comparedRelativeTime': True,
                        'db': today,
                        'de': today,
                        'relativeTime': True,
                        'scope': 'all'
                    })


        self.project_idx = {
            d[0]: d[2]
            for d in res.json()['data']['data']
        }

    def get_data(self, project, name, dim='date', begin_date=None, end_date=None, **kwargs):
        if begin_date is None:
            begin_date = self.target_date
        elif isinstance(begin_date, str):
            begin_date = date.fromisoformat(begin_date)
        if end_date is None:
            end_date = self.target_date
        elif isinstance(end_date, str):
            end_date = date.fromisoformat(end_date)
        end_date = end_date + timedelta(days=1) - timedelta(seconds=1)

        # build body
        url = self.urls[name]
        body = {
            'flts': [],
            'db': int(datetime.fromisoformat(begin_date.isoformat()).timestamp()) * 1000,
            'de': int(datetime.fromisoformat(end_date.isoformat()).timestamp()) * 1000,
            'pid': self.project_idx[project],
            'pf': '',
            'isDeWater': False,
            'dim': dim,
            'interval': 'day',
        }
        body.update(kwargs)
        if dim == 'os':
            body.pop('interval')

        # get response
        res = self.s.post(url, json=body)

        # response to df
        return _to_df(res, dim)

def _to_df(res, dim):
    data = res.json()['data']
    assert len(data) > 0
    df = pd.DataFrame(data['data'], columns=data['index'])
    if dim == 'date':
        df['key'] = df['key'].apply(lambda x: date.fromtimestamp(x / 1000))
    df.index = df['key']
    return df