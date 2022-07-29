import datetime
import re

import requests as requests
from bs4 import BeautifulSoup
import pandas as pd

da_url = 'https://eservices.ryde.nsw.gov.au/T1PRProd/WebApps/eProperty/P1/eTrack/eTrackApplicationDetails.aspx?r=COR.P1.WEBGUEST&f=$P1.ETR.APPDET.VIW&ApplicationId='
duplexes = [
    'LDA2020/0139',
    'LDA2021/0246',
    'LDA2020/0393',
    'LDA2021/0268',
    'LDA2022/0014',
    'LDA2021/0117',
    'LDA2022/0038',
    'LDA2022/0087',
    'LDA2021/0011',
    'LDA2022/0107',
    'LDA2021/0272',
    'LDA2021/0385',
    'LDA2020/0135',
    'LDA2020/0351',
    'LDA2020/0306',
    'LDA2020/0072',
    'LDA2022/0078',
    'LDA2020/0203',
    'LDA2020/0206',
    'LDA2020/0215',
    'LDA2020/0163',
    'LDA2020/0014',
    'LDA2021/0013',
    'LDA2021/0432',
    'LDA2021/0447',
    'LDA2022/0051',
    'LDA2022/0056',
    'LDA2022/0111',
    'LDA2021/0100',
    'LDA2020/0407',
    'LDA2021/0247',
    'LDA2021/0273',
    'LDA2020/0277',
    'LDA2020/0204',
    'LDA2020/0202',
    'LDA2020/0013',
    'LDA2020/0103',
    'LDA2021/0421',
    'LDA2021/0087',
    'LDA2020/0281',
    'LDA2022/0026',
    'LDA2021/0386',
    'LDA2021/0387',
    'LDA2020/0069',
    'LDA2020/0254',
    'LDA2021/0436',
    'LDA2021/0437',
    'LDA2021/0393',
    'LDA2022/0105',
    'LDA2022/0070'
]

def hello():
    results_df = pd.DataFrame({})
    results_link_df = pd.DataFrame({})
    duplexes_count = len(duplexes)
    for _i in duplexes:
        print(f'doing {duplexes_count}: {_i}')
        duplexes_count -= 1
        _i = _i.replace('/', '%2f')
        req = requests.get(f'{da_url}{_i}')
        print(f'status: {req.status_code}')
        soup = BeautifulSoup(req.text, 'html.parser')
        table_rows = soup.findAll('tr')
        result_dict = {}
        for _row in table_rows:
            table_values = _row.findAll('td')
            if len(table_values) == 0:
                continue
            row_name = table_values[0].text
            row_val = table_values[1].text
            if re.match(r'(\d|\d\d)/\d\d/\d\d\d\d', row_val):
                row_val = re.sub(r'(\d+)/(\d\d)/(\d\d\d\d)', r'\3/\2/\1', row_val)
            result_dict[row_name] = [row_val]
        result_pd = pd.DataFrame(result_dict)
        results_df = pd.concat([results_df, result_pd])

        da_links_href_list = []
        da_links_name_list = []
        da_links_id_list = []
        da_links_address_list = []
        result_link_dict = {}
        da_links = soup.findAll('div', {'class': 'detail'})[0].findAll('a')
        if len(da_links) == 0:
            continue
        for _link in da_links:
            da_links_href_list.append(_link.get('href'))
            da_links_name_list.append(_link.text)
            da_links_id_list.append(result_dict['Application ID'][0])
            da_links_address_list.append(result_dict['Address'][0])
        result_link_dict['id'] = da_links_id_list
        result_link_dict['Address'] = da_links_address_list
        result_link_dict['name'] = da_links_name_list
        result_link_dict['href'] = da_links_href_list
        result_link_pd = pd.DataFrame(result_link_dict)
        results_link_df = pd.concat([results_link_df, result_link_pd])

    results_df.to_csv('./data/duplex_list.csv')
    results_df.to_parquet('./data/duplex_list.parquet', allow_truncated_timestamps=True)

    results_link_df.to_csv('./data/duplex_href_list.csv')
    results_link_df.to_parquet('./data/duplex_href_list.parquet', allow_truncated_timestamps=True)
    print('bye')
    pass


def download_links():
    href_pd = pd.read_parquet('./data/duplex_href_list.parquet')
    href_pd_len = len(href_pd)
    for _link in href_pd[['href', 'name']].iterrows():
        name = _link[1]['name']
        href = _link[1]['href']
        print(f'{href_pd_len}: {name}')
        href_pd_len -= 1
        r = requests.get(href, allow_redirects=True)
        print(f'{r.status_code}')
        with open(f'./data/{name}.pdf', 'wb') as _f:
            _f.write(r.content)
    pass

if __name__ == '__main__':
    # hello()
    download_links()
    # pd.read_csv('./data/duplex_list.csv').to_parquet('./data/duplex_list.parquet')