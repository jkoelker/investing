#!/usr/bin/env python

import argparse
import collections
import datetime
import itertools
import random
import sys

import cloud
import MySQLdb
import parsedatetime as pdt
import requests

from lxml import etree


MIN_DELAY = 20
MAX_DELAY = 120
MAX_RETRIES = 3
MIN_RETRY_DELAY = MIN_DELAY * 2
MAX_RETRY_DELAY = min(900, MAX_DELAY * 2)

SECTOR_URL = 'http://biz.yahoo.com/p/sum_conameu.html'
INDUSTRY_URL = 'http://biz.yahoo.com/p/%(id)sconameu.html'
KEYSTATS_URL = 'http://finance.yahoo.com/q/ks?s=%(ticker)s'
KEY_MAP = {'Forward P/E': 'fw_pe',
           'Total Cash (mrq)': 'cash_mrq',
           'Shares Outstanding': 'shares_out',
           'Forward Annual Dividend Rate': 'fw_ann_div_rate',
           'Trailing Annual Dividend Yield': 'trailing_annual_div_yield',
           'Book Value Per Share (mrq)': 'bk_val_per_share_mrq',
           '52-Week Low': '52w_low',
           'PEG Ratio': 'peg_ratio',
           'Payout Ratio': 'payout_ratio',
           'Profit Margin (ttm)': 'profit_margin_ttm',
           'Price/Book (mrq)': 'price_per_book_mrq',
           'Price/Sales (ttm)': 'price_per_sales_ttm',
           'Last Split Date': 'last_split_date',
           'Shares Short': 'shares_short',
           '50-Day Moving Average': '50d_mavg',
           'Enterprise Value/EBITDA (ttm)': 'ent_val_ebita_ttm',
           'Ex-Dividend Date': 'ex_div_date',
           'Market Cap': 'market_cap',
           'Diluted EPS (ttm)': 'dil_eps_ttm',
           'Forward Annual Dividend Yield': 'fw_ann_div_yld',
           'EBITDA (ttm)': 'ebita_ttm',
           'Beta': 'beta',
           'Revenue (ttm)': 'revenue_ttm',
           'Short Ratio': 'short_ratio',
           'Total Debt (mrq)': 'debt_mrq',
           '5 Year Average Dividend Yield': '5y_avg_div_yld',
           'Operating Margin (ttm)': 'op_margin_ttm',
           'Current Ratio (mrq)': 'cur_ratio_mrq',
           'Enterprise Value/Revenue (ttm)': 'ent_val_rev_ttm',
           'Trailing P/E': 'trailing_pe',
           'Operating Cash Flow (ttm)': 'op_cash_flow_ttm',
           'Return on Equity (ttm)': 'roe_ttm',
           'Prior Month Shares Short': 'prior_m_shares_short',
           'S&P500 52-Week Change': 'sp_52w_change',
           'Short Percentage of Float': 'short_percent_of_float',
           '52-Week High': '52w_high',
           'Levered Free Cash Flow (ttm)': 'levered_free_cash_flow_ttm',
           'Last Split Factor': 'last_split_factor',
           'Revenue Per Share (ttm)': 'revenue_per_share_ttm',
           'Gross Profit (ttm)': 'gross_profit_ttm',
           'Percentage Held by Institutions': 'inst_percent',
           '10 day Avg Vol': '10d_avg_vol',
           'Enterprise Value': 'ent_val',
           '200-Day Moving Average': '200d_mavg',
           'Return on Assets (ttm)': 'roa_ttm',
           'Fiscal Year Ends': 'fsc_year_end',
           'Total Cash Per Share (mrq)': 'cash_per_share_mrq',
           'Total Debt/Equity (mrq)': 'debt_to_equity_mrq',
           'Dividend Date': 'div_date',
           'Float': 'float',
           'Most Recent Quarter (mrq)': 'mrq',
           'Percentage Held by Insiders': 'insider_percent',
           'Qtrly Earnings Growth (yoy)': 'qtrly_earnings_growth_yoy',
           'Net Income Avl to Common (ttm)': 'net_inc_avl_to_com_ttm',
           '52-Week Change': '52w_change',
           'Qtrly Revenue Growth (yoy)': 'qtrly_revenue_growth_yoy',
           '3 month Avg Vol': '3m_avg_vol'}

DATE_MAP = ['div_date', 'ex_div_date', 'fsc_year_end', 'last_split_date',
            'mrq']


class DatabaseHandler(object):
    def __init__(self, host, user, password, database, table,
                 output_queues=None):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.table = table

        if output_queues is None:
            output_queues = []

        self.output_queues = output_queues

    def pre_handling(self):
        self.db = MySQLdb.connect(host=self.host,
                                  user=self.user,
                                  passwd=self.password,
                                  db=self.database)

    def message_handler(self, msg):
        for output_queue in self.output_queues:
            if not isinstance(msg, collections.Iterable):
                msg = [msg]
            output_queue.push(msg, delay=random.randint(MIN_DELAY,
                                                        MAX_DELAY))

    def post_handling(self):
        self.db.close()


class TickerHandler(DatabaseHandler):
    def message_handler(self, tickers):
        cursor = self.db.cursor()
        fields = ('1d_price_change', 'market_cap', 'trailing_pe', 'roe_ttm',
                  'trailing_annual_div_yield', 'debt_to_equity_mrq',
                  'price_per_book_mrq')

        fields_str = ', '.join(['sector_%s' % f for f in fields] +
                               ['industry_%s' % f for f in fields])
        fields_inst_str = ', '.join(['%%(%s)s' % ('sector_%s' % f)
                                     for f in fields] +
                                    ['%%(%s)s' % ('industry_%s' % f)
                                     for f in fields])
        qry = ' '.join(['INSERT INTO %s' % self.table,
                        '(ticker, sector, industry, %s)' % fields_str,
                        'VALUES (%(ticker)s, %(sector)s, %(industry)s,',
                        '%s)' % fields_inst_str,
                        'ON DUPLICATE KEY UPDATE',
                        'last_seen = NOW(),',
                        'sector = %(sector)s,',
                        'industry = %(industry)s,',
                        ', '.join(['%s = %%(%s)s' % (('sector_%s' % f,) * 2)
                                   for f in fields]) + ',',
                        ', '.join(['%s = %%(%s)s' % (('industry_%s' % f,) * 2)
                                   for f in fields])])

        cursor.executemany(qry, tickers)
        self.db.commit()
        cursor.close()
        DatabaseHandler.message_handler(self, tickers)


class KeystatsHandler(DatabaseHandler):
    def message_handler(self, ticker):
        values = dict((KEY_MAP[k],
                       v if KEY_MAP[k] not in DATE_MAP else get_date(v))
                      for k, v in ticker['keystats'].iteritems()
                      if KEY_MAP.get(k))

        fields = ('10d_avg_vol', '200d_mavg', '3m_avg_vol',
                  '5y_avg_div_yld', '50d_mavg', '52w_change',
                  '52w_high', '52w_low', 'beta', 'bk_val_per_share_mrq',
                  'cur_ratio_mrq', 'dil_eps_ttm', 'div_date', 'ebita_ttm',
                  'ent_val', 'ent_val_ebita_ttm', 'ent_val_rev_ttm',
                  'ex_div_date', 'fsc_year_end', 'float',
                  'fw_ann_div_rate', 'fw_ann_div_yld', 'fw_pe',
                  'gross_profit_ttm', 'last_split_date',
                  'last_split_factor', 'levered_free_cash_flow_ttm',
                  'market_cap', 'mrq', 'net_inc_avl_to_com_ttm',
                  'op_cash_flow_ttm', 'op_margin_ttm', 'peg_ratio',
                  'payout_ratio', 'insider_percent', 'inst_percent',
                  'price_per_book_mrq', 'price_per_sales_ttm',
                  'prior_m_shares_short', 'profit_margin_ttm',
                  'qtrly_earnings_growth_yoy', 'qtrly_revenue_growth_yoy',
                  'roa_ttm', 'roe_ttm', 'revenue_ttm',
                  'revenue_per_share_ttm', 'sp_52w_change',
                  'shares_out', 'shares_short', 'short_percent_of_float',
                  'short_ratio', 'cash_mrq', 'cash_per_share_mrq',
                  'debt_mrq', 'debt_to_equity_mrq',
                  'trailing_annual_div_yield', 'trailing_pe')

        fields_str = ', '.join(['`%s`' % f for f in fields])
        cursor = self.db.cursor()
        qry = ' '.join(['INSERT INTO %s' % self.table,
                        '(`ticker_id`, %s)' % fields_str,
                        '\nSELECT', 'tickers.id,',
                        ', '.join(['%%(%s)s' % f for f in fields]),
                        'FROM tickers WHERE',
                        'ticker = "%s"' % ticker['ticker']])

        cursor.execute(qry, values)
        self.db.commit()
        cursor.close()
        DatabaseHandler.message_handler(self, ticker)


def get_date(s):
    if not isinstance(s, basestring):
        return s
    time_tpl = pdt.Calendar().parse(s)[0][:7]
    return datetime.datetime(*time_tpl)


def pairwise(iterable):
    args = [iter(iterable)] * 2
    return itertools.izip_longest(fillvalue=None, *args)


def numify(value):
    if not value:
        return

    if value[-1] in ('B', 'b'):
        value = int(float(value[:-1].replace(',', '')) * 1000000000)

    elif value[-1] in ('M', 'm'):
        value = int(float(value[:-1].replace(',', '')) * 1000000)

    elif value[-1] in ('K', 'k'):
        value = int(float(value[:-1].replace(',', '')) * 1000)

    elif '%' in value:
        value = value.replace('%', '').replace(',', '')
        value = float(value) / 100.00

    elif value.lower() in ('n/a', 'nan', 'na'):
        value = None

    else:
        try:
            value = float(value)
        except:
            value = value.replace(',', '')
    return value


def get_industry_ids():
    r = requests.get(SECTOR_URL)
    tree = etree.HTML(r.text)
    xpath = '//table[@width=\"100%\"]//td[@bgcolor=\"ffffee\"]/a'
    return [int(l.get('href').strip('conameu.html'))
            for l in tree.xpath(xpath)]


def get_tickers_for_industry(industry_id):
    r = requests.get(INDUSTRY_URL % {'id': industry_id})
    tree = etree.HTML(r.text)
    tickers = []
    fields = ('1d_price_change', 'market_cap', 'trailing_pe', 'roe_ttm',
              'trailing_annual_div_yield', 'debt_to_equity_mrq',
              'price_per_book_mrq')

    def get_stats(prefix, td):
        ret = {}
        for i, cell in enumerate(td.getparent().xpath('.//td')[1:-2]):
            key = '%s_%s' % (prefix, fields[i])
            ret[key] = numify(cell.xpath('.//font')[0].text)

        return ret

    xpath = '//table[@width=\"100%\"]//td[@bgcolor=\"ffffee\"]'
    cells = tree.xpath(xpath)

    sector_td, industry_td = cells[:2]

    sector = sector_td.xpath('.//a')[0].text
    industry = industry_td.xpath('.//font')[1].text

    industry = industry.replace('\n', ' ')
    industry = industry.strip().strip('(').strip()

    sector_data = get_stats('sector', sector_td)
    industry_data = get_stats('industry', industry_td)

    for td in cells[2:]:
        links = td.xpath('.//a')
        if len(links) < 2:
            continue

        ticker = links[1].text.replace('\n', ' ').strip()
        if '.' in ticker:
            continue

        data = {'sector': sector,
                'sector_data': sector_data,
                'industry': industry,
                'industry_data': industry_data,
                'ticker': ticker}
        data.update(sector_data)
        data.update(industry_data)
        tickers.append(data)

    # NOTE(jkoelker) If no tickers are found return None so it won't be
    #                pushed into a queue
    if not tickers:
        return

    return tickers


def get_keystats(ticker):
    r = requests.get(KEYSTATS_URL % ticker)

    tree = etree.HTML(r.text)
    keystats = {}

    xpath = ('//table[@class="yfnc_datamodoutline1"]/tr/td/table/tr'
             '/td[@class="yfnc_tabledata1" or @class="yfnc_tablehead1"]')

    for name, value in pairwise(tree.xpath(xpath)):
        if len(value) > 0:
            value = value.xpath('.//span')[0]

        name = name.text.strip()
        name = name.replace(':', '')
        name = name.replace('\n', ' ')
        name = name.replace('%', 'Percentage')

        if '(' in name:
            attrs = name.strip(')').split('(')
            if '(ttm)' in name or '(mrq)' in name or '(yoy)' in name:
                name = name

            elif '3 month' in name:
                name = '3 month ' + attrs[0]

            elif '10 day' in name:
                name = '10 day ' + attrs[0]

            elif 'prior month' in name:
                name = 'Prior Month ' + attrs[0]

            else:
                name = attrs[0]

        name = str(name.strip())
        value = value.text.strip()
        value = numify(value)

        keystats[name] = value

    # NOTE(jkoelker) If no keystats are found return None so it won't be
    #                pushed into a queue
    if not keystats:
        return

    ticker['keystats'] = keystats

    return ticker


def main():
    parser = argparse.ArgumentParser(description='Scrape yahoo for tickers',
                                     add_help=False)
    parser.add_argument('-h', '--host',
                        required=True,
                        help='MySQL host')
    parser.add_argument('-u', '--user',
                        required=True,
                        help='MySQL User')
    parser.add_argument('-p', '--password',
                        required=True,
                        help='MySQL password')
    parser.add_argument('database',
                        help='Database to store tickers in')
    parser.add_argument('--help',
                        action='help', default=argparse.SUPPRESS,
                        help='show this help message and exit')
    args = parser.parse_args()
    db_args = (args.host, args.user, args.password, args.database)

    input_q = cloud.queue.get('tickers-input')
    ticker_q = cloud.queue.get('tickers-tickers')
    fetch_q = cloud.queue.get('tickers-fetch-keystats')
    keystats_q = cloud.queue.get('tickers-keystats')

    input_q.attach(get_tickers_for_industry, output_queues=[ticker_q],
                   readers_per_job=2, max_parallel_jobs=4,
                   retry_on=[requests.RequestException],
                   retry_delay=random.randint(MIN_RETRY_DELAY,
                                              MAX_RETRY_DELAY),
                   max_retries=MAX_RETRIES,
                   _env='investing', _type='s1')

    ticker_q.attach(TickerHandler(*db_args, table='tickers',
                                  output_queues=[fetch_q]),
                    readers_per_job=2, max_parallel_jobs=1)

    fetch_q.attach(get_keystats, output_queues=[keystats_q],
                   readers_per_job=2, max_parallel_jobs=10,
                   retry_on=[requests.RequestException],
                   retry_delay=random.randint(MIN_RETRY_DELAY,
                                              MAX_RETRY_DELAY),
                   max_retries=MAX_RETRIES,
                   _env='investing', _type='s1')

    keystats_q.attach(KeystatsHandler(*db_args, table='fundamentals'),
                      readers_per_job=2, max_parallel_jobs=1,
                      _env='investing', )

    for industry_id in get_industry_ids():
        input_q.push([industry_id], delay=random.randint(MIN_DELAY,
                                                         MAX_DELAY))

    return 0


if __name__ == '__main__':
    try:
        ret = main()
    except:
        ret = 69
    sys.exit(ret)
