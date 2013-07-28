#!/usr/bin/env python

import argparse
import collections
import itertools
import sys
import time

import cloud
import numpy as np
import pandas as pd
import requests
import twitter

from lxml import etree


SECTOR_URL = 'http://biz.yahoo.com/p/sum_conameu.html'
INDUSTRY_URL = 'http://biz.yahoo.com/p/%(id)sconameu.html'
KEYSTATS_URL = 'http://finance.yahoo.com/q/ks?s=%(ticker)s'

EXCLUDED_INDUSTRIES = ('Accident & Health Insurance',
                       'Asset Management',
                       'Closed-End Fund - Debt',
                       'Closed-End Fund - Equity',
                       'Closed-End Fund - Foreign',
                       'Credit Services',
                       'Diversified Investments',
                       'Diversified Utilities',
                       'Electric Utilities',
                       'Foreign Money Center Banks',
                       'Foreign Regional Banks',
                       'Foreign Utilities',
                       'Gas Utilities',
                       'Independent Oil & Gas',
                       'Investment Brokerage - Regional',
                       'Investment Brokerage - Regional',
                       'Money Center Banks',
                       'Mortgage Investment',
                       'Regional - Mid-Atlantic Banks',
                       'Regional - Midwest Banks',
                       'Regional - Northeast Banks',
                       'Regional - Pacific Banks',
                       'Regional - Southeast Banks',
                       'Regional - Southwest Banks',
                       'Water Utilites')


Pipeline = collections.namedtuple('Pipeline',
                                 ('industry', 'ticker', 'keystats'))


def publish_to_twitter(df, prefix='MF', **kwargs):
    api = twitter.Api(**kwargs)
    msg = ' '.join(['$%s' % s for s in df.keys()])
    msg = '%s: %s' % (prefix, msg)
    return api.PostUpdate(msg)


def pairwise(iterable):
    args = [iter(iterable)] * 2
    return itertools.izip_longest(fillvalue=None, *args)


def get_industry_ids(exclude=None, include=None):
    r = requests.get(SECTOR_URL)
    tree = etree.HTML(r.text)
    ids = []
    sector = None

    xpath = '//table[@width=\"100%\"]//td[@bgcolor=\"ffffee\"]/a'
    for link in tree.xpath(xpath):
        industry = link[0].text.replace('\n', ' ').strip()
        if exclude is not None and industry in exclude:
            continue

        if include is not None and sector not in include:
            continue

        industry_id = link.get('href').strip('conameu.html')
        ids.append(industry_id)

    return ids


def get_tickers_for_industry(industry_id):
    r = requests.get(INDUSTRY_URL % {'id': industry_id})
    tree = etree.HTML(r.text)
    tickers = []

    xpath = '//table[@width=\"100%\"]//td[@bgcolor=\"ffffee\"]'
    for td in tree.xpath(xpath)[2:]:
        links = td.xpath('.//a')
        if len(links) < 2:
            continue

        ticker = links[1].text.replace('\n', ' ').strip()
        if '.' in ticker:
            continue

        tickers.append(ticker)

    return tickers


def get_keystats(ticker):
    r = requests.get(KEYSTATS_URL % {'ticker': ticker})
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

        if value[-1] in ('B', 'b'):
            value = int(float(value[:-1].replace(',', '')) * 1000000000)

        elif value[-1] in ('M', 'm'):
            value = int(float(value[:-1].replace(',', '')) * 1000000)

        elif value[-1] in ('K', 'k'):
            value = int(float(value[:-1].replace(',', '')) * 1000)

        elif '%' in value:
            value = value.replace('%', '').replace(',', '')
            value = float(value) / 100.00

        elif value.lower() == 'n/a':
            value = np.nan

        else:
            try:
                value = float(value)
            except:
                pass

        keystats[name] = value

    return (ticker, keystats)


def setup_pipeline():
    industry = cloud.queue.get('industry')
    ticker = cloud.queue.get('ticker')
    keystats = cloud.queue.get('keystats')

    industry.attach(get_tickers_for_industry, output_queues=[ticker],
                    iter_output=True, readers_per_job=5,
                    max_parallel_jobs=10, _env='MagicFormula', _type='s1')

    ticker.attach(get_keystats, output_queues=[keystats],
                  readers_per_job=5, max_parallel_jobs=10,
                  _env='MagicFormula', _type='s1')

    return Pipeline(industry, ticker, keystats)


def collect_pipeline(pipeline, output, prime_time=15):
    # NOTE(jkoelker) Give the queues some time to fill up
    time.sleep(prime_time)

    items = []
    keys = set()

    def collect_values():
        for k, v in output.pop():
            if k in keys:
                continue
            keys.add(k)
            items.append((k, v))

    while sum(q.count() for q in pipeline):
        collect_values()

    while output.count():
        collect_values()

    return pd.DataFrame.from_items(items)


def rank_stocks(df, min_market_cap=30000000):
    roa_key = 'Return on Assets (ttm)'
    pe_key = 'Trailing P/E'
    roa_rank = 'Return Rank'
    pe_rank = 'P/E Rank'
    market_cap = 'Market Cap'

    df = df[(df[roa_key] >= 0.25) &
            (df[pe_key] >= 5) &
            (df[market_cap] >= min_market_cap)]

    df[pe_rank] = df[pe_key].rank(method='min')
    df[roa_rank] = df[roa_key].rank(method='min', ascending=0)
    return df.sort_index(by=[pe_rank, roa_rank], ascending=[1, 1])


def get_stocks():
    queues = setup_pipeline()
    queues.industry.push(get_industry_ids(exclude=EXCLUDED_INDUSTRIES))
    return collect_pipeline(queues, queues.keystats)


def predict(num_stocks, **twitter):
    stocks = get_stocks()
    rank = rank_stocks(stocks.T)
    return publish_to_twitter(rank[:num_stocks].T, **twitter)


def main():
    parser = argparse.ArgumentParser(description='Run MagicFormula Prediction')
    parser.add_argument('-k', '--consumer-key',
                        required=True,
                        help='Twitter application consumer key')
    parser.add_argument('-s', '--consumer-secret',
                        required=True,
                        help='Twitter application consumer secret')
    parser.add_argument('-K', '--access-token-key',
                        required=True,
                        help='Twitter User access token key')
    parser.add_argument('-S', '--access-token-secret',
                        required=True,
                        help='Twitter User access token secret')
    parser.add_argument('-n', '--num_stocks',
                        default=15,
                        type=int,
                        help='Number of stocks to publish')
    args = parser.parse_args()
    twitter = {'consumer_key': args.consumer_key,
               'consumer_secret': args.consumer_secret,
               'access_token_key': args.access_token_key,
               'access_token_secret': args.access_token_secret}

    if predict(args.num_stocks, **twitter):
        return 0

    return 1


if __name__ == '__main__':
    sys.exit(main())
