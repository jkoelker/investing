#!/usr/bin/env python

import argparse
import collections
import itertools
import sys

import cloud
import numpy as np
import pandas as pd
import requests
import twitter

from lxml import etree


HTTP_TIMEOUT = 10
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


def chunk_list(lst, chunk_size):
    """split a lst into multiple lists of length chunk_size"""
    return [lst[i:i+chunk_size] for i in range(0, len(lst), chunk_size)]


def chunk_wrapper(func):
    """Return a function that processes ``func`` over a list of elements"""
    def wrapper_inner(chunked_args):
        return map(func, chunked_args)
    return wrapper_inner


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

        industry_id = int(link.get('href').strip('conameu.html'))
        ids.append(industry_id)

    return ids


def get_tickers_for_industry(industry_id):
    try:
        r = requests.get(INDUSTRY_URL % {'id': industry_id},
                         timeout=HTTP_TIMEOUT)
    except requests.exceptions.Timeout:
        return []

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
    try:
        r = requests.get(KEYSTATS_URL % {'ticker': ticker},
                         timeout=HTTP_TIMEOUT)
    except requests.exceptions.Timeout:
        return

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


def get_stocks(chunk_size=75):
    iter_chain = itertools.chain.from_iterable
    industry_ids = get_industry_ids(exclude=EXCLUDED_INDUSTRIES)
    jids = cloud.map(chunk_wrapper(get_tickers_for_industry),
                     chunk_list(industry_ids, chunk_size),
                     _env='MagicFormula', _type='s1', _label='Get Tickers',
                     _max_runtime=10)

    # NOTE(jkoelker) Somtimes scraping is iffy, so rather than block, we
    #                set the _max_runtime to two mins and allow errors in
    #                results, filtering them as they come in
    # NOTE(jkoelker) get_tickers_for_industry returns a list so we have to
    #                double unchunk it
    results = cloud.result(jids, ignore_errors=True)
    ticker_chunks = iter_chain(results)
    tickers = [t for t in iter_chain(ticker_chunks)
               if t or not isinstance(t, cloud.CloudException)]

    jids = cloud.map(chunk_wrapper(get_keystats),
                     chunk_list(tickers, chunk_size),
                     _env='MagicFormula', _type='s1', _label='Get Keystats',
                     _max_runtime=10)

    results = cloud.result(jids, ignore_errors=True)
    values = [v for v in iter_chain(results)
              if v or not isinstance(v, cloud.CloudException)]
    return pd.DataFrame(dict(values))


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
