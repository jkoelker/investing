#!/usr/bin/env python

import argparse
import sys

import eoddata
import numpy as np
import pandas as pd
import twitter


EXCLUDED_SECTORS = ('Finance - Savings and Loans',
                    'Closed-End Investment Bond Funds',
                    'Banks and Finance')
EXCLUDED_INDUSTRIES = ('Power Generation',  # Utilites
                       'Electric Utilities: Central',
                       'Savings Institutions',  # Finantial
                       'Accident &Health Insurance',
                       'Finance Companies',
                       'Finance/Investors Services',
                       'Commercial Banks',
                       'Closed-End Fund - Foreign',
                       'Closed-End Fund - Equity',
                       'Water Supply',
                       'Finance: Consumer Services',
                       'Major Banks',
                       'Investment Bankers/Brokers/Service',
                       'Diversified Financial Services',
                       'Savings & Loans',
                       'Credit Services',
                       'Diversified Investments',
                       'Financial Services',
                       'Banks',
                       'Regional - Pacific Banks',
                       'Regional - Mid-Atlantic Banks',
                       'Real Estate Investment Trusts',
                       'Investment Managers',
                       'Life Insurance',
                       'Property-Casualty Insurers',
                       'Closed-End Fund - Debt',
                       'Specialty Insurers',
                       'Oil & Gas Production',  # Oil and gass
                       'Integrated oil Companies',
                       'Oil Refining/Marketing',
                       'Oil/Gas Transmission',
                       'Oilfield Services/Equipment',
                       'Independent Oil & Gas',
                       'Natural Gas Distribution')


def publish_to_twitter(df, prefix='MF', api=None, **kwargs):
    if api is None:
        api = twitter.Api(**kwargs)

    msg = ' '.join(['$%s' % s for s in df.T.index])
    msg = '%s: %s' % (prefix, msg)

    if len(msg) > 140:
        return publish_to_twitter(df[:-1], prefix, api, **kwargs)

    return api.PostUpdate(msg)


def rank_stocks(df):
    df['roc_rank'] = df['roc'].rank(method='max', ascending=0)
    df['yield_rank'] = df['yield'].rank(method='max', ascending=0)
    df['pe_rank'] = df['pe'].rank(method='min', ascending=1)
    return df.sort_index(by=['pe_rank', 'roc_rank'],
                         ascending=[1, 1])


def get_stocks(eod_kwargs):
    client = eoddata.Client(**eod_kwargs)
    fundamentals = {}

    for exchange in ('NYSE', 'NASDAQ'):
        fundamentals.update(client.fundamentals(exchange))

    df = pd.DataFrame(fundamentals).T

    for col in ('market_cap', 'pt_b', 'ebitda', 'pe', 'yield'):
        df[col] = df[col].astype(np.float64)

    df = df[df['market_cap'] >= 50000000]
    df = df[df['pe'] > 5]
    df = df[df['pe'] < 25]
    df = df[df['yield'] > 6]
    df = df[df['sector'].map(lambda x: x not in EXCLUDED_SECTORS)]
    df = df[df['industry'].map(lambda x: x not in EXCLUDED_INDUSTRIES)]
    df = df[df['description'].map(lambda x: 'energy' not in x.lower())]
    df = df[df['description'].map(lambda x: 'financ' not in x.lower())]
    df = df[df['description'].map(lambda x: 'invest' not in x.lower())]
    df = df[df['description'].map(lambda x: 'bank' not in x.lower())]
    df = df[df['description'].map(lambda x: 'banc' not in x.lower())]
    df = df[df['description'].map(lambda x: 'equity' not in x.lower())]
    df = df[df['description'].map(lambda x: not x.lower().endswith(' ads'))]
    df = df[df['description'].map(lambda x: not x.lower().endswith(' adr'))]
    df = df[df['description'].map(lambda x: not x.lower().endswith(' s.a.'))]

    df['assets'] = df['market_cap'] / df['pt_b']
    df['roc'] = df['ebitda'] / df['assets']

    df = df[df['roc'] > 0]

    return df


def predict(num_stocks, eod_kwargs, twitter_kwargs):
    stocks = get_stocks(eod_kwargs)
    rank = rank_stocks(stocks)
    return publish_to_twitter(rank[:num_stocks].T, **twitter_kwargs)


def main():
    description = 'Run MagicFormula Prediction'
    parser = argparse.ArgumentParser(description=description)
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
    parser.add_argument('-u', '--user',
                        required=True,
                        help='EOD Data User')
    parser.add_argument('-p', '--password',
                        required=True,
                        help='EOD Data password')

    args = parser.parse_args()

    eod_kwargs = {'username': args.user,
                  'password': args.password}

    twitter_kwargs = {'consumer_key': args.consumer_key,
                      'consumer_secret': args.consumer_secret,
                      'access_token_key': args.access_token_key,
                      'access_token_secret': args.access_token_secret}

    if predict(args.num_stocks, eod_kwargs, twitter_kwargs):
        return 0

    return 1


if __name__ == '__main__':
    sys.exit(main())
