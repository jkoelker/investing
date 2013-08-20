#!/usr/bin/env python

import argparse
import sys

import MySQLdb
import pandas as pd
import twitter


def publish_to_twitter(df, prefix='MF', api=None, **kwargs):
    if api is None:
        api = twitter.Api(**kwargs)

    msg = ' '.join(['$%s' % s for s in df.T.index])
    msg = '%s: %s' % (prefix, msg)

    if len(msg) > 140:
        return publish_to_twitter(df[:-1], prefix, api, **kwargs)

    return api.PostUpdate(msg)


def rank_stocks(df):
    roa_key = 'roa_ttm'
    pe_key = 'trailing_pe'
    roa_rank = 'return_rank'
    pe_rank = 'pe_rank'

    df[pe_rank] = df[pe_key].rank(method='min')
    df[roa_rank] = df[roa_key].rank(method='min', ascending=0)
    return df.sort_index(by=[pe_rank, roa_rank], ascending=[1, 1])


def get_stocks(db_kwargs):
    qry = """
    SELECT t.ticker, f.*, MAX(f.refresh_dt)
    FROM fundamentals f, tickers t
    WHERE f.ticker_id = t.id
    AND f.refresh_dt BETWEEN DATE_SUB(NOW(), INTERVAL 1 WEEK) AND NOW()
    AND t.sector NOT IN ('Financial', 'Utilities')
    AND t.industry NOT IN ('Independent Oil & Gas',
                           'Major Integrated Oil & Gas',
                           'Oil & Gas Drilling & Exploration'
                           'Oil & Gas Equipment & Services',
                           'Oil & Gas Pipelines',
                           'Oil & Gas Refining & Marketing')
    AND f.roa_ttm >= 0.25
    AND f.trailing_pe >= 5
    AND f.market_cap >= 30000000
    GROUP BY f.ticker_id
    """
    conn = MySQLdb.connect(**db_kwargs)
    df = pd.read_sql(qry, conn, index_col='ticker')
    conn.close()
    return df


def predict(num_stocks, db_kwargs, twitter_kwargs):
    stocks = get_stocks(db_kwargs)
    rank = rank_stocks(stocks)
    return publish_to_twitter(rank[:num_stocks].T, **twitter_kwargs)


def main():
    parser = argparse.ArgumentParser(description='Run MagicFormula Prediction',
                                     add_help=False)
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

    db_kwargs = {'host': args.host,
                 'user': args.user,
                 'passwd': args.password,
                 'db': args.database}

    twitter_kwargs = {'consumer_key': args.consumer_key,
                      'consumer_secret': args.consumer_secret,
                      'access_token_key': args.access_token_key,
                      'access_token_secret': args.access_token_secret}

    if predict(args.num_stocks, db_kwargs, twitter_kwargs):
        return 0

    return 1


if __name__ == '__main__':
    sys.exit(main())
