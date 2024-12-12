#!/usr/bin/env python
'''
backtester.py:
交易驱动的模拟回测库，交易行为由该文件实现
'''
#%%
import pandas as pd
from datetime import datetime
#%%
class order:
    #number indexed dict O(1)
    #order_types = {0:'LIMIT'}
    def __init__(self, direction, ticker, price, shares, datetime):
        self.direction = direction
        self.ticker = ticker
        self.price = price
        self.shares = shares
        self.datetime = datetime #pd.datetime(time) in formal version
    
    def get_price(self):
        return self.price
    
    def get_shares(self):
        return self.shares
    
    def get_datetime(self, pd_datetime = False):
        if pd_datetime == True:
            return pd.datetime(self.datetime)
        else:
            return self.datetime
#%%
class backtest:
    def __init__(self, start_date = None, end_date = datetime.today(), start_portfolio = None, start_cash = 1000000, transaction_fee = 0):
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        if start_portfolio is None: start_portfolio = {}
        self.portfolio = start_portfolio
        self.start_cash = start_cash
        self.cash = start_cash
        self.transaction_fee = transaction_fee
    
    def buy(self, ticker, price, shares, date):
        portfolio = self.portfolio

        if ticker not in portfolio.keys():
            portfolio[ticker] = shares
        else:
            portfolio[ticker] = portfolio[ticker] + shares

        #if price*shares > self.cash: raise ValueError('Insufficent Cash. Trade canceled')
        self.cash -= price*shares*(1+self.transaction_fee)
        #if self.cash < 0: print(date, 'negative cash:', self.cash)
        #print(date, 'buy %s %0.2f shares at %0.2f'%(ticker, shares, price))

        return order('LONG', ticker, price, shares, date)
    
    def sell(self, ticker, price, shares, date):
        portfolio = self.portfolio
        
        #if ticker not in portfolio.keys():
         #   raise ValueError(ticker +' not in portfolio. Short not permitted. Trade canceled')
        #elif shares > portfolio[ticker]:
         #   raise ValueError(ticker +' trading amount exceeded holding. Short not permitted. Trade canceled')
        #else:
        if ticker not in portfolio.keys():
            portfolio[ticker] = shares*-1
        else:
            portfolio[ticker] = portfolio[ticker] - shares

        self.cash += price*shares*(1-self.transaction_fee)

        if(portfolio[ticker] == 0):
            portfolio.pop(ticker)
        #print(date, 'sell %s %0.2f shares at %0.2f'%(ticker, shares, price))

        return order('SHORT', ticker, price, shares, date)
    
    def eval_asset(self, ticker, price):
        if ticker not in self.portfolio.keys(): return 0
        return self.portfolio[ticker]*price
    
    def eval_all_asset(self, price_dict = None):
        if price_dict == None and len(self.portfolio) == 0: return self.cash
        elif price_dict == None and len(self.portfolio) != 0: raise Exception('No price passed')
        gross_amount = 0
        for ticker, shares in self.portfolio.items():
            try:
                gross_amount += shares * price_dict[ticker]
            except KeyError as e:
                print('----------------------')
                print(e, 'Price not passed. Not Evaluated')
                print(self.portfolio, price_dict)
                print('----------------------')
                #raise ValueError('WTF')
        return gross_amount+self.cash

class order_queue:
    def __init__(self):
        self.queue = []

    def pop_order(self):
        #取走前为空
        if len(self.queue) == 0:
            return None
        ret = self.queue[0]
        #取走后为空
        if len(self.queue) == 0:
            self.queue = []
        self.queue = self.queue[1:]

        return ret

    def add_order(self, order: object):
        self.queue.append(order)

    def show_orders(self):
        return tuple(self.queue)
# %%
##test
'''
test = backtest('2021-01-01')
test.eval_all_asset()
test.buy('FAKE', 1, 1000000)
#test.eval_all_asset(price_dict={'FAKE':1})
'''

'''
awaiting_orders = order_queue()
awaiting_orders.add_order(order('long', 'benchmark',1,1,'2000-01-01'))
awaiting_orders.add_order('no')
awaiting_orders.pop_order().get_datetime() == pd.to_datetime('2000-01-01')
#awaiting_orders.show_orders()
'''