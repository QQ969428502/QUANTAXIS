
import QUANTAXIS as QA
import numpy as np
import pandas as pd

# import sys,warnings
# print(sys.warnoptions)
# warnings.simplefilter("error", RuntimeWarning) 

user = QA.QA_User(username='zpf', password='zpf')
portfolio = user.new_portfolio('joinquant-yichuang')
Account=portfolio.get_account_by_cookie('test')
Broker = QA.QA_BacktestBroker()

Performance=QA.QA_Performance(Account)

print(Performance.message)