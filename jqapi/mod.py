# -*- coding: utf-8 -*-
#
# Copyright 2017 Ricequant, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import copy

import pandas as pd
import numpy as np

from rqalpha.interface import AbstractMod
from rqalpha.environment import Environment
from rqalpha.events import EVENT
from rqalpha.api import *

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.query import *
from sqlalchemy import Column, String, Integer, Float
from sqlalchemy.orm import *
from sqlalchemy import func

from mapping import *
from utils import *


# todo: 可配置
engine=create_engine('oracle://CJHJDM:CJHJDM@172.16.48.205:1521/cjhjdm', echo=False) 
#engine=create_engine('sqlite://///home/db_fund', echo=False) 
Session = sessionmaker(bind=engine)
session = Session()
    
    
# 由于oracle列名限制，做此映射
column_map = {
'reinsurance_contract_reserves_':'reinsurance_contract_reserves_receivable',
'receivings_from_vicariously_so':'receivings_from_vicariously_sold_securities',
'non_current_liability_in_one_y':'non_current_liability_in_one_year',
'foreign_currency_report_conv_d':'foreign_currency_report_conv_diff',
'goods_sale_and_service_render_':'goods_sale_and_service_render_cash',
'net_borrowing_from_central_ban':'net_borrowing_from_central_bank',
'net_cash_received_from_reinsur':'net_cash_received_from_reinsurance_business',
'handling_charges_and_commissio':'handling_charges_and_commission',
'fix_intan_other_asset_dispo_ca':'fix_intan_other_asset_dispo_cash',
'fix_intan_other_asset_acqui_ca':'fix_intan_other_asset_acqui_cash',
'proceeds_from_sub_to_mino_s':'proceeds_from_sub_to_mino_s',
'withdraw_insurance_contract_re':'withdraw_insurance_contract_reserve',
'disposal_loss_non_current_liab':'disposal_loss_non_current_liability',
'operation_profit_to_total_reve':'operation_profit_to_total_revenue',
'operating_expense_to_total_rev':'operating_expense_to_total_revenue',
'financing_expense_to_total_rev':'financing_expense_to_total_revenue',
'goods_sale_and_service_to_reve':'goods_sale_and_service_to_revenue',
'inc_operation_profit_year_on_y':'inc_operation_profit_year_on_year',
'inc_net_profit_to_shareholders':'inc_net_profit_to_shareholders_year_on_year',
'inc_net_profit_to_shareholder2':'inc_net_profit_to_shareholders_annual',
'pubdate':'pubDate',
'statdate':'statDate',
}
    

def get_fundamentals(query_object, date=None, statDate=None):
    """
        完全按照聚宽实现，参见：https://www.joinquant.com/api#get_fundamentals
    """
         
    if date is not None and statDate is not None:
        raise TypeError()

    if date is not None:
        date = parse_date(date)                       
        
    if statDate is not None:            
        statDate = parse_statdate(statDate)
        # debug
        #print (statDate)
    
    # 如果都为None，取date
    if date is None and statDate is None:
        #date = dt.date.today()-dt.timedelta(1)
        #print date
        today_str = dt.date.today()
        date = get_previous_trading_date(today_str)#-dt.timedelta(6)
        #减小查询量 只查询指定日期一年内    
        left_date = dt.date.today()-dt.timedelta(365)
    
    #减小查询量 只查询指定日期一年内    
    if date is not None:
        left_date = date-dt.timedelta(365)
    #print date
    # add filter
    q = query_object        
    first_entity = None
    visited_entity = []
    for des in query_object.column_descriptions:
        entity = des['entity']
        if entity in visited_entity:
            continue
        
        # filt code
        if first_entity is None:
            first_entity = entity
        else:
            q = q.filter(first_entity.code == entity.code)
        
        # filt date
        if entity == valuation:
            if statDate is not None:
                q = q.filter(entity.day == statDate)
            else:
                #q1 = Query([entity.code, func.max(entity.day).label('day')]).filter(entity.day<=date,entity.day>=left_date).group_by(entity.code).subquery()
                #q1 = Query([entity.code, func.max(entity.day).label('day')]).filter(entity.day<=date).group_by(entity.code).subquery()
                #q = q.filter(entity.day <= date).filter(entity.day == q1.c.day, entity.code == q1.c.code)                
                q = q.filter(entity.day == date)                
        elif entity in [balance,cash_flow,income,indicator,lico_fn_sigquafina]:                        
            if statDate is not None:
                q = q.filter( entity.statDate == statDate )
            else:
                q1 = Query([entity.code, func.max(entity.pubDate).label('pubDate')]).filter(entity.pubDate<=date,entity.pubDate>=left_date).group_by(entity.code).subquery()                
                #q1 = Query([entity.code, func.max(entity.pubDate).label('pubDate')]).filter(entity.pubDate<=date).group_by(entity.code).subquery()                
                q = q.filter(entity.pubDate <= date, entity.pubDate>=left_date).filter(entity.pubDate == q1.c.pubDate, entity.code == q1.c.code)                
                #q = q.filter(entity.pubDate <= date).filter(entity.pubDate == q1.c.pubDate, entity.code == q1.c.code)                
        else:
            raise TypeError()
            
        visited_entity.append(entity)
                
    # read from oracle
    df = pd.read_sql_query(q.statement, engine)
    
    # 由于oracle列名限制，这里做下转换
    df.rename(columns=column_map, inplace=True)

    # 时间转为字符串
    date_col_list = ['day', 'pubDate', 'statDate']
    df_col_list = list(df.columns)
    date_to_str_set = set(date_col_list) & set(df_col_list)
    for col in date_to_str_set:
        df[col] = df[col].apply(lambda x: x.strftime("%Y-%m-%d"))

    return df
    
    
def query(*args, **kwargs):
    return Query(args)

    
def history(count, unit='1d', field='avg', security_list=None, df=True, skip_paused=False, fq='pre'):
    """
        NOTICE:
            米宽接口不支持fq参数
            米宽接口多了include_now参数，取默认值
    """
    
    if security_list is None:
        return None
    
    if df:
        res = pd.DataFrame()
    else:
        res = {}
                
    for s in security_list:
        res[s] = history_bars(s, count, unit, field, skip_paused)    
    return res
    

def get_security_info(code):
        
    #todo 不完善
    code_type_map = {
    'CS':'stock',
    'INDEX':'index',
    'ETF':'etf',
    'FenjiA':'fja',
    'FenjiB':'fjb'       
    }
    
    inst = instruments(code)
    inst.display_name = inst.symbol
    inst.name = inst.abbrev_symbol
    inst.start_date = inst.listed_date.date()
    inst.end_date = inst.de_listed_date.date()
    inst.type = code_type_map[inst.type]
    inst.parent = None  #todo 米宽没有
    
    return inst
    
    
def get_extras(info, security_list, start_date='2015-01-01', end_date='2015-12-31', df=True, count=None):
    """
        NOTICE:
            只支持is_st
            日期暂不支持
        TODO：
            可以考虑用其他数据源，比如datayes，完全适配聚宽接口
    """
    
    if info != 'is_st':
        raise TypeError()
        
    if df:
        res = pd.DataFrame()
    else:
        res = {}
        
    for sec in security_list:
        res[sec] = np.array([is_st_stock(sec)])
        
    return res
    
    
def get_industry_stocks(industry_code, date=None):
    """
        NOTICE: 
            米宽没有日期参数
            只支持证监会行业分类，不能使用聚宽分类
        TODO：
            考虑用其他数据源
    """
    return industry(industry_code)    
        
    
def get_current_data():
    pass
    
    
    
class JqapiMod(AbstractMod):
    def start_up(self, env, mod_config):                    
        
        from rqalpha.api.api_base import register_api
        # api
        register_api('get_fundamentals', get_fundamentals)
        register_api('query', query)
        register_api('history', history)
        register_api('get_security_info', get_security_info)
        register_api('get_industry_stocks', get_industry_stocks)
        register_api('get_extras', get_extras)
        # model
        register_api('income', income)
        register_api('balance', balance)
        register_api('cash_flow', cash_flow)
        register_api('indicator', indicator)
        register_api('valuation', valuation)        
        register_api('lico_fn_sigquafina', lico_fn_sigquafina)        
               
    def tear_down(self, code, exception=None):
        pass
        
