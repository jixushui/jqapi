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
from sqlalchemy import or_

from mapping import *
from utils import *

import ConfigParser
conf = ConfigParser.ConfigParser()
conf.read("/home/workspace/rqalpha/general.conf")
database_path= conf.get("database", "path")
print database_path
# todo: 可配置
#engine=create_engine('oracle://CJHJDM:CJHJDM@172.16.48.205:1521/cjhjdm', echo=False) 
engine=create_engine(database_path, echo=False) 
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
        #减小查询量 只查询指定日期一年内    
        date = parse_date(date)                       
        left_date = date-dt.timedelta(365)
        
    if statDate is not None:            
        statDate = parse_statdate(statDate)
    
    #print date
    today_str = dt.date.today()
    yesterday_str = dt.date.today()-dt.timedelta(1)
    # 如果都为None，取date
    if date is None and statDate is None:
        date = get_previous_trading_date(today_str)#-dt.timedelta(1)
        #date = today_str
        #减小查询量 只查询指定日期一年内    
        left_date = dt.date.today()-dt.timedelta(365)
    #date昨天时 判断下是否为非交易日 不是的话取上一个交易日
    elif statDate is None and date == yesterday_str:
        last_trading = get_previous_trading_date(today_str)#-dt.timedelta(6)
        if date != last_trading:
            date = last_trading
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
        elif entity in [balance,cash_flow,income,indicator,lico_fn_sigquafina,lico_fn_fcrgincomes]:                        
            if statDate is not None:
                q = q.filter( entity.statDate == statDate )
            else:
                q1 = Query([entity.code, func.max(entity.statDate).label('statDate')]).filter(entity.pubDate<=date,entity.pubDate>=left_date).group_by(entity.code).subquery()                
                #q1 = Query([entity.code, func.max(entity.pubDate).label('pubDate')]).filter(entity.pubDate<=date).group_by(entity.code).subquery()                
                #q = q.filter(entity.pubDate <= date, entity.pubDate>=left_date).filter(entity.pubDate == q1.c.pubDate, entity.code == q1.c.code)                
                q = q.filter(entity.statDate == q1.c.statDate, entity.code == q1.c.code)                
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

def get_index_stocks(index_symbol, date=None):
    """
        完全按照聚宽实现，参见：https://www.joinquant.com/api#get_index_stocks
    """
    if date is not None:
        date = parse_date(date)
    if date is None:
        date = dt.date.today()
    date = date.strftime('%Y-%m-%d')

    # add filter
    q = 'select code,type from CJHJDM.INDEX_BA_SAMPLE where securitycode=\'' + index_symbol + '\''
    if date is not None:
        q += ' and opdate<=\'' + date + '\''
    q += ' order by opdate asc'
    data = pd.read_sql_query(q, con=engine)
    data = data.drop_duplicates(subset='code', keep='last')
    return sorted(data[data.type == '1'].code.unique())

def get_report_stock_predict(stock_list, indicator, date=None):
    if date is not None:
        date = parse_date(date)
    if date is None:
        date = dt.date.today()
    year = date.year
    month = date.month

    predict_indicators = {u'股票盈利预测': '001',
                      u'市盈率（PE）': '001001',
                      u'已动用资本回报率': '001002',
                      u'股本': '001003', '市净率（PB）': '001004',
                      u'市盈率相对盈利增长比率(PEG)': '001005',
                      u'市销率（EV/Sales）': '001006',
                      u'市现率（PCF）': '001007',
                      u'每股收益': '001008',
                      u'每股经营活动现金流': '001009',
                      u'每股现金股利': '001010',
                      u'每股净资产': '001011',
                      u'净资产收益率': '001012',
                      u'总资产收益率': '001013',
                      u'归属于母公司的净利润': '001014',
                      u'营业总收入': '001015',
                      u'息税前利润(EBIT)': '001016',
                      u'边际息税前利润': '001017',
                      u'扣除息税后利润(NOPLAT)': '001018',
                      u'投入资本': '001019',
                      u'息税折旧摊销前利润': '001020',
                      u'利润总额': '001021',
                      u'营业利润': '001022',
                      u'资产负债率': '001023',
                      u'企业价值倍数': '001024',
                      u'销售毛利率': '001025',
                      u'流动比率': '001026',
                      u'速动比率': '001027',
                      u'存货周转率': '001028',
                      u'每股收益同比': '001029',
                      u'净利润': '001030',
                      u'营业成本': '001031'}

    infocode = 'AP' + str(year) + '0' + str(month) + '%'
    # add filter
    q = 'select code, indicatorvalue from CJHJDM.INFO_RE_STOCKPREDICT where' \
        ' PREDICTINDICATOR=\'' + predict_indicators[indicator] + \
        '\' and SECURITYCODE in ' + str(tuple(stock_list)) + \
        ' and PREDICTYEAR=' + str(year) + \
        ' and infocode like \'' + infocode + '\'' + \
        ' order by infocode desc'
    data = pd.read_sql_query(q, con=engine)
    return pd.DataFrame(data)


#oracle不能过滤多余1000的stock  使用or做替代
def filter_code(q,stock_list,table_col_class):
    if len(stock_list)>999:
        q = q.filter(or_(table_col_class.in_(stock_list[:999]),
                      (table_col_class.in_(stock_list[999:]))))
    else:
        q = q.filter(table_col_class.in_(stock_list))
    
    return q    
    
def query(*args, **kwargs):
    return Query(args)

    
def history(count, unit='1d', field='avg', security_list=None, df=True, skip_paused=False, fq='pre'):
    """
        NOTICE:
            米宽接口不支持fq参数 --fix by xsc
            米宽接口多了include_now参数，取默认值 --add by xsc
            unit='1m' not support waiting for using new data interface
            field不支持多个 待修复
            多只股票时返回df格式与JQ不一致 待修复
            gericixin中会传多只股票 暂不知返回是否与jq一致 待修复
            his = history(5, '1d', 'close', security_list=stock, skip_paused=True) 有BUG 待修复
    """
    
    if security_list is None:
        return None
    
    if df:
        res = pd.DataFrame()
    else:
        res = {}
    
    if unit == '1d':           
        for s in security_list:
            res[s] = history_bars(s, count, unit, field, skip_paused, include_now=True, adjust_type=fq) 
    #else unit == '1m':
    #    for s in security_list:
    #        res[s] = history_bars(s, count, unit, field, skip_paused)    
    
    return res
    
#貌似有bug 会导致all_instrments去不到完整股票集合
def get_security_info(code):
        
    #todo 不完善
    code_type_map = {
    'CS':'stock',
    'INDEX':'index',
    'ETF':'etf',
    'etf':'etf',
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
        
def get_all_securities(item, date=None):
    item_map = {
        'de_listed_date':'end_date',\
        'listed_date':'start_date',\
        'abbrev_symbol':'name',\
        'symbol':'display_name'
        }
    if item == "stock":
        type_name = "CS"
    df = all_instruments(type_name,None)
    drop_col = list(set(df.columns)-set(item_map.keys()+['order_book_id']))
    tmp_df = df.drop(drop_col, axis=1)
    
    tmp_df.rename(columns=item_map, inplace=True)
    tmp_df.set_index("order_book_id", inplace=True)
    return tmp_df

def get_current_data():
    pass
    
    
    
class JqapiMod(AbstractMod):
    def start_up(self, env, mod_config):                    
        
        from rqalpha.api.api_base import register_api
        # api
        register_api('get_fundamentals', get_fundamentals)
        register_api('get_index_stocks', get_index_stocks)
        register_api('get_report_stock_predict', get_report_stock_predict)
        register_api('query', query)
        register_api('history', history)
        register_api('get_security_info', get_security_info)
        register_api('get_industry_stocks', get_industry_stocks)
        register_api('get_extras', get_extras)
        register_api('get_all_securities', get_all_securities)
        register_api('filter_code', filter_code)
        # model
        register_api('income', income)
        register_api('balance', balance)
        register_api('cash_flow', cash_flow)
        register_api('indicator', indicator)
        register_api('valuation', valuation)        
        register_api('lico_fn_sigquafina', lico_fn_sigquafina)        
        register_api('lico_fn_fcrgincomes', lico_fn_fcrgincomes)        
        
        register_api('base_get_stock_list', base_get_stock_list)        
               
    def tear_down(self, code, exception=None):
        pass

import talib
import pandas
import datetime as dt
        
def filter_code(q, stock_list, table_col_class):
    if len(stock_list)>999:
        q = q.filter(or_(table_col_class.in_(stock_list[:999]),
                      (table_col_class.in_(stock_list[999:]))))
    else:
        q = q.filter(table_col_class.in_(stock_list))

    return q
    
def base_get_stock_list(context, stock_list):
    statsDate = context.now.date()
    stock_list = get_stock_list(statsDate, stock_list)
    return stock_list   

def get_stock_list(statsDate, stock_list):
    def date_to_quarter(date_str):
        month = int(date_str[5:7])
        if month in [1,2,3]:
            return 1
        if month in [4,5,6]:
            return 2
        if month in [7,8,9]:
            return 3
        if month in [10,11,12]:
            return 4    
        return 'error'
    
    def quarter_to_date(quarter_str, year_str):
        res = year_str;
        if quarter_str == 1:
            res = res+'-03-31';
        if quarter_str == 2:
            res = res+'-06-30';
        if quarter_str == 3:
            res = res+'-09-30';
        if quarter_str == 4:
            res = res+'-12-31';
        return res
    
    #stock_list = filte_b(stock_list)
    #stock_list = filte_st(stock_list)
    #stock_list = sorted(unpaused(stock_list))
    
    q = query(valuation.code, valuation.pb_ratio)
    #q = filter_code(q, stock_list, valuation.code)

    df = get_fundamentals(
        q,
        date = statsDate - dt.timedelta(1)
    )        

    df = df[df.code.isin(stock_list)]
    df = df.sort_values(by=['pb_ratio'], ascending=[True])
    df = df.reset_index(drop = True)
    df = df[df.pb_ratio > 0]
    df = df.reset_index(drop = True)
    df = df[0:int(len(df)*0.20)]  
    stock_list = list(df['code'])   
    #print '002048.XSHE' in stock_list
    '''
    df = get_fundamentals( 
        query(indicator.code, indicator.roe).filter(
            indicator.code.in_(stock_list)),
        date = statsDate - dt.timedelta(1)
    )
    '''
    df = get_fundamentals( 
        query(lico_fn_sigquafina.code, lico_fn_sigquafina.roe).filter(
            lico_fn_sigquafina.code.in_(stock_list)),
        date = statsDate - dt.timedelta(1)
    )
    #df = df[df.code.isin(stock_list)]
    df = df.reset_index(drop = True)
    df = df[df.roe > 0]
    df = df.reset_index(drop=True)
    list_roe = list(df['code'])

    stock_list  = list_roe
    #print '600580.XSHG' in stock_list
    #FFScore = {}
    #FFScore = __cal_FFScore(stock_list, FFScore, list_roe)
    
    '''
    df = get_fundamentals(
        query(indicator.code, indicator.roa, indicator.statDate).filter(
            indicator.code.in_(stock_list)),
        date = statsDate - dt.timedelta(1)
    )
    '''
    df = get_fundamentals( 
        query(lico_fn_sigquafina.code, lico_fn_sigquafina.zzcjll, lico_fn_sigquafina.statDate).filter(
            lico_fn_sigquafina.code.in_(stock_list)),
        date = statsDate - dt.timedelta(1)
    ) 
    df.rename(columns={'zzcjll': 'roa'}, inplace=True)
    date_list = list(set(df['statDate']))
    res_df = None
    for date_str in date_list:
        year = int(date_str[:4])
        last_date = quarter_to_date(date_to_quarter(date_str),str(year-1))
        tmp_df = df[df['statDate']==date_str]
        tmp_code = list(tmp_df['code'])
        '''
        df2 = get_fundamentals(
            query(indicator.code, indicator.roa,indicator.statDate).filter(
                indicator.code.in_(tmp_code)),
            statDate = dt.datetime.strptime(last_date,'%Y-%m-%d')
        )
        df2.rename(columns={'roa': 'last_roa', 'statDate': 'last_statDate'}, inplace=True)
        '''
        df2 = get_fundamentals(
            query(lico_fn_sigquafina.code, lico_fn_sigquafina.zzcjll, lico_fn_sigquafina.statDate).filter(
                lico_fn_sigquafina.code.in_(tmp_code)),
            statDate = dt.datetime.strptime(last_date,'%Y-%m-%d')
        )
        df2.rename(columns={'zzcjll': 'last_roa', 'statDate': 'last_statDate'}, inplace=True)

        tmp_df = tmp_df.merge(df2, left_on='code', right_on='code', how='left')
        if res_df is None:
            res_df = tmp_df
        else:
            res_df = pandas.concat([res_df,tmp_df])
    res_df = res_df.dropna(how='any')
    res_df['delta_roa'] = res_df['roa'] - res_df['last_roa']
    #print res_df.sort_values('code')
    tmp_list = list(res_df[res_df['delta_roa']>0]['code'])
    stock_list = tmp_list
    #print '600580.XSHG' in stock_list
    #FFScore = __cal_FFScore(stock_list, FFScore, tmp_list)

    df = get_fundamentals(
        query(balance.code, balance.total_non_current_assets, 
            balance.total_non_current_liability, balance.statDate).filter(
            balance.code.in_(stock_list)),
        date = statsDate - dt.timedelta(1)
    )
    df = df.dropna(how='any')
    df['LEVER'] = df['total_non_current_liability']/df['total_non_current_assets']
    
    date_list = list(set(df['statDate']))
    res_df = None
    for date_str in date_list:
        year = int(date_str[:4])
        last_date = quarter_to_date(date_to_quarter(date_str),str(year-1))
        tmp_df = df[df['statDate']==date_str]
        tmp_code = list(tmp_df['code'])
        df2 = get_fundamentals(
            query(balance.code, balance.total_non_current_assets, 
                balance.total_non_current_liability, balance.statDate).filter(
                balance.code.in_(tmp_code)),
            statDate = dt.datetime.strptime(last_date,'%Y-%m-%d')
        )
        df2 = df2.dropna(how='any')
        df2['last_LEVER'] = df2['total_non_current_liability']/df2['total_non_current_assets']
        df2.rename(columns={'total_non_current_assets': 'last_total_non_current_assets', \
        'total_non_current_liability': 'last_total_non_current_liability', 'statDate':'last_statDate'}, inplace=True)
        tmp_df = tmp_df.merge(df2, left_on='code', right_on='code', how='left')
        if res_df is None:
            res_df = tmp_df
        else:
            res_df = pandas.concat([res_df,tmp_df])
    
    res_df = res_df.dropna()
    res_df['delta_LEVER'] = res_df['LEVER'] - res_df['last_LEVER']
    #print res_df.sort_values('code')
    tmp_list = list(res_df[res_df['delta_LEVER']>0]['code'])
    stock_list = tmp_list
    #print '600580.XSHG' in stock_list
    #FFScore = __cal_FFScore(stock_list, FFScore, tmp_list)

    '''
    df = get_fundamentals(
        query(balance.code, balance.total_current_assets, income.total_operating_revenue,\
            income.non_operating_revenue, income.statDate).filter(balance.code.in_(stock_list)),
        date = statsDate - dt.timedelta(1)
    )
    '''
    df = get_fundamentals(
        query(balance.code, balance.total_current_assets, lico_fn_fcrgincomes.totaloperatereve_s,\
            lico_fn_fcrgincomes.nonoperatereve_s, income.statDate).filter(balance.code.in_(stock_list)),
        date = statsDate - dt.timedelta(1)
    )
    df.rename(columns={'totaloperatereve_s':'total_operating_revenue','nonoperatereve_s':'non_operating_revenue'},inplace=True)
    
    df = df.dropna(how='any')
    date_list = list(set(df['statDate']))
    res_df = None
    for date_str in date_list:
        year = int(date_str[:4])
        last_date = quarter_to_date(date_to_quarter(date_str),str(year-1))
        tmp_df = df[df['statDate']==date_str]
        tmp_code = list(tmp_df['code'])
        '''
        df2 = get_fundamentals(
            query(balance.code, balance.total_current_assets, income.total_operating_revenue, \
                income.non_operating_revenue, income.statDate).filter(balance.code.in_(tmp_code)),
            statDate = dt.datetime.strptime(last_date,'%Y-%m-%d')
        )  
        '''
        df2 = get_fundamentals(
            query(balance.code, balance.total_current_assets, lico_fn_fcrgincomes.totaloperatereve_s, \
                lico_fn_fcrgincomes.nonoperatereve_s, lico_fn_fcrgincomes.statDate).filter(balance.code.in_(tmp_code)),
            statDate = dt.datetime.strptime(last_date,'%Y-%m-%d')
        )    
        df2.rename(columns={'totaloperatereve_s':'total_operating_revenue','nonoperatereve_s':'non_operating_revenue'},inplace=True)

        df2 = df2.dropna(how='any')
        #算上期。。nm
        last_last_date = quarter_to_date(date_to_quarter(date_str),str(year-2))
        df3 = get_fundamentals(
            query(balance.code, balance.total_current_assets, \
                    balance.statDate).filter(balance.code.in_(tmp_code)),
            statDate = dt.datetime.strptime(last_last_date,'%Y-%m-%d')                
        )
        df3.rename(columns={'total_current_assets':'last_last_total_current_assets','statDate':'last_last_statDate'},inplace=True)
        
        df3 = df3.dropna(how='any')

        df2 = df2.merge(df3, left_on='code', right_on='code', how='left')
        df2 = df2.dropna(how='any')
        
        df2['last_CATURN'] =  2*(df2['total_operating_revenue']-df2['non_operating_revenue'])\
                            /(df2['total_current_assets']+df2['last_last_total_current_assets'])
        
        df2.rename(columns={'total_operating_revenue': 'last_total_operating_revenue', \
        'non_operating_revenue': 'last_non_operating_revenue', 'total_current_assets':'last_total_current_assets',\
        'statDate':'last_statDate'}, inplace=True)  
        
        #算当期
        #tmp_df['last_total_current_assets'] = df2['total_current_assets']
        tmp_df = tmp_df.merge(df2, left_on='code', right_on='code', how='left')
        tmp_df = tmp_df.dropna(how='any')

        tmp_df['CATURN'] =  2*(tmp_df['total_operating_revenue']-tmp_df['non_operating_revenue'])\
                            /(tmp_df['total_current_assets']+tmp_df['last_total_current_assets'])    
        if res_df is None:
            res_df = tmp_df
        else:
            res_df = pandas.concat([res_df,tmp_df])
            
    res_df = res_df.dropna()
    res_df['delta_CATURN'] = res_df['CATURN'] - res_df['last_CATURN']
    #print res_df[res_df['code']=='600580.XSHG']
    #print res_df.sort_values('code')
    tmp_list = list(res_df[res_df['delta_CATURN']>0]['code'])
    stock_list = tmp_list
    #FFScore = __cal_FFScore(stock_list, FFScore, tmp_list)

    '''
    df = get_fundamentals(
        query(balance.code, income.total_operating_revenue, income.non_operating_revenue,\
        balance.total_current_assets, balance.total_non_current_assets,balance.statDate).filter(balance.code.in_(stock_list)),
        date = statsDate - dt.timedelta(1)
    )
    '''
    df = get_fundamentals(
        query(balance.code, lico_fn_fcrgincomes.totaloperatereve_s, lico_fn_fcrgincomes.nonoperatereve_s,\
        balance.total_current_assets, balance.total_non_current_assets,balance.statDate).filter(balance.code.in_(stock_list)),
        date = statsDate - dt.timedelta(1)
    )
    df.rename(columns={'totaloperatereve_s':'total_operating_revenue','nonoperatereve_s':'non_operating_revenue'},inplace=True)
    
    df['total_assets'] = df['total_current_assets'] + df['total_non_current_assets']
    df = df.dropna(how='any')
    date_list = list(set(df['statDate']))
    res_df = None
    for date_str in date_list:
        year = int(date_str[:4])
        last_date = quarter_to_date(date_to_quarter(date_str),str(year-1))
        tmp_df = df[df['statDate']==date_str]
        tmp_code = list(tmp_df['code'])
        '''
        df2 = get_fundamentals(
            query(balance.code, income.total_operating_revenue, income.non_operating_revenue, \
            balance.total_current_assets, balance.total_non_current_assets,balance.statDate).filter(balance.code.in_(tmp_code)),
            statDate = dt.datetime.strptime(last_date,'%Y-%m-%d')
        )
        '''
        df2 = get_fundamentals(
            query(balance.code, lico_fn_fcrgincomes.totaloperatereve_s, lico_fn_fcrgincomes.nonoperatereve_s, \
            balance.total_current_assets, balance.total_non_current_assets,balance.statDate).filter(balance.code.in_(tmp_code)),
            statDate = dt.datetime.strptime(last_date,'%Y-%m-%d')
        )
        df2.rename(columns={'totaloperatereve_s':'total_operating_revenue','nonoperatereve_s':'non_operating_revenue'},inplace=True)
        
        df2['total_assets'] = df2['total_current_assets'] + df2['total_non_current_assets']
        df2 = df2.dropna(how='any')
        #算上期。。nm
        last_last_date = quarter_to_date(date_to_quarter(date_str),str(year-2))
        df3 = get_fundamentals(
            query(balance.code, balance.total_current_assets, \
            balance.total_non_current_assets,balance.statDate).filter(balance.code.in_(tmp_code)),
            statDate = dt.datetime.strptime(last_last_date,'%Y-%m-%d')
        )
        df3['last_last_total_assets'] = df3['total_current_assets'] + df3['total_non_current_assets']
        df3.rename(columns={'total_current_assets':'last_last_total_current_assets',\
        'total_non_current_assets':'last_last_total_non_current_assets','statDate':'last_last_statDate'},inplace=True)
        df3 = df3.dropna(how='any')       
        
        df2 = df2.merge(df3, left_on='code', right_on='code', how='left')
        df2 = df2.dropna(how='any')
        
        df2['last_TURN'] = 2*(df2['total_operating_revenue'] - df2['non_operating_revenue']) / (df2['total_assets'] + df2['last_last_total_assets'])
        df2.rename(columns={'total_operating_revenue': 'last_total_operating_revenue', \
        'non_operating_revenue': 'last_non_operating_revenue', 'total_current_assets':'last_total_current_assets',\
        'total_non_current_assets':'last_total_non_current_assets', 'total_assets':'last_total_assets',\
        'statDate':'last_statDate'}, inplace=True)     
        
        #算当期
        #tmp_df['last_total_current_assets'] = df2['total_current_assets']
        tmp_df = tmp_df.merge(df2, left_on='code', right_on='code', how='left')
        tmp_df = tmp_df.dropna(how='any')
        tmp_df['TURN'] =  2*(tmp_df['total_operating_revenue']-tmp_df['non_operating_revenue'])\
                            /(tmp_df['total_assets']+tmp_df['last_total_assets'])    
        
        if res_df is None:
            res_df = tmp_df
        else:
            res_df = pandas.concat([res_df,tmp_df])  

    df['total_assets'] = df['total_current_assets'] + df['total_non_current_assets']
    df = df.dropna(how='any')
    date_list = list(set(df['statDate']))
    res_df = None
    for date_str in date_list:
        year = int(date_str[:4])
        last_date = quarter_to_date(date_to_quarter(date_str),str(year-1))
        tmp_df = df[df['statDate']==date_str]
        tmp_code = list(tmp_df['code'])
        '''
        df2 = get_fundamentals(
            query(balance.code, income.total_operating_revenue, income.non_operating_revenue, \
            balance.total_current_assets, balance.total_non_current_assets,balance.statDate).filter(balance.code.in_(tmp_code)),
            statDate = dt.datetime.strptime(last_date,'%Y-%m-%d')
        )
        '''
        df2 = get_fundamentals(
            query(balance.code, lico_fn_fcrgincomes.totaloperatereve_s, lico_fn_fcrgincomes.nonoperatereve_s, \
            balance.total_current_assets, balance.total_non_current_assets,balance.statDate).filter(balance.code.in_(tmp_code)),
            statDate = dt.datetime.strptime(last_date,'%Y-%m-%d')
        )
        df2.rename(columns={'totaloperatereve_s':'total_operating_revenue','nonoperatereve_s':'non_operating_revenue'},inplace=True)

        df2['total_assets'] = df2['total_current_assets'] + df2['total_non_current_assets']
        df2 = df2.dropna(how='any')
        #算上期。。nm
        last_last_date = quarter_to_date(date_to_quarter(date_str),str(year-2))
        df3 = get_fundamentals(
            query(balance.code, balance.total_current_assets, \
            balance.total_non_current_assets,balance.statDate).filter(balance.code.in_(tmp_code)),
            statDate = dt.datetime.strptime(last_last_date,'%Y-%m-%d')
        )
        df3['last_last_total_assets'] = df3['total_current_assets'] + df3['total_non_current_assets']
        df3.rename(columns={'total_current_assets':'last_last_total_current_assets',\
        'total_non_current_assets':'last_last_total_non_current_assets','statDate':'last_last_statDate'},inplace=True)
        df3 = df3.dropna(how='any')       
        
        df2 = df2.merge(df3, left_on='code', right_on='code', how='left')
        df2 = df2.dropna(how='any')
        
        df2['last_TURN'] = 2*(df2['total_operating_revenue'] - df2['non_operating_revenue']) / (df2['total_assets'] + df2['last_last_total_assets'])
        df2.rename(columns={'total_operating_revenue': 'last_total_operating_revenue', \
        'non_operating_revenue': 'last_non_operating_revenue', 'total_current_assets':'last_total_current_assets',\
        'total_non_current_assets':'last_total_non_current_assets', 'total_assets':'last_total_assets',\
        'statDate':'last_statDate'}, inplace=True)     
        
        #算当期
        #tmp_df['last_total_current_assets'] = df2['total_current_assets']
        tmp_df = tmp_df.merge(df2, left_on='code', right_on='code', how='left')
        tmp_df = tmp_df.dropna(how='any')
        tmp_df['TURN'] =  2*(tmp_df['total_operating_revenue']-tmp_df['non_operating_revenue'])\
                            /(tmp_df['total_assets']+tmp_df['last_total_assets'])    
        
        if res_df is None:
            res_df = tmp_df
        else:
            res_df = pandas.concat([res_df,tmp_df])  

    res_df = res_df.dropna()
    res_df['delta_TURN'] = res_df['TURN'] - res_df['last_TURN']
    #print res_df.sort_values('code')
    
    tmp_list = list(res_df[res_df['delta_TURN']>0]['code'])
    stock_list = tmp_list
    #FFScore = __cal_FFScore(stock_list, FFScore, tmp_list)

    return stock_list
