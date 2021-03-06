# coding=utf-8
# @author: bryan

import pandas as pd

"""
7号之前所有天的统计特征
用户/商品/品牌/店铺/类别/城市/page/query 点击次数，购买次数，转化率(buy/cnt+3)

"""
def all_days_feature(orgin_data):
    data=orgin_data[orgin_data['day'] < 7]
    cols=['user_id', 'item_id', 'item_brand_id', 'shop_id', 'item_category_list', 'item_city_id', 'query1', 'query', 'context_page_id', 'predict_category_property']
    train=orgin_data[orgin_data['day'] == 7][['instance_id'] + cols]
    # 显式指定了列名,因此是对is_trade分别计算了sum, count, sum重命名为user_buy, count重命名为user_cnt
    user=data.groupby('user_id',as_index=False)['is_trade'].agg({'user_buy':'sum','user_cnt':'count'})
    user['user_7days_cvr']=(user['user_buy'])/(user['user_cnt']+3)
    col_list= cols[1:]
    train=pd.merge(train,user[['user_id','user_7days_cvr']],on='user_id',how='left')
    for col in col_list:# 统计 item_id, item_brand_id,shop_id等不同维度的行为次数以及成交次数以及成交率
        tmp=data.groupby(col, as_index=False)['is_trade'].agg({col + '_buy': 'sum', col + '_cnt': 'count'})
        tmp[col + '_7days_cvr'] = tmp[col + '_buy'] / tmp[col + '_cnt'] # 计算该维度上的转化率
        train = pd.merge(train, tmp[[col, col + '_7days_cvr']], on=col, how='left') #
        print(col)

    for i in range(len(col_list)):
        for j in range(i+1, len(col_list)): # 计算任意两个维度上的转化率
            two_keys=[col_list[i], col_list[j]]
            tmp = data.groupby(two_keys, as_index=False)['is_trade'].agg({'_'.join(two_keys) + '_buy': 'sum', '_'.join(two_keys) + '_cnt': 'count'})
            tmp['_'.join(two_keys) + '_7days_cvr'] = tmp['_'.join(two_keys) + '_buy'] / tmp['_'.join(two_keys) + '_cnt']
            train = pd.merge(train, tmp[two_keys + ['_'.join(two_keys) + '_7days_cvr']], on=two_keys, how='left')
            print(two_keys)
    train.drop(cols, axis=1).to_csv('../data/7days_cvr_feature.csv', index=False)
    return train

"""
用户行为编码
"""
def user_encoder_feature(org):
    data = org[org['day'] < 7]
    train = org[org['day'] == 7]
    # 统计用户7天内的行为次数以及成交次数
    user7 = data.groupby('user_id', as_index=False)['is_trade'].agg({'user_buy': 'sum', 'user_cnt': 'count'})
    user7['user_allday_buy_click']=user7.apply(lambda x:str(x['user_buy'])+'-'+str(x['user_cnt']),axis=1)
    data=org[org['day']==6]
    user6=data.groupby('user_id', as_index=False)['is_trade'].agg({'user_buy': 'sum', 'user_cnt': 'count'})
    user6['user_6day_buy_click'] = user6.apply(lambda x: str(x['user_buy']) + '-' + str(x['user_cnt']), axis=1)

    train=pd.merge(train,user7,on='user_id',how='left')
    train = pd.merge(train, user6, on='user_id', how='left')
    train[['instance_id','user_allday_buy_click','user_6day_buy_click']].to_csv('../data/user_buy_click_feature.csv')
	

"""
7号前一天，6号的统计特征
用户/商品/品牌/店铺/类别/城市 点击次数，购买次数，转化率，占前面所有天的占比
"""
def latest_day_feature(org):
    data = org[org['day'] ==6]
    col = ['user_id', 'item_id', 'item_brand_id', 'shop_id', 'item_category_list', 'item_city_id', 'query1', 'query','context_page_id','predict_category_property']
    train = org[org['day'] == 7][['instance_id'] + col]
    user = data.groupby('user_id', as_index=False)['is_trade'].agg({'user_buy': 'sum', 'user_cnt': 'count'})
    user['user_6day_cvr'] = (user['user_buy']) / (user['user_cnt'] + 3)
    train = pd.merge(train, user[['user_id', 'user_6day_cvr']], on='user_id', how='left')
    candidate_cols = col[1:]
    for col in candidate_cols:
        tmp=data.groupby(col, as_index=False)['is_trade'].agg({col + '_buy': 'sum', col + '_cnt': 'count'})
        tmp[col + '_6day_cvr'] = tmp[col + '_buy'] / tmp[col + '_cnt']
        train = pd.merge(train, tmp[[col, col + '_6day_cvr']], on=col, how='left')
        print(col)

    for i in range(len(candidate_cols)):
        for j in range(i+1, len(candidate_cols)):
            join_cols=[candidate_cols[i], candidate_cols[j]]
            tmp = data.groupby(join_cols, as_index=False)['is_trade'].agg({'_'.join(join_cols) + '_buy': 'sum', '_'.join(join_cols) + '_cnt': 'count'})
            tmp['_'.join(join_cols) + '_6day_cvr'] = tmp['_'.join(join_cols) + '_buy'] / tmp['_'.join(join_cols) + '_cnt']
            train = pd.merge(train, tmp[join_cols + ['_'.join(join_cols) + '_6day_cvr']], on=join_cols, how='left')
            print(join_cols)

    train.drop(col, axis=1).to_csv('../data/6day_cvr_feature.csv',index=False)
    return train

"""
当天的交易率特征，交叉统计
"""
# calc data，join data
# user_id,item_id,item_brand_id,shop_id,item_category_list,item_city_id,predict_category_property
def cvr(train_data, predict_data):
    user = train_data.groupby('user_id', as_index=False)['is_trade'].agg({'user_buy': 'sum', 'user_cnt': 'count'})
    user['user_today_cvr'] = (user['user_buy']) / (user['user_cnt'] + 3)

    cols=['user_id', 'item_id', 'item_brand_id', 'shop_id', 'item_category_list', 'item_city_id', 'predict_category_property', 'context_page_id', 'query1', 'query']
    predict_data=predict_data[['instance_id'] + cols]
    predict_data = pd.merge(predict_data, user[['user_id', 'user_today_cvr']], on='user_id', how='left')

    for col in cols[1:]:
        tmp=train_data.groupby(col, as_index=False)['is_trade'].agg({col + '_today_cvr': 'mean'})
        predict_data = pd.merge(predict_data, tmp, on=col, how='left')

    # 任意两个维度的交叉特征来统计转化率
    for i in range(len(cols)):
        for j in range(i+1, len(cols)):
            print([cols[i], cols[j]])
            tmp=train_data.groupby([cols[i], cols[j]], as_index=False)['is_trade'].agg({'today_' + cols[i] + cols[j] + '_cvr': 'mean'})
            predict_data = pd.merge(predict_data, tmp, on=[cols[i], cols[j]], how='left') # 两个key需要相等

    return predict_data
# [['instance_id','today_user_cvr','today_item_cvr','today_brand_cvr','today_shop_cvr','today_cate_cvr','today_city_cvr']]

def get_batch_data(data, index, size):
    import math
    size = math.ceil(len(data) / size)
    start = size * index
    end = (index + 1) * size if (index + 1) * size < len(data) else len(data)
    return data[start:end]

def today_cvr_feature(org):
    col = ['user_id',
           'item_id',
           'item_brand_id',
           'shop_id',
           'item_category_list',
           'item_city_id',
           'predict_category_property',
           'context_page_id',
           'query1',
           'query']
    data=org[org['day']==7]
    train=data[data['is_trade']>-1] # 训练数据
    predict=data[data['is_trade']<0] # is_trade=-1,为待预测数据
    predict=cvr(train, predict)

    trains=[]
    size=10 # 将train数据划分为10等分
    for i in range(size):
        trains.append(get_batch_data(train, i, size))

    res=[]
    res.append(predict)
    for i in range(size):
        # 任意两份数据之间计算转化率
        res.append(cvr(pd.concat([trains[j] for j in range(size) if i !=j]).reset_index(drop=True), trains[i]))
    data=pd.concat(res, axis=0).reset_index(drop=True)
    #data=data[['instance_id','today_user_cvr','today_item_cvr','today_brand_cvr','today_shop_cvr','today_cate_cvr','today_city_cvr','today_query_cvr']]
    data=data.drop(col,axis=1) # 将原始数据中的这些列去掉后保存
    data.to_csv('../data/today_cvr_feature.csv', index=False)
    return data

"""
#todo
排名特征,前7天的算一次，第7天的算一次
用户转化率在品牌，店铺，类别，城市下面的排名

商品转化率在店铺下面的排名
商品转化率在品牌下面的排名
商品转化率在类别下面的排名
商品转化率在城市下面的排名
商品转化率在query1下面的排名
商品转化率在query下面的排名

店铺转化率在品牌下面的排名
店铺转化率在城市下面的排名
店铺转化率在类别下面的排名
店铺转化率在query1下面的排名
店铺转化率在query下面的排名

品牌在城市下面的转化率排名
品牌在店铺下面的转化率排名
品牌转化率在query1下面的排名
品牌转化率在query下面的排名

类别在城市下面的转换率排名
类别在店铺下面的转换率排名
"""
# ['user_id','item_id','item_brand_id','shop_id','item_category_list','item_city_id','predict_category_property','context_page_id', 'query1', 'query']
def rank_6day_feature(data):
    data['user_cvr_brand_6day_rank']=data.groupby('item_brand_id')['user_6day_cvr'].rank(ascending=False,method='dense') # 商品转化率在品牌下面的排名
    data['user_cvr_shop_6day_rank'] = data.groupby('shop_id')['user_6day_cvr'].rank(ascending=False, method='dense')
    data['user_cvr_cate_6day_rank'] = data.groupby('item_category_list')['user_6day_cvr'].rank(ascending=False, method='dense')
    data['user_cvr_city_6day_rank'] = data.groupby('item_city_id')['user_6day_cvr'].rank(ascending=False, method='dense')
    data['item_cvr_shop_6day_rank'] = data.groupby('shop_id')['item_id_6day_cvr'].rank(ascending=False, method='dense')
    data['item_cvr_brand_6day_rank'] = data.groupby('item_brand_id')['item_id_6day_cvr'].rank(ascending=False, method='dense')
    data['item_cvr_cate_6day_rank'] = data.groupby('item_category_list')['item_id_6day_cvr'].rank(ascending=False, method='dense')
    data['item_cvr_city_6day_rank'] = data.groupby('item_city_id')['item_id_6day_cvr'].rank(ascending=False, method='dense')
    data['shop_cvr_brand_6day_rank'] = data.groupby('item_brand_id')['shop_id_6day_cvr'].rank(ascending=False, method='dense')
    data['shop_cvr_cate_6day_rank'] = data.groupby('item_category_list')['shop_id_6day_cvr'].rank(ascending=False, method='dense')
    data['shop_cvr_city_6day_rank'] = data.groupby('item_city_id')['shop_id_6day_cvr'].rank(ascending=False, method='dense')
    data['brand_cvr_city_6day_rank'] = data.groupby('item_city_id')['item_brand_id_6day_cvr'].rank(ascending=False, method='dense')
    data['brand_cvr_shop_6day_rank'] = data.groupby('shop_id')['item_brand_id_6day_cvr'].rank(ascending=False, method='dense')
    data['cate_cvr_city_6day_rank'] = data.groupby('item_city_id')['item_category_list_6day_cvr'].rank(ascending=False, method='dense')
    data['cate_cvr_shop_6day_rank'] = data.groupby('shop_id')['item_category_list_6day_cvr'].rank(ascending=False, method='dense')
    data['item_cvr_query_6day_rank'] = data.groupby('query')['item_id_6day_cvr'].rank(ascending=False, method='dense')
    data['item_cvr_query1_6day_rank'] = data.groupby('query1')['item_id_6day_cvr'].rank(ascending=False, method='dense')
    data['shop_cvr_query_6day_rank'] = data.groupby('query')['shop_id_6day_cvr'].rank(ascending=False, method='dense')
    data['shop_cvr_query1_6day_rank'] = data.groupby('query1')['shop_id_6day_cvr'].rank(ascending=False, method='dense')
    data['brand_cvr_query_6day_rank'] = data.groupby('query')['item_brand_id_6day_cvr'].rank(ascending=False, method='dense')
    data['brand_cvr_query1_6day_rank'] = data.groupby('query1')['item_brand_id_6day_cvr'].rank(ascending=False, method='dense')
    # 只选取部分列进行保存
    data=data[['instance_id','user_cvr_brand_6day_rank','user_cvr_shop_6day_rank','user_cvr_cate_6day_rank','user_cvr_city_6day_rank','item_cvr_shop_6day_rank','item_cvr_brand_6day_rank','item_cvr_cate_6day_rank','item_cvr_city_6day_rank','shop_cvr_brand_6day_rank','shop_cvr_cate_6day_rank','shop_cvr_city_6day_rank','brand_cvr_city_6day_rank','brand_cvr_shop_6day_rank','cate_cvr_city_6day_rank','cate_cvr_shop_6day_rank','item_cvr_query_6day_rank','item_cvr_query1_6day_rank','shop_cvr_query_6day_rank','shop_cvr_query1_6day_rank','brand_cvr_query_6day_rank','brand_cvr_query1_6day_rank'
    ]]
    data.to_csv('../data/rank_feature_6day.csv',index=False)

def rank_7days_feature(data):
    data['user_cvr_brand_7days_rank']=data.groupby('item_brand_id')['user_7days_cvr'].rank(ascending=False,method='dense')
    data['user_cvr_shop_7days_rank'] = data.groupby('shop_id')['user_7days_cvr'].rank(ascending=False, method='dense')
    data['user_cvr_cate_7days_rank'] = data.groupby('item_category_list')['user_7days_cvr'].rank(ascending=False, method='dense')
    data['user_cvr_city_7days_rank'] = data.groupby('item_city_id')['user_7days_cvr'].rank(ascending=False, method='dense')
    data['item_cvr_shop_7days_rank'] = data.groupby('shop_id')['item_id_7days_cvr'].rank(ascending=False, method='dense')
    data['item_cvr_brand_7days_rank'] = data.groupby('item_brand_id')['item_id_7days_cvr'].rank(ascending=False, method='dense')
    data['item_cvr_cate_7days_rank'] = data.groupby('item_category_list')['item_id_7days_cvr'].rank(ascending=False, method='dense')
    data['item_cvr_city_7days_rank'] = data.groupby('item_city_id')['item_id_7days_cvr'].rank(ascending=False, method='dense')
    data['shop_cvr_brand_7days_rank'] = data.groupby('item_brand_id')['shop_id_7days_cvr'].rank(ascending=False, method='dense')
    data['shop_cvr_cate_7days_rank'] = data.groupby('item_category_list')['shop_id_7days_cvr'].rank(ascending=False, method='dense')
    data['shop_cvr_city_7days_rank'] = data.groupby('item_city_id')['shop_id_7days_cvr'].rank(ascending=False, method='dense')
    data['brand_cvr_city_7days_rank'] = data.groupby('item_city_id')['item_brand_id_7days_cvr'].rank(ascending=False, method='dense')
    data['brand_cvr_shop_7days_rank'] = data.groupby('shop_id')['item_brand_id_7days_cvr'].rank(ascending=False, method='dense')
    data['cate_cvr_city_7days_rank'] = data.groupby('item_city_id')['item_category_list_7days_cvr'].rank(ascending=False, method='dense')
    data['cate_cvr_shop_7days_rank'] = data.groupby('shop_id')['item_category_list_7days_cvr'].rank(ascending=False, method='dense')
    data['item_cvr_query_7days_rank'] = data.groupby('query')['item_id_7days_cvr'].rank(ascending=False, method='dense')
    data['item_cvr_query1_7days_rank'] = data.groupby('query1')['item_id_7days_cvr'].rank(ascending=False, method='dense')
    data['shop_cvr_query_7days_rank'] = data.groupby('query')['shop_id_7days_cvr'].rank(ascending=False, method='dense')
    data['shop_cvr_query1_7days_rank'] = data.groupby('query1')['shop_id_7days_cvr'].rank(ascending=False, method='dense')
    data['brand_cvr_query_7days_rank'] = data.groupby('query')['item_brand_id_7days_cvr'].rank(ascending=False, method='dense')
    data['brand_cvr_query1_7days_rank'] = data.groupby('query1')['item_brand_id_7days_cvr'].rank(ascending=False, method='dense')
    data=data[['instance_id','user_cvr_brand_7days_rank','user_cvr_shop_7days_rank','user_cvr_cate_7days_rank','user_cvr_city_7days_rank','item_cvr_shop_7days_rank','item_cvr_brand_7days_rank','item_cvr_cate_7days_rank','item_cvr_city_7days_rank','shop_cvr_brand_7days_rank','shop_cvr_cate_7days_rank','shop_cvr_city_7days_rank','brand_cvr_city_7days_rank','brand_cvr_shop_7days_rank','cate_cvr_city_7days_rank','cate_cvr_shop_7days_rank','item_cvr_query_7days_rank','item_cvr_query1_7days_rank','shop_cvr_query_7days_rank','shop_cvr_query1_7days_rank','brand_cvr_query_7days_rank','brand_cvr_query1_7days_rank'
    ]]
    data.to_csv('../data/rank_feature_7days.csv',index=False)

def rank_today_feature(data):
    data=data.reset_index(drop=True)
    data['user_cvr_brand_today_rank']=data.groupby('item_brand_id')['user_today_cvr'].rank(ascending=False,method='dense')
    data['user_cvr_shop_today_rank'] = data.groupby('shop_id')['user_today_cvr'].rank(ascending=False, method='dense')
    data['user_cvr_cate_today_rank'] = data.groupby('item_category_list')['user_today_cvr'].rank(ascending=False, method='dense')
    data['user_cvr_city_today_rank'] = data.groupby('item_city_id')['user_today_cvr'].rank(ascending=False, method='dense')
    data['item_cvr_shop_today_rank'] = data.groupby('shop_id')['item_id_today_cvr'].rank(ascending=False, method='dense')
    data['item_cvr_brand_today_rank'] = data.groupby('item_brand_id')['item_id_today_cvr'].rank(ascending=False, method='dense')
    data['item_cvr_cate_today_rank'] = data.groupby('item_category_list')['item_id_today_cvr'].rank(ascending=False, method='dense')
    data['item_cvr_city_today_rank'] = data.groupby('item_city_id')['item_id_today_cvr'].rank(ascending=False, method='dense')
    data['shop_cvr_brand_today_rank'] = data.groupby('item_brand_id')['shop_id_today_cvr'].rank(ascending=False, method='dense')
    data['shop_cvr_cate_today_rank'] = data.groupby('item_category_list')['shop_id_today_cvr'].rank(ascending=False, method='dense')
    data['shop_cvr_city_today_rank'] = data.groupby('item_city_id')['shop_id_today_cvr'].rank(ascending=False, method='dense')
    data['brand_cvr_city_today_rank'] = data.groupby('item_city_id')['item_brand_id_today_cvr'].rank(ascending=False, method='dense')
    data['brand_cvr_shop_today_rank'] = data.groupby('shop_id')['item_brand_id_today_cvr'].rank(ascending=False, method='dense')
    data['cate_cvr_city_today_rank'] = data.groupby('item_city_id')['item_category_list_today_cvr'].rank(ascending=False, method='dense')
    data['cate_cvr_shop_today_rank'] = data.groupby('shop_id')['item_category_list_today_cvr'].rank(ascending=False, method='dense')
    data['item_cvr_query_today_rank'] = data.groupby('query')['item_id_today_cvr'].rank(ascending=False, method='dense')
    data['item_cvr_query1_today_rank'] = data.groupby('query1')['item_id_today_cvr'].rank(ascending=False, method='dense')
    data['shop_cvr_query_today_rank'] = data.groupby('query')['shop_id_today_cvr'].rank(ascending=False, method='dense')
    data['shop_cvr_query1_today_rank'] = data.groupby('query1')['shop_id_today_cvr'].rank(ascending=False, method='dense')
    data['brand_cvr_query_today_rank'] = data.groupby('query')['item_brand_id_today_cvr'].rank(ascending=False, method='dense')
    data['brand_cvr_query1_today_rank'] = data.groupby('query1')['item_brand_id_today_cvr'].rank(ascending=False, method='dense')
    data=data[['instance_id','user_cvr_brand_today_rank','user_cvr_shop_today_rank','user_cvr_cate_today_rank','user_cvr_city_today_rank','item_cvr_shop_today_rank','item_cvr_brand_today_rank','item_cvr_cate_today_rank','item_cvr_city_today_rank','shop_cvr_brand_today_rank','shop_cvr_cate_today_rank','shop_cvr_city_today_rank','brand_cvr_city_today_rank','brand_cvr_shop_today_rank','cate_cvr_city_today_rank','cate_cvr_shop_today_rank','item_cvr_query_today_rank','item_cvr_query1_today_rank','shop_cvr_query_today_rank','shop_cvr_query1_today_rank','brand_cvr_query_today_rank','brand_cvr_query1_today_rank'
    ]]
    data.to_csv('../data/rank_feature_today.csv',index=False)

if __name__ == '__main__':
    orgin_data=pd.read_csv('../data/origion_concat.csv')
    user_encoder_feature(orgin_data)
    rank_7days_feature(all_days_feature(orgin_data))
    rank_6day_feature(latest_day_feature(orgin_data))
    rank_today_feature(today_cvr_feature(orgin_data))