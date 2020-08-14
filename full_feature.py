# coding=utf-8
# @author:bryan
"""
使用全量数据提取特征，点击数，交叉点击数，占比
"""
import pandas as pd

def full_count_feature(org,name):
    cols=['user_id', 'item_id', 'item_brand_id', 'shop_id', 'item_category_list', 'item_city_id', 'cate', 'top10',
           'predict_category_property', 'context_page_id', 'query1', 'query']
    train=org[org.day==7][['instance_id'] + cols]
    if name=='day6':
        data = org[org.day==6][cols]
    elif name=='days7':
        data=org[org.day<7][cols]
    elif name == 'day7':
        data = org[org.day == 7][cols]
    elif name=='full':
        data=org[cols]

    for col in cols:
        print(col)
        train=pd.merge(train, data.groupby(col, as_index=False)['user_id'].agg({'_'.join([name, col, 'cnt']): 'count'}), on=col, how='left')

    for cross_col in range(len(cols)):
        for j in range(cross_col + 1, len(cols)):
            con_col=[cols[cross_col], cols[j]]
            print(con_col)
            tmp = data.groupby(con_col, as_index=False)['user_id'].agg({'_'.join([name, cols[cross_col], cols[j], 'cnt']): 'count'})
            train = pd.merge(train, tmp, on=con_col, how='left')

    cross_cols=[['user_id','query'],
               ['user_id','query1'],
               ['user_id','shop_id'],
               ['user_id','item_id'],
               ['item_id','shop_id'],
               ['item_id', 'item_brand_id'],
               ['item_brand_id', 'shop_id'],
               ['item_id','item_category_list'],
               ['item_id','query'],
               [ 'item_id','item_city_id'],
               ['item_id','cate'],
               ['item_id','top10'],
               ['item_id','context_page_id'],
               ['item_id','query1'],
               ['item_brand_id', 'shop_id'],
               ['shop_id','item_city_id'],
               [ 'shop_id','context_page_id']
              ]

    for cross_col in cross_cols:
        print(cross_col)
        train['_'.join(cross_col + ['cross'])]= train['_'.join([name, cross_col[0], cross_col[1], 'cnt'])] / train['_'.join([name, cross_col[1], 'cnt'])]

    train=train.drop(cols, axis=1)
    train.to_csv('../data/'+name+'_count_feature.csv',index=False)
    # return train

if __name__ == '__main__':
    org=pd.read_csv('../data/origion_concat.csv')
    full_count_feature(org, 'day6')
    full_count_feature(org, 'days7')
    full_count_feature(org, 'full')