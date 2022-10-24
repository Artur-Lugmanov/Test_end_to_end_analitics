import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings('ignore')

def corr_purchase_date(lead_dt, purchase_dt):
    '''
    Ф-я принимает на вход список дат по заявкам и список дат по покупкам,
    возвращает словарь, в котором каждой дате заявки (ключу) ставится в соответствие дата покупки (значение)
    Если нет соответствующей покупки, то в качестве значения возвращает '2099-01-01'.

        Параметры:
            lead_dt (list):     список дат формирования заявок
            purchase_dt (list): список дат формирования покупок
        Возвращаемое значение:
            ld_pr_dct (dict):   ключи словаря - даты заявкок, 
                                значения словаря- соответствующие даты покупок
    '''

    ld_pr_dct = {}
    for ld_ind, ld_dt in enumerate(lead_dt):
        for pr_ind, pr_dt in enumerate(purchase_dt):
            if  ld_dt not in ld_pr_dct:
                if pd.Timedelta('0 days') <= pr_dt - ld_dt  <= pd.Timedelta('15 days'):
                    if ld_dt == lead_dt[-1]:
                        ld_pr_dct[ld_dt] = pr_dt
                    elif pr_dt - lead_dt[ld_ind+1]  < pd.Timedelta('0 days'):
                        ld_pr_dct[ld_dt] = pr_dt
                    else:
                        ld_pr_dct[ld_dt] = pd.to_datetime('2099-01-01')
        if  ld_dt not in ld_pr_dct:
            ld_pr_dct[ld_dt] = pd.to_datetime('2099-01-01')

    return ld_pr_dct


def count_if_more_zero(x):
    '''
    Функция возвращает количество ненулевых значений в массиве
    '''

    cnt_lst = []
    for i in x:
        if i != 0:
            cnt_lst.append(i)
    return len(cnt_lst)



class Analitics:

    def __init__(self, path_input='./Data_sources/'):
        self.ads = pd.read_csv(path_input+'ads.csv')
        self.leads = pd.read_csv(path_input+'leads.csv')
        self.purchases = pd.read_csv(path_input+'purchases.csv')
    

    def data_check(self):

        self.mistakes = []
        
        if not len(self.ads.columns) == 9:
            self.mistakes.append(f'В таблице ads {len(self.ads.columns)} столбцов, \
                должно быть 9')
        
        if not len(self.leads.columns) == 8:
            self.mistakes.append(f'В таблице leads {len(self.leads.columns)} столбцов, \
                должно быть 8')

        if not len(self.purchases.columns) == 4:
            self.mistakes.append(f'В таблице purchases {len(self.purchases.columns)} столбцов, \
                должно быть 4')

        if not all(self.ads.columns == ['created_at', 'd_ad_account_id', \
                'd_utm_source', 'd_utm_medium','d_utm_campaign', 'd_utm_content', \
                'd_utm_term','m_clicks', 'm_cost']):
            self.mistakes.append('Не соответствуют названия столбцов таблицы ads')
        
        if not all(self.leads.columns == ['lead_created_at', 'lead_id', \
            'd_lead_utm_source', 'd_lead_utm_medium', 'd_lead_utm_campaign', \
            'd_lead_utm_content', 'd_lead_utm_term', 'client_id']):
            self.mistakes.append('Не соответствуют названия столбцов таблицы leads')
        
        if not all(self.purchases.columns == ['purchase_created_at', 'purchase_id', \
                'client_id', 'm_purchase_amount']):
            self.mistakes.append('Не соответствуют названия столбцов таблицы purchases')

        try:
            self.ads['created_at'] = pd.to_datetime(self.ads['created_at'], format='%Y-%m-%d')
            self.leads['lead_created_at'] = pd.to_datetime(self.leads['lead_created_at'] , \
                format='%Y-%m-%d')
            self.purchases['purchase_created_at'] = pd.to_datetime(self.purchases['purchase_created_at'], \
                format='%Y-%m-%d')
        except Exception as err:
            self.mistakes.append(f'Ошибка обработки дат, проверьте формат столбцов с датами. \n {err}')
        
        if len(self.mistakes) > 0:
            print("Обнаружены следующие ошибки в данных:")
            for mistake in self.mistakes:
                print(mistake)
                print()
        else:
            print("Критических ошибок в данных не обнаружено")
            print()
        
        return self.mistakes


    def data_profile(self):
        for table in [self.ads, self.leads, self.purchases]:
            if table.duplicated().sum() > 0:
                print(f'Обнаружено и удалено {table.duplicated().sum()} дубликатов')
                table.drop_duplicates()

            for col in table.columns:
                if table[col].isna().sum() > 0:
                    print(f'В столбце {col} - {table[col].isna().sum()} пустых строк')
                    print(f'\t\tчто составляет {table[col].isna().sum() / len(table):.0%} данных')            
            print()


    def report_create(self, path_output='./Report/'):

        self.ads.drop(columns='d_utm_term', inplace=True)
        self.ads['d_utm_campaign'] = self.ads['d_utm_campaign'].astype('str')
        self.ads['d_utm_content'] = self.ads['d_utm_content'].astype('str')

        self.leads.drop(columns='d_lead_utm_term', inplace=True)
        self.leads.dropna(subset=['d_lead_utm_source', 'd_lead_utm_medium', 'd_lead_utm_campaign', \
            'd_lead_utm_content', 'client_id'], how='any', inplace=True)

        self.purchases.dropna(subset=['client_id'], how='any', inplace=True)
        self.purchases = self.purchases.drop(self.purchases[self.purchases['m_purchase_amount'] == 0].index)

        total = self.ads.merge(self.leads, how='left', 
                    left_on=['created_at', 'd_utm_source', 'd_utm_medium', 'd_utm_campaign', \
                        'd_utm_content'],
                    right_on=['lead_created_at', 'd_lead_utm_source', 'd_lead_utm_medium', 
                        'd_lead_utm_campaign', 'd_lead_utm_content', ])
        total.drop(columns=['d_ad_account_id','lead_id', 'd_lead_utm_source', 'd_lead_utm_medium', \
                    'd_lead_utm_campaign', 'd_lead_utm_content'], inplace=True)
        
        leads_gr = self.leads.groupby('client_id', as_index=False).agg({'lead_created_at':set})
        pur_gr = self.purchases.groupby('client_id', as_index=False).agg({'purchase_created_at':set})
        
        ld_pr = leads_gr.merge(pur_gr, how='left', on='client_id')
        ld_pr['lead_created_at'] = ld_pr['lead_created_at'].apply(lambda x: sorted(list(x)))
        ld_pr['purchase_created_at'] = ld_pr['purchase_created_at'].apply(lambda x: sorted(list(x)) \
                                                                            if pd.notna(x) else x)
        ld_pr_notna = ld_pr[pd.notna(ld_pr['purchase_created_at'])]
        ld_pr_isna = ld_pr[pd.isna(ld_pr['purchase_created_at'])]
        ld_pr_notna['lead_purchase'] = ld_pr_notna.apply(lambda x: corr_purchase_date(x.lead_created_at, \
                                                            x.purchase_created_at), axis=1)
        ld_pr_notna = ld_pr_notna.explode('lead_created_at')
        ld_pr_notna['purchase_created_at'] = ld_pr_notna.apply(lambda x: \
                                                        x.lead_purchase.get(x.lead_created_at), axis=1)
        ld_pr_notna.drop(columns='lead_purchase', inplace=True)
        ld_pr = pd.concat([ld_pr_notna, ld_pr_isna.fillna(pd.to_datetime('2099-01-01')).\
                                                                        explode('lead_created_at')])
        
        total =  total.merge(ld_pr, on=['client_id', 'lead_created_at'], how='left')
        
        total['count'] = total.groupby(by=['created_at','d_utm_source','d_utm_medium','d_utm_campaign', \
                                    'd_utm_content'])['m_cost'].transform(lambda x: x.count())
        total['m_clicks_per_lead'] = total['m_clicks'] / total['count']
        total['m_cost_per_lead'] = total['m_cost'] / total['count']
        total['count_lead'] = total.groupby(by=['client_id', 'lead_created_at'])['client_id'].transform(lambda x: \
                                                                                                     1/x.count())
        total =  total.merge(self.purchases, on=['client_id', 'purchase_created_at'], how='left')
        total['count_purch'] = total.groupby(by=['client_id', 'purchase_created_at'])\
                                                ['m_purchase_amount'].transform(lambda x: x.count())
        total['purchase_sum'] = total['m_purchase_amount'] / total['count_purch']
        total.fillna(0, inplace=True)

        final_df = total.groupby(['created_at', 'd_utm_source', 'd_utm_medium', 'd_utm_campaign'], \
            as_index=False).agg({'m_clicks_per_lead':'sum', 'm_cost_per_lead':'sum', \
                                'count_lead':[count_if_more_zero], 'purchase_sum':[count_if_more_zero,'sum']})
        final_df.columns=['Дата', 'UTM source', 'UTM medium', 'UTM campaign', 'Количество кликов', \
                'Расходы на рекламу', 'Количество лидов', 'Количество покупок', 'Выручка от продаж']
        final_df['CPL'] = final_df['Расходы на рекламу'] / final_df['Количество лидов']
        final_df['ROAS'] = final_df['Выручка от продаж'] / final_df['Расходы на рекламу']
        final_df.fillna(0, inplace=True)
        final_df.replace(np.inf, '-', inplace=True)

        final_df.to_excel(path_output+'report.xlsx')


if __name__ == "__main__":

    test = Analitics()

    test_mistakes = test.data_check()
    test_mistakes
    
    if len(test_mistakes) == 0:
        test.data_profile()
        test.report_create()


