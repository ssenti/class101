df_aw_ue_pandas_final = df_aw_ue_pandas[(df_aw_ue_pandas['order_ticket_diff']<1440*10)&(df_aw_ue_pandas['order_ticket_diff']> -1440*10)]

df_aw_ue_pandas_final.loc[df_aw_ue_pandas_final['sum_revenue_182days'] == df_aw_ue_pandas_final['amount'], 'rebuy_182days'] = 0
df_aw_ue_pandas_final.loc[df_aw_ue_pandas_final['sum_revenue_182days'] != df_aw_ue_pandas_final['amount'], 'rebuy_182days'] = 1

df_final_drop = ['klass_m_id','account_type','order_m_id',
                 'first_avail_kst',
                'first_avail_1day_kst', 'first_avail_3days_kst', 
                 'first_avail_10days_kst', 'first_avail_30days_kst',
                 'first_avail_90days_kst', 'first_avail_150days_kst',
                 'orders_avail_at_kst', 'min_order_ticket_diff',
                  'completed_lectures_1day',
                    'completed_lectures_3days',
                    'completed_lectures_10days',
                    'completed_lectures_30days',
                    'completed_lectures_90days',
                    'completed_lectures_150days']
df_final = df_aw_ue_pandas_final.drop(df_final_drop, axis=1)

df_final['discount_ratio'] = df_final['discount_amount']/(df_final['amount']+df_final['discount_amount'])
df_final = df_final.drop(columns='discount_amount')

df_final['internal_category'][df_final.internal_category.str.contains('career|dataAndDevelopment|careerVideoAndDesign|writeContent', na=False)] = 'career'
df_final['internal_category'][df_final.internal_category.str.contains('signature|oa|workout|cooking|lifestyle|photograph|music|sns', na=False)] = 'other'
df_final['internal_category'][df_final.internal_category.str.contains('stock|onlineShop|founded', na=False)] = 'money'

df_final['brand'][df_final.brand.str.contains('gym|signature', na=False)] = 'other'

df_final['package_name'][df_final.package_name.str.contains('only|Only|ONLY', na=False)] = 'only'
df_final['package_name'][df_final.package_name.str.contains('올인원', na=False)] = 'allin'
df_final['package_name'][df_final.package_name.str.contains('베이직', na=False)] = 'basic'
df_final['package_name'][df_final.package_name.str.contains('코칭', na=False)] = 'coaching'
df_final['package_name'][~df_final.package_name.str.contains('only|allin|coaching', na=False)] = 'others'

df_final = df_final.rename(columns={'first_row':'utm_count'})

df_final_user_f_id_drop = ['user_f_id']
df_final = df_final.drop(df_final_user_f_id_drop, axis=1)

df_final['order_ticket_diff'] = abs(df_final['order_ticket_diff'])


df_final['comments_1day'] = df_final['comments_1day'].astype('float')
df_final['comments_1day'] = df_final['comments_1day'].astype('float')
df_final['comments_1day'] = df_final['comments_1day'].astype('float')
df_final['comments_1day'] = df_final['comments_1day'].astype('float')

#for classification
df_final = df_final.drop("sum_revenue_182days", axis=1)
