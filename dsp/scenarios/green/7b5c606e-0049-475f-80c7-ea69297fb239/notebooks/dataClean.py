import pandas as pd    
    
def datacleaner(df):    
    final = pd.DataFrame()
    for i in df['material'].unique():
        temp = pd.DataFrame()
        temp = df[df['material'] == i]
        temp['qty_final'] = pd.to_numeric(temp['qty_final'], errors="coerce")
        temp['qty_final'].fillna((temp['qty_final'].mean()), inplace = True)
    
        final = final.append(temp,ignore_index = True)
    
    return final