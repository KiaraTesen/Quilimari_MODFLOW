# -*- coding: utf-8 -*-

import pandas as pd
import os
from flopy.utils.zonbud import ZoneBudget, read_zbarray
from datetime import datetime as dt
import glob

################################################
####    Funciones Procesar archivos .ccf    ####
################################################

def get_scenario(temp):
    return temp[-15:-12]

def get_date(temp):
    diferencia_TS = 0
    temp1 = '1_' + temp[-11:-4]
    temp2 = dt.strptime(temp1, '%d_%Y_%W')
    return temp2 - pd.DateOffset(months=diferencia_TS)   

def get_TS(directorio, zone_analysis, output, zones):
    zones = zones
    new_df = pd.DataFrame()
    os.chdir(directorio)
    for file in glob.glob('*.csv'):
        df = pd.read_csv(file)
        melted = df.melt(id_vars=['name'], value_vars=zones)
        wk2 = melted.loc[melted['variable'] == zone_analysis]
        wk2 = wk2.drop(['variable'], axis=1)
        wk2 = wk2.T
        wk2['name_file'] = file
        new_df = pd.concat([wk2, new_df])

    new_df.columns = new_df.iloc[0]
    new_df = new_df.drop(['name'], axis=0)
    column_list = list(new_df.columns.values)
    last_name = column_list[-1]
    new_df.rename(columns={last_name: 'file'}, inplace=True)
    new_df['Scenario'] = new_df.apply(lambda x: get_scenario(x['file']), axis=1)
    new_df['date'] = new_df.apply(lambda x: get_date(x['file']), axis=1)
    new_df.set_index('date', inplace=True)
    new_df = new_df.sort_values(['date'],ascending=True)
    new_df.drop(['file'], axis=1, inplace=True)
    dir_out = output + '/' + zone_analysis + '.csv'
    new_df.to_csv(dir_out)

def get_full_balance(path_balance, path_ZB, dir_exit, temp_path, aliases, zones):
    zonefile = read_zbarray(path_ZB)

    # Leer binarios de la carpeta WEAP
    for file in os.listdir(path_balance):
        filename = os.fsdecode(file)
        if filename.endswith(".ccf"):
            t = temp_path + '/' + filename[:-4] + '.csv'
            zb = ZoneBudget(path_balance + '\\' + filename, zonefile, aliases=aliases)
            zb.to_csv(t)
            
    zones = zones
    for zone in zones:
        get_TS(temp_path, zone, dir_exit, zones)

    filelist = [ f for f in os.listdir(temp_path) if f.endswith(".csv") ]
    for f in filelist:
        os.remove(os.path.join(temp_path, f))

##########################################
####    Processing MODFLOW Results    ####
##########################################

def get_df_ls(df, fecha):
    df_ls = pd.DataFrame()
    for i in df.columns.values[1:-2]:
        df_ls[i] = pd.DataFrame((df[i].to_numpy())/86400)
    df_ls.set_index(fecha['Fecha'],inplace = True)
    df_temp = df_ls.iloc[156:,:] # Para año hidrológico
    #df_temp = df_ls.iloc[143:,:] # Para año calendario
    return df_temp

def get_balance_cuenca(ruta_export_BALANCE, inicio, fin, zones, variables, años, cuenca):
    Res = (pd.read_excel(ruta_export_BALANCE + '/Resumen_balance_' + str(zones[inicio]) + '.xlsx').iloc[:,1:12]).to_numpy()
    for q in range (inicio + 1,fin):
        dato = (pd.read_excel(ruta_export_BALANCE + '/Resumen_balance_' + str(zones[q]) + '.xlsx').iloc[:,1:12]).to_numpy()
        Res = Res + dato
    Res_cuenca = pd.DataFrame(Res, columns = variables)
    Res_cuenca.set_index(años['Fecha'],inplace = True)
    return Res_cuenca.to_excel(ruta_export_BALANCE + '/Resumen_balance_' + str(cuenca) + '.xlsx')