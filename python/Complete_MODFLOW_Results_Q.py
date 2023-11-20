# -*- coding: utf-8 -*-

import multiprocessing as mp
import numpy as np
import os
import pandas as pd
from utils_Q import *
import warnings
warnings.filterwarnings('ignore')

############################################################################
####                  PRE-PROCESSING MODFLOW RESULTS                    ####
####    COMENTARIO: La versión hace referencia al ID de la ejecución    ####
####                ruta_WEAP se especifica según la PC del usuario     ####
############################################################################

version = 'SWI2_v1'
start_year = 1980
end_year = 2020

ruta_output = r'C:\Users\aimee\OneDrive\Escritorio\GitHub\Quilimari_MODFLOW\output'
ruta_data = r'C:\Users\aimee\OneDrive\Escritorio\GitHub\Quilimari_MODFLOW\data'
ruta_WEAP = r'C:\Users\aimee\OneDrive\Documentos\WEAP Areas\Quilimari_WEAP_MODFLOW_RDM\MODFLOW'

#ZB = ['Zones.zbr', 'Zones_RL.zbr']
ZB = ['Zones.zbr']

# Se crea ruta según la versión
dir_version = ruta_output + '/' + version
if not os.path.isdir(dir_version):
    os.mkdir(dir_version)

# COMPLETE BALANCE
if __name__ == "__main__":
    # start the MP pool for asynchronous parallelization
    pool = mp.Pool(int(mp.cpu_count()/2))

    for i in ZB:
        # Creación de sub-carpetas para análisis separados
        directorio = ruta_output + '/' + version + '/' + i[0:-4]
        if not os.path.isdir(directorio):
            os.mkdir(directorio)

        dir_temp = directorio + '/temp'
        if not os.path.isdir(dir_temp):
            os.mkdir(dir_temp)
        
        # Variables
        nombre_archivo_ZB = i
        nombre_carpeta_MF = 'SWI2_v1'
        zones = ['Q01','Q02','Q03','Q04','Q05','Q06','Q07','Q08','Q09']  # Zone Budget Zones
        aliases = {1: 'Q01',2: 'Q02',3: 'Q03',4:'Q04',5:'Q05',6:'Q06',7:'Q07',8:'Q08',9:'Q09'} # Alias Zone Budget Zone
            
        path_salida = directorio
        path_balance = ruta_WEAP + '/' + nombre_carpeta_MF
        path_ZB = ruta_data + '/' + nombre_archivo_ZB
        temp_path = dir_temp
        
        # Ejecución funciones de Procesamiento
        pool.apply_async(
            get_full_balance,
            args = (
                path_balance, path_ZB, path_salida, temp_path, aliases, zones
            )
        )
        
    pool.close()
    pool.join()

if __name__ == "__main__":
    for i in ZB:
        temp_path = ruta_output + '/' + version + '/' + i[0:-4] + '/temp'
        # Elimina carpeta temporal
        try:
            os.rmdir(temp_path)
        except OSError as e:
            print("Error: %s: %s" % (temp_path, e.strerror))

###################################################
####    POST - PROCESSING - MODFLOW RESULTS    ####
###################################################

if __name__ == "__main__":
    ruta_BALANCE_ZB = ruta_output + '/' + version + '/Zones'
    #ruta_BALANCE_ZB = ruta_output + '/' + version + '/' + ZB[0][0:-4]
    #ruta_BALANCE_ZB_RL = ruta_output + '/' + version + '/' + ZB[1][0:-4]

    ruta_export_BALANCE = ruta_output + '/' + version + '/BALANCE'
    if not os.path.isdir(ruta_export_BALANCE):
        os.mkdir(ruta_export_BALANCE)

    fecha = pd.read_csv(ruta_data + '/Fechas.csv')
    fecha["anios"] = fecha["Fecha"].apply(lambda x: int(x[-4:]))
    fecha = fecha.query(f"anios <= {end_year}")
    fecha = fecha.iloc[:-39,:]
    #anios = pd.DataFrame({'Fecha': range(start_year + 2, end_year + 1)}) # Para año calendario
    anios = pd.DataFrame({'Fecha': range(start_year + 2 , end_year)}) # Para año hidrológico

    variables = ['Variacion Neta Flujo Interacuifero', 'Recarga desde río', 'Recarga Lateral', 
                 'Recarga distribuida', 'Recarga', 'Variacion Neta Flujo Mar', 'Afloramiento - DRAIN', 
                 'Afloramiento - RIVER', 'Afloramiento total', 'Bombeos', 'Almacenamiento']

    # SERIES ANUALES - AÑO HIDROLÓGICO
    for j in zones:
        Resumen = pd.DataFrame(columns = variables)

        df = pd.read_csv(ruta_BALANCE_ZB + '/' + j + '.csv')
        #df_RL = pd.read_csv(ruta_BALANCE_ZB_RL + '/' + j + '.csv')
    
        df_temp = get_df_ls(df, fecha)
        #df_RL_temp = get_df_ls(df_RL, fecha)
           
        # ANALISIS
        FI_in = (df_temp['FROM_ZONE_0'].to_numpy() + df_temp['FROM_Q01'].to_numpy() + df_temp['FROM_Q02'].to_numpy() + df_temp['FROM_Q03'].to_numpy() + df_temp['FROM_Q04'].to_numpy() + 
                 df_temp['FROM_Q05'].to_numpy() + df_temp['FROM_Q06'].to_numpy() + df_temp['FROM_Q07'].to_numpy() + df_temp['FROM_Q08'].to_numpy() + df_temp['FROM_Q09'].to_numpy())
        FI_out = (df_temp['TO_ZONE_0'].to_numpy() + df_temp['TO_Q01'].to_numpy() + df_temp['TO_Q02'].to_numpy() + df_temp['TO_Q03'].to_numpy() + df_temp['TO_Q04'].to_numpy() + 
                  df_temp['TO_Q05'].to_numpy() + df_temp['TO_Q06'].to_numpy() + df_temp['TO_Q07'].to_numpy() + df_temp['TO_Q08'].to_numpy() + df_temp['TO_Q09'].to_numpy())
        Resumen.loc[:,'Variacion Neta Flujo Interacuifero'] = FI_in - FI_out
        
        Rch_rio = (df_temp['FROM_RIVER_LEAKAGE'].to_numpy())
        Resumen.loc[:,'Recarga desde río'] = Rch_rio
        
        Rch_lat = (df_temp['FROM_WELLS'].to_numpy())
        #Rch_lat = (df_RL_temp['FROM_WELLS'].to_numpy())
        Resumen.loc[:,'Recarga Lateral'] = Rch_lat

        #Rch_well = (df_temp['FROM_WELLS'].to_numpy()) - Rch_lat
        Rch_dist = (df_temp['FROM_RECHARGE'].to_numpy())
        Resumen.loc[:,'Recarga distribuida'] = Rch_dist
        #Resumen.loc[:,'Recarga distribuida'] = Rch_dist + Rch_well

        Resumen.loc[:, 'Recarga'] = Rch_rio + Rch_lat + Rch_dist
    
        Resumen.loc[:,'Variacion Neta Flujo Mar'] = (df_temp['FROM_CONSTANT_HEAD'].to_numpy() - df_temp['TO_CONSTANT_HEAD'].to_numpy())
        
        Af_Drain = -(df_temp['TO_DRAINS'].to_numpy())
        Resumen.loc[:,'Afloramiento - DRAIN'] = Af_Drain

        Af_RIVER = -(df_temp['TO_RIVER_LEAKAGE'].to_numpy())
        Resumen.loc[:,'Afloramiento - RIVER'] = Af_RIVER

        Resumen.loc[:,'Afloramiento total'] = Af_Drain + Af_RIVER
        
        Resumen.loc[:,'Bombeos'] = -(df_temp['TO_WELLS'].to_numpy())
    
        Resumen.loc[:,'Almacenamiento'] = -(df_temp['FROM_STORAGE'].to_numpy() - df_temp['TO_STORAGE'].to_numpy())

        Resumen = Resumen.to_numpy()
        data_prom = np.zeros((len(anios),11))
        for n in range(0,11):
            for m in range(0,len(anios)):
                data_prom[m,n] = np.mean(Resumen[:,n][52*m:52*m+52])   

        Res_anual = pd.DataFrame(data_prom, columns = variables)
        Res_anual.set_index(anios['Fecha'],inplace = True)
        Res_anual.to_excel(ruta_export_BALANCE + '/Resumen_balance_' + str(j) + '.xlsx')

    Quilimari = get_balance_cuenca(ruta_export_BALANCE, 0, 9, zones, variables, anios, 'Quilimari')
