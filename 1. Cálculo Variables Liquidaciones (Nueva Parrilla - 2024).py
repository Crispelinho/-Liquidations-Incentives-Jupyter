#!/usr/bin/env python
# coding: utf-8

# # INICIO

# In[1]:


import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
import import_ipynb
from shared_functions import DataframeLoader, Shared
import import_ipynb
from libraries.calculosba_areacalculo import AreaCalculoSBA
from libraries.calculo_objetivos_cualitativos import calculo_cumplimiento_objetivos_cualitativos
base_dir = 'C:\\Users\\Crisp\\Documents\\QualitySoftGroup\\Proyectos\\Liquidations-Incentives-Jupyter'
sub_direct = 'archivos-fuente-112'
folder_bbraun_source = os.path.join(base_dir, 'source', sub_direct)
folder_bbraun_output = os.path.join(base_dir, 'output')
print(folder_bbraun_source)


# In[2]:


dataframe_loader = DataframeLoader(
    base_dir=base_dir,
    sub_direct=sub_direct
)
shared = Shared()
print(dataframe_loader.cargar_dataframes)


# In[3]:


(
        df_resultado_detallado_previo,
        df_resultado_liquidaciones_previo,
        df_empleados,
        df_empleados_inactivos,
        df_tipos_empleados,
        df_clusters_empleado,
        df_zonas_empleado,
        df_salarios_variables,
        df_parrillas,
        df_venta_recaudo_real,
        df_venta_recaudo_presupuesto,
        df_venta_por_zona,
        df_recaudo_por_zona,
        df_rentabilidad,
        df_venta_otras_companias,
        df_recaudo_otras_companias,
        df_venta_recaudo_kams,
        df_rentabilidad_kam,
        df_venta_servicios_mtto,
        df_objetivos_cualitativos,
        df_incentivos_por_empleado,
        df_area_calculo_sba,
        df_factores_liquidacion,
        df_cluster_plan_real_recaudo,
        df_zona_plan_real_venta_ch,
        df_rentabilidad_zona,
        df_resultados_variables_cualitativas,
        df_zonas_clusters_empleado,
        df_centros_costos,
        df_empleados_centros_costos,
        df_centros_costos_grupos_productos_divisiones,
        df_parrillas_tipos_calculos,
        df_area_calculo_sba_centros_costos,
        df_renal_ambulatorio,
        codigos_ve_vc,
        _dataframes_entrada,
        fecha_liquidacion,
        meses_incentivos,
        OUTPUT_FOLDER
    ) = dataframe_loader.cargar_dataframes(base_dir, folder_bbraun_source)


# In[4]:


df_area_calculo_sba[df_area_calculo_sba['TipoEmpleado'] == 'AD4']


# In[5]:


def unir_dataframes_por_columnas_comunes(df1, df2, how='inner'):
    """
    Une dos DataFrames basándose en sus columnas comunes con un tipo de unión especificado.
    
    Parámetros:
    - df1: Primer DataFrame.
    - df2: Segundo DataFrame.
    - how: Tipo de unión ('left', 'right', 'outer', 'inner'). Por defecto es 'inner'.
    
    Retorna:
    - DataFrame resultante de la unión.
    """
    # Encuentra las columnas comunes entre ambos DataFrames
    columnas_comunes = df1.columns.intersection(df2.columns)
    print(columnas_comunes)
    # Une los DataFrames basándose en las columnas comunes y el tipo de unión especificado
    df_unido = pd.merge(df1, df2, on=columnas_comunes.tolist(), how=how)
    
    return df_unido


# In[ ]:





# In[6]:


columns_name_euros = ['RealCOP', 'PresupuestoCOP', 'PorcentajeCumplimientoCOP']
columns_name_list = ['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']
colums_name = columns_name_list + columns_name_euros
colums_name


# In[7]:


dataframe_loader.df_renal_ambulatorio


# In[8]:


# Traer áreas de cálculo y tipo de empleado de la maestra de empleados
empleados = df_empleados[['TipoEmpleado', 'CodigoEmpleado', 'AreaCalculo', 'FechaIngreso']].copy()
empleados_inactivos = df_empleados_inactivos[['TipoEmpleado', 'CodigoEmpleado', 'AreaCalculo', 'FechaIngreso']].copy()
total_empleados = pd.concat([empleados, empleados_inactivos])
total_empleados


# In[9]:


df_venta_recaudo_real.head()


# In[10]:


df_venta_recaudo_real['ZonaGeografica'] = df_venta_recaudo_real['ZonaGeografica'].str.replace(r'\b\w+\b', lambda x: x.group().title())
df_venta_recaudo_presupuesto['ZonaGeografica'] = df_venta_recaudo_presupuesto['ZonaGeografica'].str.replace(r'\b\w+\b', lambda x: x.group().title())


# In[11]:


df_venta_recaudo_real.head()


# In[12]:


df_venta_recaudo_presupuesto.head()


# In[13]:


df_parrillas[df_parrillas['Variable'] == 'VentaFacturadaPortafolioRenalSub']


# In[14]:


df_area_calculo_sba[df_area_calculo_sba['TipoEmpleado'] == 'GD']


# ### Se validan los dataframes de entrada

# In[15]:


for df in _dataframes_entrada:
    print(f'*** {df["name"]} ***')
    print(' - Head:')
    print(df['df'].head())
    print('')


# In[ ]:





# ### Funciones útiles

# In[16]:


# def sobreescribir_resultados_cualitativos(main, variable, columnas_extra, columnas_extra_merge, pre_merge_lambda=None):
#     print(columnas_extra, columnas_extra_merge)
#     resultados_cualitativos_variable = df_resultados_variables_cualitativas[columnas_extra + ['Variable', 'Fecha', 'PorcentajeCumplimiento']]
#     resultados_cualitativos_variable = resultados_cualitativos_variable[resultados_cualitativos_variable['Variable'] == variable]
#     if pre_merge_lambda:
#         resultados_cualitativos_variable = pre_merge_lambda(resultados_cualitativos_variable)
#     main_df = pd.merge(
#         left=main,
#         right=resultados_cualitativos_variable.rename(columns={'PorcentajeCumplimiento': 'PorcentajeCumplimientoPrecargado'}),
#         on=columnas_extra_merge+['Fecha', 'Variable'],
#         how='outer'
#     )
#     main_df['Real'] = np.where(
#         main_df['PorcentajeCumplimientoPrecargado'].isnull(),
#         main_df['Real'],
#         np.nan
#     )
#     main_df['Presupuesto'] = np.where(
#         main_df['PorcentajeCumplimientoPrecargado'].isnull(),
#         main_df['Presupuesto'],
#         np.nan
#     )
#     main_df['PorcentajeCumplimiento'] = np.where(
#         main_df['PorcentajeCumplimientoPrecargado'].isnull(),
#         main_df['PorcentajeCumplimiento'],
#         main_df['PorcentajeCumplimientoPrecargado']
#     )
#     main_df.drop(columns=['PorcentajeCumplimientoPrecargado'], inplace=True)

#     return main_df


# In[17]:


# def f_calculo_centro_costo_variable(main , variable):
#     if 'CodigoCentroCosto' not in main.columns:
#         main = main.merge(
#             df_centros_costos_grupos_productos_divisiones.rename(columns={'ConsecutivoParrilla':'Consecutivo'}),
#             on=['Division', 'GrupoProducto'],
#             how='left'
#         )
#     main['Consecutivo'] = main['Consecutivo'].astype(str)
#     main['Consecutivo'] = main['Consecutivo'].str.replace('.0', '')
#     main['Contexto'] = main['CodigoEmpleado'] + '_' + main['Consecutivo']
#     main = main.groupby(['Contexto', 'Fecha', 'GrupoProducto', 'Division', 'CodigoCentroCosto'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
#     main['Real'] = main.groupby(['Contexto', 'GrupoProducto', 'Division', 'CodigoCentroCosto'])['Real'].transform(pd.Series.cumsum)
#     main['Presupuesto'] = main.groupby(['Contexto', 'GrupoProducto', 'Division', 'CodigoCentroCosto'])['Presupuesto'].transform(pd.Series.cumsum)
#     #     main['RealTotal'] = main.groupby(['Contexto', 'Fecha'], as_index=False)[['Real']].transform(pd.Series.sum)
#     #     main['Porcentaje Aplicado'] = main['Real'] / main['RealTotal']
#     main['Variable'] = variable
#     return main


# In[18]:


# def f_calculo_centro_costo_variable_tipo_empleado(main, variable, tipo_empleado):    
#     main = main.merge(
#         df_centros_costos_grupos_productos_divisiones.rename(columns={'ConsecutivoParrilla':'Consecutivo'}),
#         on=['Division', 'GrupoProducto'],
#         how='left'
#     )
#     main['AreaCalculo'] = main['AreaCalculo'].astype(str)
#     main['Consecutivo'] = main['Consecutivo'].astype(str)
#     main['Contexto'] = tipo_empleado.lower() +'_'+ main['AreaCalculo'].str.cat(main['Consecutivo'],sep="_")
#     main = main.groupby(['Contexto', 'Fecha', 'GrupoProducto', 'Division', 'CodigoCentroCosto'], as_index=False)[['Real', 'Presupuesto']].apply(sum)    
#     main['Real'] = main.groupby(['Contexto', 'GrupoProducto', 'Division', 'CodigoCentroCosto'])['Real'].transform(pd.Series.cumsum)
#     main['Presupuesto'] = main.groupby(['Contexto', 'GrupoProducto', 'Division', 'CodigoCentroCosto'])['Presupuesto'].transform(pd.Series.cumsum)
#     main['Variable'] = variable
#     return main


# In[19]:


# def validar_calculo_centro_costo(main, contexto, fecha):
    
#     real = main[
#         (main['Contexto'] == contexto) &
#         (main['Fecha'] == fecha)
#     ]['Real'].sum()
    
#     presupuesto = main[
#         (main['Contexto'] == contexto) &
#         (main['Fecha'] == fecha)
#     ]['Presupuesto'].sum()
    
#     return real, presupuesto


# In[20]:


# def validar_calculo_centro_costo_division_sub(main, contexto, fecha):
    
#     real = main[
#         (main['Contexto'] == contexto) &
#         (main['Fecha'] == fecha)
#     ].groupby(['AreaCalculo', 'Consecutivo', 'Variable'])['Real'].transform(pd.Series.cumsum)
    
#     presupuesto = main[
#         (main['Contexto'] == contexto) &
#         (main['Fecha'] == fecha)
#     ].groupby(['AreaCalculo', 'Consecutivo', 'Variable'])['Presupuesto'].transform(pd.Series.cumsum)
    
#     return real, presupuesto


# ### Completamos el dataframe de SBAs y Áreas de cálculo

# In[21]:


productos_venta = df_venta_recaudo_real[['Division', 'GrupoProducto', 'SBA']]
productos_presupuesto = df_venta_recaudo_presupuesto[['Division', 'GrupoProducto', 'SBA']]
productos = pd.concat([productos_venta, productos_presupuesto])
productos = productos.drop_duplicates()
productos


# In[22]:


# TODO: Dejar de usar esta funcionalidad - utilizar la refactorizada

# productos_venta = df_venta_recaudo_real[['Division', 'GrupoProducto', 'SBA']]
# productos_presupuesto = df_venta_recaudo_presupuesto[['Division', 'GrupoProducto', 'SBA']]
# productos = pd.concat([productos_venta, productos_presupuesto])
# productos = productos.drop_duplicates()

##
_area_calculo_sba_notnull = df_area_calculo_sba[
    (df_area_calculo_sba['SBA'].notnull()) |
    (df_area_calculo_sba['SBA'] != '')
]
##
_area_calculo_sba_notnull


# In[23]:


_area_calculo_sba_por_grupo = df_area_calculo_sba[
    (df_area_calculo_sba['GrupoProducto'].isnull()) |
    (df_area_calculo_sba['GrupoProducto'] == '')
]
_area_calculo_sba_por_grupo


# In[24]:


_area_calculo_sba_por_grupo = pd.merge(
    left=_area_calculo_sba_por_grupo[['TipoEmpleado', 'AreaCalculo', 'Consecutivo', 'Division']],
    right=productos,
    on='Division',
    how='left'
)
_area_calculo_sba_por_grupo
##


# In[25]:


_area_calculo_sba_por_grupo[_area_calculo_sba_por_grupo['TipoEmpleado'] == 'GD']


# In[26]:



_area_calculo_sba_por_sba = df_area_calculo_sba[
    (
        (df_area_calculo_sba['SBA'].isnull()) |
        (df_area_calculo_sba['SBA'] == '')
    ) &
    (
        (df_area_calculo_sba['GrupoProducto'].notnull()) |
        (df_area_calculo_sba['GrupoProducto'] != '')
    )
]
_area_calculo_sba_por_sba = pd.merge(
    left=_area_calculo_sba_por_sba[['TipoEmpleado', 'AreaCalculo', 'GrupoProducto', 'Consecutivo', 'Division']],
    right=productos[['GrupoProducto', 'SBA']],
    on='GrupoProducto',
    how='left'
)

area_calculo_sba_completo = pd.concat([
    _area_calculo_sba_notnull,
    _area_calculo_sba_por_grupo,
    _area_calculo_sba_por_sba
])
area_calculo_sba_completo = area_calculo_sba_completo[['AreaCalculo', 'Consecutivo', 'Division', 'GrupoProducto', 'SBA', 'TipoEmpleado']]
area_calculo_sba_completo.fillna('', inplace=True)
area_calculo_sba_completo.drop_duplicates(inplace=True, subset=area_calculo_sba_completo.columns)
area_calculo_sba_completo


# In[27]:


area_calculo_sba_completo[
    (area_calculo_sba_completo['TipoEmpleado'] == 'GD') &
    (area_calculo_sba_completo['AreaCalculo'] == 3) &
    (area_calculo_sba_completo['Consecutivo'] == 5)
]


# In[28]:


area_calculo_sba_completo[area_calculo_sba_completo['TipoEmpleado'] == 'AD4']


# ### Remplazar funcionalidad - Funcionalidad con Refactorización

# In[29]:


c_area_calculo_sba = AreaCalculoSBA(dataframe_loader)
area_calculo_sba_completo = c_area_calculo_sba.get_area_calculo_sba_completo()
area_calculo_sba_completo


# In[30]:


area_calculo_sba_completo[
    (area_calculo_sba_completo['TipoEmpleado'] == 'GD') &
    (area_calculo_sba_completo['AreaCalculo'] == 3) &
    (area_calculo_sba_completo['Consecutivo'] == 5)
]


# In[31]:


c_area_calculo_sba.get_area_calculo_sba_completo_by_tipo_empleado('VC')


# # Se calculan los resultados para la maestra de resultados

# ## 1. Objetivos cualitativos (ObjetivosCualitativos)

# In[32]:


cumplimiento_objetivos_cualitativos = pd.DataFrame()
cumplimiento_objetivos_cualitativos['CodigoEmpleado'] = df_empleados['CodigoEmpleado']
cumplimiento_objetivos_cualitativos['merge'] = 1
##
fechas = pd.DataFrame(df_venta_recaudo_real['Fecha'].drop_duplicates())
fechas['merge'] = 1
##
cumplimiento_objetivos_cualitativos = cumplimiento_objetivos_cualitativos.merge(
    fechas,
    on='merge',
    how='left'
)
cumplimiento_objetivos_cualitativos.drop(columns='merge', inplace=True)

cumplimiento_objetivos_cualitativos = pd.merge(
    left=cumplimiento_objetivos_cualitativos,
    right=df_objetivos_cualitativos,
    on=['CodigoEmpleado', 'Fecha'],
    how='left'
)

cumplimiento_objetivos_cualitativos['Variable'] = 'ObjetivosCualitativos'
cumplimiento_objetivos_cualitativos['PorcentajeCumplimiento'].fillna(0.94, inplace=True)
cumplimiento_objetivos_cualitativos.rename(columns={'CodigoEmpleado': 'Contexto'}, inplace=True)
cumplimiento_objetivos_cualitativos.head()


# In[33]:


cumplimiento_objetivos_cualitativos[
    cumplimiento_objetivos_cualitativos['Contexto'] == '5004293'
]


# ## 2. Objetivos de recaudo por zona (VentaCobradaZonaBraunCrossDivisional)
# ### TODO: Ahora el empleado tiene varias zonas

# In[34]:


cumplimiento_recaudo_por_zona = df_recaudo_por_zona.copy()

cumplimiento_recaudo_por_zona['Real'] = cumplimiento_recaudo_por_zona.groupby(['Zona'])['Real'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_zona['Presupuesto'] = cumplimiento_recaudo_por_zona.groupby(['Zona'])['Presupuesto'].transform(pd.Series.cumsum)

cumplimiento_recaudo_por_zona['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_zona['Real'] / cumplimiento_recaudo_por_zona['Presupuesto']
cumplimiento_recaudo_por_zona['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_zona['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))

cumplimiento_recaudo_por_zona['Variable'] = 'VentaCobradaZonaBraunCrossDivisional'
cumplimiento_recaudo_por_zona.rename(columns={'Zona': 'Contexto'}, inplace=True)

cumplimiento_recaudo_por_zona = cumplimiento_recaudo_por_zona[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_recaudo_por_zona.head()


# ## 3. Objetivos de venta individuales (VentaFacturadaZonaCargoIndividual y VentaFacturadaZonaCargoIndividualSub)

# In[35]:


# venta real con información
venta_recaudo_real_info = df_venta_recaudo_real.merge(
    df_empleados[['CodigoEmpleado', 'TipoEmpleado', 'AreaCalculo', 'FechaIngreso']],
    on='CodigoEmpleado',
    how='left'
).merge(
    area_calculo_sba_completo,
    on=['Division', 'GrupoProducto', 'SBA', 'TipoEmpleado', 'AreaCalculo'],
    how='left'
)

# venta presupuesto con información
venta_recaudo_presupuesto_info = df_venta_recaudo_presupuesto.merge(
    df_empleados[['CodigoEmpleado', 'TipoEmpleado', 'AreaCalculo', 'FechaIngreso']],
    on='CodigoEmpleado',
    how='left'
).merge(
    area_calculo_sba_completo,
    on=['Division', 'GrupoProducto', 'SBA', 'TipoEmpleado', 'AreaCalculo'],
    how='left'
)


# In[36]:


_excepciones_venta_facturada = [
    # {
    #     'CodigoEmpleado': '5003527',
    #     'AreaCalculo': 7,
    #     'Consecutivo': 4,
    #     'FechaInicio': '2021-02-01',
    #     'FechaFin': '2021-06-01'
    # }
]

# venta real por empleado por area de calculo por fecha acumulada
venta_recaudo_real_empleado_sub = venta_recaudo_real_info[venta_recaudo_real_info['Fecha'] >= venta_recaudo_real_info['FechaIngreso']]

for excepcion in _excepciones_venta_facturada:
    venta_recaudo_real_empleado_sub = venta_recaudo_real_empleado_sub[
        (venta_recaudo_real_empleado_sub['CodigoEmpleado'] != excepcion['CodigoEmpleado']) |
        (venta_recaudo_real_empleado_sub['AreaCalculo'] != excepcion['AreaCalculo']) |
        (venta_recaudo_real_empleado_sub['Consecutivo'] != excepcion['Consecutivo']) |
        (venta_recaudo_real_empleado_sub['Fecha'] < excepcion['FechaInicio']) |
        (venta_recaudo_real_empleado_sub['Fecha'] > excepcion['FechaFin'])
    ]

venta_recaudo_real_empleado_sub_1 = venta_recaudo_real_empleado_sub.copy()[
    (venta_recaudo_real_empleado_sub['CodigoEmpleado'] == '5004563') &
    (venta_recaudo_real_empleado_sub['SBA'] == '84')
]
venta_recaudo_real_empleado_sub_1['CodigoEmpleado'] = '5005371'
venta_recaudo_real_empleado_sub_1['AreaCalculo'] = 14
venta_recaudo_real_empleado_sub_1['Consecutivo'] = 2

venta_recaudo_real_empleado_sub_2 = venta_recaudo_real_empleado_sub.copy()[
    (venta_recaudo_real_empleado_sub['CodigoEmpleado'] == '5004563') &
    (venta_recaudo_real_empleado_sub['SBA'] == '84')
]
venta_recaudo_real_empleado_sub_2['CodigoEmpleado'] = '5039251'
venta_recaudo_real_empleado_sub_2['AreaCalculo'] = 15
venta_recaudo_real_empleado_sub_2['Consecutivo'] = 2

venta_recaudo_real_empleado_sub_3 = venta_recaudo_real_empleado_sub.copy()[
    (venta_recaudo_real_empleado_sub['CodigoEmpleado'] == '5018231') & 
    (venta_recaudo_real_empleado_sub['Fecha'] >= '2021-08-01')
]
venta_recaudo_real_empleado_sub_3['CodigoEmpleado'] = '1128448666'
venta_recaudo_real_empleado_sub_3['AreaCalculo'] = 9


venta_recaudo_real_empleado_sub = pd.concat([
    venta_recaudo_real_empleado_sub,
    venta_recaudo_real_empleado_sub_1,
    venta_recaudo_real_empleado_sub_2,
    venta_recaudo_real_empleado_sub_3
])
venta_recaudo_real_empleado_sub = venta_recaudo_real_empleado_sub.groupby(['CodigoEmpleado', 'AreaCalculo',
                                                                           'Consecutivo', 'Fecha'], as_index=False).agg({'Venta': 'sum'})
venta_recaudo_real_empleado_sub['Real'] = venta_recaudo_real_empleado_sub['Venta']
venta_recaudo_real_empleado_sub.drop(columns=['Venta'], inplace=True)
venta_recaudo_real_empleado = venta_recaudo_real_empleado_sub.groupby(['CodigoEmpleado', 'AreaCalculo', 'Fecha'], as_index=False).agg({'Real': 'sum'})

# venta presupuesto por empleado por area de calculo por fecha acumulada
venta_recaudo_presupuesto_empleado_sub = venta_recaudo_presupuesto_info[venta_recaudo_presupuesto_info['Fecha'] >= venta_recaudo_presupuesto_info['FechaIngreso']]

for excepcion in _excepciones_venta_facturada:
    venta_recaudo_presupuesto_empleado_sub = venta_recaudo_presupuesto_empleado_sub[
        (venta_recaudo_presupuesto_empleado_sub['CodigoEmpleado'] != excepcion['CodigoEmpleado']) |
        (venta_recaudo_presupuesto_empleado_sub['AreaCalculo'] != excepcion['AreaCalculo']) |
        (venta_recaudo_presupuesto_empleado_sub['Consecutivo'] != excepcion['Consecutivo']) |
        (venta_recaudo_presupuesto_empleado_sub['Fecha'] < excepcion['FechaInicio']) |
        (venta_recaudo_presupuesto_empleado_sub['Fecha'] > excepcion['FechaFin'])
    ]

venta_recaudo_presupuesto_empleado_sub_1 = venta_recaudo_presupuesto_empleado_sub.copy()[
    (venta_recaudo_presupuesto_empleado_sub['CodigoEmpleado'] == '5004563') &
    (venta_recaudo_presupuesto_empleado_sub['SBA'] == '84')
]
venta_recaudo_presupuesto_empleado_sub_1['CodigoEmpleado'] = '5005371'
venta_recaudo_presupuesto_empleado_sub_1['AreaCalculo'] = 14
venta_recaudo_presupuesto_empleado_sub_1['Consecutivo'] = 2

venta_recaudo_presupuesto_empleado_sub_2 = venta_recaudo_presupuesto_empleado_sub.copy()[
    (venta_recaudo_presupuesto_empleado_sub['CodigoEmpleado'] == '5004563') &
    (venta_recaudo_presupuesto_empleado_sub['SBA'] == '84')
]
venta_recaudo_presupuesto_empleado_sub_2['CodigoEmpleado'] = '5039251'
venta_recaudo_presupuesto_empleado_sub_2['AreaCalculo'] = 15
venta_recaudo_presupuesto_empleado_sub_2['Consecutivo'] = 2

venta_recaudo_presupuesto_empleado_sub_3 = venta_recaudo_presupuesto_empleado_sub.copy()[
    (venta_recaudo_presupuesto_empleado_sub['CodigoEmpleado'] == '5018231') & 
    (venta_recaudo_presupuesto_empleado_sub['Fecha'] >= '2021-08-01')
]
venta_recaudo_presupuesto_empleado_sub_3['CodigoEmpleado'] = '1128448666'
venta_recaudo_presupuesto_empleado_sub_3['AreaCalculo'] = 9

venta_recaudo_presupuesto_empleado_sub = pd.concat([
    venta_recaudo_presupuesto_empleado_sub,
    venta_recaudo_presupuesto_empleado_sub_1,
    venta_recaudo_presupuesto_empleado_sub_2,
    venta_recaudo_presupuesto_empleado_sub_3
])
venta_recaudo_presupuesto_empleado_sub = venta_recaudo_presupuesto_empleado_sub.groupby(['CodigoEmpleado', 'AreaCalculo', 'Consecutivo', 'Fecha'], as_index=False).agg({'Venta': 'sum'})
venta_recaudo_presupuesto_empleado_sub['Presupuesto'] = venta_recaudo_presupuesto_empleado_sub['Venta']
venta_recaudo_presupuesto_empleado_sub.drop(columns=['Venta'], inplace=True)
venta_recaudo_presupuesto_empleado = venta_recaudo_presupuesto_empleado_sub.groupby(['CodigoEmpleado', 'AreaCalculo', 'Fecha'], as_index=False).agg({'Presupuesto': 'sum'})


## VentaFacturadaZonaCargoIndividual
cumplimiento_venta_individual = pd.merge(
    left=venta_recaudo_presupuesto_empleado,
    right=venta_recaudo_real_empleado,
    on=['CodigoEmpleado', 'AreaCalculo', 'Fecha'],
    how='outer'
)
cumplimiento_venta_individual['Presupuesto'].fillna(0.0, inplace=True)
cumplimiento_venta_individual['Real'].fillna(0.0, inplace=True)
cumplimiento_venta_individual['Presupuesto'] = cumplimiento_venta_individual.groupby(['CodigoEmpleado', 'AreaCalculo'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_venta_individual['Real'] = cumplimiento_venta_individual.groupby(['CodigoEmpleado', 'AreaCalculo'])['Real'].transform(pd.Series.cumsum)
cumplimiento_venta_individual['PorcentajeCumplimiento'] = cumplimiento_venta_individual['Real'] / cumplimiento_venta_individual['Presupuesto']
cumplimiento_venta_individual['PorcentajeCumplimiento'] = cumplimiento_venta_individual['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_venta_individual.rename(columns={'CodigoEmpleado': 'Contexto'}, inplace=True)
cumplimiento_venta_individual['Variable'] = 'VentaFacturadaZonaCargoIndividual'
cumplimiento_venta_individual = cumplimiento_venta_individual[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_venta_individual.head()

## VentaFacturadaZonaCargoIndividualSub
cumplimiento_venta_individual_sub = pd.merge(
    left=venta_recaudo_presupuesto_empleado_sub,
    right=venta_recaudo_real_empleado_sub,
    on=['CodigoEmpleado', 'AreaCalculo', 'Consecutivo', 'Fecha'],
    how='outer'
)
cumplimiento_venta_individual_sub['Real'].fillna(0.0, inplace=True)
cumplimiento_venta_individual_sub['Presupuesto'].fillna(0.0, inplace=True)

cumplimiento_venta_individual_sub['Presupuesto'] = cumplimiento_venta_individual_sub.groupby(['CodigoEmpleado', 'AreaCalculo', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_venta_individual_sub['Real'] = cumplimiento_venta_individual_sub.groupby(['CodigoEmpleado', 'AreaCalculo', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
cumplimiento_venta_individual_sub['PorcentajeCumplimiento'] = cumplimiento_venta_individual_sub['Real'] / cumplimiento_venta_individual_sub['Presupuesto']
cumplimiento_venta_individual_sub['PorcentajeCumplimiento'] = cumplimiento_venta_individual_sub['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_venta_individual_sub['PorcentajeCumplimiento'].fillna(0.0, inplace=True)
cumplimiento_venta_individual_sub['Consecutivo'] = cumplimiento_venta_individual_sub['Consecutivo'].astype(int).astype('str')
cumplimiento_venta_individual_sub['Contexto'] = cumplimiento_venta_individual_sub['CodigoEmpleado'].str.cat(cumplimiento_venta_individual_sub['Consecutivo'],sep="_")
cumplimiento_venta_individual_sub['Variable'] = 'VentaFacturadaZonaCargoIndividualSub'

cumplimiento_venta_individual_sub = cumplimiento_venta_individual_sub[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_venta_individual_sub.head()


# ## 4. Objetivos de recaudo individuales (VentasCobradaZonaCargoIndividual y VentasCobradaZonaCargoIndividualSub)

# In[37]:


_excepciones_venta_recaudo = [
    # {
    #     'CodigoEmpleado': '5003527',
    #     'AreaCalculo': 7,
    #     'Consecutivo': 4,
    #     'FechaInicio': '2021-02-01',
    #     'FechaFin': '2021-06-01'
    # }
]

# recaudo real por empleado por area de calculo y consecutivo por fecha acumulada
venta_recaudo_real_empleado_sub = venta_recaudo_real_info[venta_recaudo_real_info['Fecha'] >= venta_recaudo_real_info['FechaIngreso']]

for excepcion in _excepciones_venta_recaudo:
    venta_recaudo_real_empleado_sub = venta_recaudo_real_empleado_sub[
        (venta_recaudo_real_empleado_sub['CodigoEmpleado'] != excepcion['CodigoEmpleado']) |
        (venta_recaudo_real_empleado_sub['AreaCalculo'] != excepcion['AreaCalculo']) |
        (venta_recaudo_real_empleado_sub['Consecutivo'] != excepcion['Consecutivo']) |
        (venta_recaudo_real_empleado_sub['Fecha'] < excepcion['FechaInicio']) |
        (venta_recaudo_real_empleado_sub['Fecha'] > excepcion['FechaFin'])
    ]

venta_recaudo_real_empleado_sub_1 = venta_recaudo_real_empleado_sub.copy()[
    (venta_recaudo_real_empleado_sub['CodigoEmpleado'] == '5004563') &
    (venta_recaudo_real_empleado_sub['SBA'] == '84')
]
venta_recaudo_real_empleado_sub_1['CodigoEmpleado'] = '5005371'
venta_recaudo_real_empleado_sub_1['AreaCalculo'] = 14
venta_recaudo_real_empleado_sub_1['Consecutivo'] = 2

venta_recaudo_real_empleado_sub_2 = venta_recaudo_real_empleado_sub.copy()[
    (venta_recaudo_real_empleado_sub['CodigoEmpleado'] == '5004563') &
    (venta_recaudo_real_empleado_sub['SBA'] == '84')
]
venta_recaudo_real_empleado_sub_2['CodigoEmpleado'] = '5039251'
venta_recaudo_real_empleado_sub_2['AreaCalculo'] = 15
venta_recaudo_real_empleado_sub_2['Consecutivo'] = 2

venta_recaudo_real_empleado_sub_3 = venta_recaudo_real_empleado_sub.copy()[
    (venta_recaudo_real_empleado_sub['CodigoEmpleado'] == '5018231') & 
    (venta_recaudo_real_empleado_sub['Fecha'] >= '2021-08-01')
]
venta_recaudo_real_empleado_sub_3['CodigoEmpleado'] = '1128448666'
venta_recaudo_real_empleado_sub_3['AreaCalculo'] = 9

venta_recaudo_real_empleado_sub = pd.concat([
    venta_recaudo_real_empleado_sub,
    venta_recaudo_real_empleado_sub_1,
    venta_recaudo_real_empleado_sub_2,
    venta_recaudo_real_empleado_sub_3
])

venta_recaudo_real_empleado_sub = venta_recaudo_real_empleado_sub.groupby(['CodigoEmpleado', 'AreaCalculo', 'Consecutivo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
venta_recaudo_real_empleado_sub['Real'] = venta_recaudo_real_empleado_sub['Recaudo']
venta_recaudo_real_empleado_sub.drop(columns=['Recaudo'], inplace=True)
venta_recaudo_real_empleado = venta_recaudo_real_empleado_sub.groupby(['CodigoEmpleado', 'AreaCalculo', 'Fecha'], as_index=False).agg({'Real': 'sum'})

# recaudo presupuesto por empleado por area de calculo y consecutivo por fecha acumulada
venta_recaudo_presupuesto_empleado_sub = venta_recaudo_presupuesto_info[venta_recaudo_presupuesto_info['Fecha'] >= venta_recaudo_presupuesto_info['FechaIngreso']]

for excepcion in _excepciones_venta_recaudo:
    venta_recaudo_presupuesto_empleado_sub = venta_recaudo_presupuesto_empleado_sub[
        (venta_recaudo_presupuesto_empleado_sub['CodigoEmpleado'] != excepcion['CodigoEmpleado']) |
        (venta_recaudo_presupuesto_empleado_sub['AreaCalculo'] != excepcion['AreaCalculo']) |
        (venta_recaudo_presupuesto_empleado_sub['Consecutivo'] != excepcion['Consecutivo']) |
        (venta_recaudo_presupuesto_empleado_sub['Fecha'] < excepcion['FechaInicio']) |
        (venta_recaudo_presupuesto_empleado_sub['Fecha'] > excepcion['FechaFin'])
    ]

venta_recaudo_presupuesto_empleado_sub_1 = venta_recaudo_presupuesto_empleado_sub.copy()[
    (venta_recaudo_presupuesto_empleado_sub['CodigoEmpleado'] == '5004563') &
    (venta_recaudo_presupuesto_empleado_sub['SBA'] == '84')
]
venta_recaudo_presupuesto_empleado_sub_1['CodigoEmpleado'] = '5005371'
venta_recaudo_presupuesto_empleado_sub_1['AreaCalculo'] = 14
venta_recaudo_presupuesto_empleado_sub_1['Consecutivo'] = 2

venta_recaudo_presupuesto_empleado_sub_2 = venta_recaudo_presupuesto_empleado_sub.copy()[
    (venta_recaudo_presupuesto_empleado_sub['CodigoEmpleado'] == '5004563') &
    (venta_recaudo_presupuesto_empleado_sub['SBA'] == '84')
]
venta_recaudo_presupuesto_empleado_sub_2['CodigoEmpleado'] = '5039251'
venta_recaudo_presupuesto_empleado_sub_2['AreaCalculo'] = 15
venta_recaudo_presupuesto_empleado_sub_2['Consecutivo'] = 2

venta_recaudo_presupuesto_empleado_sub_3 = venta_recaudo_presupuesto_empleado_sub.copy()[
    (venta_recaudo_presupuesto_empleado_sub['CodigoEmpleado'] == '5018231') & 
    (venta_recaudo_presupuesto_empleado_sub['Fecha'] >= '2021-08-01')
]
venta_recaudo_presupuesto_empleado_sub_3['CodigoEmpleado'] = '1128448666'
venta_recaudo_presupuesto_empleado_sub_3['AreaCalculo'] = 9

venta_recaudo_presupuesto_empleado_sub = pd.concat([
    venta_recaudo_presupuesto_empleado_sub,
    venta_recaudo_presupuesto_empleado_sub_1,
    venta_recaudo_presupuesto_empleado_sub_2,
    venta_recaudo_presupuesto_empleado_sub_3
])

venta_recaudo_presupuesto_empleado_sub = venta_recaudo_presupuesto_empleado_sub.groupby(['CodigoEmpleado', 'AreaCalculo', 'Consecutivo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
venta_recaudo_presupuesto_empleado_sub['Presupuesto'] = venta_recaudo_presupuesto_empleado_sub['Recaudo']
venta_recaudo_presupuesto_empleado_sub.drop(columns=['Recaudo'], inplace=True)
venta_recaudo_presupuesto_empleado = venta_recaudo_presupuesto_empleado_sub.groupby(['CodigoEmpleado', 'AreaCalculo', 'Fecha'], as_index=False).agg({'Presupuesto': 'sum'})

## VentasCobradaZonaCargoIndividual
cumplimiento_recaudo_individual = pd.merge(
    left=venta_recaudo_presupuesto_empleado,
    right=venta_recaudo_real_empleado,
    on=['CodigoEmpleado', 'AreaCalculo', 'Fecha'],
    how='outer'
)
cumplimiento_recaudo_individual['Real'].fillna(0.0, inplace=True)
cumplimiento_recaudo_individual['Presupuesto'] = cumplimiento_recaudo_individual.groupby(['CodigoEmpleado', 'AreaCalculo'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_recaudo_individual['Real'] = cumplimiento_recaudo_individual.groupby(['CodigoEmpleado', 'AreaCalculo'])['Real'].transform(pd.Series.cumsum)
cumplimiento_recaudo_individual['PorcentajeCumplimiento'] = cumplimiento_recaudo_individual['Real'] / cumplimiento_recaudo_individual['Presupuesto']
cumplimiento_recaudo_individual['PorcentajeCumplimiento'] = cumplimiento_recaudo_individual['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_recaudo_individual.rename(columns={'CodigoEmpleado': 'Contexto'}, inplace=True)
cumplimiento_recaudo_individual['Variable'] = 'VentasCobradaZonaCargoIndividual'
cumplimiento_recaudo_individual = cumplimiento_recaudo_individual[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_recaudo_individual.head()


## VentasCobradaZonaCargoIndividualSub
cumplimiento_recaudo_individual_sub = pd.merge(
    left=venta_recaudo_presupuesto_empleado_sub,
    right=venta_recaudo_real_empleado_sub,
    on=['CodigoEmpleado', 'AreaCalculo', 'Consecutivo', 'Fecha'],
    how='outer'
)
cumplimiento_recaudo_individual_sub['Real'].fillna(0.0, inplace=True)
cumplimiento_recaudo_individual_sub['Presupuesto'].fillna(0.0, inplace=True)

_excepciones_recaudo_sub = []

for excepcion in _excepciones_recaudo_sub:
    cumplimiento_recaudo_individual_sub['Presupuesto'] = np.where(
        (
            (cumplimiento_recaudo_individual_sub['CodigoEmpleado'].isin(excepcion['Codigos'])) &
            (cumplimiento_recaudo_individual_sub['Consecutivo'] == excepcion['Consecutivo']) &
            # (cumplimiento_recaudo_individual_sub['Variable'] == 'VentasCobradaZonaCargoIndividualSub') &
            (cumplimiento_recaudo_individual_sub['Fecha'] >= excepcion['FechaInicial']) &
            (cumplimiento_recaudo_individual_sub['Fecha'] <= excepcion['FechaFinal'])
        ),
        excepcion['ValorPresupuesto'],
        cumplimiento_recaudo_individual_sub['Presupuesto']
    )
    cumplimiento_recaudo_individual_sub['Real'] = np.where(
        (
            (cumplimiento_recaudo_individual_sub['CodigoEmpleado'].isin(excepcion['Codigos'])) &
            (cumplimiento_recaudo_individual_sub['Consecutivo'] == excepcion['Consecutivo']) &
            # (cumplimiento_recaudo_individual_sub['Variable'] == 'VentasCobradaZonaCargoIndividualSub') &
            (cumplimiento_recaudo_individual_sub['Fecha'] >= excepcion['FechaInicial']) &
            (cumplimiento_recaudo_individual_sub['Fecha'] <= excepcion['FechaFinal'])
        ),
        excepcion['ValorReal'],
        cumplimiento_recaudo_individual_sub['Real']
    )


cumplimiento_recaudo_individual_sub['Presupuesto'] = cumplimiento_recaudo_individual_sub.groupby(['CodigoEmpleado', 'AreaCalculo', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_recaudo_individual_sub['Real'] = cumplimiento_recaudo_individual_sub.groupby(['CodigoEmpleado', 'AreaCalculo', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
cumplimiento_recaudo_individual_sub['PorcentajeCumplimiento'] = cumplimiento_recaudo_individual_sub['Real'] / cumplimiento_recaudo_individual_sub['Presupuesto']
cumplimiento_recaudo_individual_sub['PorcentajeCumplimiento'] = cumplimiento_recaudo_individual_sub['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_recaudo_individual_sub['PorcentajeCumplimiento'].fillna(0.0, inplace=True)
cumplimiento_recaudo_individual_sub['Consecutivo'] = cumplimiento_recaudo_individual_sub['Consecutivo'].astype(int).astype('str')
cumplimiento_recaudo_individual_sub['Contexto'] = cumplimiento_recaudo_individual_sub['CodigoEmpleado'].str.cat(cumplimiento_recaudo_individual_sub['Consecutivo'],sep="_")
cumplimiento_recaudo_individual_sub['Variable'] = 'VentasCobradaZonaCargoIndividualSub'
cumplimiento_recaudo_individual_sub = cumplimiento_recaudo_individual_sub[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_recaudo_individual_sub.head()


# ## 5. Objetivos de venta por área de cálculo (VentaFacturadaPorAreaCalculo y VentaFacturadaPorAreaCalculoSub)

# In[38]:


area_calculo_sba_completo_ve7 = area_calculo_sba_completo[
    (area_calculo_sba_completo['TipoEmpleado'] == 'VE')&
    (area_calculo_sba_completo['AreaCalculo'] == 7)
][['TipoEmpleado', 'AreaCalculo', 'Consecutivo', 'GrupoProducto', 'SBA']]


# In[39]:


resultados_venta_area_calculo_real = pd.merge(
    left=venta_recaudo_real_info[[
        'GrupoProducto', 
        'SBA', 
        'Fecha',
        'Venta'
    ]],
    right=area_calculo_sba_completo_ve7,
    on=['GrupoProducto', 'SBA']
)
resultados_venta_area_calculo_real_sub = resultados_venta_area_calculo_real.groupby(
    ['TipoEmpleado', 'AreaCalculo', 'Consecutivo', 'Fecha'],
    as_index=False
).agg({'Venta': 'sum'})
resultados_venta_area_calculo_real = resultados_venta_area_calculo_real.groupby(
    ['TipoEmpleado', 'AreaCalculo', 'Fecha'],
    as_index=False
).agg({'Venta': 'sum'})


resultados_venta_area_calculo_presupuesto = pd.merge(
    left=venta_recaudo_presupuesto_info[[
        'GrupoProducto', 
        'SBA', 
        'Fecha',
        'Venta'
    ]],
    right=area_calculo_sba_completo_ve7,
    on=['GrupoProducto', 'SBA']
)
resultados_venta_area_calculo_presupuesto_sub = resultados_venta_area_calculo_presupuesto.groupby(
    ['TipoEmpleado', 'AreaCalculo', 'Consecutivo', 'Fecha'],
    as_index=False
).agg({'Venta': 'sum'})
resultados_venta_area_calculo_presupuesto = resultados_venta_area_calculo_presupuesto.groupby(
    ['TipoEmpleado', 'AreaCalculo', 'Fecha'],
    as_index=False
).agg({'Venta': 'sum'})

## VentaFacturadaPorAreaCalculo
cumplimiento_venta_por_area_calculo = pd.merge(
    left=resultados_venta_area_calculo_presupuesto.rename(columns={'Venta': 'Presupuesto'}),
    right=resultados_venta_area_calculo_real.rename(columns={'Venta': 'Real'}),
    on=['TipoEmpleado', 'AreaCalculo', 'Fecha'],
    how='left'
)
cumplimiento_venta_por_area_calculo['Real'].fillna(0.0, inplace=True)

cumplimiento_venta_por_area_calculo['Variable'] = 'VentaFacturadaPorAreaCalculo'
cumplimiento_venta_por_area_calculo['Contexto'] = cumplimiento_venta_por_area_calculo['TipoEmpleado'].str.cat(cumplimiento_venta_por_area_calculo['AreaCalculo'].astype('str'),sep="_")
cumplimiento_venta_por_area_calculo['Real'] = cumplimiento_venta_por_area_calculo.groupby(['Contexto'])['Real'].transform(pd.Series.cumsum)
cumplimiento_venta_por_area_calculo['Presupuesto'] = cumplimiento_venta_por_area_calculo.groupby(['Contexto'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_venta_por_area_calculo['PorcentajeCumplimiento'] = cumplimiento_venta_por_area_calculo['Real'] / cumplimiento_venta_por_area_calculo['Presupuesto']

cumplimiento_venta_por_area_calculo = cumplimiento_venta_por_area_calculo[[
    'Contexto',
    'Variable',
    'Fecha',
    'Real',
    'Presupuesto',
    'PorcentajeCumplimiento'
]]

cumplimiento_venta_por_area_calculo.head()

## VentaFacturadaPorAreaCalculoSub
cumplimiento_venta_por_area_calculo_sub = pd.merge(
    left=resultados_venta_area_calculo_presupuesto_sub.rename(columns={'Venta': 'Presupuesto'}),
    right=resultados_venta_area_calculo_real_sub.rename(columns={'Venta': 'Real'}),
    on=['TipoEmpleado', 'AreaCalculo', 'Consecutivo', 'Fecha'],
    how='left'
)
cumplimiento_venta_por_area_calculo_sub['Real'].fillna(0.0, inplace=True)


cumplimiento_venta_por_area_calculo_sub['Variable'] = 'VentaFacturadaPorAreaCalculoSub'
cumplimiento_venta_por_area_calculo_sub['Contexto'] = cumplimiento_venta_por_area_calculo_sub['TipoEmpleado'].str.cat(cumplimiento_venta_por_area_calculo_sub['AreaCalculo'].astype('str'),sep="_")
cumplimiento_venta_por_area_calculo_sub['Contexto'] = cumplimiento_venta_por_area_calculo_sub['Contexto'].str.cat(cumplimiento_venta_por_area_calculo_sub['Consecutivo'].astype('str'), sep='_')
cumplimiento_venta_por_area_calculo_sub['Real'] = cumplimiento_venta_por_area_calculo_sub.groupby(['Contexto'])['Real'].transform(pd.Series.cumsum)
cumplimiento_venta_por_area_calculo_sub['Presupuesto'] = cumplimiento_venta_por_area_calculo_sub.groupby(['Contexto'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_venta_por_area_calculo_sub['PorcentajeCumplimiento'] = cumplimiento_venta_por_area_calculo_sub['Real'] / cumplimiento_venta_por_area_calculo_sub['Presupuesto']

cumplimiento_venta_por_area_calculo_sub = cumplimiento_venta_por_area_calculo_sub[[
    'Contexto',
    'Variable',
    'Fecha',
    'Real',
    'Presupuesto',
    'PorcentajeCumplimiento'
]]

cumplimiento_venta_por_area_calculo_sub.head()


# ## 6. Objetivos de recaudo por área de cálculo (VentasCobradaPorAreaCalculo y VentasCobradaPorAreaCalculoSub)

# In[40]:


resultados_recaudo_area_calculo_real = pd.merge(
    left=venta_recaudo_real_info[[
        'GrupoProducto', 
        'SBA', 
        'Fecha',
        'Recaudo'
    ]],
    right=area_calculo_sba_completo_ve7,
    on=['GrupoProducto', 'SBA']
)
resultados_recaudo_area_calculo_real_sub = resultados_recaudo_area_calculo_real.groupby(
    ['TipoEmpleado', 'AreaCalculo', 'Consecutivo', 'Fecha'],
    as_index=False
).agg({'Recaudo': 'sum'})
resultados_recaudo_area_calculo_real = resultados_recaudo_area_calculo_real.groupby(
    ['TipoEmpleado', 'AreaCalculo', 'Fecha'],
    as_index=False
).agg({'Recaudo': 'sum'})


resultados_recaudo_area_calculo_presupuesto = pd.merge(
    left=venta_recaudo_presupuesto_info[[
        'GrupoProducto', 
        'SBA', 
        'Fecha',
        'Recaudo'
    ]],
    right=area_calculo_sba_completo_ve7,
    on=['GrupoProducto', 'SBA']
)
resultados_recaudo_area_calculo_presupuesto_sub = resultados_recaudo_area_calculo_presupuesto.groupby(
    ['TipoEmpleado', 'AreaCalculo', 'Consecutivo', 'Fecha'],
    as_index=False
).agg({'Recaudo': 'sum'})
resultados_recaudo_area_calculo_presupuesto = resultados_recaudo_area_calculo_presupuesto.groupby(
    ['TipoEmpleado', 'AreaCalculo', 'Fecha'],
    as_index=False
).agg({'Recaudo': 'sum'})

## VentasCobradaPorAreaCalculo
cumplimiento_recaudo_por_area_calculo = pd.merge(
    left=resultados_recaudo_area_calculo_presupuesto.rename(columns={'Recaudo': 'Presupuesto'}),
    right=resultados_recaudo_area_calculo_real.rename(columns={'Recaudo': 'Real'}),
    on=['TipoEmpleado', 'AreaCalculo', 'Fecha'],
    how='left'
)
cumplimiento_recaudo_por_area_calculo['Real'].fillna(0.0, inplace=True)

cumplimiento_recaudo_por_area_calculo['Variable'] = 'VentasCobradaPorAreaCalculo'
cumplimiento_recaudo_por_area_calculo['Contexto'] = cumplimiento_recaudo_por_area_calculo['TipoEmpleado'].str.cat(cumplimiento_recaudo_por_area_calculo['AreaCalculo'].astype('str'),sep="_")
cumplimiento_recaudo_por_area_calculo['Real'] = cumplimiento_recaudo_por_area_calculo.groupby(['Contexto'])['Real'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_area_calculo['Presupuesto'] = cumplimiento_recaudo_por_area_calculo.groupby(['Contexto'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_area_calculo['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_area_calculo['Real'] / cumplimiento_venta_por_area_calculo['Presupuesto']

cumplimiento_recaudo_por_area_calculo = cumplimiento_recaudo_por_area_calculo[[
    'Contexto',
    'Variable',
    'Fecha',
    'Real',
    'Presupuesto',
    'PorcentajeCumplimiento'
]]

## VentasCobradaPorAreaCalculoSub
cumplimiento_recaudo_por_area_calculo_sub = pd.merge(
    left=resultados_recaudo_area_calculo_presupuesto_sub.rename(columns={'Recaudo': 'Presupuesto'}),
    right=resultados_recaudo_area_calculo_real_sub.rename(columns={'Recaudo': 'Real'}),
    on=['TipoEmpleado', 'AreaCalculo', 'Consecutivo', 'Fecha'],
    how='left'
)
cumplimiento_recaudo_por_area_calculo_sub['Real'].fillna(0.0, inplace=True)

cumplimiento_recaudo_por_area_calculo_sub['Variable'] = 'VentasCobradaPorAreaCalculoSub'
cumplimiento_recaudo_por_area_calculo_sub['Contexto'] = cumplimiento_recaudo_por_area_calculo_sub['TipoEmpleado'].str.cat(cumplimiento_recaudo_por_area_calculo_sub['AreaCalculo'].astype('str'),sep="_")
cumplimiento_recaudo_por_area_calculo_sub['Contexto'] = cumplimiento_recaudo_por_area_calculo_sub['Contexto'].str.cat(cumplimiento_recaudo_por_area_calculo_sub['Consecutivo'].astype('str'), sep='_')
cumplimiento_recaudo_por_area_calculo_sub['Real'] = cumplimiento_recaudo_por_area_calculo_sub.groupby(['Contexto'])['Real'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_area_calculo_sub['Presupuesto'] = cumplimiento_recaudo_por_area_calculo_sub.groupby(['Contexto'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_area_calculo_sub['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_area_calculo_sub['Real'] / cumplimiento_recaudo_por_area_calculo_sub['Presupuesto']

cumplimiento_recaudo_por_area_calculo_sub = cumplimiento_recaudo_por_area_calculo_sub[[
    'Contexto',
    'Variable',
    'Fecha',
    'Real',
    'Presupuesto',
    'PorcentajeCumplimiento'
]]

cumplimiento_recaudo_por_area_calculo_sub.head()


# ## 7. Objetivos de recaudo a nivel país (VentaCobradaBraunPais)

# In[41]:


# recaudo real pais
venta_recaudo_real_pais = df_venta_recaudo_real.groupby(['Fecha'], as_index=False).agg({'Recaudo': 'sum'})
venta_recaudo_real_pais['Real'] = venta_recaudo_real_pais['Recaudo']
venta_recaudo_real_pais.drop(columns=['Recaudo'], inplace=True)

# recaudo presupuesto pais
venta_recaudo_presupuesto_pais = df_venta_recaudo_presupuesto.groupby(['Fecha'], as_index=False).agg({'Recaudo': 'sum'})
venta_recaudo_presupuesto_pais['Presupuesto'] = venta_recaudo_presupuesto_pais['Recaudo']
venta_recaudo_presupuesto_pais.drop(columns=['Recaudo'], inplace=True)

##
cumplimiento_recaudo_nivel_pais = pd.merge(
    left=venta_recaudo_presupuesto_pais,
    right=venta_recaudo_real_pais,
    on=['Fecha'],
    how='left'
)
cumplimiento_recaudo_nivel_pais['Real'].fillna(0.0, inplace=True)
cumplimiento_recaudo_nivel_pais['Real'] = cumplimiento_recaudo_nivel_pais['Real'].transform(pd.Series.cumsum)
cumplimiento_recaudo_nivel_pais['Presupuesto'] = cumplimiento_recaudo_nivel_pais['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_recaudo_nivel_pais['PorcentajeCumplimiento'] = cumplimiento_recaudo_nivel_pais['Real'] / cumplimiento_recaudo_nivel_pais['Presupuesto']
cumplimiento_recaudo_nivel_pais['PorcentajeCumplimiento'] = cumplimiento_recaudo_nivel_pais['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))

cumplimiento_recaudo_nivel_pais['Contexto'] = 'Pais'
cumplimiento_recaudo_nivel_pais['Variable'] = 'VentaCobradaBraunPais'
cumplimiento_recaudo_nivel_pais = cumplimiento_recaudo_nivel_pais[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_recaudo_nivel_pais.head()


# ## 8. Objetivos de recaudo por unidad de negocio (VentaCobradaUnidadNegocio y VentaCobradaUnidadNegocioSub)

# ### 8.1. Para GU (VentaFacturadaUnidadNegocio - VentaFacturadaUnidadNegocioSub y VentaCobradaUnidadNegocio - VentaCobradaUnidadNegocioSub)

# In[42]:


area_calculo_sba_completo_gu = area_calculo_sba_completo[area_calculo_sba_completo['TipoEmpleado']=='GU']

#Se halla la informacion de la venta y recaudo real de los representantes de ventas de los GU
venta_recaudo_presupuesto_original_gu_real = area_calculo_sba_completo_gu.merge(
    df_venta_recaudo_real[['Division', 'GrupoProducto', 'SBA', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)

#Se halla la informacion de la venta y recaudo presupuesto de los representantes de ventas de los GU
venta_recaudo_presupuesto_original_gu_presupuesto = area_calculo_sba_completo_gu.merge(
    df_venta_recaudo_presupuesto[['Division', 'GrupoProducto', 'SBA', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)


# #### 8.2.1 VentaFacturadaUnidadNegocio - VentaFacturadaUnidadNegocioSub

# ##### VentaFacturadaUnidadNegocio

# In[43]:


venta_real_area_calculo_gu = venta_recaudo_presupuesto_original_gu_real.groupby(['AreaCalculo', 'Fecha'], as_index=False).agg({'Venta': 'sum'})
venta_real_area_calculo_gu['Real'] = venta_real_area_calculo_gu['Venta']
venta_real_area_calculo_gu.drop(columns=['Venta'], inplace=True)

venta_presupuesto_area_calculo_gu = venta_recaudo_presupuesto_original_gu_presupuesto.groupby(['AreaCalculo', 'Fecha'], as_index=False).agg({'Venta': 'sum'})
venta_presupuesto_area_calculo_gu['Presupuesto'] = venta_presupuesto_area_calculo_gu['Venta']
venta_presupuesto_area_calculo_gu.drop(columns=['Venta'], inplace=True)

cumplimiento_venta_por_unidad_negocio_gu = pd.merge(
    left=venta_presupuesto_area_calculo_gu,
    right=venta_real_area_calculo_gu,
    on=['AreaCalculo', 'Fecha'],
    how='left'
)
cumplimiento_venta_por_unidad_negocio_gu['Real'].fillna(0.0, inplace=True)
cumplimiento_venta_por_unidad_negocio_gu['Real'] = cumplimiento_venta_por_unidad_negocio_gu.groupby(['AreaCalculo'])['Real'].transform(pd.Series.cumsum)
cumplimiento_venta_por_unidad_negocio_gu['Presupuesto'] = cumplimiento_venta_por_unidad_negocio_gu.groupby(['AreaCalculo'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_venta_por_unidad_negocio_gu['PorcentajeCumplimiento'] = cumplimiento_venta_por_unidad_negocio_gu['Real'] / cumplimiento_venta_por_unidad_negocio_gu['Presupuesto']
cumplimiento_venta_por_unidad_negocio_gu['PorcentajeCumplimiento'] = cumplimiento_venta_por_unidad_negocio_gu['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_venta_por_unidad_negocio_gu['Variable'] = 'VentaFacturadaUnidadNegocio'

### Cálculo de resultados cualitativos precargados
cumplimiento_venta_por_unidad_negocio_gu = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_venta_por_unidad_negocio_gu,
    variable='VentaFacturadaUnidadNegocio',
    columnas_extra=['TipoEmpleado', 'AreaCalculo'], 
    columnas_extra_merge=['AreaCalculo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'GU']
)
###

cumplimiento_venta_por_unidad_negocio_gu['AreaCalculo'] = cumplimiento_venta_por_unidad_negocio_gu['AreaCalculo'].astype('str')
cumplimiento_venta_por_unidad_negocio_gu['Contexto'] = 'gu_' + cumplimiento_venta_por_unidad_negocio_gu['AreaCalculo']
cumplimiento_venta_por_unidad_negocio_gu = cumplimiento_venta_por_unidad_negocio_gu[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_venta_por_unidad_negocio_gu.head()


# ##### VentaFacturadaUnidadNegocioSub

# In[44]:


venta_real_area_calculo_gu_sub = venta_recaudo_presupuesto_original_gu_real.groupby(['AreaCalculo', 'Consecutivo', 'Fecha', 'Division', 'GrupoProducto'], as_index=False).agg({'Venta': 'sum'})
venta_real_area_calculo_gu_sub['Real'] = venta_real_area_calculo_gu_sub['Venta']
venta_real_area_calculo_gu_sub.drop(columns=['Venta'], inplace=True)

venta_presupuesto_area_calculo_gu_sub = venta_recaudo_presupuesto_original_gu_presupuesto.groupby(['AreaCalculo', 'Consecutivo', 'Fecha', 'Division', 'GrupoProducto'], as_index=False).agg({'Venta': 'sum'})
venta_presupuesto_area_calculo_gu_sub['Presupuesto'] = venta_presupuesto_area_calculo_gu_sub['Venta']
venta_presupuesto_area_calculo_gu_sub.drop(columns=['Venta'], inplace=True)

cumplimiento_venta_por_unidad_negocio_gu_sub = pd.merge(
    left=venta_presupuesto_area_calculo_gu_sub,
    right=venta_real_area_calculo_gu_sub,
    on=['AreaCalculo', 'Consecutivo', 'Fecha', 'Division', 'GrupoProducto'],
    how='left'
)
cumplimiento_venta_por_unidad_negocio_gu_sub['Real'].fillna(0.0, inplace=True)
cumplimiento_venta_por_unidad_negocio_gu_sub


# In[45]:


cumplimiento_venta_por_unidad_negocio_gu_sub_centro_costo = (
    dataframe_loader.f_calculo_centro_costo_variable_tipo_empleado(cumplimiento_venta_por_unidad_negocio_gu_sub, 'VentaFacturadaUnidadNegocioSub', 'GU')
)
cumplimiento_venta_por_unidad_negocio_gu_sub_centro_costo.head(20)


# In[46]:


cumplimiento_venta_por_unidad_negocio_gu_sub['Real'] = cumplimiento_venta_por_unidad_negocio_gu_sub.groupby(['AreaCalculo', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
cumplimiento_venta_por_unidad_negocio_gu_sub['Presupuesto'] = cumplimiento_venta_por_unidad_negocio_gu_sub.groupby(['AreaCalculo', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_venta_por_unidad_negocio_gu_sub['PorcentajeCumplimiento'] = cumplimiento_venta_por_unidad_negocio_gu_sub['Real'] / cumplimiento_venta_por_unidad_negocio_gu_sub['Presupuesto']
cumplimiento_venta_por_unidad_negocio_gu_sub['PorcentajeCumplimiento'] = cumplimiento_venta_por_unidad_negocio_gu_sub['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))

cumplimiento_venta_por_unidad_negocio_gu_sub['Variable'] = 'VentaFacturadaUnidadNegocioSub'

### Cálculo de resultados cualitativos precargados
cumplimiento_venta_por_unidad_negocio_gu_sub = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_venta_por_unidad_negocio_gu_sub,
    variable='VentaFacturadaUnidadNegocioSub',
    columnas_extra=['TipoEmpleado', 'AreaCalculo', 'Consecutivo'], 
    columnas_extra_merge=['AreaCalculo', 'Consecutivo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'GU']
)
###

cumplimiento_venta_por_unidad_negocio_gu_sub['Consecutivo'] = cumplimiento_venta_por_unidad_negocio_gu_sub['Consecutivo'].astype('str')
cumplimiento_venta_por_unidad_negocio_gu_sub['AreaCalculo'] = cumplimiento_venta_por_unidad_negocio_gu_sub['AreaCalculo'].astype('str')
cumplimiento_venta_por_unidad_negocio_gu_sub['Contexto'] = 'gu_' + cumplimiento_venta_por_unidad_negocio_gu_sub['AreaCalculo'].str.cat(cumplimiento_venta_por_unidad_negocio_gu_sub['Consecutivo'],sep="_")
cumplimiento_venta_por_unidad_negocio_gu_sub = cumplimiento_venta_por_unidad_negocio_gu_sub[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_venta_por_unidad_negocio_gu_sub.head()


# #### 8.2.2 VentaCobradaUnidadNegocio - VentaCobradaUnidadNegocioSub

# ##### VentaCobradaUnidadNegocio

# In[47]:


recaudo_real_area_calculo_gu = venta_recaudo_presupuesto_original_gu_real.groupby(['AreaCalculo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
recaudo_real_area_calculo_gu['Real'] = recaudo_real_area_calculo_gu['Recaudo']
recaudo_real_area_calculo_gu.drop(columns=['Recaudo'], inplace=True)

recaudo_presupuesto_area_calculo_gu = venta_recaudo_presupuesto_original_gu_presupuesto.groupby(['AreaCalculo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
recaudo_presupuesto_area_calculo_gu['Presupuesto'] = recaudo_presupuesto_area_calculo_gu['Recaudo']
recaudo_presupuesto_area_calculo_gu.drop(columns=['Recaudo'], inplace=True)

cumplimiento_recaudo_por_unidad_negocio_gu = pd.merge(
    left=recaudo_presupuesto_area_calculo_gu,
    right=recaudo_real_area_calculo_gu,
    on=['AreaCalculo', 'Fecha'],
    how='left'
)
cumplimiento_recaudo_por_unidad_negocio_gu['Real'].fillna(0.0, inplace=True)
cumplimiento_recaudo_por_unidad_negocio_gu['Real'] = cumplimiento_recaudo_por_unidad_negocio_gu.groupby(['AreaCalculo'])['Real'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_unidad_negocio_gu['Presupuesto'] = cumplimiento_recaudo_por_unidad_negocio_gu.groupby(['AreaCalculo'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_unidad_negocio_gu['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_unidad_negocio_gu['Real'] / cumplimiento_recaudo_por_unidad_negocio_gu['Presupuesto']
cumplimiento_recaudo_por_unidad_negocio_gu['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_unidad_negocio_gu['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_recaudo_por_unidad_negocio_gu['Variable'] = 'VentaCobradaUnidadNegocio'

### Cálculo de resultados cualitativos precargados
cumplimiento_recaudo_por_unidad_negocio_gu = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_recaudo_por_unidad_negocio_gu,
    variable='VentaCobradaUnidadNegocio',
    columnas_extra=['TipoEmpleado', 'AreaCalculo'], 
    columnas_extra_merge=['AreaCalculo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'GU']
)
###

cumplimiento_recaudo_por_unidad_negocio_gu['AreaCalculo'] = cumplimiento_recaudo_por_unidad_negocio_gu['AreaCalculo'].astype('str')
cumplimiento_recaudo_por_unidad_negocio_gu['Contexto'] = 'gu_' + cumplimiento_recaudo_por_unidad_negocio_gu['AreaCalculo']
cumplimiento_recaudo_por_unidad_negocio_gu = cumplimiento_recaudo_por_unidad_negocio_gu[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_recaudo_por_unidad_negocio_gu.head()


# ##### VentaCobradaUnidadNegocioSub

# In[48]:


recaudo_real_area_calculo_gu_sub = venta_recaudo_presupuesto_original_gu_real.groupby(['AreaCalculo', 'Consecutivo', 'Fecha', 'Division', 'GrupoProducto'], as_index=False).agg({'Recaudo': 'sum'})
recaudo_real_area_calculo_gu_sub['Real'] = recaudo_real_area_calculo_gu_sub['Recaudo']
recaudo_real_area_calculo_gu_sub.drop(columns=['Recaudo'], inplace=True)

recaudo_presupuesto_area_calculo_gu_sub = venta_recaudo_presupuesto_original_gu_presupuesto.groupby(['AreaCalculo', 'Consecutivo', 'Fecha', 'Division', 'GrupoProducto'], as_index=False).agg({'Recaudo': 'sum'})
recaudo_presupuesto_area_calculo_gu_sub['Presupuesto'] = recaudo_presupuesto_area_calculo_gu_sub['Recaudo']
recaudo_presupuesto_area_calculo_gu_sub.drop(columns=['Recaudo'], inplace=True)

cumplimiento_recaudo_por_unidad_negocio_gu_sub = pd.merge(
    left=recaudo_presupuesto_area_calculo_gu_sub,
    right=recaudo_real_area_calculo_gu_sub,
    on=['AreaCalculo', 'Consecutivo', 'Fecha', 'Division', 'GrupoProducto'],
    how='left'
)
cumplimiento_recaudo_por_unidad_negocio_gu_sub['Real'].fillna(0.0, inplace=True)
cumplimiento_recaudo_por_unidad_negocio_gu_sub


# In[49]:


cumplimiento_recaudo_por_unidad_negocio_gu_sub_centro_costo = (
    dataframe_loader.f_calculo_centro_costo_variable_tipo_empleado(cumplimiento_recaudo_por_unidad_negocio_gu_sub, 'VentaCobradaUnidadNegocioSub', 'GU')
)
cumplimiento_recaudo_por_unidad_negocio_gu_sub_centro_costo.head(20)


# In[50]:


cumplimiento_recaudo_por_unidad_negocio_gu_sub = cumplimiento_recaudo_por_unidad_negocio_gu_sub.groupby(['AreaCalculo', 'Consecutivo', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
cumplimiento_recaudo_por_unidad_negocio_gu_sub['Real'] = cumplimiento_recaudo_por_unidad_negocio_gu_sub.groupby(['AreaCalculo', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_unidad_negocio_gu_sub['Presupuesto'] = cumplimiento_recaudo_por_unidad_negocio_gu_sub.groupby(['AreaCalculo', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_unidad_negocio_gu_sub['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_unidad_negocio_gu_sub['Real'] / cumplimiento_recaudo_por_unidad_negocio_gu_sub['Presupuesto']
cumplimiento_recaudo_por_unidad_negocio_gu_sub['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_unidad_negocio_gu_sub['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_recaudo_por_unidad_negocio_gu_sub['Variable'] = 'VentaCobradaUnidadNegocioSub'

### Cálculo de resultados cualitativos precargados
cumplimiento_recaudo_por_unidad_negocio_gu_sub = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_recaudo_por_unidad_negocio_gu_sub,
    variable='VentaCobradaUnidadNegocioSub',
    columnas_extra=['TipoEmpleado', 'AreaCalculo', 'Consecutivo'], 
    columnas_extra_merge=['AreaCalculo', 'Consecutivo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'GU']
)
###

cumplimiento_recaudo_por_unidad_negocio_gu_sub['Consecutivo'] = cumplimiento_recaudo_por_unidad_negocio_gu_sub['Consecutivo'].astype('str')
cumplimiento_recaudo_por_unidad_negocio_gu_sub['AreaCalculo'] = cumplimiento_recaudo_por_unidad_negocio_gu_sub['AreaCalculo'].astype('str')
cumplimiento_recaudo_por_unidad_negocio_gu_sub['Contexto'] = 'gu_' + cumplimiento_recaudo_por_unidad_negocio_gu_sub['AreaCalculo'].str.cat(cumplimiento_recaudo_por_unidad_negocio_gu_sub['Consecutivo'],sep="_")
cumplimiento_recaudo_por_unidad_negocio_gu_sub = cumplimiento_recaudo_por_unidad_negocio_gu_sub[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_recaudo_por_unidad_negocio_gu_sub.head()


# In[51]:


cumplimiento_recaudo_por_unidad_negocio_gu_sub[
    (cumplimiento_recaudo_por_unidad_negocio_gu_sub['Fecha'] == '2023-01-01') &
     (cumplimiento_recaudo_por_unidad_negocio_gu_sub['Contexto'] == 'gu_8_3')
]


# In[52]:


cumplimiento_recaudo_por_unidad_negocio_gu_sub[
    (cumplimiento_recaudo_por_unidad_negocio_gu_sub['Fecha'] == '2023-01-01') &
     (cumplimiento_recaudo_por_unidad_negocio_gu_sub['Contexto'] == 'gu_1_1')
]


# ### 8.2. Para MK (VentaFacturadaUnidadNegocio - VentaFacturadaUnidadNegocioSub y VentaCobradaUnidadNegocio - VentaCobradaUnidadNegocioSub)

# In[53]:


area_calculo_sba_completo_mk = area_calculo_sba_completo[area_calculo_sba_completo['TipoEmpleado']=='MK']

#Se halla la informacion de la venta y recaudo real de los representantes de ventas de los MK
venta_recaudo_presupuesto_original_mk_real = area_calculo_sba_completo_mk.merge(
    df_venta_recaudo_real[['Division', 'GrupoProducto', 'SBA', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)

#Se halla la informacion de la venta y recaudo presupuesto de los representantes de ventas de los MK
venta_recaudo_presupuesto_original_mk_presupuesto = area_calculo_sba_completo_mk.merge(
    df_venta_recaudo_presupuesto[['Division', 'GrupoProducto', 'SBA', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)


# #### 8.2.1 VentaFacturadaUnidadNegocio - VentaFacturadaUnidadNegocioSub

# ##### VentaFacturadaUnidadNegocio

# In[54]:


venta_real_area_calculo_mk = venta_recaudo_presupuesto_original_mk_real.groupby(['AreaCalculo', 'Fecha'], as_index=False).agg({'Venta': 'sum'})
venta_real_area_calculo_mk['Real'] = venta_real_area_calculo_mk['Venta']
venta_real_area_calculo_mk.drop(columns=['Venta'], inplace=True)

venta_presupuesto_area_calculo_mk = venta_recaudo_presupuesto_original_mk_presupuesto.groupby(['AreaCalculo', 'Fecha'], as_index=False).agg({'Venta': 'sum'})
venta_presupuesto_area_calculo_mk['Presupuesto'] = venta_presupuesto_area_calculo_mk['Venta']
venta_presupuesto_area_calculo_mk.drop(columns=['Venta'], inplace=True)

cumplimiento_venta_por_unidad_negocio_mk = pd.merge(
    left=venta_presupuesto_area_calculo_mk,
    right=venta_real_area_calculo_mk,
    on=['AreaCalculo', 'Fecha'],
    how='left'
)
cumplimiento_venta_por_unidad_negocio_mk['Real'].fillna(0.0, inplace=True)
cumplimiento_venta_por_unidad_negocio_mk['Real'] = cumplimiento_venta_por_unidad_negocio_mk.groupby(['AreaCalculo'])['Real'].transform(pd.Series.cumsum)
cumplimiento_venta_por_unidad_negocio_mk['Presupuesto'] = cumplimiento_venta_por_unidad_negocio_mk.groupby(['AreaCalculo'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_venta_por_unidad_negocio_mk['PorcentajeCumplimiento'] = cumplimiento_venta_por_unidad_negocio_mk['Real'] / cumplimiento_venta_por_unidad_negocio_mk['Presupuesto']
cumplimiento_venta_por_unidad_negocio_mk['PorcentajeCumplimiento'] = cumplimiento_venta_por_unidad_negocio_mk['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_venta_por_unidad_negocio_mk['Variable'] = 'VentaFacturadaUnidadNegocio'
#areas de calculo son unidades de negocio

### Cálculo de resultados cualitativos precargados
cumplimiento_venta_por_unidad_negocio_mk = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_venta_por_unidad_negocio_mk,
    variable='VentaFacturadaUnidadNegocio',
    columnas_extra=['TipoEmpleado', 'AreaCalculo'], 
    columnas_extra_merge=['AreaCalculo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'MK']
)
###

cumplimiento_venta_por_unidad_negocio_mk['AreaCalculo'] = cumplimiento_venta_por_unidad_negocio_mk['AreaCalculo'].astype('str')
cumplimiento_venta_por_unidad_negocio_mk['Contexto'] = 'mk_' + cumplimiento_venta_por_unidad_negocio_mk['AreaCalculo']
cumplimiento_venta_por_unidad_negocio_mk = cumplimiento_venta_por_unidad_negocio_mk[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_venta_por_unidad_negocio_mk.head()


# ##### VentaFacturadaUnidadNegocioSub

# In[55]:


venta_real_area_calculo_mk_sub = venta_recaudo_presupuesto_original_mk_real.groupby(['AreaCalculo', 'Division', 'GrupoProducto','Consecutivo', 'Fecha'], as_index=False).agg({'Venta': 'sum'})
venta_real_area_calculo_mk_sub['Real'] = venta_real_area_calculo_mk_sub['Venta']
venta_real_area_calculo_mk_sub.drop(columns=['Venta'], inplace=True)

venta_presupuesto_area_calculo_mk_sub = venta_recaudo_presupuesto_original_mk_presupuesto.groupby(['AreaCalculo', 'Division', 'GrupoProducto', 'Consecutivo', 'Fecha'], as_index=False).agg({'Venta': 'sum'})
venta_presupuesto_area_calculo_mk_sub['Presupuesto'] = venta_presupuesto_area_calculo_mk_sub['Venta']
venta_presupuesto_area_calculo_mk_sub.drop(columns=['Venta'], inplace=True)

cumplimiento_venta_por_unidad_negocio_mk_sub = pd.merge(
    left=venta_presupuesto_area_calculo_mk_sub,
    right=venta_real_area_calculo_mk_sub,
    on=['AreaCalculo', 'Division', 'GrupoProducto' ,'Consecutivo', 'Fecha'],
    how='left'
)
cumplimiento_venta_por_unidad_negocio_mk_sub['Real'].fillna(0.0, inplace=True)
cumplimiento_venta_por_unidad_negocio_mk_sub


# In[56]:


# def f_calculo_centro_costo_variable_tipo_empleado(main, variable, tipo_empleado):
# #     df_area_calculo_sba_centros_costos_tipo_empleado = (
# #         df_area_calculo_sba_centros_costos[df_area_calculo_sba_centros_costos['TipoEmpleado'] == tipo_empleado]
# #     )    
    
#     main = main.merge(
#         df_centros_costos_grupos_productos_divisiones.rename(columns={'ConsecutivoParrilla':'Consecutivo'}),
#         on=['Division', 'GrupoProducto'],
#         how='left'
#     )
#     main['AreaCalculo'] = main['AreaCalculo'].astype(str)
#     main['Consecutivo'] = main['Consecutivo'].astype(str)
#     main['Contexto'] = tipo_empleado.lower() +'_'+ main['AreaCalculo'].str.cat(main['Consecutivo'],sep="_")
#     main = main.groupby(['Contexto', 'Fecha', 'GrupoProducto', 'Division', 'CodigoCentroCosto'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
    
# #     main['Real'] = main.groupby(['Contexto', 'CentroCosto'])['Real'].transform(pd.Series.sum)
# #     main['Presupuesto'] = main.groupby(['Contexto', 'CentroCosto'])['Presupuesto'].transform(pd.Series.sum)
    
#     main['Real'] = main.groupby(['Contexto', 'GrupoProducto', 'Division', 'CodigoCentroCosto'])['Real'].transform(pd.Series.cumsum)
#     main['Presupuesto'] = main.groupby(['Contexto', 'GrupoProducto', 'Division', 'CodigoCentroCosto'])['Presupuesto'].transform(pd.Series.cumsum)
#     main['Variable'] = variable
#     return main


# In[57]:


cumplimiento_venta_por_unidad_negocio_mk_sub_centro_costo = (
    dataframe_loader.f_calculo_centro_costo_variable_tipo_empleado(cumplimiento_venta_por_unidad_negocio_mk_sub, 'VentaFacturadaUnidadNegocioSub', 'MK')
)
cumplimiento_venta_por_unidad_negocio_mk_sub_centro_costo.head()


# In[58]:


cumplimiento_venta_por_unidad_negocio_mk_sub_centro_costo.columns


# In[59]:


pd.DataFrame(cumplimiento_venta_por_unidad_negocio_mk_sub_centro_costo.columns, columns=['Columna']).to_excel("cumplimiento_venta_por_unidad_negocio_mk_sub_centro_costo_columns.xlsx")


# In[60]:


cumplimiento_venta_por_unidad_negocio_mk_sub_centro_costo[
    (cumplimiento_venta_por_unidad_negocio_mk_sub_centro_costo['Contexto'] == 'mk_1_1') &
    (cumplimiento_venta_por_unidad_negocio_mk_sub_centro_costo['Fecha'] == '2023-01-01')
]


# In[61]:


dataframe_loader.validar_calculo_centro_costo(cumplimiento_venta_por_unidad_negocio_mk_sub_centro_costo, 'mk_1_1', '2023-01-01')


# In[62]:


cumplimiento_venta_por_unidad_negocio_mk_sub = cumplimiento_venta_por_unidad_negocio_mk_sub.groupby(
    ['AreaCalculo' ,'Consecutivo', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
cumplimiento_venta_por_unidad_negocio_mk_sub['Real'] = cumplimiento_venta_por_unidad_negocio_mk_sub.groupby(['AreaCalculo', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
cumplimiento_venta_por_unidad_negocio_mk_sub['Presupuesto'] = cumplimiento_venta_por_unidad_negocio_mk_sub.groupby(['AreaCalculo', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_venta_por_unidad_negocio_mk_sub['PorcentajeCumplimiento'] = cumplimiento_venta_por_unidad_negocio_mk_sub['Real'] / cumplimiento_venta_por_unidad_negocio_mk_sub['Presupuesto']
cumplimiento_venta_por_unidad_negocio_mk_sub['PorcentajeCumplimiento'] = cumplimiento_venta_por_unidad_negocio_mk_sub['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_venta_por_unidad_negocio_mk_sub['Variable'] = 'VentaFacturadaUnidadNegocioSub'
#areas de calculo son unidades de negocio

### Cálculo de resultados cualitativos precargados
cumplimiento_venta_por_unidad_negocio_mk_sub = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_venta_por_unidad_negocio_mk_sub,
    variable='VentaFacturadaUnidadNegocioSub',
    columnas_extra=['TipoEmpleado', 'AreaCalculo', 'Consecutivo'], 
    columnas_extra_merge=['AreaCalculo', 'Consecutivo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'MK']
)
###

cumplimiento_venta_por_unidad_negocio_mk_sub['Consecutivo'] = cumplimiento_venta_por_unidad_negocio_mk_sub['Consecutivo'].astype('str')
cumplimiento_venta_por_unidad_negocio_mk_sub['AreaCalculo'] = cumplimiento_venta_por_unidad_negocio_mk_sub['AreaCalculo'].astype('str')
cumplimiento_venta_por_unidad_negocio_mk_sub['Contexto'] = 'mk_' + cumplimiento_venta_por_unidad_negocio_mk_sub['AreaCalculo'].str.cat(cumplimiento_venta_por_unidad_negocio_mk_sub['Consecutivo'],sep="_")
cumplimiento_venta_por_unidad_negocio_mk_sub = cumplimiento_venta_por_unidad_negocio_mk_sub[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_venta_por_unidad_negocio_mk_sub[
    (cumplimiento_venta_por_unidad_negocio_mk_sub['Contexto'] == 'mk_1_1')
]


# #### 8.2.2 VentaCobradaUnidadNegocio - VentaCobradaUnidadNegocioSub

# ##### VentaCobradaUnidadNegocio

# In[63]:


recaudo_real_area_calculo_mk = venta_recaudo_presupuesto_original_mk_real.groupby(['AreaCalculo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
recaudo_real_area_calculo_mk['Real'] = recaudo_real_area_calculo_mk['Recaudo']
recaudo_real_area_calculo_mk.drop(columns=['Recaudo'], inplace=True)

recaudo_presupuesto_area_calculo_mk = venta_recaudo_presupuesto_original_mk_presupuesto.groupby(['AreaCalculo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
recaudo_presupuesto_area_calculo_mk['Presupuesto'] = recaudo_presupuesto_area_calculo_mk['Recaudo']
recaudo_presupuesto_area_calculo_mk.drop(columns=['Recaudo'], inplace=True)

cumplimiento_recaudo_por_unidad_negocio_mk = pd.merge(
    left=recaudo_presupuesto_area_calculo_mk,
    right=recaudo_real_area_calculo_mk,
    on=['AreaCalculo', 'Fecha'],
    how='left'
)
cumplimiento_recaudo_por_unidad_negocio_mk['Real'].fillna(0.0, inplace=True)
cumplimiento_recaudo_por_unidad_negocio_mk['Real'] = cumplimiento_recaudo_por_unidad_negocio_mk.groupby(['AreaCalculo'])['Real'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_unidad_negocio_mk['Presupuesto'] = cumplimiento_recaudo_por_unidad_negocio_mk.groupby(['AreaCalculo'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_unidad_negocio_mk['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_unidad_negocio_mk['Real'] / cumplimiento_recaudo_por_unidad_negocio_mk['Presupuesto']
cumplimiento_recaudo_por_unidad_negocio_mk['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_unidad_negocio_mk['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_recaudo_por_unidad_negocio_mk['Variable'] = 'VentaCobradaUnidadNegocio'
#areas de calculo son unidades de negocio

### Cálculo de resultados cualitativos precargados
cumplimiento_recaudo_por_unidad_negocio_mk = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_recaudo_por_unidad_negocio_mk,
    variable='VentaCobradaUnidadNegocio',
    columnas_extra=['TipoEmpleado', 'AreaCalculo'], 
    columnas_extra_merge=['AreaCalculo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'MK']
)
###

cumplimiento_recaudo_por_unidad_negocio_mk['AreaCalculo'] = cumplimiento_recaudo_por_unidad_negocio_mk['AreaCalculo'].astype('str')
cumplimiento_recaudo_por_unidad_negocio_mk['Contexto'] = 'mk_' + cumplimiento_recaudo_por_unidad_negocio_mk['AreaCalculo']
cumplimiento_recaudo_por_unidad_negocio_mk = cumplimiento_recaudo_por_unidad_negocio_mk[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_recaudo_por_unidad_negocio_mk.head()


# ##### VentaCobradaUnidadNegocioSub

# In[64]:


recaudo_real_area_calculo_mk_sub = venta_recaudo_presupuesto_original_mk_real.groupby(['AreaCalculo', 'Division', 'GrupoProducto','Consecutivo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
recaudo_real_area_calculo_mk_sub['Real'] = recaudo_real_area_calculo_mk_sub['Recaudo']
recaudo_real_area_calculo_mk_sub.drop(columns=['Recaudo'], inplace=True)

recaudo_presupuesto_area_calculo_mk_sub = venta_recaudo_presupuesto_original_mk_presupuesto.groupby(['AreaCalculo', 'Division', 'GrupoProducto', 'Consecutivo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
recaudo_presupuesto_area_calculo_mk_sub['Presupuesto'] = recaudo_presupuesto_area_calculo_mk_sub['Recaudo']
recaudo_presupuesto_area_calculo_mk_sub.drop(columns=['Recaudo'], inplace=True)

cumplimiento_recaudo_por_unidad_negocio_mk_sub = pd.merge(
    left=recaudo_presupuesto_area_calculo_mk_sub,
    right=recaudo_real_area_calculo_mk_sub,
    on=['AreaCalculo', 'Consecutivo', 'Fecha', 'Division', 'GrupoProducto'],
    how='left'
)
cumplimiento_recaudo_por_unidad_negocio_mk_sub['Real'].fillna(0.0, inplace=True)
cumplimiento_recaudo_por_unidad_negocio_mk_sub


# In[65]:


cumplimiento_recaudo_por_unidad_negocio_mk_sub_centro_costo = (
    dataframe_loader.f_calculo_centro_costo_variable_tipo_empleado(cumplimiento_recaudo_por_unidad_negocio_mk_sub, 'VentaCobradaUnidadNegocioSub', 'MK')
)
cumplimiento_recaudo_por_unidad_negocio_mk_sub_centro_costo


# In[66]:


dataframe_loader.validar_calculo_centro_costo(cumplimiento_recaudo_por_unidad_negocio_mk_sub_centro_costo, 'mk_1_1', '2023-02-01')


# In[67]:


cumplimiento_recaudo_por_unidad_negocio_mk_sub = (
    cumplimiento_recaudo_por_unidad_negocio_mk_sub.groupby(
        ['AreaCalculo', 'Consecutivo', 'Fecha'], as_index=False
    )[['Real', 'Presupuesto']].apply(sum)
)
cumplimiento_recaudo_por_unidad_negocio_mk_sub['Real'] = cumplimiento_recaudo_por_unidad_negocio_mk_sub.groupby(['AreaCalculo', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_unidad_negocio_mk_sub['Presupuesto'] = cumplimiento_recaudo_por_unidad_negocio_mk_sub.groupby(['AreaCalculo', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_unidad_negocio_mk_sub['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_unidad_negocio_mk_sub['Real'] / cumplimiento_recaudo_por_unidad_negocio_mk_sub['Presupuesto']
cumplimiento_recaudo_por_unidad_negocio_mk_sub['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_unidad_negocio_mk_sub['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_recaudo_por_unidad_negocio_mk_sub['Variable'] = 'VentaCobradaUnidadNegocioSub'
#areas de calculo son unidades de negocio

### Cálculo de resultados cualitativos precargados
cumplimiento_recaudo_por_unidad_negocio_mk_sub = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_recaudo_por_unidad_negocio_mk_sub,
    variable='VentaCobradaUnidadNegocioSub',
    columnas_extra=['TipoEmpleado', 'AreaCalculo', 'Consecutivo'], 
    columnas_extra_merge=['AreaCalculo', 'Consecutivo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'MK']
)
###

cumplimiento_recaudo_por_unidad_negocio_mk_sub['Consecutivo'] = cumplimiento_recaudo_por_unidad_negocio_mk_sub['Consecutivo'].astype('str')
cumplimiento_recaudo_por_unidad_negocio_mk_sub['AreaCalculo'] = cumplimiento_recaudo_por_unidad_negocio_mk_sub['AreaCalculo'].astype('str')
cumplimiento_recaudo_por_unidad_negocio_mk_sub['Contexto'] = 'mk_' + cumplimiento_recaudo_por_unidad_negocio_mk_sub['AreaCalculo'].str.cat(cumplimiento_recaudo_por_unidad_negocio_mk_sub['Consecutivo'],sep="_")
cumplimiento_recaudo_por_unidad_negocio_mk_sub = cumplimiento_recaudo_por_unidad_negocio_mk_sub[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_recaudo_por_unidad_negocio_mk_sub.head()


# ## 9. Objetivos de rentabilidad por unidad de negocio (RentabilidadUnidadNegocioCM2 y RentabilidadUnidadNegocioCM3)

# ### 9.1. Para GU (RentabilidadUnidadNegocioCM2 y RentabilidadUnidadNegocioCM3)

# In[68]:


rentabilidad_gu = df_rentabilidad.copy()
rentabilidad_gu['TipoEmpleado'] = 'GU'
rentabilidad_gu = rentabilidad_gu.merge(
    area_calculo_sba_completo[['TipoEmpleado', 'AreaCalculo', 'GrupoProducto', 'SBA']],
    on=['TipoEmpleado', 'GrupoProducto', 'SBA'],
    how='left'
)
rentabilidad_gu = rentabilidad_gu.groupby(
    ['TipoEmpleado', 'ClaseRentabilidad', 'Fecha', 'AreaCalculo'],
    as_index=False
).agg({'Real': 'sum', 'Presupuesto': 'sum'})

rentabilidad_gu['PorcentajeCumplimiento'] = rentabilidad_gu['Real'] / rentabilidad_gu['Presupuesto']
rentabilidad_gu['PorcentajeCumplimiento'] = rentabilidad_gu['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
rentabilidad_gu.head()


# In[69]:


cumplimiento_rentabilidad_por_unidad_negocio_cm2_gu = rentabilidad_gu[
    rentabilidad_gu['ClaseRentabilidad'] == 'CMII']
cumplimiento_rentabilidad_por_unidad_negocio_cm2_gu.drop(columns='ClaseRentabilidad', inplace=True)

cumplimiento_rentabilidad_por_unidad_negocio_cm2_gu['Variable'] = 'RentabilidadUnidadNegocioCM2'
### Cálculo de resultados cualitativos precargados
cumplimiento_rentabilidad_por_unidad_negocio_cm2_gu = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_rentabilidad_por_unidad_negocio_cm2_gu,
    variable='RentabilidadUnidadNegocioCM2',
    columnas_extra=['TipoEmpleado', 'AreaCalculo'],
    columnas_extra_merge=['TipoEmpleado', 'AreaCalculo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'GU']
)
###
cumplimiento_rentabilidad_por_unidad_negocio_cm2_gu['AreaCalculo'] = cumplimiento_rentabilidad_por_unidad_negocio_cm2_gu['AreaCalculo'].astype(int).astype('str')
cumplimiento_rentabilidad_por_unidad_negocio_cm2_gu['Contexto'] = 'gu_' +                                                                   cumplimiento_rentabilidad_por_unidad_negocio_cm2_gu[
                                                                      'AreaCalculo']
cumplimiento_rentabilidad_por_unidad_negocio_cm2_gu = cumplimiento_rentabilidad_por_unidad_negocio_cm2_gu[
    ['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_rentabilidad_por_unidad_negocio_cm2_gu.head()


# In[ ]:





# In[70]:


cumplimiento_rentabilidad_por_unidad_negocio_cm3_gu = rentabilidad_gu[
    rentabilidad_gu['ClaseRentabilidad'] == 'CMIII']
cumplimiento_rentabilidad_por_unidad_negocio_cm3_gu.drop(columns='ClaseRentabilidad', inplace=True)

cumplimiento_rentabilidad_por_unidad_negocio_cm3_gu['Variable'] = 'RentabilidadUnidadNegocioCM3'
### Cálculo de resultados cualitativos precargados
cumplimiento_rentabilidad_por_unidad_negocio_cm3_gu = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_rentabilidad_por_unidad_negocio_cm3_gu,
    variable='RentabilidadUnidadNegocioCM3',
    columnas_extra=['TipoEmpleado', 'AreaCalculo'],
    columnas_extra_merge=['TipoEmpleado', 'AreaCalculo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'GU']
)
###
cumplimiento_rentabilidad_por_unidad_negocio_cm3_gu['AreaCalculo'] = cumplimiento_rentabilidad_por_unidad_negocio_cm3_gu['AreaCalculo'].astype(int).astype('str')
cumplimiento_rentabilidad_por_unidad_negocio_cm3_gu['Contexto'] = 'gu_' +                                                                   cumplimiento_rentabilidad_por_unidad_negocio_cm3_gu[
                                                                      'AreaCalculo']
cumplimiento_rentabilidad_por_unidad_negocio_cm3_gu = cumplimiento_rentabilidad_por_unidad_negocio_cm3_gu[
    ['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
cumplimiento_rentabilidad_por_unidad_negocio_cm3_gu.head()


# ### 9.2. Para MK (RentabilidadUnidadNegocioCM2 y RentabilidadUnidadNegocioCM3)

# In[71]:


rentabilidad_mk = df_rentabilidad.copy()
rentabilidad_mk['TipoEmpleado'] = 'MK'
rentabilidad_mk = rentabilidad_mk.merge(
    area_calculo_sba_completo[['TipoEmpleado', 'AreaCalculo', 'GrupoProducto', 'SBA']],
    on=['TipoEmpleado', 'GrupoProducto', 'SBA'],
    how='left'
)
rentabilidad_mk = rentabilidad_mk.groupby(
    ['TipoEmpleado', 'ClaseRentabilidad', 'Fecha', 'AreaCalculo'],
    as_index=False
).agg({'Real': 'sum', 'Presupuesto': 'sum'})

# rentabilidad_mk_sub['Presupuesto'] = rentabilidad_mk_sub.groupby(['ClaseRentabilidad', 'AreaCalculo', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
# rentabilidad_mk_sub['Real'] = rentabilidad_mk_sub.groupby(['ClaseRentabilidad', 'AreaCalculo', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
rentabilidad_mk['PorcentajeCumplimiento'] = rentabilidad_mk['Real']/rentabilidad_mk['Presupuesto']
rentabilidad_mk['PorcentajeCumplimiento'] = rentabilidad_mk['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))

cumplimiento_rentabilidad_por_unidad_negocio_cm2_mk = rentabilidad_mk[rentabilidad_mk['ClaseRentabilidad']=='CMII']
cumplimiento_rentabilidad_por_unidad_negocio_cm2_mk.drop(columns='ClaseRentabilidad', inplace=True)
cumplimiento_rentabilidad_por_unidad_negocio_cm3_mk = rentabilidad_mk[rentabilidad_mk['ClaseRentabilidad']=='CMIII']
cumplimiento_rentabilidad_por_unidad_negocio_cm3_mk.drop(columns='ClaseRentabilidad', inplace=True)

cumplimiento_rentabilidad_por_unidad_negocio_cm2_mk['Variable'] = 'RentabilidadUnidadNegocioCM2'
### Cálculo de resultados cualitativos precargados
cumplimiento_rentabilidad_por_unidad_negocio_cm2_mk = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_rentabilidad_por_unidad_negocio_cm2_mk,
    variable='RentabilidadUnidadNegocioCM2',
    columnas_extra=['TipoEmpleado', 'AreaCalculo'], 
    columnas_extra_merge=['TipoEmpleado', 'AreaCalculo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'MK']
)
###
cumplimiento_rentabilidad_por_unidad_negocio_cm2_mk['AreaCalculo'] = cumplimiento_rentabilidad_por_unidad_negocio_cm2_mk['AreaCalculo'].astype(int).astype('str')
cumplimiento_rentabilidad_por_unidad_negocio_cm2_mk['Contexto'] = 'mk_' + cumplimiento_rentabilidad_por_unidad_negocio_cm2_mk['AreaCalculo']
cumplimiento_rentabilidad_por_unidad_negocio_cm2_mk = cumplimiento_rentabilidad_por_unidad_negocio_cm2_mk[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_rentabilidad_por_unidad_negocio_cm3_mk['Variable'] = 'RentabilidadUnidadNegocioCM3'
### Cálculo de resultados cualitativos precargados
cumplimiento_rentabilidad_por_unidad_negocio_cm3_mk = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_rentabilidad_por_unidad_negocio_cm3_mk,
    variable='RentabilidadUnidadNegocioCM3',
    columnas_extra=['TipoEmpleado', 'AreaCalculo'], 
    columnas_extra_merge=['TipoEmpleado', 'AreaCalculo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'MK']
)
###
cumplimiento_rentabilidad_por_unidad_negocio_cm3_mk['AreaCalculo'] = cumplimiento_rentabilidad_por_unidad_negocio_cm3_mk['AreaCalculo'].astype(int).astype('str')
cumplimiento_rentabilidad_por_unidad_negocio_cm3_mk['Contexto'] = 'mk_' + cumplimiento_rentabilidad_por_unidad_negocio_cm3_mk['AreaCalculo']
cumplimiento_rentabilidad_por_unidad_negocio_cm3_mk = cumplimiento_rentabilidad_por_unidad_negocio_cm3_mk[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
cumplimiento_rentabilidad_por_unidad_negocio_cm3_mk.head()


# ## 10. Objetivos de rentabilidad por unidad de negocio (RentabilidadUnidadNegocioCM2Sub y RentabilidadUnidadNegocioCM3Sub)

# ### 10.1. Para GU (RentabilidadUnidadNegocioCM2Sub y RentabilidadUnidadNegocioCM3Sub)

# In[72]:


# def f_calculo_centro_costo_variable_rentabilidad(main, variable, clase_rentabilidad, tipo_empleado):
#     main = main.merge(
#         df_centros_costos_grupos_productos_divisiones.rename(columns={'ConsecutivoParrilla':'Consecutivo'}),
#         on=['Division', 'GrupoProducto'],
#         how='left'
#     )
#     main['Consecutivo'] = main['Consecutivo'].astype(int).astype('str')
#     main['AreaCalculo'] = main['AreaCalculo'].astype(int).astype('str')
#     main['Contexto'] = tipo_empleado.lower()+'_' + main['AreaCalculo'] + '_' +main['Consecutivo']
#     main = main.groupby(
#         ['TipoEmpleado', 'ClaseRentabilidad', 'Fecha', 'AreaCalculo', 'Consecutivo', 'CodigoCentroCosto', 'Contexto'],
#         as_index=False
#     ).agg({'Real': 'sum', 'Presupuesto': 'sum'})
#     main = main[main['ClaseRentabilidad']==clase_rentabilidad]
#     main.drop(columns='ClaseRentabilidad', inplace=True)
#     main['Variable'] = variable
#     return main

# def calculo_porcentaje_cumplimiento(main, variable, tipo_empleado):

#     main = main.groupby(
#         ['TipoEmpleado', 'Fecha', 'AreaCalculo', 'Consecutivo', 'Variable'],
#         as_index=False
#     ).agg({'Real': 'sum', 'Presupuesto': 'sum'})
    
#     main['PorcentajeCumplimiento'] = (
#         main['Real']/
#         main['Presupuesto'])
#     main['PorcentajeCumplimiento'] = (
#         main['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
#     )
#     ### Cálculo de resultados cualitativos precargados
#     main = dataframe_loader.sobreescribir_resultados_cualitativos(
#         main=main,
#         variable=variable,
#         columnas_extra=['TipoEmpleado', 'AreaCalculo'], 
#         columnas_extra_merge=['TipoEmpleado', 'AreaCalculo'],
#         pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == tipo_empleado]
#     )
#     ###
#     main['Consecutivo'] = main['Consecutivo'].astype(int).astype('str')
#     main['AreaCalculo'] = main['AreaCalculo'].astype(int).astype('str')
    
#     main['Contexto'] = tipo_empleado.lower()+'_' + main['AreaCalculo'] + '_' +main['Consecutivo']
#     main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
#     return main


# In[73]:


#Calculamos la rentabilidad para GU adicionando toda la info de area_calculo_sba_completo
rentabilidad_gu_sub = df_rentabilidad.copy()
rentabilidad_gu_sub['TipoEmpleado'] = 'GU'
rentabilidad_gu_sub = rentabilidad_gu_sub.merge(
    area_calculo_sba_completo[['TipoEmpleado', 'AreaCalculo', 'GrupoProducto', 'Division', 'Consecutivo', 'SBA']],
    on=['TipoEmpleado', 'GrupoProducto', 'SBA'],
    how='left'
)
rentabilidad_gu_sub = rentabilidad_gu_sub.groupby(
    ['TipoEmpleado', 'ClaseRentabilidad', 'Fecha', 'AreaCalculo', 'GrupoProducto', 'Division', 'Consecutivo'],
    as_index=False
).agg({'Real': 'sum', 'Presupuesto': 'sum'})
rentabilidad_gu_sub


# In[74]:


cumplimiento_rentabilidad_por_unidad_negocio_cm2_gu_centro_costo = (
    dataframe_loader.f_calculo_centro_costo_variable_rentabilidad(rentabilidad_gu_sub, 'RentabilidadUnidadNegocioCM2Sub', 'CMII', 'GU')
)
cumplimiento_rentabilidad_por_unidad_negocio_cm2_gu_centro_costo


# In[75]:


cumplimiento_rentabilidad_por_unidad_negocio_cm2_gu_centro_costo[
    cumplimiento_rentabilidad_por_unidad_negocio_cm2_gu_centro_costo['Contexto'] == 'gu_2_3'
]


# In[76]:


dataframe_loader.validar_calculo_centro_costo(cumplimiento_rentabilidad_por_unidad_negocio_cm2_gu_centro_costo, 'gu_2_3', '2023-03-01')


# In[77]:


cumplimiento_rentabilidad_por_unidad_negocio_cm3_gu_centro_costo = (
    dataframe_loader.f_calculo_centro_costo_variable_rentabilidad(rentabilidad_gu_sub, 'RentabilidadUnidadNegocioCM3Sub', 'CMIII', 'GU')
)
cumplimiento_rentabilidad_por_unidad_negocio_cm3_gu_centro_costo


# In[78]:


dataframe_loader.validar_calculo_centro_costo(cumplimiento_rentabilidad_por_unidad_negocio_cm3_gu_centro_costo, 'gu_2_3', '2023-03-01')


# In[79]:


cumplimiento_rentabilidad_por_unidad_negocio_cm2_gu_sub = (
    dataframe_loader.calculo_porcentaje_cumplimiento(cumplimiento_rentabilidad_por_unidad_negocio_cm2_gu_centro_costo, 'RentabilidadUnidadNegocioCM2Sub', 'GU')
)
cumplimiento_rentabilidad_por_unidad_negocio_cm2_gu_sub.head()


# In[80]:


cumplimiento_rentabilidad_por_unidad_negocio_cm3_gu_sub = (
    dataframe_loader.calculo_porcentaje_cumplimiento(cumplimiento_rentabilidad_por_unidad_negocio_cm3_gu_centro_costo, 'RentabilidadUnidadNegocioCM3Sub', 'GU')
)
cumplimiento_rentabilidad_por_unidad_negocio_cm3_gu_sub.head()


# In[ ]:





# In[ ]:





# ### 10.2. Para MK (RentabilidadUnidadNegocioCM2Sub y RentabilidadUnidadNegocioCM3Sub)

# In[ ]:





# In[81]:


#Calculamos la rentabilidad para MK adicionando toda la info de area_calculo_sba_completo
rentabilidad_mk_sub = df_rentabilidad.copy()
rentabilidad_mk_sub['TipoEmpleado'] = 'MK'
rentabilidad_mk_sub = rentabilidad_mk_sub.merge(
    area_calculo_sba_completo[['TipoEmpleado', 'AreaCalculo', 'GrupoProducto', 'Division', 'Consecutivo', 'SBA']],
    on=['TipoEmpleado', 'GrupoProducto', 'SBA'],
    how='left'
)
rentabilidad_mk_sub = rentabilidad_mk_sub.groupby(
    ['TipoEmpleado', 'ClaseRentabilidad', 'Fecha', 'AreaCalculo', 'GrupoProducto', 'Division','Consecutivo'],
    as_index=False
).agg({'Real': 'sum', 'Presupuesto': 'sum'})
rentabilidad_mk_sub


# In[82]:


cumplimiento_rentabilidad_por_unidad_negocio_cm2_mk_centro_costo = (
    dataframe_loader.f_calculo_centro_costo_variable_rentabilidad(rentabilidad_mk_sub, 'RentabilidadUnidadNegocioCM2Sub', 'CMII', 'MK')
)
cumplimiento_rentabilidad_por_unidad_negocio_cm2_mk_centro_costo


# In[83]:


cumplimiento_rentabilidad_por_unidad_negocio_cm3_mk_centro_costo = (
    dataframe_loader.f_calculo_centro_costo_variable_rentabilidad(rentabilidad_mk_sub, 'RentabilidadUnidadNegocioCM3Sub', 'CMIII', 'MK')
)
cumplimiento_rentabilidad_por_unidad_negocio_cm3_mk_centro_costo


# In[84]:


cumplimiento_rentabilidad_por_unidad_negocio_cm2_mk_sub = (
    dataframe_loader.calculo_porcentaje_cumplimiento(cumplimiento_rentabilidad_por_unidad_negocio_cm2_mk_centro_costo, 'RentabilidadUnidadNegocioCM2Sub', 'MK')
)
cumplimiento_rentabilidad_por_unidad_negocio_cm2_mk_sub.head()


# In[85]:


cumplimiento_rentabilidad_por_unidad_negocio_cm3_mk_sub = (
    dataframe_loader.calculo_porcentaje_cumplimiento(cumplimiento_rentabilidad_por_unidad_negocio_cm3_mk_centro_costo, 'RentabilidadUnidadNegocioCM3Sub', 'MK')
)
cumplimiento_rentabilidad_por_unidad_negocio_cm3_mk_sub.head()


# ## 11. Objetivos de recaudo por división (VentaCobradaDivision)

# In[86]:


recaudo_real_por_division = df_venta_recaudo_real[['Division', 'Fecha', 'Recaudo']]
recaudo_real_por_division = recaudo_real_por_division.groupby(['Division', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
recaudo_real_por_division['Real'] = recaudo_real_por_division['Recaudo']
recaudo_real_por_division.drop(columns=['Recaudo'], inplace=True)
recaudo_real_por_division

recaudo_presupuesto_por_division = df_venta_recaudo_presupuesto[['Division', 'Fecha', 'Recaudo']]
recaudo_presupuesto_por_division = recaudo_presupuesto_por_division.groupby(['Division', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
recaudo_presupuesto_por_division['Presupuesto'] = recaudo_presupuesto_por_division['Recaudo']
recaudo_presupuesto_por_division.drop(columns=['Recaudo'], inplace=True)
recaudo_presupuesto_por_division

cumplimiento_recaudo_por_division = pd.merge(
    left=recaudo_presupuesto_por_division,
    right=recaudo_real_por_division,
    on=['Division', 'Fecha'],
    how='left'
)
cumplimiento_recaudo_por_division['Real'].fillna(0.0, inplace=True)
cumplimiento_recaudo_por_division['Real'] = cumplimiento_recaudo_por_division.groupby(['Division'])['Real'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_division['Presupuesto'] = cumplimiento_recaudo_por_division.groupby(['Division'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_division['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_division['Real'] / cumplimiento_recaudo_por_division['Presupuesto']
cumplimiento_recaudo_por_division['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_division['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))

cumplimiento_recaudo_por_division['Contexto'] = cumplimiento_recaudo_por_division['Division']
cumplimiento_recaudo_por_division['Variable'] = 'VentaCobradaDivision'
cumplimiento_recaudo_por_division = cumplimiento_recaudo_por_division[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
cumplimiento_recaudo_por_division.head()


# ## 12. Objetivos de recaudo por division con consecutivos (VentaCobradaDivisionSub)

# In[87]:


area_calculo_sba_completo_renal_vc_gd = area_calculo_sba_completo[
    (
        (area_calculo_sba_completo['TipoEmpleado'] == 'VC') &
#         (area_calculo_sba_completo['Consecutivo'] == 1) &
        (area_calculo_sba_completo['AreaCalculo'] == 5) 
    ) 
#     |
#     (
#         (area_calculo_sba_completo['TipoEmpleado'] == 'GD') &
#         (area_calculo_sba_completo['Consecutivo'] == 5) &
#         (area_calculo_sba_completo['AreaCalculo'] == 3) 
#     )
]

area_calculo_sba_completo_renal_vc_gd


# In[88]:


df_merged = area_calculo_sba_completo.merge(
    area_calculo_sba_completo_renal_vc_gd,
    how='left',
    indicator=True
).drop_duplicates()

area_calculo_sba_completo_without_renal = df_merged[df_merged['_merge'] == 'left_only']
area_calculo_sba_completo_without_renal


# In[89]:


area_calculo_sba_completo_with_renal = df_merged[df_merged['_merge'] == 'both']
area_calculo_sba_completo_with_renal


# ### 12.1. Para MK (VentaCobradaDivisionSub)

# In[90]:


# def f_calculo_centro_costo_variable_division_sub(main, variable, tipo_empleado):
# #     df_area_calculo_sba_centros_costos_tipo_empleado = (
# #         df_area_calculo_sba_centros_costos[df_area_calculo_sba_centros_costos['TipoEmpleado'] == tipo_empleado]
# #     )    
    
#     main = main.merge(
#         df_centros_costos_grupos_productos_divisiones.rename(columns={'ConsecutivoParrilla':'Consecutivo'}),
#         on=['Division', 'GrupoProducto'],
#         how='left'
#     )
#     main['AreaCalculo'] = main['AreaCalculo'].astype(str)
#     main['Consecutivo'] = main['Consecutivo'].astype(str)
#     main['Contexto'] = tipo_empleado.lower() +'_'+ main['AreaCalculo'].str.cat(main['Consecutivo'],sep="_")
# #     main = main.groupby(['Contexto', 'Fecha', 'GrupoProducto', 'Division', 'CodigoCentroCosto'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
    
# #     main['Real'] = main.groupby(['Contexto', 'CentroCosto'])['Real'].transform(pd.Series.sum)
# #     main['Presupuesto'] = main.groupby(['Contexto', 'CentroCosto'])['PresupuesVentaCobradaBraunPaisto'].transform(pd.Series.sum)
    
#     main['Real'] = main.groupby(['Contexto', 'GrupoProducto', 'Division', 'CodigoCentroCosto'])['Real'].transform(pd.Series.cumsum)
#     main['Presupuesto'] = main.groupby(['Contexto', 'GrupoProducto', 'Division', 'CodigoCentroCosto'])['Presupuesto'].transform(pd.Series.cumsum)
#     main['Variable'] = variable
#     return main


# In[91]:


cumplimiento_recaudo_por_division_sub_mk = pd.merge(
    left=recaudo_presupuesto_area_calculo_mk_sub,
    right=recaudo_real_area_calculo_mk_sub,
    on=['AreaCalculo', 'Consecutivo', 'Fecha', 'Division', 'GrupoProducto'],
    how='left'
)
cumplimiento_recaudo_por_division_sub_mk['Real'].fillna(0.0, inplace=True)
cumplimiento_recaudo_por_division_sub_mk


# In[92]:


cumplimiento_recaudo_por_division_sub_mk_centro_costo = (
    dataframe_loader.f_calculo_centro_costo_variable_division_sub(cumplimiento_recaudo_por_division_sub_mk, 'VentaCobradaDivisionSub', 'MK')
)
cumplimiento_recaudo_por_division_sub_mk_centro_costo


# In[ ]:





# In[93]:


cumplimiento_recaudo_por_division_sub_mk_centro_costo[
    (cumplimiento_recaudo_por_division_sub_mk_centro_costo['Contexto'] == 'mk_9_3') &
    (cumplimiento_recaudo_por_division_sub_mk_centro_costo['Fecha'] == '2023-09-01')
]


# In[94]:


cumplimiento_recaudo_por_division_sub_mk_centro_costo[
    (cumplimiento_recaudo_por_division_sub_mk_centro_costo['Contexto'] == 'mk_9_3') &
    (cumplimiento_recaudo_por_division_sub_mk_centro_costo['Fecha'] == '2023-01-01')
].groupby(['AreaCalculo', 'Consecutivo', 'Variable'])['Real'].transform(pd.Series.cumsum)


# In[95]:


cumplimiento_recaudo_por_division_sub_mk_centro_costo[
    (cumplimiento_recaudo_por_division_sub_mk_centro_costo['Contexto'] == 'mk_1_2') &
    (cumplimiento_recaudo_por_division_sub_mk_centro_costo['Fecha'] == '2023-01-01')
].groupby(['AreaCalculo', 'Consecutivo', 'Variable'])['Presupuesto'].transform(pd.Series.cumsum)


# In[96]:


dataframe_loader.validar_calculo_centro_costo_division_sub(cumplimiento_recaudo_por_division_sub_mk_centro_costo, 'mk_1_2', '2023-01-01')


# In[ ]:





# In[97]:


dataframe_loader.validar_calculo_centro_costo(cumplimiento_recaudo_por_division_sub_mk_centro_costo, 'mk_9_3', '2023-01-01')


# In[98]:


cumplimiento_recaudo_por_division_sub_mk = cumplimiento_recaudo_por_division_sub_mk.groupby(['AreaCalculo', 'Consecutivo', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
cumplimiento_recaudo_por_division_sub_mk['Real'] = cumplimiento_recaudo_por_division_sub_mk.groupby(['AreaCalculo', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_division_sub_mk['Presupuesto'] = cumplimiento_recaudo_por_division_sub_mk.groupby(['AreaCalculo', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_division_sub_mk['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_division_sub_mk['Real'] / cumplimiento_recaudo_por_division_sub_mk['Presupuesto']
cumplimiento_recaudo_por_division_sub_mk['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_division_sub_mk['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_recaudo_por_division_sub_mk['Variable'] = 'VentaCobradaDivisionSub'

### Cálculo de resultados cualitativos precargados
cumplimiento_recaudo_por_division_sub_mk = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_recaudo_por_division_sub_mk,
    variable='VentaCobradaDivisionSub',
    columnas_extra=['TipoEmpleado', 'AreaCalculo', 'Consecutivo'], 
    columnas_extra_merge=['AreaCalculo', 'Consecutivo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'MK'].drop(columns=['TipoEmpleado'])
)
###

cumplimiento_recaudo_por_division_sub_mk['Contexto'] = 'mk_' + cumplimiento_recaudo_por_division_sub_mk['AreaCalculo'].astype('str').str.cat(cumplimiento_recaudo_por_division_sub_mk['Consecutivo'].astype('str'), sep="_")
cumplimiento_recaudo_por_division_sub_mk = cumplimiento_recaudo_por_division_sub_mk[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_recaudo_por_division_sub_mk.head()


# In[99]:


cumplimiento_recaudo_por_division_sub_mk[
    (cumplimiento_recaudo_por_division_sub_mk['Contexto'] == 'mk_9_3') &
    (cumplimiento_recaudo_por_division_sub_mk['Fecha'] == '2023-01-01')
]


# In[100]:


cumplimiento_recaudo_por_division_sub_mk[
    (cumplimiento_recaudo_por_division_sub_mk['Contexto'] == 'mk_9_1') &
    (cumplimiento_recaudo_por_division_sub_mk['Fecha'] == '2023-01-01')
]


# In[101]:


cumplimiento_recaudo_por_division_sub_mk[
    (cumplimiento_recaudo_por_division_sub_mk['Contexto'] == 'mk_7_3') &
    (cumplimiento_recaudo_por_division_sub_mk['Fecha'] == '2023-01-01')
]


# In[102]:


cumplimiento_recaudo_por_division_sub_mk[
    (cumplimiento_recaudo_por_division_sub_mk['Contexto'].str.contains('mk_1_1')) &
    (cumplimiento_recaudo_por_division_sub_mk['Fecha'] == '2023-01-01')
]


# In[103]:


cumplimiento_recaudo_otras_companias_mk = pd.merge(
    left=area_calculo_sba_completo_mk,
    right=df_recaudo_otras_companias.rename(columns={'Compania': 'Division'}),
    on=['Division']
)
cumplimiento_recaudo_otras_companias_mk['PorcentajeCumplimiento'] = cumplimiento_recaudo_otras_companias_mk['Real']/cumplimiento_recaudo_otras_companias_mk['Presupuesto']
cumplimiento_recaudo_otras_companias_mk['PorcentajeCumplimiento'] = cumplimiento_recaudo_otras_companias_mk['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_recaudo_otras_companias_mk['Variable'] = 'VentaCobradaDivisionSub'
cumplimiento_recaudo_otras_companias_mk['Contexto'] = 'mk_' + cumplimiento_recaudo_otras_companias_mk['AreaCalculo'].astype('str').str.cat(cumplimiento_recaudo_otras_companias_mk['Consecutivo'].astype('str'), sep="_")
cumplimiento_recaudo_otras_companias_mk = cumplimiento_recaudo_otras_companias_mk[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_recaudo_otras_companias_mk.head()


# ### 12.2. Para GD (VentaCobradaDivisionSub)

# #### Cálculo sin Renal Ambulatorio

# In[104]:


# area_calculo_sba_completo_gd = area_calculo_sba_completo[area_calculo_sba_completo['TipoEmpleado']=='GD']
area_calculo_sba_completo_gd = area_calculo_sba_completo_without_renal[area_calculo_sba_completo_without_renal['TipoEmpleado']=='GD']
area_calculo_sba_completo_gd


# In[105]:


#Se halla la informacion de la venta y recaudo real de los representantes de ventas de los GD
venta_recaudo_presupuesto_original_gd_real = area_calculo_sba_completo_gd.merge(
    df_venta_recaudo_real[['Division', 'GrupoProducto', 'SBA', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)

#Se halla la informacion de la venta y recaudo presupuesto de los representantes de ventas de los GD
venta_recaudo_presupuesto_original_gd_presupuesto = area_calculo_sba_completo_gd.merge(
    df_venta_recaudo_presupuesto[['Division', 'GrupoProducto', 'SBA', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)

venta_recaudo_real_area_calculo_gd = venta_recaudo_presupuesto_original_gd_real.groupby(['AreaCalculo', 'Consecutivo', 'GrupoProducto', 'Division', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
venta_recaudo_real_area_calculo_gd['Real'] = venta_recaudo_real_area_calculo_gd['Recaudo']
venta_recaudo_real_area_calculo_gd.drop(columns=['Recaudo'], inplace=True)

venta_recaudo_presupuesto_area_calculo_gd = venta_recaudo_presupuesto_original_gd_presupuesto.groupby(['AreaCalculo', 'Consecutivo', 'GrupoProducto', 'Division', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
venta_recaudo_presupuesto_area_calculo_gd['Presupuesto'] = venta_recaudo_presupuesto_area_calculo_gd['Recaudo']
venta_recaudo_presupuesto_area_calculo_gd.drop(columns=['Recaudo'], inplace=True)

cumplimiento_recaudo_por_division_sub_gd = pd.merge(
    left=venta_recaudo_presupuesto_area_calculo_gd,
    right=venta_recaudo_real_area_calculo_gd,
    on=['AreaCalculo', 'Consecutivo', 'GrupoProducto', 'Division', 'Fecha'],
    how='left'
)
cumplimiento_recaudo_por_division_sub_gd


# In[106]:


cumplimiento_recaudo_por_division_sub_gd_centro_costo = (
    dataframe_loader.f_calculo_centro_costo_variable_division_sub(cumplimiento_recaudo_por_division_sub_gd, 'VentaCobradaDivisionSub', 'GD')
)
cumplimiento_recaudo_por_division_sub_gd_centro_costo


# In[107]:


cumplimiento_recaudo_por_division_sub_gd = cumplimiento_recaudo_por_division_sub_gd.groupby(['AreaCalculo', 'Consecutivo', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
cumplimiento_recaudo_por_division_sub_gd['Real'].fillna(0.0, inplace=True)
cumplimiento_recaudo_por_division_sub_gd['Real'] = cumplimiento_recaudo_por_division_sub_gd.groupby(['AreaCalculo', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_division_sub_gd['Presupuesto'] = cumplimiento_recaudo_por_division_sub_gd.groupby(['AreaCalculo', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_division_sub_gd['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_division_sub_gd['Real'] / cumplimiento_recaudo_por_division_sub_gd['Presupuesto']
cumplimiento_recaudo_por_division_sub_gd['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_division_sub_gd['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_recaudo_por_division_sub_gd['Variable'] = 'VentaCobradaDivisionSub'

### Cálculo de resultados cualitativos precargados
cumplimiento_recaudo_por_division_sub_gd = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_recaudo_por_division_sub_gd,
    variable='VentaCobradaDivisionSub',
    columnas_extra=['TipoEmpleado', 'AreaCalculo', 'Consecutivo'], 
    columnas_extra_merge=['AreaCalculo', 'Consecutivo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'GD'].drop(columns=['TipoEmpleado'])
)
###

cumplimiento_recaudo_por_division_sub_gd['Contexto'] = 'gd_' + cumplimiento_recaudo_por_division_sub_gd['AreaCalculo'].astype('str').str.cat(cumplimiento_recaudo_por_division_sub_gd['Consecutivo'].astype('str'), sep="_")
cumplimiento_recaudo_por_division_sub_gd = cumplimiento_recaudo_por_division_sub_gd[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
cumplimiento_recaudo_por_division_sub_gd


# #### Cálculo con Renal Ambulatorio

# In[108]:


area_calculo_sba_completo_gd_with_renal = area_calculo_sba_completo_with_renal[area_calculo_sba_completo_with_renal['TipoEmpleado']=='GD']
area_calculo_sba_completo_gd_with_renal


# In[109]:


df_renal_ambulatorio_gd = df_renal_ambulatorio[df_renal_ambulatorio['TipoEmpleado'] == 'GD']
df_renal_ambulatorio_gd


# In[110]:


area_calculo_sba_completo_gd_with_renal


# In[111]:


renal_ambulatorio_gd = df_renal_ambulatorio_gd.merge(
    area_calculo_sba_completo_gd_with_renal,
    on=['TipoEmpleado'],
    how='left'
)
renal_ambulatorio_gd


# In[112]:


# Calcular

main = renal_ambulatorio_gd.rename(columns={'PlanVentaCobradaRenal': 'Presupuesto', 'RealVentaCobradaRenal': 'Real'})

main['Contexto'] = 'gd_' + main['AreaCalculo'].astype('str').str.cat(main['Consecutivo'].astype('str'), sep="_")

# main['Contexto'] = main['CodigoEmpleado']

main = main.groupby(['Contexto', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main['Real'] = main.groupby(['Contexto'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['Contexto'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaCobradaDivisionSub'


#Formatear
main.reset_index(inplace=True)
main = main[['Contexto', 'Variable', 'Fecha', 'Real','Presupuesto', 'PorcentajeCumplimiento']]


cumplimiento_recaudo_por_division_sub_gd = pd.concat(
    [
        cumplimiento_recaudo_por_division_sub_gd.copy(),
        main.copy()
    ],
    ignore_index=True
)


main = None
cumplimiento_recaudo_por_division_sub_gd


# In[ ]:





# In[ ]:





# ### 12.2.1 Para GD (VentaFacturadaDivisionSub)

# In[113]:


#Se halla la informacion de la venta y recaudo real de los representantes de ventas de los GD
venta_facturada_presupuesto_original_gd_real = area_calculo_sba_completo_gd.merge(
    df_venta_recaudo_real[['Division', 'GrupoProducto', 'SBA', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)

#Se halla la informacion de la venta y recaudo presupuesto de los representantes de ventas de los GD
venta_facturada_presupuesto_original_gd_presupuesto = area_calculo_sba_completo_gd.merge(
    df_venta_recaudo_presupuesto[['Division', 'GrupoProducto', 'SBA', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)

venta_facturada_real_area_calculo_gd = venta_facturada_presupuesto_original_gd_real.groupby(['AreaCalculo', 'Consecutivo', 'GrupoProducto', 'Division', 'Fecha'], as_index=False).agg({'Venta': 'sum'})
venta_facturada_real_area_calculo_gd['Real'] = venta_facturada_real_area_calculo_gd['Venta']
venta_facturada_real_area_calculo_gd.drop(columns=['Venta'], inplace=True)

venta_facturada_presupuesto_area_calculo_gd = venta_facturada_presupuesto_original_gd_presupuesto.groupby(['AreaCalculo', 'Consecutivo', 'GrupoProducto', 'Division', 'Fecha'], as_index=False).agg({'Venta': 'sum'})
venta_facturada_presupuesto_area_calculo_gd['Presupuesto'] = venta_facturada_presupuesto_area_calculo_gd['Venta']
venta_facturada_presupuesto_area_calculo_gd.drop(columns=['Venta'], inplace=True)

cumplimiento_facturada_por_division_sub_gd = pd.merge(
    left=venta_facturada_presupuesto_area_calculo_gd,
    right=venta_facturada_real_area_calculo_gd,
    on=['AreaCalculo', 'Consecutivo', 'GrupoProducto', 'Division', 'Fecha'],
    how='left'
)
cumplimiento_facturada_por_division_sub_gd


# In[114]:


cumplimiento_facturada_por_division_sub_gd_centro_costo = (
    dataframe_loader.f_calculo_centro_costo_variable_division_sub(cumplimiento_facturada_por_division_sub_gd, 'VentaFacturadaDivisionSub', 'GD')
)
cumplimiento_facturada_por_division_sub_gd_centro_costo


# In[115]:


cumplimiento_facturada_por_division_sub_gd = cumplimiento_facturada_por_division_sub_gd.groupby(['AreaCalculo', 'Consecutivo', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
cumplimiento_facturada_por_division_sub_gd['Real'].fillna(0.0, inplace=True)
cumplimiento_facturada_por_division_sub_gd['Real'] = cumplimiento_facturada_por_division_sub_gd.groupby(['AreaCalculo', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
cumplimiento_facturada_por_division_sub_gd['Presupuesto'] = cumplimiento_facturada_por_division_sub_gd.groupby(['AreaCalculo', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_facturada_por_division_sub_gd['PorcentajeCumplimiento'] = cumplimiento_facturada_por_division_sub_gd['Real'] / cumplimiento_facturada_por_division_sub_gd['Presupuesto']
cumplimiento_facturada_por_division_sub_gd['PorcentajeCumplimiento'] = cumplimiento_facturada_por_division_sub_gd['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_facturada_por_division_sub_gd['Variable'] = 'VentaFacturadaDivisionSub'

### Cálculo de resultados cualitativos precargados
cumplimiento_facturada_por_division_sub_gd = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_facturada_por_division_sub_gd,
    variable='VentaCobradaDivisionSub',
    columnas_extra=['TipoEmpleado', 'AreaCalculo', 'Consecutivo'], 
    columnas_extra_merge=['AreaCalculo', 'Consecutivo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'GD'].drop(columns=['TipoEmpleado'])
)
###

cumplimiento_facturada_por_division_sub_gd['Contexto'] = 'gd_' + cumplimiento_facturada_por_division_sub_gd['AreaCalculo'].astype('str').str.cat(cumplimiento_facturada_por_division_sub_gd['Consecutivo'].astype('str'), sep="_")
cumplimiento_facturada_por_division_sub_gd = cumplimiento_facturada_por_division_sub_gd[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
cumplimiento_facturada_por_division_sub_gd


# In[116]:


cumplimiento_facturada_otras_companias_gd = pd.merge(
    left=area_calculo_sba_completo_gd,
    right=df_recaudo_otras_companias.rename(columns={'Compania': 'Division'}),
    on=['Division']
)
cumplimiento_facturada_otras_companias_gd['PorcentajeCumplimiento'] = cumplimiento_facturada_otras_companias_gd['Real']/cumplimiento_facturada_otras_companias_gd['Presupuesto']
cumplimiento_facturada_otras_companias_gd['PorcentajeCumplimiento'] = cumplimiento_facturada_otras_companias_gd['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_facturada_otras_companias_gd['Contexto'] = 'gd_' + cumplimiento_facturada_otras_companias_gd['AreaCalculo'].astype('str').str.cat(cumplimiento_facturada_otras_companias_gd['Consecutivo'].astype('str'), sep="_")
cumplimiento_facturada_otras_companias_gd['Variable'] = 'VentaFacturadaDivisionSub'
cumplimiento_facturada_otras_companias_gd = cumplimiento_facturada_otras_companias_gd[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_facturada_otras_companias_gd.head()


# In[ ]:





# ### 12.3. Para AD (VentaCobradaDivisionSub)

# In[117]:


area_calculo_sba_completo_ad = area_calculo_sba_completo[area_calculo_sba_completo['TipoEmpleado']=='AD']

#Se halla la informacion de la venta y recaudo real de los representantes de ventas de los AD
venta_recaudo_presupuesto_original_ad_real = area_calculo_sba_completo_ad.merge(
    df_venta_recaudo_real[['Division', 'GrupoProducto', 'SBA', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)

#Se halla la informacion de la venta y recaudo presupuesto de los representantes de ventas de los AD
venta_recaudo_presupuesto_original_ad_presupuesto = area_calculo_sba_completo_ad.merge(
    df_venta_recaudo_presupuesto[['Division', 'GrupoProducto', 'SBA', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)

venta_recaudo_real_area_calculo_ad = venta_recaudo_presupuesto_original_ad_real.groupby(['AreaCalculo', 'Consecutivo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
venta_recaudo_real_area_calculo_ad['Real'] = venta_recaudo_real_area_calculo_ad['Recaudo']
venta_recaudo_real_area_calculo_ad.drop(columns=['Recaudo'], inplace=True)

venta_recaudo_presupuesto_area_calculo_ad = venta_recaudo_presupuesto_original_ad_presupuesto.groupby(['AreaCalculo', 'Consecutivo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
venta_recaudo_presupuesto_area_calculo_ad['Presupuesto'] = venta_recaudo_presupuesto_area_calculo_ad['Recaudo']
venta_recaudo_presupuesto_area_calculo_ad.drop(columns=['Recaudo'], inplace=True)

cumplimiento_recaudo_por_division_sub_ad = pd.merge(
    left=venta_recaudo_presupuesto_area_calculo_ad,
    right=venta_recaudo_real_area_calculo_ad,
    on=['AreaCalculo', 'Consecutivo', 'Fecha'],
    how='left'
)
cumplimiento_recaudo_por_division_sub_ad['Real'].fillna(0.0, inplace=True)
cumplimiento_recaudo_por_division_sub_ad['Real'] = cumplimiento_recaudo_por_division_sub_ad.groupby(['AreaCalculo', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_division_sub_ad['Presupuesto'] = cumplimiento_recaudo_por_division_sub_ad.groupby(['AreaCalculo', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_division_sub_ad['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_division_sub_ad['Real'] / cumplimiento_recaudo_por_division_sub_ad['Presupuesto']
cumplimiento_recaudo_por_division_sub_ad['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_division_sub_ad['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_recaudo_por_division_sub_ad['Variable'] = 'VentaCobradaDivisionSub'

### Cálculo de resultados cualitativos precargados
cumplimiento_recaudo_por_division_sub_ad = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_recaudo_por_division_sub_ad,
    variable='VentaCobradaDivisionSub',
    columnas_extra=['TipoEmpleado', 'AreaCalculo', 'Consecutivo'], 
    columnas_extra_merge=['AreaCalculo', 'Consecutivo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'AD'].drop(columns=['TipoEmpleado'])
)
###

cumplimiento_recaudo_por_division_sub_ad['Contexto'] = 'ad_' + cumplimiento_recaudo_por_division_sub_ad['AreaCalculo'].astype('str').str.cat(cumplimiento_recaudo_por_division_sub_ad['Consecutivo'].astype('str'), sep="_")
cumplimiento_recaudo_por_division_sub_ad = cumplimiento_recaudo_por_division_sub_ad[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
cumplimiento_recaudo_por_division_sub_ad.head()


# In[118]:


cumplimiento_recaudo_por_division_sub_ad[cumplimiento_recaudo_por_division_sub_ad['Contexto'] == 'ad_3_1']


# In[119]:


cumplimiento_recaudo_otras_companias_ad = pd.merge(
    left=area_calculo_sba_completo_ad,
    right=df_recaudo_otras_companias.rename(columns={'Compania': 'Division'}),
    on=['Division']
)
cumplimiento_recaudo_otras_companias_ad['PorcentajeCumplimiento'] = cumplimiento_recaudo_otras_companias_ad['Real']/cumplimiento_recaudo_otras_companias_ad['Presupuesto']
cumplimiento_recaudo_otras_companias_ad['PorcentajeCumplimiento'] = cumplimiento_recaudo_otras_companias_ad['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_recaudo_otras_companias_ad['Contexto'] = 'ad_' + cumplimiento_recaudo_otras_companias_ad['AreaCalculo'].astype('str').str.cat(cumplimiento_recaudo_otras_companias_ad['Consecutivo'].astype('str'), sep="_")
cumplimiento_recaudo_otras_companias_ad['Variable'] = 'VentaCobradaDivisionSub'
cumplimiento_recaudo_otras_companias_ad = cumplimiento_recaudo_otras_companias_ad[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_recaudo_otras_companias_ad


# ### 12.4. Para AD1 (VentaCobradaDivisionSub)

# In[120]:


area_calculo_sba_completo_ad1 = area_calculo_sba_completo[area_calculo_sba_completo['TipoEmpleado']=='AD1']

#Se halla la informacion de la venta y recaudo real de los representantes de ventas de los AD1
venta_recaudo_presupuesto_original_ad1_real = area_calculo_sba_completo_ad1.merge(
    df_venta_recaudo_real[['Division', 'GrupoProducto', 'SBA', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)

#Se halla la informacion de la venta y recaudo presupuesto de los representantes de ventas de los AD1
venta_recaudo_presupuesto_original_ad1_presupuesto = area_calculo_sba_completo_ad1.merge(
    df_venta_recaudo_presupuesto[['Division', 'GrupoProducto', 'SBA', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)

venta_recaudo_real_area_calculo_ad1 = venta_recaudo_presupuesto_original_ad1_real.groupby(['AreaCalculo', 'Consecutivo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
venta_recaudo_real_area_calculo_ad1['Real'] = venta_recaudo_real_area_calculo_ad1['Recaudo']
venta_recaudo_real_area_calculo_ad1.drop(columns=['Recaudo'], inplace=True)

venta_recaudo_presupuesto_area_calculo_ad1 = venta_recaudo_presupuesto_original_ad1_presupuesto.groupby(['AreaCalculo', 'Consecutivo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
venta_recaudo_presupuesto_area_calculo_ad1['Presupuesto'] = venta_recaudo_presupuesto_area_calculo_ad1['Recaudo']
venta_recaudo_presupuesto_area_calculo_ad1.drop(columns=['Recaudo'], inplace=True)

cumplimiento_recaudo_por_division_sub_ad1 = pd.merge(
    left=venta_recaudo_presupuesto_area_calculo_ad1,
    right=venta_recaudo_real_area_calculo_ad1,
    on=['AreaCalculo', 'Consecutivo', 'Fecha'],
    how='left'
)
cumplimiento_recaudo_por_division_sub_ad1['Real'].fillna(0.0, inplace=True)
cumplimiento_recaudo_por_division_sub_ad1['Real'] = cumplimiento_recaudo_por_division_sub_ad1.groupby(['AreaCalculo', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_division_sub_ad1['Presupuesto'] = cumplimiento_recaudo_por_division_sub_ad1.groupby(['AreaCalculo', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_division_sub_ad1['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_division_sub_ad1['Real'] / cumplimiento_recaudo_por_division_sub_ad1['Presupuesto']
cumplimiento_recaudo_por_division_sub_ad1['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_division_sub_ad1['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_recaudo_por_division_sub_ad1['Variable'] = 'VentaCobradaDivisionSub'

### Cálculo de resultados cualitativos precargados
cumplimiento_recaudo_por_division_sub_ad1 = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_recaudo_por_division_sub_ad1,
    variable='VentaCobradaDivisionSub',
    columnas_extra=['TipoEmpleado', 'AreaCalculo', 'Consecutivo'], 
    columnas_extra_merge=['AreaCalculo', 'Consecutivo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'AD1'].drop(columns=['TipoEmpleado'])
)
###

cumplimiento_recaudo_por_division_sub_ad1['Contexto'] = 'ad1_' + cumplimiento_recaudo_por_division_sub_ad1['AreaCalculo'].astype('str').str.cat(cumplimiento_recaudo_por_division_sub_ad1['Consecutivo'].astype('str'), sep="_")
cumplimiento_recaudo_por_division_sub_ad1 = cumplimiento_recaudo_por_division_sub_ad1[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
cumplimiento_recaudo_por_division_sub_ad1.head()


# In[121]:


cumplimiento_recaudo_otras_companias_ad1 = pd.merge(
    left=area_calculo_sba_completo_ad1,
    right=df_recaudo_otras_companias.rename(columns={'Compania': 'Division'}),
    on=['Division']
)
cumplimiento_recaudo_otras_companias_ad1['PorcentajeCumplimiento'] = cumplimiento_recaudo_otras_companias_ad1['Real']/cumplimiento_recaudo_otras_companias_ad1['Presupuesto']
cumplimiento_recaudo_otras_companias_ad1['PorcentajeCumplimiento'] = cumplimiento_recaudo_otras_companias_ad1['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_recaudo_otras_companias_ad1['Contexto'] = 'ad1_' + cumplimiento_recaudo_otras_companias_ad1['AreaCalculo'].astype('str').str.cat(cumplimiento_recaudo_otras_companias_ad1['Consecutivo'].astype('str'), sep="_")
cumplimiento_recaudo_otras_companias_ad1['Variable'] = 'VentaCobradaDivisionSub'
cumplimiento_recaudo_otras_companias_ad1 = cumplimiento_recaudo_otras_companias_ad1[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_recaudo_otras_companias_ad1


# ### 12.5. Para AD3 (VentaCobradaDivisionSub)

# In[122]:


area_calculo_sba_completo_ad3 = area_calculo_sba_completo[area_calculo_sba_completo['TipoEmpleado']=='AD3']

#Se halla la informacion de la venta y recaudo real de los representantes de ventas de los AD3
venta_recaudo_presupuesto_original_ad3_real = area_calculo_sba_completo_ad3.merge(
    df_venta_recaudo_real[['Division', 'GrupoProducto', 'SBA', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)

#Se halla la informacion de la venta y recaudo presupuesto de los representantes de ventas de los AD3
venta_recaudo_presupuesto_original_ad3_presupuesto = area_calculo_sba_completo_ad3.merge(
    df_venta_recaudo_presupuesto[['Division', 'GrupoProducto', 'SBA', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)

venta_recaudo_real_area_calculo_ad3 = venta_recaudo_presupuesto_original_ad3_real.groupby(['AreaCalculo', 'Consecutivo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
venta_recaudo_real_area_calculo_ad3['Real'] = venta_recaudo_real_area_calculo_ad3['Recaudo']
venta_recaudo_real_area_calculo_ad3.drop(columns=['Recaudo'], inplace=True)

venta_recaudo_presupuesto_area_calculo_ad3 = venta_recaudo_presupuesto_original_ad3_presupuesto.groupby(['AreaCalculo', 'Consecutivo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
venta_recaudo_presupuesto_area_calculo_ad3['Presupuesto'] = venta_recaudo_presupuesto_area_calculo_ad3['Recaudo']
venta_recaudo_presupuesto_area_calculo_ad3.drop(columns=['Recaudo'], inplace=True)

cumplimiento_recaudo_por_division_sub_ad3 = pd.merge(
    left=venta_recaudo_presupuesto_area_calculo_ad3,
    right=venta_recaudo_real_area_calculo_ad3,
    on=['AreaCalculo', 'Consecutivo', 'Fecha'],
    how='left'
)
cumplimiento_recaudo_por_division_sub_ad3['Real'].fillna(0.0, inplace=True)
cumplimiento_recaudo_por_division_sub_ad3['Real'] = cumplimiento_recaudo_por_division_sub_ad3.groupby(['AreaCalculo', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_division_sub_ad3['Presupuesto'] = cumplimiento_recaudo_por_division_sub_ad3.groupby(['AreaCalculo', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_division_sub_ad3['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_division_sub_ad3['Real'] / cumplimiento_recaudo_por_division_sub_ad3['Presupuesto']
cumplimiento_recaudo_por_division_sub_ad3['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_division_sub_ad3['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_recaudo_por_division_sub_ad3['Variable'] = 'VentaCobradaDivisionSub'

### Cálculo de resultados cualitativos precargados
cumplimiento_recaudo_por_division_sub_ad3 = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_recaudo_por_division_sub_ad3,
    variable='VentaCobradaDivisionSub',
    columnas_extra=['TipoEmpleado', 'AreaCalculo', 'Consecutivo'], 
    columnas_extra_merge=['AreaCalculo', 'Consecutivo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'AD3'].drop(columns=['TipoEmpleado'])
)
###

cumplimiento_recaudo_por_division_sub_ad3['Contexto'] = 'ad3_' + cumplimiento_recaudo_por_division_sub_ad3['AreaCalculo'].astype('str').str.cat(cumplimiento_recaudo_por_division_sub_ad3['Consecutivo'].astype('str'), sep="_")
cumplimiento_recaudo_por_division_sub_ad3 = cumplimiento_recaudo_por_division_sub_ad3[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
cumplimiento_recaudo_por_division_sub_ad3.head()


# In[123]:


cumplimiento_recaudo_otras_companias_ad3 = pd.merge(
    left=area_calculo_sba_completo_ad3,
    right=df_recaudo_otras_companias.rename(columns={'Compania': 'Division'}),
    on=['Division']
)
cumplimiento_recaudo_otras_companias_ad3['PorcentajeCumplimiento'] = cumplimiento_recaudo_otras_companias_ad3['Real']/cumplimiento_recaudo_otras_companias_ad3['Presupuesto']
cumplimiento_recaudo_otras_companias_ad3['PorcentajeCumplimiento'] = cumplimiento_recaudo_otras_companias_ad3['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_recaudo_otras_companias_ad3['Contexto'] = 'ad3_' + cumplimiento_recaudo_otras_companias_ad3['AreaCalculo'].astype('str').str.cat(cumplimiento_recaudo_otras_companias_ad3['Consecutivo'].astype('str'), sep="_")
cumplimiento_recaudo_otras_companias_ad3['Variable'] = 'VentaCobradaDivisionSub'
cumplimiento_recaudo_otras_companias_ad3 = cumplimiento_recaudo_otras_companias_ad3[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_recaudo_otras_companias_ad3


# ### 12.6. Para AD4 (VentaCobradaDivisionSub)

# In[124]:


area_calculo_sba_completo_ad4 = area_calculo_sba_completo[area_calculo_sba_completo['TipoEmpleado']=='AD4']

#Se halla la informacion de la venta y recaudo real de los representantes de ventas de los AD4
venta_recaudo_presupuesto_original_ad4_real = area_calculo_sba_completo_ad4.merge(
    df_venta_recaudo_real[['Division', 'GrupoProducto', 'SBA', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)

#Se halla la informacion de la venta y recaudo presupuesto de los representantes de ventas de los AD4
venta_recaudo_presupuesto_original_ad4_presupuesto = area_calculo_sba_completo_ad4.merge(
    df_venta_recaudo_presupuesto[['Division', 'GrupoProducto', 'SBA', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)

venta_recaudo_real_area_calculo_ad4 = venta_recaudo_presupuesto_original_ad4_real.groupby(['AreaCalculo', 'Consecutivo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
venta_recaudo_real_area_calculo_ad4['Real'] = venta_recaudo_real_area_calculo_ad4['Recaudo']
venta_recaudo_real_area_calculo_ad4.drop(columns=['Recaudo'], inplace=True)

venta_recaudo_presupuesto_area_calculo_ad4 = venta_recaudo_presupuesto_original_ad4_presupuesto.groupby(['AreaCalculo', 'Consecutivo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
venta_recaudo_presupuesto_area_calculo_ad4['Presupuesto'] = venta_recaudo_presupuesto_area_calculo_ad4['Recaudo']
venta_recaudo_presupuesto_area_calculo_ad4.drop(columns=['Recaudo'], inplace=True)

cumplimiento_recaudo_por_division_sub_ad4 = pd.merge(
    left=venta_recaudo_presupuesto_area_calculo_ad4,
    right=venta_recaudo_real_area_calculo_ad4,
    on=['AreaCalculo', 'Consecutivo', 'Fecha'],
    how='left'
)
cumplimiento_recaudo_por_division_sub_ad4['Real'].fillna(0.0, inplace=True)
cumplimiento_recaudo_por_division_sub_ad4['Real'] = cumplimiento_recaudo_por_division_sub_ad4.groupby(['AreaCalculo', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_division_sub_ad4['Presupuesto'] = cumplimiento_recaudo_por_division_sub_ad4.groupby(['AreaCalculo', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_division_sub_ad4['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_division_sub_ad4['Real'] / cumplimiento_recaudo_por_division_sub_ad4['Presupuesto']
cumplimiento_recaudo_por_division_sub_ad4['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_division_sub_ad4['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_recaudo_por_division_sub_ad4['Variable'] = 'VentaCobradaDivisionSub'

### Cálculo de resultados cualitativos precargados
cumplimiento_recaudo_por_division_sub_ad4 = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_recaudo_por_division_sub_ad4,
    variable='VentaCobradaDivisionSub',
    columnas_extra=['TipoEmpleado', 'AreaCalculo', 'Consecutivo'], 
    columnas_extra_merge=['AreaCalculo', 'Consecutivo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'AD4'].drop(columns=['TipoEmpleado'])
)
###

cumplimiento_recaudo_por_division_sub_ad4['Contexto'] = 'ad4_' + cumplimiento_recaudo_por_division_sub_ad4['AreaCalculo'].astype('str').str.cat(cumplimiento_recaudo_por_division_sub_ad4['Consecutivo'].astype('str'), sep="_")
cumplimiento_recaudo_por_division_sub_ad4 = cumplimiento_recaudo_por_division_sub_ad4[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
cumplimiento_recaudo_por_division_sub_ad4.head()


# In[125]:


cumplimiento_recaudo_otras_companias_ad4 = pd.merge(
    left=area_calculo_sba_completo_ad4,
    right=df_recaudo_otras_companias.rename(columns={'Compania': 'Division'}),
    on=['Division']
)
cumplimiento_recaudo_otras_companias_ad4['PorcentajeCumplimiento'] = cumplimiento_recaudo_otras_companias_ad4['Real']/cumplimiento_recaudo_otras_companias_ad4['Presupuesto']
cumplimiento_recaudo_otras_companias_ad4['PorcentajeCumplimiento'] = cumplimiento_recaudo_otras_companias_ad4['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_recaudo_otras_companias_ad4['Contexto'] = 'ad4_' + cumplimiento_recaudo_otras_companias_ad4['AreaCalculo'].astype('str').str.cat(cumplimiento_recaudo_otras_companias_ad4['Consecutivo'].astype('str'), sep="_")
cumplimiento_recaudo_otras_companias_ad4['Variable'] = 'VentaCobradaDivisionSub'
cumplimiento_recaudo_otras_companias_ad4 = cumplimiento_recaudo_otras_companias_ad4[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_recaudo_otras_companias_ad4


# ### 12.7. Para AD5 (VentaCobradaDivisionSub)

# In[126]:


area_calculo_sba_completo_ad5 = area_calculo_sba_completo[area_calculo_sba_completo['TipoEmpleado']=='AD5']

#Se halla la informacion de la venta y recaudo real de los representantes de ventas de los AD5
venta_recaudo_presupuesto_original_ad5_real = area_calculo_sba_completo_ad5.merge(
    df_venta_recaudo_real[['Division', 'GrupoProducto', 'SBA', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)

#Se halla la informacion de la venta y recaudo presupuesto de los representantes de ventas de los AD5
venta_recaudo_presupuesto_original_ad5_presupuesto = area_calculo_sba_completo_ad5.merge(
    df_venta_recaudo_presupuesto[['Division', 'GrupoProducto', 'SBA', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)

venta_recaudo_real_area_calculo_ad5 = venta_recaudo_presupuesto_original_ad5_real.groupby(['AreaCalculo', 'Consecutivo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
venta_recaudo_real_area_calculo_ad5['Real'] = venta_recaudo_real_area_calculo_ad5['Recaudo']
venta_recaudo_real_area_calculo_ad5.drop(columns=['Recaudo'], inplace=True)

venta_recaudo_presupuesto_area_calculo_ad5 = venta_recaudo_presupuesto_original_ad5_presupuesto.groupby(['AreaCalculo', 'Consecutivo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
venta_recaudo_presupuesto_area_calculo_ad5['Presupuesto'] = venta_recaudo_presupuesto_area_calculo_ad5['Recaudo']
venta_recaudo_presupuesto_area_calculo_ad5.drop(columns=['Recaudo'], inplace=True)

cumplimiento_recaudo_por_division_sub_ad5 = pd.merge(
    left=venta_recaudo_presupuesto_area_calculo_ad5,
    right=venta_recaudo_real_area_calculo_ad5,
    on=['AreaCalculo', 'Consecutivo', 'Fecha'],
    how='left'
)
cumplimiento_recaudo_por_division_sub_ad5['Real'].fillna(0.0, inplace=True)
cumplimiento_recaudo_por_division_sub_ad5['Real'] = cumplimiento_recaudo_por_division_sub_ad5.groupby(['AreaCalculo', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_division_sub_ad5['Presupuesto'] = cumplimiento_recaudo_por_division_sub_ad5.groupby(['AreaCalculo', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_recaudo_por_division_sub_ad5['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_division_sub_ad5['Real'] / cumplimiento_recaudo_por_division_sub_ad5['Presupuesto']
cumplimiento_recaudo_por_division_sub_ad5['PorcentajeCumplimiento'] = cumplimiento_recaudo_por_division_sub_ad5['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_recaudo_por_division_sub_ad5['Variable'] = 'VentaCobradaDivisionSub'

### Cálculo de resultados cualitativos precargados
cumplimiento_recaudo_por_division_sub_ad5 = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_recaudo_por_division_sub_ad5,
    variable='VentaCobradaDivisionSub',
    columnas_extra=['TipoEmpleado', 'AreaCalculo', 'Consecutivo'], 
    columnas_extra_merge=['AreaCalculo', 'Consecutivo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'AD5'].drop(columns=['TipoEmpleado'])
)
###

cumplimiento_recaudo_por_division_sub_ad5['Contexto'] = 'ad5_' + cumplimiento_recaudo_por_division_sub_ad5['AreaCalculo'].astype('str').str.cat(cumplimiento_recaudo_por_division_sub_ad5['Consecutivo'].astype('str'), sep="_")
cumplimiento_recaudo_por_division_sub_ad5 = cumplimiento_recaudo_por_division_sub_ad5[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
cumplimiento_recaudo_por_division_sub_ad5.head()


# In[127]:


cumplimiento_recaudo_otras_companias_ad5 = pd.merge(
    left=area_calculo_sba_completo_ad5,
    right=df_recaudo_otras_companias.rename(columns={'Compania': 'Division'}),
    on=['Division']
)
cumplimiento_recaudo_otras_companias_ad5['PorcentajeCumplimiento'] = cumplimiento_recaudo_otras_companias_ad5['Real']/cumplimiento_recaudo_otras_companias_ad5['Presupuesto']
cumplimiento_recaudo_otras_companias_ad5['PorcentajeCumplimiento'] = cumplimiento_recaudo_otras_companias_ad5['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_recaudo_otras_companias_ad5['Contexto'] = 'ad5_' + cumplimiento_recaudo_otras_companias_ad5['AreaCalculo'].astype('str').str.cat(cumplimiento_recaudo_otras_companias_ad5['Consecutivo'].astype('str'), sep="_")
cumplimiento_recaudo_otras_companias_ad5['Variable'] = 'VentaCobradaDivisionSub'
cumplimiento_recaudo_otras_companias_ad5 = cumplimiento_recaudo_otras_companias_ad5[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_recaudo_otras_companias_ad5


# ## 13. Objetivos de rentabilidad CM5 a nivel país (RentabilidadBbraunPaisAntesDeImpuestos)

# In[128]:


cumplimiento_rentabilidad_cm5_nivel_pais = df_rentabilidad[(df_rentabilidad['Compania']=='BBMCO')&(df_rentabilidad['ClaseRentabilidad']=='CMV')]

cumplimiento_rentabilidad_cm5_nivel_pais = cumplimiento_rentabilidad_cm5_nivel_pais.groupby(
    ['Fecha'],
    as_index=False
).agg({'Real': 'sum', 'Presupuesto': 'sum'})

# cumplimiento_rentabilidad_cm5_nivel_pais['Presupuesto'] = cumplimiento_rentabilidad_cm5_nivel_pais['Presupuesto'].transform(pd.Series.cumsum)
# cumplimiento_rentabilidad_cm5_nivel_pais['Real'] = cumplimiento_rentabilidad_cm5_nivel_pais['Real'].transform(pd.Series.cumsum)

cumplimiento_rentabilidad_cm5_nivel_pais['PorcentajeCumplimiento'] = cumplimiento_rentabilidad_cm5_nivel_pais['Real'] / cumplimiento_rentabilidad_cm5_nivel_pais['Presupuesto']
cumplimiento_rentabilidad_cm5_nivel_pais['PorcentajeCumplimiento'] = cumplimiento_rentabilidad_cm5_nivel_pais['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_rentabilidad_cm5_nivel_pais['Contexto'] = 'CM5'
cumplimiento_rentabilidad_cm5_nivel_pais['Variable'] = 'RentabilidadBbraunPaisAntesDeImpuestos'

cumplimiento_rentabilidad_cm5_nivel_pais.head()


# ## 14. Objetivos de rentabilidad CM3 por grupo clientes con consecutivos (RentabilidadDivisionGrupoClientesCM3Sub)

# In[129]:


rentabilidad_division_cm3_gd = pd.merge(
    left=df_rentabilidad[(df_rentabilidad['ClaseRentabilidad']=='CMIII')],
    right=area_calculo_sba_completo_gd,
    on=['GrupoProducto', 'SBA'],
    how='left'
)
cumplimiento_rentabilidad_cm3_grupo_clientes_sub = rentabilidad_division_cm3_gd.groupby(
    ['Division', 'Consecutivo', 'Fecha'],
    as_index=False
).agg({'Real': 'sum', 'Presupuesto': 'sum'})

cumplimiento_rentabilidad_cm3_grupo_clientes_sub['PorcentajeCumplimiento'] = cumplimiento_rentabilidad_cm3_grupo_clientes_sub['Real']/cumplimiento_rentabilidad_cm3_grupo_clientes_sub['Presupuesto']
cumplimiento_rentabilidad_cm3_grupo_clientes_sub['PorcentajeCumplimiento'] = cumplimiento_rentabilidad_cm3_grupo_clientes_sub['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))

cumplimiento_rentabilidad_cm3_grupo_clientes_sub['Contexto'] = 'gd_' + cumplimiento_rentabilidad_cm3_grupo_clientes_sub['Division'].astype('str').str.cat(cumplimiento_rentabilidad_cm3_grupo_clientes_sub['Consecutivo'].astype('str'), sep="_")
cumplimiento_rentabilidad_cm3_grupo_clientes_sub['Variable'] = 'RentabilidadDivisionGrupoClientesCM3Sub'
cumplimiento_rentabilidad_cm3_grupo_clientes_sub = cumplimiento_rentabilidad_cm3_grupo_clientes_sub[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_rentabilidad_cm3_grupo_clientes_sub


# ## 15. Objetivos de rentabilidad CM3 por grupo clientes (RentabilidadDivisionGrupoClientesCM3)

# In[130]:


cumplimiento_rentabilidad_cm3_grupo_clientes = pd.merge(
    left=df_rentabilidad[(df_rentabilidad['ClaseRentabilidad']=='CMIII')],
    right=area_calculo_sba_completo_gd[['GrupoProducto', 'SBA', 'Division', 'AreaCalculo']].drop_duplicates(['GrupoProducto', 'SBA', 'AreaCalculo', 'Division']),
    on=['GrupoProducto', 'SBA'],
    how='left'
)

cumplimiento_rentabilidad_cm3_grupo_clientes = cumplimiento_rentabilidad_cm3_grupo_clientes.groupby(
    ['AreaCalculo', 'Division', 'Fecha'],
    as_index=False
).agg({'Real': 'sum', 'Presupuesto': 'sum'})
cumplimiento_rentabilidad_cm3_grupo_clientes['AreaCalculo'] = cumplimiento_rentabilidad_cm3_grupo_clientes['AreaCalculo'].astype(int).astype(str)

cumplimiento_rentabilidad_cm3_grupo_clientes['PorcentajeCumplimiento'] = cumplimiento_rentabilidad_cm3_grupo_clientes['Real']/cumplimiento_rentabilidad_cm3_grupo_clientes['Presupuesto']
cumplimiento_rentabilidad_cm3_grupo_clientes['PorcentajeCumplimiento'] = cumplimiento_rentabilidad_cm3_grupo_clientes['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))

cumplimiento_rentabilidad_cm3_grupo_clientes['Contexto'] = 'gd_' + cumplimiento_rentabilidad_cm3_grupo_clientes['AreaCalculo'] + '_' + cumplimiento_rentabilidad_cm3_grupo_clientes['Division']
cumplimiento_rentabilidad_cm3_grupo_clientes['Variable'] = 'RentabilidadDivisionGrupoClientesCM3'
cumplimiento_rentabilidad_cm3_grupo_clientes = cumplimiento_rentabilidad_cm3_grupo_clientes[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_rentabilidad_cm3_grupo_clientes


# ## 16. Objetivos de rentabilidad por grupo clientes (RentabilidadGrupoClientesCM2)

# In[131]:


area_calculo_sba_completo


# In[ ]:





# In[132]:


area_calculo_sba_completo_kam = area_calculo_sba_completo[area_calculo_sba_completo['TipoEmpleado'] == 'KAM']

cumplimiento_rentabilidad_cm2_grupo_clientes = pd.merge(
    left=area_calculo_sba_completo_kam[['Division', 'AreaCalculo', 'Consecutivo']].drop_duplicates(),
    right=df_rentabilidad_kam,
    on='Division',
    how='left'
)
cumplimiento_rentabilidad_cm2_grupo_clientes['Presupuesto'] = cumplimiento_rentabilidad_cm2_grupo_clientes.groupby(['Division', 'AreaCalculo', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_rentabilidad_cm2_grupo_clientes['Real'] = cumplimiento_rentabilidad_cm2_grupo_clientes.groupby(['Division', 'AreaCalculo', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)

cumplimiento_rentabilidad_cm2_grupo_clientes['PorcentajeCumplimiento'] = cumplimiento_rentabilidad_cm2_grupo_clientes['Real']/cumplimiento_rentabilidad_cm2_grupo_clientes['Presupuesto']
cumplimiento_rentabilidad_cm2_grupo_clientes['PorcentajeCumplimiento'] = cumplimiento_rentabilidad_cm2_grupo_clientes['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))

cumplimiento_rentabilidad_cm2_grupo_clientes['Contexto'] = 'kam_' + cumplimiento_rentabilidad_cm2_grupo_clientes['AreaCalculo'].astype('str').str.cat(cumplimiento_rentabilidad_cm2_grupo_clientes['Consecutivo'].astype('str'), sep="_")
cumplimiento_rentabilidad_cm2_grupo_clientes['Variable'] = 'RentabilidadGrupoClientesCM2'
cumplimiento_rentabilidad_cm2_grupo_clientes = cumplimiento_rentabilidad_cm2_grupo_clientes[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

cumplimiento_rentabilidad_cm2_grupo_clientes


# ## 17. Objetivos de rentabilidad antes de impuestos CM5 (TotalRentabilidadBraunAntesDeImpuestosCM5)

# In[133]:


rentabilidad_antes_de_impuestos_cm5 = df_rentabilidad[(df_rentabilidad['ClaseRentabilidad']=='CMV')]
rentabilidad_antes_de_impuestos_cm5.rename(columns={'Compania': 'Division'}, inplace=True)
rentabilidad_antes_de_impuestos_cm5 = rentabilidad_antes_de_impuestos_cm5.groupby(
    ['Division', 'Fecha'],
    as_index=False
).agg({'Real': 'sum', 'Presupuesto': 'sum'})

# rentabilidad_antes_de_impuestos_cm5['Presupuesto'] = rentabilidad_antes_de_impuestos_cm5.groupby(['Division'])['Presupuesto'].transform(pd.Series.cumsum)
# rentabilidad_antes_de_impuestos_cm5['Real'] = rentabilidad_antes_de_impuestos_cm5.groupby(['Division'])['Real'].transform(pd.Series.cumsum)
rentabilidad_antes_de_impuestos_cm5['PorcentajeCumplimiento'] = rentabilidad_antes_de_impuestos_cm5['Real'] / rentabilidad_antes_de_impuestos_cm5['Presupuesto']
rentabilidad_antes_de_impuestos_cm5['PorcentajeCumplimiento'] = rentabilidad_antes_de_impuestos_cm5['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
rentabilidad_antes_de_impuestos_cm5['Variable'] = 'TotalRentabilidadBraunAntesDeImpuestosCM5'

rentabilidad_antes_de_impuestos_cm5


# ### 17.1. Para AD (TotalRentabilidadBraunAntesDeImpuestosCM5)

# In[134]:


cumplimiento_rentabilidad_cm5_ad = pd.merge(
    left=area_calculo_sba_completo[area_calculo_sba_completo['TipoEmpleado']=='AD'],
    right=rentabilidad_antes_de_impuestos_cm5,
    on=['Division'],
    how='left'
)

### Cálculo de resultados cualitativos precargados
cumplimiento_rentabilidad_cm5_ad = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_rentabilidad_cm5_ad,
    variable='TotalRentabilidadBraunAntesDeImpuestosCM5',
    columnas_extra=['TipoEmpleado', 'AreaCalculo', 'Consecutivo'], 
    columnas_extra_merge=['AreaCalculo', 'Consecutivo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'AD'].drop(columns=['TipoEmpleado'])
)
###

cumplimiento_rentabilidad_cm5_ad['Contexto'] = 'ad_' + cumplimiento_rentabilidad_cm5_ad['AreaCalculo'].astype('str').str.cat(cumplimiento_rentabilidad_cm5_ad['Consecutivo'].astype('str'), sep="_")
cumplimiento_rentabilidad_cm5_ad = cumplimiento_rentabilidad_cm5_ad[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
cumplimiento_rentabilidad_cm5_ad


# ### 17.2. Para AD1 (TotalRentabilidadBraunAntesDeImpuestosCM5)

# In[135]:


cumplimiento_rentabilidad_cm5_ad1 = pd.merge(
    left=area_calculo_sba_completo[area_calculo_sba_completo['TipoEmpleado']=='AD1'],
    right=rentabilidad_antes_de_impuestos_cm5,
    on=['Division'],
    how='left'
)

### Cálculo de resultados cualitativos precargados
cumplimiento_rentabilidad_cm5_ad1 = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_rentabilidad_cm5_ad1,
    variable='TotalRentabilidadBraunAntesDeImpuestosCM5',
    columnas_extra=['TipoEmpleado', 'AreaCalculo', 'Consecutivo'], 
    columnas_extra_merge=['AreaCalculo', 'Consecutivo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'AD1'].drop(columns=['TipoEmpleado'])
)
###

cumplimiento_rentabilidad_cm5_ad1['Contexto'] = 'ad1_' + cumplimiento_rentabilidad_cm5_ad1['AreaCalculo'].astype('str').str.cat(cumplimiento_rentabilidad_cm5_ad1['Consecutivo'].astype('str'), sep="_")
cumplimiento_rentabilidad_cm5_ad1 = cumplimiento_rentabilidad_cm5_ad1[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
cumplimiento_rentabilidad_cm5_ad1


# ### 17.3. Para AD2 (TotalRentabilidadBraunAntesDeImpuestosCM5)

# In[136]:


cumplimiento_rentabilidad_cm5_ad2 = pd.merge(
    left=area_calculo_sba_completo[area_calculo_sba_completo['TipoEmpleado']=='AD2'],
    right=rentabilidad_antes_de_impuestos_cm5,
    on=['Division'],
    how='left'
)

### Cálculo de resultados cualitativos precargados
cumplimiento_rentabilidad_cm5_ad2 = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_rentabilidad_cm5_ad2,
    variable='TotalRentabilidadBraunAntesDeImpuestosCM5',
    columnas_extra=['TipoEmpleado', 'AreaCalculo', 'Consecutivo'], 
    columnas_extra_merge=['AreaCalculo', 'Consecutivo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'AD2'].drop(columns=['TipoEmpleado'])
)
###

cumplimiento_rentabilidad_cm5_ad2['Contexto'] = 'ad2_' + cumplimiento_rentabilidad_cm5_ad2['AreaCalculo'].astype('str').str.cat(cumplimiento_rentabilidad_cm5_ad2['Consecutivo'].astype('str'), sep="_")
cumplimiento_rentabilidad_cm5_ad2 = cumplimiento_rentabilidad_cm5_ad2[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
cumplimiento_rentabilidad_cm5_ad2


# ### 17.4. Para AD3 (TotalRentabilidadBraunAntesDeImpuestosCM5)

# In[137]:


cumplimiento_rentabilidad_cm5_ad3 = pd.merge(
    left=area_calculo_sba_completo[area_calculo_sba_completo['TipoEmpleado']=='AD3'],
    right=rentabilidad_antes_de_impuestos_cm5,
    on=['Division'],
    how='left'
)

### Cálculo de resultados cualitativos precargados
cumplimiento_rentabilidad_cm5_ad3 = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_rentabilidad_cm5_ad3,
    variable='TotalRentabilidadBraunAntesDeImpuestosCM5',
    columnas_extra=['TipoEmpleado', 'AreaCalculo', 'Consecutivo'], 
    columnas_extra_merge=['AreaCalculo', 'Consecutivo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'AD3'].drop(columns=['TipoEmpleado'])
)
###

cumplimiento_rentabilidad_cm5_ad3['Contexto'] = 'ad3_' + cumplimiento_rentabilidad_cm5_ad3['AreaCalculo'].astype('str').str.cat(cumplimiento_rentabilidad_cm5_ad3['Consecutivo'].astype('str'), sep="_")
cumplimiento_rentabilidad_cm5_ad3 = cumplimiento_rentabilidad_cm5_ad3[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
cumplimiento_rentabilidad_cm5_ad3


# ### 17.5. Para AD4 (TotalRentabilidadBraunAntesDeImpuestosCM5)

# In[138]:


cumplimiento_rentabilidad_cm5_ad4 = pd.merge(
    left=area_calculo_sba_completo[area_calculo_sba_completo['TipoEmpleado']=='AD4'],
    right=rentabilidad_antes_de_impuestos_cm5,
    on=['Division'],
    how='left'
)

### Cálculo de resultados cualitativos precargados
cumplimiento_rentabilidad_cm5_ad4 = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_rentabilidad_cm5_ad4,
    variable='TotalRentabilidadBraunAntesDeImpuestosCM5',
    columnas_extra=['TipoEmpleado', 'AreaCalculo', 'Consecutivo'], 
    columnas_extra_merge=['AreaCalculo', 'Consecutivo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'AD4'].drop(columns=['TipoEmpleado'])
)
###

cumplimiento_rentabilidad_cm5_ad4['Contexto'] = 'ad4_' + cumplimiento_rentabilidad_cm5_ad4['AreaCalculo'].astype('str').str.cat(cumplimiento_rentabilidad_cm5_ad4['Consecutivo'].astype('str'), sep="_")
cumplimiento_rentabilidad_cm5_ad4 = cumplimiento_rentabilidad_cm5_ad4[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
cumplimiento_rentabilidad_cm5_ad4


# ### 17.6. Para AD5 (TotalRentabilidadBraunAntesDeImpuestosCM5)

# In[139]:


cumplimiento_rentabilidad_cm5_ad5 = pd.merge(
    left=area_calculo_sba_completo[area_calculo_sba_completo['TipoEmpleado']=='AD5'],
    right=rentabilidad_antes_de_impuestos_cm5,
    on=['Division'],
    how='left'
)

### Cálculo de resultados cualitativos precargados
cumplimiento_rentabilidad_cm5_ad5 = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=cumplimiento_rentabilidad_cm5_ad5,
    variable='TotalRentabilidadBraunAntesDeImpuestosCM5',
    columnas_extra=['TipoEmpleado', 'AreaCalculo', 'Consecutivo'], 
    columnas_extra_merge=['AreaCalculo', 'Consecutivo'],
    pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'AD5'].drop(columns=['TipoEmpleado'])
)
###

cumplimiento_rentabilidad_cm5_ad5['Contexto'] = 'ad5_' + cumplimiento_rentabilidad_cm5_ad5['AreaCalculo'].astype('str').str.cat(cumplimiento_rentabilidad_cm5_ad5['Consecutivo'].astype('str'), sep="_")
cumplimiento_rentabilidad_cm5_ad5 = cumplimiento_rentabilidad_cm5_ad5[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
cumplimiento_rentabilidad_cm5_ad5


# ## 18. Objetivos de venta facturada previo notas crédito TP (VentaFacturadaPrevioNotasCreditoTP)

# In[140]:


cumplimiento_venta_previo_notas_credito_tp = pd.merge(
    left=area_calculo_sba_completo[area_calculo_sba_completo['TipoEmpleado']=='AD2'],
    right=df_venta_otras_companias.rename(columns={'Compania': 'Division'}),
    on=['Division'],
    how='left'
)

# cumplimiento_venta_previo_notas_credito_tp['Presupuesto'] = cumplimiento_venta_previo_notas_credito_tp.groupby(['Division', 'AreaCalculo', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
# cumplimiento_venta_previo_notas_credito_tp['Real'] = cumplimiento_venta_previo_notas_credito_tp.groupby(['Division', 'AreaCalculo', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)

cumplimiento_venta_previo_notas_credito_tp['PorcentajeCumplimiento'] = cumplimiento_venta_previo_notas_credito_tp['Real'] / cumplimiento_venta_previo_notas_credito_tp['Presupuesto']
cumplimiento_venta_previo_notas_credito_tp['PorcentajeCumplimiento'] = cumplimiento_venta_previo_notas_credito_tp['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))

cumplimiento_venta_previo_notas_credito_tp['Variable'] = 'VentaFacturadaPrevioNotasCreditoTP'
cumplimiento_venta_previo_notas_credito_tp['Contexto'] = 'ad2_' + cumplimiento_venta_previo_notas_credito_tp['AreaCalculo'].astype('str').str.cat(cumplimiento_venta_previo_notas_credito_tp['Consecutivo'].astype('str'), sep="_")
cumplimiento_venta_previo_notas_credito_tp = cumplimiento_venta_previo_notas_credito_tp[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
cumplimiento_venta_previo_notas_credito_tp


# ## 19. Venta de servicios Mtto (VentaServiciosMtto)

# In[141]:


cumplimiento_venta_servicios_mtto = df_venta_servicios_mtto

cumplimiento_venta_servicios_mtto['Presupuesto'] = cumplimiento_venta_servicios_mtto['Presupuesto'].transform(pd.Series.cumsum)
cumplimiento_venta_servicios_mtto['Real'] = cumplimiento_venta_servicios_mtto['Real'].transform(pd.Series.cumsum)

cumplimiento_venta_servicios_mtto['PorcentajeCumplimiento'] = cumplimiento_venta_servicios_mtto['Real'] / cumplimiento_venta_servicios_mtto['Presupuesto']
cumplimiento_venta_servicios_mtto['PorcentajeCumplimiento'] = cumplimiento_venta_servicios_mtto['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
cumplimiento_venta_servicios_mtto['Variable'] = 'VentaServiciosMtto'
cumplimiento_venta_servicios_mtto['Contexto'] = 'VentaServiciosMtto'
cumplimiento_venta_servicios_mtto = cumplimiento_venta_servicios_mtto[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
cumplimiento_venta_servicios_mtto


# # Nuevos rubros 2022

# ## 20. Venta Cobrada BBraun Cluster (UCI + OR) (VentaCobradaBraunCluster)

# In[142]:


main = df_cluster_plan_real_recaudo.copy()
main


# In[143]:


print(main)
main = df_cluster_plan_real_recaudo.copy()
# TODO Validar si se ignoran para TODOS los resultados

# Calcular
main = main.groupby(['Cluster', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)

# Variable solo de VCs #
if len(codigos_ve_vc) > 0 and fecha_liquidacion >= '2022-03-01' and fecha_liquidacion <= '2022-12-01':
    main.reset_index(inplace=True)
    main = main[main['Fecha'] >= '2022-03-01']
###

main['Real'] = main.groupby(['Cluster'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['Cluster'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real'] / main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaCobradaBraunCluster'

#print(main)
#Formatear
main.reset_index(inplace=True)
main.rename(columns={'Cluster': 'Contexto'}, inplace=True)
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

venta_cobrada_braun_cluster = main.copy()
main = None

venta_cobrada_braun_cluster

# Contexto: Cluster


# In[144]:


venta_cobrada_braun_cluster[
    (venta_cobrada_braun_cluster['Contexto'] == '16')
]


# In[145]:


venta_cobrada_braun_cluster[
    (venta_cobrada_braun_cluster['Contexto'] == '14')
]


# ## 21. Venta Cobrada Portafolio Cluster (VentaCobradaPortafolioCluster)

# In[146]:


df_clusters_empleado[
    (df_clusters_empleado['CodigoEmpleado'] == '5002899')
]


# In[147]:


drop_cols = ['Venta', 'Division', 'ZonaGeografica', 'GrupoProducto', 'Canal', 'SBA']
real = df_venta_recaudo_real.copy().drop(columns=drop_cols)
ppto = df_venta_recaudo_presupuesto.copy().drop(columns=drop_cols)

# Sumamos los registros que tienen el mismo conexto para que exista un solo registro
ppto = ppto.groupby(['CodigoEmpleado', 'Cluster',  'Fecha'], as_index=False)[['Recaudo']].apply(sum)
real = real.groupby(['CodigoEmpleado', 'Cluster',  'Fecha'], as_index=False)[['Recaudo']].apply(sum)
# ppto.reset_index(inplace=True)
# real.reset_index(inplace=True)

main = ppto.rename(columns={'Recaudo': 'Presupuesto'}).merge(
    real.rename(columns={'Recaudo': 'Real'}),
    on=['CodigoEmpleado', 'Cluster','Fecha'],
    how='left'
)
main['Real'].fillna(0, inplace=True)


# Traer áreas de cálculo y tipo de empleado de la maestra de empleados
empleados = df_empleados[['TipoEmpleado', 'CodigoEmpleado', 'AreaCalculo', 'FechaIngreso']].copy()
main = main.merge(empleados, on=['CodigoEmpleado'], how='left')

main = main[main['Fecha'] >= main['FechaIngreso']]

main


# In[148]:


# Validación. Se eliminan presupuestos  = 0
main['Real'].fillna(0, inplace=True)
main['AreaCalculo'].fillna(0, inplace=True)
main = main[main.Presupuesto.notnull()]

# Calcular
main = main.groupby(['CodigoEmpleado', 'Cluster', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main.reset_index(inplace=True)

### Agrupación a nivel de empleado por clusters
main = pd.merge(
    df_clusters_empleado[['CodigoEmpleado', 'Cluster']],
    main,
    on=['CodigoEmpleado', 'Cluster'],
    how='left'
)

main = main.groupby(['CodigoEmpleado', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)

# Variable solo de VCs y VEs #
if len(codigos_ve_vc) > 0 and fecha_liquidacion >= '2022-03-01' and fecha_liquidacion <= '2022-12-01':
    main.reset_index(inplace=True)
    main = main[main['Fecha'] >= '2022-03-01']
###

main['Real'] = main.groupby(['CodigoEmpleado'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['CodigoEmpleado'])['Presupuesto'].transform(pd.Series.cumsum)
main.reset_index(inplace=True)
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaCobradaPortafolioCluster'

### Cálculo de resultados cualitativos precargados
main = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=main,
    variable='VentaCobradaPortafolioCluster',
    columnas_extra=['CodigoEmpleado'],
    columnas_extra_merge=['CodigoEmpleado']
)
###

main['Contexto'] = main['CodigoEmpleado']
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]   
###


venta_cobrada_portafolio_cluster = main.copy()
main = None
venta_cobrada_portafolio_cluster


# In[149]:


venta_cobrada_portafolio_cluster[
    (venta_cobrada_portafolio_cluster['Contexto'] == '1111128')
]


# ## 22. Venta Cobrada Portafolio Cluster sub (VentaCobradaPortafolioClusterSub)

# In[150]:


# VentaRecaudoPresupuesto
# VentaRecaudoReal (recaudo)
# Cálculo de acuerdo a SBA
# Agrupar por División - Cluster - Grupo de Producto - SBA

drop_cols = ['Venta', 'Canal', 'ZonaGeografica']
area_calculo_sba = area_calculo_sba_completo_without_renal.copy()
real = df_venta_recaudo_real.copy().drop(columns=drop_cols)
ppto = df_venta_recaudo_presupuesto.copy().drop(columns=drop_cols)


# Sumamos los registros que tienen el mismo conexto para que exista un solo registro
ppto = ppto.groupby(['CodigoEmpleado','Division', 'Cluster', 'GrupoProducto', 'SBA', 'Fecha'], as_index=False)[['Recaudo']].apply(sum)
real = real.groupby(['CodigoEmpleado','Division', 'Cluster', 'GrupoProducto', 'SBA', 'Fecha'], as_index=False)[['Recaudo']].apply(sum)
# ppto.reset_index(inplace=True)
# real.reset_index(inplace=True)

main = pd.merge(
    ppto.rename(columns={'Recaudo': 'Presupuesto'}),
    real.rename(columns={'Recaudo': 'Real'}),
    on=['CodigoEmpleado','Division', 'Cluster', 'GrupoProducto', 'SBA', 'Fecha'],
    how='left'
)
main['Real'].fillna(0, inplace=True)

main = main.merge(total_empleados, on=['CodigoEmpleado'], how='left')
main = main[main['Fecha'] >= main['FechaIngreso']]

# Traer Consecutivos
main = main.merge(
    area_calculo_sba,
    on=['AreaCalculo', 'Division', 'GrupoProducto', 'SBA', 'TipoEmpleado'],
    how='left'
)
main.dropna(subset=['Consecutivo'], inplace=True)
main = main[main.Presupuesto.notnull()]

### Agrupación a nivel de empleado por clusters
main = pd.merge(
    df_clusters_empleado[['CodigoEmpleado', 'TipoEmpleado', 'AreaCalculo', 'Cluster']],
    main,
#     on=['CodigoEmpleado','TipoEmpleado', 'AreaCalculo', 'Cluster'],
    how='left'
)

main


# In[151]:


calculo_centro_costo_variable = main.merge(
    df_centros_costos_grupos_productos_divisiones.rename(columns={'ConsecutivoParrilla':'Consecutivo'}),
    on=['Division', 'GrupoProducto'],
    how='left'
)
calculo_centro_costo_variable = calculo_centro_costo_variable.groupby(['CodigoEmpleado', 'Consecutivo', 'Fecha', 'GrupoProducto', 'Division', 'CodigoCentroCosto'], as_index=False)[['Real']].apply(sum)
calculo_centro_costo_variable['Real'] = calculo_centro_costo_variable.groupby(['CodigoEmpleado', 'Consecutivo', 'GrupoProducto', 'Division', 'CodigoCentroCosto'])['Real'].transform(pd.Series.cumsum)
calculo_centro_costo_variable['RealTotal'] = calculo_centro_costo_variable.groupby(['CodigoEmpleado', 'Consecutivo', 'Fecha'], as_index=False)[['Real']].transform(pd.Series.sum)
calculo_centro_costo_variable['Porcentaje Aplicado'] = calculo_centro_costo_variable['Real'] / calculo_centro_costo_variable['RealTotal']
calculo_centro_costo_variable['Variable'] = 'VentaCobradaPortafolioClusterSub'
calculo_centro_costo_variable


# In[152]:


venta_cobrada_portafolio_cluster_sub_centro_costo = calculo_centro_costo_variable.copy()
venta_cobrada_portafolio_cluster_sub_centro_costo = dataframe_loader.f_calculo_centro_costo_variable(main, 'VentaCobradaPortafolioClusterSub')
venta_cobrada_portafolio_cluster_sub_centro_costo


# In[153]:


main = main.groupby(['CodigoEmpleado', 'Consecutivo', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main.reset_index(inplace=True)

# Variable solo de VCs y VEs #
if len(codigos_ve_vc) > 0 and fecha_liquidacion >= '2022-03-01' and fecha_liquidacion <= '2022-12-01':
    main = main[main['Fecha'] >= '2022-03-01']
###

main['Real'] = main.groupby(['CodigoEmpleado', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['CodigoEmpleado', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real'] / main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaCobradaPortafolioClusterSub'

### Cálculo de resultados cualitativos precargados
main = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=main,
    variable='VentaCobradaPortafolioClusterSub',
    columnas_extra=['CodigoEmpleado', 'Consecutivo'],
    columnas_extra_merge=['CodigoEmpleado', 'Consecutivo']
)
###

main['Contexto'] = main['CodigoEmpleado'] + '_' + main['Consecutivo'].astype(int).astype('str')
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
###

venta_cobrada_portafolio_cluster_sub = main.copy()
main = None
venta_cobrada_portafolio_cluster_sub


# In[154]:


venta_cobrada_portafolio_cluster_sub[
    (venta_cobrada_portafolio_cluster_sub['Contexto'].str.contains('1111128')) & 
    (venta_cobrada_portafolio_cluster_sub['Fecha'] == '2023-12-01')
]


# ### 22.1 Grupo Renal (Hospitalario + Ambulatorio)

# In[155]:


area_calculo_sba_renal_drop_duplicates = area_calculo_sba_completo_with_renal[['AreaCalculo','Consecutivo','TipoEmpleado']].drop_duplicates(['AreaCalculo', 'Consecutivo'])
area_calculo_sba_renal_drop_duplicates


# In[156]:


area_calculo_sba_renal_vc_drop_duplicates = area_calculo_sba_renal_drop_duplicates[area_calculo_sba_renal_drop_duplicates['TipoEmpleado'] == 'VC']
area_calculo_sba_renal_vc_drop_duplicates


# In[157]:


# VentaRecaudoPresupuesto
# VentaRecaudoReal (recaudo)
# Cálculo de acuerdo a SBA
# Agrupar por División - Cluster - Grupo de Producto - SBA

df_renal_ambulatorio_vc = df_renal_ambulatorio[df_renal_ambulatorio['TipoEmpleado'] == 'VC']
df_renal_ambulatorio_vc


# In[158]:


# Calcular

main = df_renal_ambulatorio_vc.rename(columns={'PlanVentaCobradaRenal': 'Presupuesto', 'RealVentaCobradaRenal': 'Real'})
main = main.merge(
    area_calculo_sba_renal_vc_drop_duplicates,
    on=['TipoEmpleado'],
    how='left'
)
main


# In[159]:


main['Contexto'] = main['CodigoEmpleado'] + '_' +  main['Consecutivo'].astype(int).astype('str')

main = main.groupby(['Contexto', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main['Real'] = main.groupby(['Contexto'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['Contexto'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaCobradaPortafolioClusterSub'


#Formatear
main.reset_index(inplace=True)
main = main[['Contexto', 'Variable', 'Fecha', 'Real','Presupuesto', 'PorcentajeCumplimiento']]

venta_cobrada_portafolio_cluster_sub = pd.concat([
     venta_cobrada_portafolio_cluster_sub.copy(),
     main.copy()
   ], 
   ignore_index=True
 )
# venta_cobrada_portafolio_cluster_sub = main.copy()
main = None
venta_cobrada_portafolio_cluster_sub


# ## 23. Venta Facturada Portafolio Cluster (VentaFacturadaPortafolioCluster)

# In[160]:


# VentaRecaudoPresupuesto
# VentaRecaudoReal (Venta)

# Merge de tablas
drop_cols = ['Recaudo', 'ZonaGeografica', 'Canal']
area_calculo_sba = area_calculo_sba_completo.copy()
real = df_venta_recaudo_real.copy().drop(columns=drop_cols)
ppto = df_venta_recaudo_presupuesto.copy().drop(columns=drop_cols)

# Sumamos los registros que tienen el mismo conexto para que exista un solo registro
ppto = ppto.groupby(['CodigoEmpleado', 'Cluster', 'Division', 'GrupoProducto', 'SBA', 'Fecha'], as_index=False)[['Venta']].apply(sum)
real = real.groupby(['CodigoEmpleado', 'Cluster', 'Division', 'GrupoProducto', 'SBA', 'Fecha'], as_index=False)[['Venta']].apply(sum)
ppto.reset_index(inplace=True)
real.reset_index(inplace=True)

main = ppto.rename(columns={'Venta': 'Presupuesto'}).merge(
    real.rename(columns={'Venta': 'Real'}),
    on=['CodigoEmpleado', 'Cluster', 'Division', 'GrupoProducto', 'SBA', 'Fecha'],
    how='left'
)
main['Real'].fillna(0, inplace=True)

# Traer áreas de cálculo y tipo de empleado de la maestra de empleados
empleados = df_empleados[['TipoEmpleado', 'CodigoEmpleado', 'AreaCalculo', 'FechaIngreso']].copy()
main = main.merge(empleados, on=['CodigoEmpleado'], how='left')

main = main[main['Fecha'] >= main['FechaIngreso']]

main = main.merge(
    area_calculo_sba,
    on=['AreaCalculo', 'Division', 'GrupoProducto', 'SBA', 'TipoEmpleado'],
    how='left'
)
main.dropna(subset=['Consecutivo'], inplace=True)
main = main[main.Presupuesto.notnull()]

main = main.groupby(['CodigoEmpleado', 'AreaCalculo', 'Cluster', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main.reset_index(inplace=True)

# Validación. Se eliminan presupuestos  = 0
main['Real'].fillna(0, inplace=True)
main['AreaCalculo'].fillna(0, inplace=True)
main = main[main.Presupuesto.notnull()]

# Calcular
main = main.groupby(['CodigoEmpleado', 'Cluster', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main.reset_index(inplace=True)

### Agrupación a nivel de empleado por clusters
main = pd.merge(
    df_clusters_empleado[['CodigoEmpleado', 'Cluster']],
    main,
    on=['CodigoEmpleado', 'Cluster'],
    how='left'
)

main = main.groupby(['CodigoEmpleado', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main.reset_index(inplace=True)

# Variable solo de VCs y VEs #
if len(codigos_ve_vc) > 0 and fecha_liquidacion >= '2022-03-01' and fecha_liquidacion <= '2022-12-01':
    main = main[main['Fecha'] >= '2022-03-01']
###

main['Real'] = main.groupby(['CodigoEmpleado'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['CodigoEmpleado'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaFacturadaPortafolioCluster'
main['Contexto'] = main['CodigoEmpleado']
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]   
###


venta_facturada_portafolio_cluster = main.copy()
main = None
venta_facturada_portafolio_cluster


# In[161]:


venta_facturada_portafolio_cluster[
    (venta_facturada_portafolio_cluster['Contexto'] == '1111115')
]


# ## 24. Venta Facturada Portafolio Cluster sub (VentaFacturadaPortafolioClusterSub)

# In[162]:


drop_cols = ['Recaudo', 'Canal', 'ZonaGeografica']
area_calculo_sba = area_calculo_sba_completo.copy()
real = df_venta_recaudo_real.copy().drop(columns=drop_cols)
ppto = df_venta_recaudo_presupuesto.copy().drop(columns=drop_cols)


# Sumamos los registros que tienen el mismo conexto para que exista un solo registro
ppto = ppto.groupby(['CodigoEmpleado','Division', 'Cluster', 'GrupoProducto', 'SBA', 'Fecha'], as_index=False)[['Venta']].apply(sum)
real = real.groupby(['CodigoEmpleado','Division', 'Cluster', 'GrupoProducto', 'SBA', 'Fecha'], as_index=False)[['Venta']].apply(sum)
ppto.reset_index(inplace=True)
real.reset_index(inplace=True)

main = pd.merge(
    ppto.rename(columns={'Venta': 'Presupuesto'}),
    real.rename(columns={'Venta': 'Real'}),
    on=['CodigoEmpleado','Division', 'Cluster', 'GrupoProducto', 'SBA', 'Fecha'],
    how='left'
)
main['Real'].fillna(0, inplace=True)


# Traer áreas de cálculo y tipo de empleado de la maestra de empleados
empleados = df_empleados[['TipoEmpleado', 'CodigoEmpleado', 'AreaCalculo', 'FechaIngreso']].copy()
main = main.merge(empleados, on=['CodigoEmpleado'], how='left')

main = main[main['Fecha'] >= main['FechaIngreso']]

# Traer Consecutivos
main = main.merge(
    area_calculo_sba,
    on=['AreaCalculo', 'Division', 'GrupoProducto', 'SBA', 'TipoEmpleado'],
    how='left'
)
main.dropna(subset=['Consecutivo'], inplace=True)
main = main[main.Presupuesto.notnull()]

main = main.groupby(['TipoEmpleado', 'AreaCalculo', 'Consecutivo', 'Cluster', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main.reset_index(inplace=True)

### Agrupación a nivel de empleado por clusters
main = pd.merge(
    df_clusters_empleado[['CodigoEmpleado', 'TipoEmpleado', 'AreaCalculo', 'Cluster']],
    main,
    on=['TipoEmpleado', 'AreaCalculo', 'Cluster'],
    how='left'
)

main = main.groupby(['CodigoEmpleado', 'Consecutivo', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main.reset_index(inplace=True)

# Variable solo de VCs y VEs #
if len(codigos_ve_vc) > 0 and fecha_liquidacion >= '2022-03-01' and fecha_liquidacion <= '2022-12-01':
    main = main[main['Fecha'] >= '2022-03-01']
###

main['Real'] = main.groupby(['CodigoEmpleado', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['CodigoEmpleado', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaFacturadaPortafolioClusterSub'
main['Contexto'] = main['CodigoEmpleado'] + '_' +  main['Consecutivo'].astype(int).astype('str')
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]   
###


venta_facturada_portafolio_cluster_sub = main.copy()
main = None
venta_facturada_portafolio_cluster_sub


# ## 25. Venta cobrada y facturada portafolio (VentaCobradaPortafolioPais, VentaFacturadaPortafolioZonaSub, VentaCobradaPortafolioZona, VentaCobradaPortafolioClusterSub, VentaFacturadaPorfafolioCluster)

# In[163]:


def pre_merge_venta_cobrada_zona_hospitalaria_sub(x):
    x['Consecutivo'] = x['Consecutivo'].astype('str')
    return x


# ### 25.1 Para VE

# In[164]:


area_calculo_sba_completo_ve = area_calculo_sba_completo[area_calculo_sba_completo['TipoEmpleado']=='VE']

#Se halla la informacion de la venta y recaudo real de los representantes de ventas de los VE
venta_recaudo_presupuesto_original_ve_real = area_calculo_sba_completo_ve.merge(
    df_venta_recaudo_real[['Division', 'GrupoProducto', 'SBA', 'ZonaGeografica', 'Cluster', 'Canal', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)

#Se halla la informacion de la venta y recaudo presupuesto de los representantes de ventas de los VE
venta_recaudo_presupuesto_original_ve_presupuesto = area_calculo_sba_completo_ve.merge(
    df_venta_recaudo_presupuesto[['Division', 'GrupoProducto', 'SBA', 'ZonaGeografica', 'Cluster', 'Canal', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)


# #### 25.1.1 VentaCobradaPortafolioPais

# In[165]:


recaudo_real_ve = venta_recaudo_presupuesto_original_ve_real.groupby(['AreaCalculo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
recaudo_real_ve.rename(columns={'Recaudo': 'Real'}, inplace=True)

recaudo_presupuesto_ve = venta_recaudo_presupuesto_original_ve_presupuesto.groupby(['AreaCalculo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
recaudo_presupuesto_ve.rename(columns={'Recaudo': 'Presupuesto'}, inplace=True)

main = pd.merge(
    left=recaudo_presupuesto_ve,
    right=recaudo_real_ve,
    on=['AreaCalculo', 'Fecha'],
    how='left'
)

# TODO Validar si los resultados anteriores a fecha de ingreso se ignoran para TODOS

main['Real'].fillna(0.0, inplace=True)

# Variable solo de VCs y VEs #
if len(codigos_ve_vc) > 0 and fecha_liquidacion >= '2022-03-01' and fecha_liquidacion <= '2022-12-01':
    main = main[main['Fecha'] >= '2022-03-01']
###

main['Real'] = main.groupby(['AreaCalculo'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['AreaCalculo'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = main['Real'] / main['Presupuesto']
main['PorcentajeCumplimiento'] = main['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))

main['AreaCalculo'] = main['AreaCalculo'].astype('str')
main['Contexto'] = 'VE_' + main['AreaCalculo']
main['Variable'] = 'VentaCobradaPortafolioPais'
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

venta_cobrada_portafolio_pais_ve = main.copy()
main = None

venta_cobrada_portafolio_pais_ve


# In[166]:


venta_cobrada_portafolio_pais_ve[
    (venta_cobrada_portafolio_pais_ve['Contexto'] == 'VE_4')
]


# In[167]:


venta_cobrada_portafolio_pais_ve[
    (venta_cobrada_portafolio_pais_ve['Contexto'] == 'VE_7')
]


# #### 25.1.2 VentaCobradaPortafolioZona

# In[168]:


print(venta_recaudo_presupuesto_original_ve_real)

recaudo_real_zona_ve = venta_recaudo_presupuesto_original_ve_real.groupby(['AreaCalculo', 'ZonaGeografica', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
recaudo_real_zona_ve.rename(columns={'Recaudo': 'Real'}, inplace=True)

recaudo_presupuesto_zona_ve = venta_recaudo_presupuesto_original_ve_presupuesto.groupby(['AreaCalculo', 'ZonaGeografica', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
recaudo_presupuesto_zona_ve.rename(columns={'Recaudo': 'Presupuesto'}, inplace=True)

main = pd.merge(
    left=recaudo_presupuesto_zona_ve,
    right=recaudo_real_zona_ve,
    on=['ZonaGeografica', 'AreaCalculo', 'Fecha'],
    how='left'
)

main['Real'].fillna(0.0, inplace=True)

main['Real'] = main.groupby(['AreaCalculo', 'ZonaGeografica', 'Fecha'])['Real'].transform(pd.Series.sum)
main['Presupuesto'] = main.groupby(['AreaCalculo', 'ZonaGeografica', 'Fecha'])['Presupuesto'].transform(pd.Series.sum)

main['TipoEmpleado'] = 'VE'

### Agrupación a nivel de empleado por zonas
main = pd.merge(
    df_zonas_empleado[['CodigoEmpleado', 'TipoEmpleado', 'AreaCalculo', 'Zona']].rename(columns={'Zona': 'ZonaGeografica'}),
    main,
    on=['TipoEmpleado', 'AreaCalculo', 'ZonaGeografica'],
    how='left'
)

main = pd.merge(
    main,
    df_empleados[['CodigoEmpleado', 'FechaIngreso']],
    on='CodigoEmpleado',
    how='left'
)
main = main[main['Fecha'] >= main['FechaIngreso']]

main = main.groupby(['CodigoEmpleado', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main.reset_index(inplace=True)

# Variable solo de VCs y VEs #
if len(codigos_ve_vc) > 0 and fecha_liquidacion >= '2022-03-01' and fecha_liquidacion <= '2022-12-01':
    main = main[main['Fecha'] >= '2022-03-01']
###


main['Real'] = main.groupby(['CodigoEmpleado'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['CodigoEmpleado'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaCobradaPortafolioZona'
main['Contexto'] = main['CodigoEmpleado']
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
###

venta_cobrada_portafolio_zona_ve = main.copy()
main = None

venta_cobrada_portafolio_zona_ve


# In[169]:


def _get_or_create_zona_cluster(empleado, cluster, zona):
    if not empleado or not (cluster or zona):
        return False
    return True

_get_or_create_zona_cluster(True, True, None)


# #### 25.1.3 VentaFacturadaPortafolioZonaSub

# In[170]:


venta_real_zona_consecutivo_ve = venta_recaudo_presupuesto_original_ve_real.groupby(['AreaCalculo', 'ZonaGeografica', 'Consecutivo', 'Fecha'], as_index=False).agg({'Venta': 'sum'})
venta_real_zona_consecutivo_ve.rename(columns={'Venta': 'Real'}, inplace=True)

venta_presupuesto_zona_consecutivo_ve = venta_recaudo_presupuesto_original_ve_presupuesto.groupby(['AreaCalculo', 'ZonaGeografica', 'Consecutivo', 'Fecha'], as_index=False).agg({'Venta': 'sum'})
venta_presupuesto_zona_consecutivo_ve.rename(columns={'Venta': 'Presupuesto'}, inplace=True)

main = pd.merge(
    left=venta_presupuesto_zona_consecutivo_ve,
    right=venta_real_zona_consecutivo_ve,
    on=['ZonaGeografica', 'AreaCalculo', 'Consecutivo', 'Fecha'],
    how='left'
)

main['Real'].fillna(0.0, inplace=True)

main['Real'] = main.groupby(['AreaCalculo', 'ZonaGeografica', 'Consecutivo', 'Fecha'])['Real'].transform(pd.Series.sum)
main['Presupuesto'] = main.groupby(['AreaCalculo', 'ZonaGeografica', 'Consecutivo', 'Fecha'])['Presupuesto'].transform(pd.Series.sum)

main['Consecutivo'] = main['Consecutivo'].astype('str')
main['TipoEmpleado'] = 'VE'

### Agrupación a nivel de empleado por zonas
main = pd.merge(
    df_zonas_empleado[['CodigoEmpleado', 'TipoEmpleado', 'AreaCalculo', 'Zona']].rename(columns={'Zona': 'ZonaGeografica'}),
    main,
    on=['TipoEmpleado', 'AreaCalculo', 'ZonaGeografica'],
    how='left'
)

main = pd.merge(
    main,
    df_empleados[['CodigoEmpleado', 'FechaIngreso']],
    on='CodigoEmpleado',
    how='left'
)
main = main[main['Fecha'] >= main['FechaIngreso']]

main = main.groupby(['CodigoEmpleado', 'Consecutivo', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main.reset_index(inplace=True)

# Variable solo de VCs y VEs #
if len(codigos_ve_vc) > 0 and fecha_liquidacion >= '2022-03-01' and fecha_liquidacion <= '2022-12-01':
    main = main[main['Fecha'] >= '2022-03-01']
###

main['Real'] = main.groupby(['CodigoEmpleado', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['CodigoEmpleado', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaFacturadaPortafolioZonaSub'
main['Contexto'] = main['CodigoEmpleado'] + '_' +  main['Consecutivo'].astype(int).astype('str')
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
###

venta_facturada_portafolio_zona_sub_ve = main.copy()
main = None

venta_facturada_portafolio_zona_sub_ve


# #### 25.1.4 VentaCobradaPortafolioCluster

# In[171]:


recaudo_real_cluster_ve = venta_recaudo_presupuesto_original_ve_real.groupby(['AreaCalculo', 'Cluster', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
recaudo_real_cluster_ve.rename(columns={'Recaudo': 'Real'}, inplace=True)

recaudo_presupuesto_cluster_ve = venta_recaudo_presupuesto_original_ve_presupuesto.groupby(['AreaCalculo', 'Cluster', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
recaudo_presupuesto_cluster_ve.rename(columns={'Recaudo': 'Presupuesto'}, inplace=True)

main = pd.merge(
    left=recaudo_presupuesto_cluster_ve,
    right=recaudo_real_cluster_ve,
    on=['Cluster', 'AreaCalculo', 'Fecha'],
    how='left'
)

main['Real'].fillna(0.0, inplace=True)

main['Real'] = main.groupby(['AreaCalculo', 'Cluster', 'Fecha'])['Real'].transform(pd.Series.sum)
main['Presupuesto'] = main.groupby(['AreaCalculo', 'Cluster', 'Fecha'])['Presupuesto'].transform(pd.Series.sum)

main['TipoEmpleado'] = 'VE'

### Agrupación a nivel de empleado por zonas
main = pd.merge(
    df_clusters_empleado[['CodigoEmpleado', 'TipoEmpleado', 'AreaCalculo', 'Cluster']],
    main,
    on=['TipoEmpleado', 'AreaCalculo', 'Cluster'],
    how='left'
)
main = main.groupby(['CodigoEmpleado', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main.reset_index(inplace=True)

main = pd.merge(
    left=main,
    right=df_empleados[['CodigoEmpleado', 'FechaIngreso']],
    on=['CodigoEmpleado'],
    how='left'
)
main = main[main['Fecha'] >= main['FechaIngreso']]
main.drop(columns=['FechaIngreso'], inplace=True)

# Variable solo de VCs y VEs #
if len(codigos_ve_vc) > 0 and fecha_liquidacion >= '2022-03-01' and fecha_liquidacion <= '2022-12-01':
    main = main[main['Fecha'] >= '2022-03-01']
###

main['Real'] = main.groupby(['CodigoEmpleado'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['CodigoEmpleado'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaCobradaPortafolioCluster'

### Cálculo de resultados cualitativos precargados
main = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=main,
    variable='VentaCobradaPortafolioCluster',
    columnas_extra=['CodigoEmpleado'],
    columnas_extra_merge=['CodigoEmpleado']
)
###

main['Contexto'] = main['CodigoEmpleado']
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
###

venta_cobrada_portafolio_cluster_ve = main.copy()
main = None

venta_cobrada_portafolio_cluster_ve


# #### 25.1.5 VentaFacturadaPortafolioClusterSub

# In[172]:


venta_real_cluster_consecutivo_ve = venta_recaudo_presupuesto_original_ve_real.groupby(['AreaCalculo', 'Cluster', 'Consecutivo', 'Fecha'], as_index=False).agg({'Venta': 'sum'})
venta_real_cluster_consecutivo_ve.rename(columns={'Venta': 'Real'}, inplace=True)

venta_presupuesto_cluster_consecutivo_ve = venta_recaudo_presupuesto_original_ve_presupuesto.groupby(['AreaCalculo', 'Cluster', 'Consecutivo', 'Fecha'], as_index=False).agg({'Venta': 'sum'})
venta_presupuesto_cluster_consecutivo_ve.rename(columns={'Venta': 'Presupuesto'}, inplace=True)

main = pd.merge(
    left=venta_presupuesto_cluster_consecutivo_ve,
    right=venta_real_cluster_consecutivo_ve,
    on=['Cluster', 'AreaCalculo', 'Consecutivo', 'Fecha'],
    how='left'
)

main['Real'].fillna(0.0, inplace=True)

main['Real'] = main.groupby(['AreaCalculo', 'Cluster', 'Consecutivo', 'Fecha'])['Real'].transform(pd.Series.sum)
main['Presupuesto'] = main.groupby(['AreaCalculo', 'Cluster', 'Consecutivo', 'Fecha'])['Presupuesto'].transform(pd.Series.sum)

main['Consecutivo'] = main['Consecutivo'].astype('str')
main['TipoEmpleado'] = 'VE'

### Agrupación a nivel de empleado por zonas
main = pd.merge(
    df_clusters_empleado[['CodigoEmpleado', 'TipoEmpleado', 'AreaCalculo', 'Cluster']],
    main,
    on=['TipoEmpleado', 'AreaCalculo', 'Cluster'],
    how='left'
)

main = pd.merge(
    left=main,
    right=df_empleados[['CodigoEmpleado', 'FechaIngreso']],
    on=['CodigoEmpleado'],
    how='left'
)
main = main[main['Fecha'] >= main['FechaIngreso']]
main.drop(columns=['FechaIngreso'], inplace=True)

main = main.groupby(['CodigoEmpleado', 'Consecutivo', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main.reset_index(inplace=True)

# Variable solo de VCs y VEs #
if len(codigos_ve_vc) > 0 and fecha_liquidacion >= '2022-03-01' and fecha_liquidacion <= '2022-12-01':
    main = main[main['Fecha'] >= '2022-03-01']
###

main['Real'] = main.groupby(['CodigoEmpleado', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['CodigoEmpleado', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaFacturadaPortafolioClusterSub'

### Cálculo de resultados cualitativos precargados
main = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=main,
    variable='VentaFacturadaPortafolioClusterSub',
    columnas_extra=['CodigoEmpleado', 'Consecutivo'],
    columnas_extra_merge=['CodigoEmpleado', 'Consecutivo']
)
###

main['Contexto'] = main['CodigoEmpleado'] + '_' +  main['Consecutivo'].astype(int).astype('str')
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
###

venta_facturada_portafolio_cluster_sub_ve = main.copy()
main = None

venta_facturada_portafolio_cluster_sub_ve


# ### 25.2 Para GC 

# In[173]:


area_calculo_sba_completo_gc = area_calculo_sba_completo[area_calculo_sba_completo['TipoEmpleado']=='GC']

#Se halla la informacion de la venta y recaudo real de los representantes de ventas de los GC
venta_recaudo_presupuesto_original_gc_real = area_calculo_sba_completo_gc.merge(
    df_venta_recaudo_real[['Division', 'GrupoProducto', 'SBA', 'ZonaGeografica', 'Canal', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)

#Se halla la informacion de la venta y recaudo presupuesto de los representantes de ventas de los GC
venta_recaudo_presupuesto_original_gc_presupuesto = area_calculo_sba_completo_gc.merge(
    df_venta_recaudo_presupuesto[['Division', 'GrupoProducto', 'SBA', 'ZonaGeografica', 'Canal', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)


# #### 25.2.1 VentaCobradaZonaHospitalarioSub

# In[174]:


recaudo_real_zona_sub_ch_gc = venta_recaudo_presupuesto_original_gc_real.query('Canal=="Hospital" or Canal=="Distribuidor" or Canal=="Opl" or Canal=="Asegurador" or Canal=="Central De Mezcla"')
recaudo_real_zona_sub_ch_gc = recaudo_real_zona_sub_ch_gc.groupby(['AreaCalculo', 'ZonaGeografica', 'Consecutivo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
recaudo_real_zona_sub_ch_gc.rename(columns={'Recaudo': 'Real'}, inplace=True)

recaudo_presupuesto_zona_sub_ch_gc = venta_recaudo_presupuesto_original_gc_presupuesto.query('Canal=="Hospital" or Canal=="Distribuidor" or Canal=="Opl" or Canal=="Asegurador" or Canal=="Central De Mezcla"')
recaudo_presupuesto_zona_sub_ch_gc = recaudo_presupuesto_zona_sub_ch_gc.groupby(['AreaCalculo', 'ZonaGeografica', 'Consecutivo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
recaudo_presupuesto_zona_sub_ch_gc.rename(columns={'Recaudo': 'Presupuesto'}, inplace=True)

main = pd.merge(
    left=recaudo_presupuesto_zona_sub_ch_gc,
    right=recaudo_real_zona_sub_ch_gc,
    on=['ZonaGeografica', 'AreaCalculo', 'Consecutivo', 'Fecha'],
    how='left'
)

main['Real'].fillna(0.0, inplace=True)

main['Real'] = main.groupby(['AreaCalculo', 'ZonaGeografica', 'Consecutivo', 'Fecha'])['Real'].transform(pd.Series.sum)
main['Presupuesto'] = main.groupby(['AreaCalculo', 'ZonaGeografica', 'Consecutivo', 'Fecha'])['Presupuesto'].transform(pd.Series.sum)

main['Consecutivo'] = main['Consecutivo'].astype('str')
main['TipoEmpleado'] = 'GC'

### Agrupación a nivel de empleado por zonas
main = pd.merge(
    df_zonas_empleado[['CodigoEmpleado', 'TipoEmpleado', 'AreaCalculo', 'Zona']].rename(columns={'Zona': 'ZonaGeografica'}),
    main,
    on=['TipoEmpleado', 'AreaCalculo', 'ZonaGeografica'],
    how='left'
)
main = main.groupby(['CodigoEmpleado', 'Consecutivo', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main.reset_index(inplace=True)
main['Real'] = main.groupby(['CodigoEmpleado', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['CodigoEmpleado', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaCobradaZonaHospitalarioSub'
###

### Cálculo de resultados cualitativos precargados
main = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=main,
    variable='VentaCobradaZonaHospitalarioSub',
    columnas_extra=['CodigoEmpleado', 'Consecutivo'], 
    columnas_extra_merge=['CodigoEmpleado', 'Consecutivo'],
    pre_merge_lambda=pre_merge_venta_cobrada_zona_hospitalaria_sub
)
###

main['Contexto'] = main['CodigoEmpleado'] + '_' +  main['Consecutivo'].astype(int).astype('str')
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]


venta_cobrada_zona_hospitalario_sub_gc = main.copy()
main = None
resultado_cualitativos_variable = None

venta_cobrada_zona_hospitalario_sub_gc


# ### 25.3 Para GD

# In[175]:


area_calculo_sba_completo_gd = area_calculo_sba_completo[area_calculo_sba_completo['TipoEmpleado']=='GD']

#Se halla la informacion de la venta y recaudo real de los representantes de ventas de los GD
venta_recaudo_presupuesto_original_gd_real = area_calculo_sba_completo_gd.merge(
    df_venta_recaudo_real[['Division', 'GrupoProducto', 'SBA', 'ZonaGeografica', 'Canal', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)

#Se halla la informacion de la venta y recaudo presupuesto de los representantes de ventas de los GD
venta_recaudo_presupuesto_original_gd_presupuesto = area_calculo_sba_completo_gd.merge(
    df_venta_recaudo_presupuesto[['Division', 'GrupoProducto', 'SBA', 'ZonaGeografica', 'Canal', 'Fecha', 'Venta', 'Recaudo']],
    on=['Division', 'GrupoProducto', 'SBA']
)


# #### 25.3.1 VentaCobradaZonaHospitalarioSub

# In[176]:


recaudo_real_zona_sub_ch_gd = venta_recaudo_presupuesto_original_gd_real.query('Canal=="Hospital" or Canal=="Distribuidor" or Canal=="Opl" or Canal=="Asegurador" or Canal=="Central De Mezcla"')
recaudo_real_zona_sub_ch_gd = recaudo_real_zona_sub_ch_gd.groupby(['AreaCalculo', 'ZonaGeografica', 'Consecutivo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
recaudo_real_zona_sub_ch_gd.rename(columns={'Recaudo': 'Real'}, inplace=True)

recaudo_presupuesto_zona_sub_ch_gd = venta_recaudo_presupuesto_original_gd_presupuesto.query('Canal=="Hospital" or Canal=="Distribuidor" or Canal=="Opl" or Canal=="Asegurador" or Canal=="Central De Mezcla"')
recaudo_presupuesto_zona_sub_ch_gd = recaudo_presupuesto_zona_sub_ch_gd.groupby(['AreaCalculo', 'ZonaGeografica', 'Consecutivo', 'Fecha'], as_index=False).agg({'Recaudo': 'sum'})
recaudo_presupuesto_zona_sub_ch_gd.rename(columns={'Recaudo': 'Presupuesto'}, inplace=True)

main = pd.merge(
    left=recaudo_presupuesto_zona_sub_ch_gd,
    right=recaudo_real_zona_sub_ch_gd,
    on=['ZonaGeografica', 'AreaCalculo', 'Consecutivo', 'Fecha'],
    how='left'
)

main['Real'].fillna(0.0, inplace=True)

main['Real'] = main.groupby(['AreaCalculo', 'ZonaGeografica', 'Consecutivo', 'Fecha'])['Real'].transform(pd.Series.sum)
main['Presupuesto'] = main.groupby(['AreaCalculo', 'ZonaGeografica', 'Consecutivo', 'Fecha'])['Presupuesto'].transform(pd.Series.sum)

main['Consecutivo'] = main['Consecutivo'].astype('str')
main['TipoEmpleado'] = 'GD'

### Agrupación a nivel de empleado por zonas
main = pd.merge(
    df_zonas_empleado[['CodigoEmpleado', 'TipoEmpleado', 'AreaCalculo', 'Zona']].rename(columns={'Zona': 'ZonaGeografica'}),
    main,
    on=['TipoEmpleado', 'AreaCalculo', 'ZonaGeografica'],
    how='left'
)
main = main.groupby(['CodigoEmpleado', 'Consecutivo', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main.reset_index(inplace=True)
main['Real'] = main.groupby(['CodigoEmpleado', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['CodigoEmpleado', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaCobradaZonaHospitalarioSub'
###

### Cálculo de resultados cualitativos precargados
main = dataframe_loader.sobreescribir_resultados_cualitativos(
    main=main,
    variable='VentaCobradaZonaHospitalarioSub',
    columnas_extra=['CodigoEmpleado', 'Consecutivo'], 
    columnas_extra_merge=['CodigoEmpleado', 'Consecutivo'],
    pre_merge_lambda=pre_merge_venta_cobrada_zona_hospitalaria_sub
)
###

main['Contexto'] = main['CodigoEmpleado'] + '_' +  main['Consecutivo'].astype(int).astype('str')
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

venta_cobrada_zona_hospitalario_sub_gd = main.copy()
main = None

venta_cobrada_zona_hospitalario_sub_gd


# 
# ## 26. Venta Facturada Nacional Canal Hospitalario (VentaFacturadaPaisHospitalario)

# In[177]:


# Preparar
main = df_zona_plan_real_venta_ch.copy()
main = main.drop(columns=['Zona'])

# Calcular
main = main.groupby(['Fecha'], as_index=True)[['Real', 'Presupuesto']].apply(sum)
main['Real'] = main['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaFacturadaPaisHospitalario'

# Formatear
main.reset_index(inplace=True)
main['Contexto']= 'BBMCO'
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

# Limpiar
venta_facturada_pais_ch = main.copy()
main = None
venta_facturada_pais_ch


# ## 27. Venta Facturada Zona Hospitalario (VentaFacturadaZonaHospitalario)

# In[178]:


# Preparar
main = df_zona_plan_real_venta_ch.copy()

# Calcular
main = main.groupby(['Fecha', 'Zona'], as_index=True)[['Real', 'Presupuesto']].apply(sum)
main['Real'] = main.groupby(['Zona'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['Zona'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaFacturadaZonaHospitalario'

# Formatear
main.reset_index(inplace=True)
main.rename(columns={'Zona': 'Contexto'}, inplace=True)
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

# Limpiar
venta_facturada_zona_ch = main.copy()
main = None
venta_facturada_zona_ch


# ## 28. Rentabilidad Zona Hospitalario CM2 (RentabilidadZonaHospitalarioCM2)

# In[179]:


main = df_rentabilidad_zona.copy()

# Calcular
main = main.groupby(['Zona', 'Fecha'], as_index =False)[['Real', 'Presupuesto']].apply(sum)
main['PorcentajeCumplimiento'] = (main['Real'] / main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'RentabilidadZonaHospitalarioCM2'

#Formatear
main.reset_index(inplace=True)
main.rename(columns={'Zona': 'Contexto'}, inplace=True)
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

# Zona - Fecha - Rentabilidad

# No hay datos

rentabilidad_zona_hospitalario_cm2 =  main.copy()
main = None
rentabilidad_zona_hospitalario_cm2


# ## 29. Venta Facturada Braun País (VentaFacturadaBraunPais)

# In[180]:


# VentaRecaudoReal (venta)
# VentaRecaudoPresupuesto (venta)

# Preparar
drop_cols = ['Recaudo', 'Division', 'Cluster','CodigoEmpleado', 'ZonaGeografica','GrupoProducto', 'Canal', 'SBA']
real = df_venta_recaudo_real.copy().drop(columns=drop_cols)
ppto = df_venta_recaudo_presupuesto.copy().drop(columns=drop_cols)

# Sumamos los registros que tienen el mismo conexto para que exista un solo registro
ppto = ppto.groupby(['Fecha'], as_index=True)[['Venta']].apply(sum)
real = real.groupby(['Fecha'], as_index=True)[['Venta']].apply(sum)
ppto.reset_index(inplace=True)
real.reset_index(inplace=True)

main = ppto.rename(columns={'Venta': 'Presupuesto'}).merge(
    real.rename(columns={'Venta': 'Real'}),
    on=['Fecha'],
    how='left'
)
main['Real'].fillna(0, inplace=True)


# Calcular
main['Contexto']= 'BBMCO'
main = main.groupby(['Contexto', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main['Real'] = main.groupby(['Contexto'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['Contexto'])['Presupuesto'].transform(pd.Series.cumsum)

# main = convertir_a_euros(main)

main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaFacturadaBraunPais'


#Formatear
main.reset_index(inplace=True)

main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]




venta_facturada_braun_pais = main.copy()
main = None
venta_facturada_braun_pais


# ## 30. Rentabilidad Compañía CM3 (RentabilidadCompaniaCM3)

# In[181]:


# Rentabilidad Real / Presupuesto
# Filtar paras CM3 compañía BBMCO
# Totalizar nivel país

main = df_rentabilidad.copy()
main.query('Compania == "BBMCO" and ClaseRentabilidad == "CMIII"', inplace=True)
main.drop(columns=['GrupoProducto', 'SBA', 'ClaseRentabilidad'], inplace=True)
main = main.groupby(['Compania', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)

main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'RentabilidadCompaniaCM3'


#Formatear
main.reset_index(inplace=True)
main.rename(columns={'Compania':'Contexto'}, inplace=True)
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

rentabilidad_compania_cm3 = main.copy()
main= None
rentabilidad_compania_cm3


# ## 31. Venta Facturada Grupo clientes (KAM) VentaFacturadaGrupoClientesKAMS 

# In[182]:


df_venta_recaudo_kams


# In[183]:


main = df_venta_recaudo_kams.drop(columns=['RecaudoReal', 'RecaudoPresupuesto'])
main.rename(columns={'VentaReal': 'Real', 'VentaPresupuesto':'Presupuesto'}, inplace=True)

main = main.groupby(['TipoEmpleado', 'AreaCalculo', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)

main.reset_index(inplace=True)
main['Real'] = main.groupby(['TipoEmpleado', 'AreaCalculo'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['TipoEmpleado', 'AreaCalculo'])['Presupuesto'].transform(pd.Series.cumsum)

main['Contexto'] = main['TipoEmpleado'].str.lower() + '_' +  main['AreaCalculo'].astype(int).astype('str')
main['Variable'] = 'VentaFacturadaGrupoClientesKAMS'
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

venta_facturada_grupo_clientes_kams = main
main = None
venta_facturada_grupo_clientes_kams

# Contexto: TipoEmpleado_AreaCalculo


# In[184]:


venta_facturada_grupo_clientes_kams[
    (venta_facturada_grupo_clientes_kams['Contexto'] == 'kam_2')
]


# ## 32. Venta Cobrada Grupo clientes (KAM) VentaCobradaGrupoClientesKAMSSub

# In[185]:


df_venta_recaudo_kams.head()


# In[186]:


main = df_venta_recaudo_kams.drop(columns=['VentaReal', 'VentaPresupuesto'])
main.rename(columns={'RecaudoReal': 'Real', 'RecaudoPresupuesto':'Presupuesto'}, inplace=True)
main


# In[187]:


main = main.groupby(['TipoEmpleado', 'AreaCalculo', 'Consecutivo', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main.reset_index(inplace=True)
main['Real'] = main.groupby(['TipoEmpleado', 'AreaCalculo', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['TipoEmpleado', 'AreaCalculo', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)

main['Contexto'] = main['TipoEmpleado'].str.lower() + '_' +  main['AreaCalculo'].astype(int).astype('str') + '_'+ main['Consecutivo'].astype(int).astype('str')
main['Variable'] = 'VentaCobradaGrupoClientesKAMSSub'
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

venta_cobrada_grupo_clientes_kams_sub = main
main = None
venta_cobrada_grupo_clientes_kams_sub

# Contexto: TipoEmpleado_AreaCalculo_Consecutivo


# ## 32.1 Venta Facturada Grupo clientes (KAM) VentaFacturadaGrupoClientesKAMSSub (2024)

# In[188]:


main = df_venta_recaudo_kams.drop(columns=['RecaudoReal', 'RecaudoPresupuesto'])
main.rename(columns={'VentaReal': 'Real', 'VentaPresupuesto':'Presupuesto'}, inplace=True)
main


# In[189]:


main = main.groupby(['TipoEmpleado', 'AreaCalculo', 'Consecutivo', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main.reset_index(inplace=True)
main['Real'] = main.groupby(['TipoEmpleado', 'AreaCalculo', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['TipoEmpleado', 'AreaCalculo', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)

main['Contexto'] = main['TipoEmpleado'].str.lower() + '_' +  main['AreaCalculo'].astype(int).astype('str') + '_'+ main['Consecutivo'].astype(int).astype('str')
main['Variable'] = 'VentaFacturadaGrupoClientesKAMSSub'
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

venta_facturada_grupo_clientes_kams_sub = main
main = None
venta_facturada_grupo_clientes_kams_sub

# Contexto: TipoEmpleado_AreaCalculo_Consecutivo


# In[ ]:





# ## 33. KPIs Surgical

# In[190]:


### Cálculo de resultados cualitativos precargados
main = df_resultados_variables_cualitativas[df_resultados_variables_cualitativas['Variable'] == 'KpisSurgical'].copy()
main['Contexto'] = main['CodigoEmpleado'].str.lower() + '_' + main['Consecutivo'].astype(int).astype('str')
main['Real'] = np.nan
main['Presupuesto'] = np.nan
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
###
kpis_surgical = main
main = None
kpis_surgical


# # Nuevos rubros 2023

# ## 34. VentaCobradaPortafolioZonaClusterSub

# In[191]:


df_area_calculo_sba_centros_costos_ve = df_area_calculo_sba_centros_costos[
    df_area_calculo_sba_centros_costos['TipoEmpleado'] == 'VE'
]
df_area_calculo_sba_centros_costos_ve =  df_area_calculo_sba_centros_costos_ve.rename(columns={'ConsecutivoParrilla':'Consecutivo'})

df_area_calculo_sba_centros_costos_ve.info()


# In[192]:


area_calculo_sba_completo_ve = area_calculo_sba_completo[area_calculo_sba_completo['TipoEmpleado'] == 'VE']
area_calculo_sba_completo_ve.info()


# In[193]:


area_calculo_sba_completo_ve.merge(
    df_area_calculo_sba_centros_costos_ve.fillna(''),
    on=['AreaCalculo', 'Consecutivo', 'Division','GrupoProducto', 'SBA','TipoEmpleado'],
    how='left'
).head()


# In[194]:


df_area_calculo_sba_centros_costos_ve = area_calculo_sba_completo_ve.merge(
    df_centros_costos_grupos_productos_divisiones,
    on=['Division','GrupoProducto'],
    how='left'
)
df_area_calculo_sba_centros_costos_ve


# In[195]:


venta_recaudo_presupuesto_original_ve_real.head()


# ### Merge entre area de cálculo SBA y Venta Recaudo Presupuesto

# In[196]:


# Se remplaza lógica de área de cálculo SBA
# drop_cols = ['Canal', 'TipoEmpleado','Venta']
# venta_recaudo_presupuesto_original_ve_real_zona_cluster = pd.merge(
#     left=area_calculo_sba_completo_ve,
#     right=venta_recaudo_presupuesto_original_ve_real,
#     on=['AreaCalculo', 'Consecutivo', 'Division','GrupoProducto', 'SBA', 'TipoEmpleado'],
#     how='left'
# )

# venta_recaudo_presupuesto_original_ve_real_zona_cluster


# In[197]:


drop_cols = ['Canal', 'TipoEmpleado','Venta']
venta_recaudo_presupuesto_original_ve_real_zona_cluster = pd.merge(
    left=df_area_calculo_sba_centros_costos_ve,
    right=venta_recaudo_presupuesto_original_ve_real,
    on=['AreaCalculo', 'Consecutivo', 'Division','GrupoProducto', 'SBA','TipoEmpleado'],
    how='left'
)

venta_recaudo_presupuesto_original_ve_real_zona_cluster


# ### Merge entre area de cálculo SBA y Venta Recaudo Presupuesto y Empleado

# In[198]:


# Se remplaza lógica de área de cálculo SBA
# venta_recaudo_presupuesto_original_ve_presupuesto_zona_cluster = pd.merge(
#     left=area_calculo_sba_completo_ve,
#     right=venta_recaudo_presupuesto_original_ve_presupuesto,
#     on=['AreaCalculo', 'Consecutivo', 'Division','GrupoProducto', 'SBA', 'TipoEmpleado'],
#     how='left'
# )
# venta_recaudo_presupuesto_original_ve_presupuesto_zona_cluster


# In[199]:


venta_recaudo_presupuesto_original_ve_presupuesto_zona_cluster = pd.merge(
    left=df_area_calculo_sba_centros_costos_ve, 
    right=venta_recaudo_presupuesto_original_ve_presupuesto,
    on=['AreaCalculo', 'Consecutivo', 'Division','GrupoProducto', 'SBA','TipoEmpleado'],
    how='left'
)
venta_recaudo_presupuesto_original_ve_presupuesto_zona_cluster


# In[200]:


recaudo_real_zona_ve = venta_recaudo_presupuesto_original_ve_real_zona_cluster.groupby(['AreaCalculo', 'ZonaGeografica', 'Fecha','Cluster','Consecutivo', 'Division','GrupoProducto', 'CodigoCentroCosto'], as_index=False).agg({'Recaudo': 'sum'})
recaudo_real_zona_ve.rename(columns={'Recaudo': 'Real'}, inplace=True)
recaudo_real_zona_ve


# In[201]:


recaudo_presupuesto_zona_ve = venta_recaudo_presupuesto_original_ve_presupuesto_zona_cluster.groupby(['AreaCalculo', 'ZonaGeografica', 'Fecha','Cluster','Consecutivo', 'Division','GrupoProducto', 'CodigoCentroCosto'], as_index=False).agg({'Recaudo': 'sum'})
recaudo_presupuesto_zona_ve.rename(columns={'Recaudo': 'Presupuesto'}, inplace=True)
recaudo_presupuesto_zona_ve


# In[202]:


# recaudo_real_zona_ve = venta_recaudo_presupuesto_original_ve_real_zona_cluster.groupby(['AreaCalculo', 'ZonaGeografica', 'Fecha','Cluster', 'Consecutivo'], as_index=False).agg({'Recaudo': 'sum'})
# recaudo_real_zona_ve.rename(columns={'Recaudo': 'Real'}, inplace=True)

# recaudo_presupuesto_zona_ve = venta_recaudo_presupuesto_original_ve_presupuesto_zona_cluster.groupby(['AreaCalculo', 'ZonaGeografica', 'Fecha','Cluster', 'Consecutivo'], as_index=False).agg({'Recaudo': 'sum'})
# recaudo_presupuesto_zona_ve.rename(columns={'Recaudo': 'Presupuesto'}, inplace=True)

main = pd.merge(
    left=recaudo_presupuesto_zona_ve,
    right=recaudo_real_zona_ve,
    on=['ZonaGeografica', 'AreaCalculo', 'Fecha','Cluster', 'Consecutivo', 'GrupoProducto', 'Division', 'CodigoCentroCosto'],
    how='left'
)

main['Real'].fillna(0.0, inplace=True)

main


# In[203]:


df_zonas_clusters_empleado[
    (df_zonas_clusters_empleado['TipoEmpleado'] == 'VE') &
    ((df_zonas_clusters_empleado['AreaCalculo'] == '1') | (df_zonas_clusters_empleado['AreaCalculo'] == '11'))
]


# In[204]:


# main['Real'] = main.groupby(['AreaCalculo', 'ZonaGeografica', 'Fecha','Cluster', 'Consecutivo'])['Real'].transform(pd.Series.sum)
# main['Presupuesto'] = main.groupby(['AreaCalculo', 'ZonaGeografica', 'Fecha','Cluster', 'Consecutivo'])['Presupuesto'].transform(pd.Series.sum)
main['TipoEmpleado'] = 'VE'

### Agrupación a nivel de empleado por zonas

main = pd.merge(
    main,
    df_zonas_clusters_empleado[['CodigoEmpleado', 'TipoEmpleado', 'AreaCalculo', 'Zona', 'Cluster']].rename(columns={'Zona': 'ZonaGeografica'}),
    on=['TipoEmpleado', 'AreaCalculo', 'ZonaGeografica', 'Cluster'],
    how='left'
)

main = pd.merge(
    main,
    df_empleados[['CodigoEmpleado', 'FechaIngreso']],
    on='CodigoEmpleado',
    how='left'
)

main = main[main['Fecha'] >= main['FechaIngreso']]
main


# In[205]:


main[
    (main['TipoEmpleado'] == 'VE')
]


# In[206]:


venta_cobrada_portafolio_zona_cluster_sub_centro_costo = dataframe_loader.f_calculo_centro_costo_variable(main, 'VentaCobradaPortafolioZonaClusterSub')
venta_cobrada_portafolio_zona_cluster_sub_centro_costo


# In[207]:


# main['Real'] = main.groupby(['AreaCalculo', 'ZonaGeografica', 'Fecha','Cluster', 'Consecutivo'])['Real'].transform(pd.Series.sum)
# main['Presupuesto'] = main.groupby(['AreaCalculo', 'ZonaGeografica', 'Fecha','Cluster', 'Consecutivo'])['Presupuesto'].transform(pd.Series.sum)
main_copy = main.copy()


main = main.groupby(['CodigoEmpleado', 'Fecha', 'Consecutivo'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main.reset_index(inplace=True)

# Variable solo de VCs y VEs #
if len(codigos_ve_vc) > 0 and fecha_liquidacion >= '2022-03-01' and fecha_liquidacion <= '2022-12-01':
    main = main[main['Fecha'] >= '2022-03-01']
###

main['Contexto'] = main['CodigoEmpleado'] + '_' +  main['Consecutivo'].astype(int).astype('str')

main['Real'] = main.groupby(['Contexto'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['Contexto'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaCobradaPortafolioZonaClusterSub'
# main['Contexto'] = main['CodigoEmpleado']
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
###

venta_cobrada_portafolio_zona_cluster_sub = main.copy()
main = None

venta_cobrada_portafolio_zona_cluster_sub.head()


# In[208]:


venta_cobrada_portafolio_zona_cluster_sub[
    venta_cobrada_portafolio_zona_cluster_sub['Contexto'].str.contains('1060654227')
]


# ## 34.1 VentaCobradaPortafolioZonaCluster

# In[209]:


main = main_copy.copy()

main = main.groupby(['CodigoEmpleado', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main.reset_index(inplace=True)

# Variable solo de VCs y VEs #
if len(codigos_ve_vc) > 0 and fecha_liquidacion >= '2022-03-01' and fecha_liquidacion <= '2022-12-01':
    main = main[main['Fecha'] >= '2022-03-01']
###

main['Contexto'] = main['CodigoEmpleado']

main['Real'] = main.groupby(['Contexto'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['Contexto'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaCobradaPortafolioZonaCluster'
# main['Contexto'] = main['CodigoEmpleado']
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
###

venta_cobrada_portafolio_zona_cluster = main.copy()
main = None

venta_cobrada_portafolio_zona_cluster.head()


# ## 35. VentaCobradaUnidadNegocioSub

# In[210]:


# main = None

# venta_cobrada_portafolio_cluster_sub = main
# main = None
# venta_cobrada_portafolio_cluster_sub

# # Contexto: 


# ## 36. VentaCobradaZonaPaisSub

# In[211]:


area_calculo_sba_completo_gc = area_calculo_sba_completo[
    (area_calculo_sba_completo['TipoEmpleado'] == 'GC') |
    (area_calculo_sba_completo['TipoEmpleado'] == 'GD')
]

area_calculo_sba_completo_gc_without_dup = area_calculo_sba_completo_gc[['TipoEmpleado','Consecutivo', 'Division']].drop_duplicates()
area_calculo_sba_completo_gc_without_dup.head()


# ### Se hace merge entre area de cálculo por GC y Venta Recaudo

# In[212]:


drop_cols = ['Venta'] #'GrupoProducto', 'SBA'
recaudo_real_zona_gc = pd.merge(
    left=area_calculo_sba_completo_gc_without_dup,
    right=df_venta_recaudo_real,
    on=['Division'],
    how='left'
)
recaudo_real_zona_gc = recaudo_real_zona_gc.dropna(subset=['ZonaGeografica']).drop(columns=drop_cols)
recaudo_real_zona_gc.rename(columns={'Recaudo': 'Real'}, inplace=True)
recaudo_real_zona_gc


# In[213]:


recaudo_presupuesto_zona_gc = pd.merge(
    left=area_calculo_sba_completo_gc_without_dup,
    right=df_venta_recaudo_presupuesto,
    on=['Division'],
    how='left'
)

recaudo_presupuesto_zona_gc = recaudo_presupuesto_zona_gc.drop(columns=drop_cols).dropna(subset=['ZonaGeografica'])
recaudo_presupuesto_zona_gc.rename(columns={'Recaudo': 'Presupuesto'}, inplace=True)
recaudo_presupuesto_zona_gc


# ### Se hace merge entre recaudo presupuesto y recaudo real

# In[214]:


main = pd.merge(
    left=recaudo_presupuesto_zona_gc,
    right=recaudo_real_zona_gc,
    on=['ZonaGeografica', 'Fecha', 'Consecutivo', 'Division', 'CodigoEmpleado', 'TipoEmpleado', 'Cluster', 'GrupoProducto', 'SBA', 'Canal'],
    how='outer'
)
main = main.dropna(subset=['ZonaGeografica']).drop(columns=['CodigoEmpleado'])
main


# In[215]:


df_zonas_empleado_gc = df_zonas_empleado[
    (df_zonas_empleado['TipoEmpleado']== 'GC') |
    (df_zonas_empleado['TipoEmpleado']== 'GD')
]
df_zonas_empleado_gc


# ## Se hace merge entre el dataframe anterior y zonas empleados para obtener los código de los GC

# In[216]:


main = main.merge(
    df_zonas_empleado_gc.rename(columns={'Zona': 'ZonaGeografica'}),
    on=['ZonaGeografica', 'TipoEmpleado'],
    how='left'
)
main['Real'].fillna(0.0, inplace=True)
main.dropna(subset=['CodigoEmpleado'])


# In[217]:


# main[
#     (main['CodigoEmpleado'] == '5002711') &
#     (main['Consecutivo'] == 1)
# ].to_excel("VentaCobradaZonaPaisSub.xlsx")


# In[ ]:





# In[218]:


venta_cobrada_zona_pais_sub_centro_costo = dataframe_loader.f_calculo_centro_costo_variable(main=main, variable='VentaCobradaZonaPaisSub').copy()
venta_cobrada_zona_pais_sub_centro_costo


# In[219]:


main_copy = main.copy()
# Calcular
main['Contexto']= main['CodigoEmpleado'] + '_' +  main['Consecutivo'].astype(int).astype('str')

main = main.groupby(['Contexto', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main.reset_index(inplace=True)

main['Real'] = main.groupby(['Contexto'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['Contexto'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaCobradaZonaPaisSub'

#Formatear
main.reset_index(inplace=True)
main = main[['Contexto', 'Variable', 'Fecha', 'Real','Presupuesto','PorcentajeCumplimiento']]

venta_cobrada_zona_pais_sub = main
main = None
venta_cobrada_zona_pais_sub

# Contexto: Código de empleado, gerentes de canal.


# In[220]:


venta_cobrada_zona_pais_sub[venta_cobrada_zona_pais_sub['Contexto'] == '6474959_1']


# In[221]:


venta_cobrada_zona_pais_sub[venta_cobrada_zona_pais_sub['Contexto'] == '6474959_2']


# In[222]:


venta_cobrada_zona_pais_sub[venta_cobrada_zona_pais_sub['Contexto'] == '5002711_3']


# ## 36.1 VentaCobradaZonaPais

# In[223]:


main = main_copy
# Calcular
main['Contexto']= main['CodigoEmpleado']

main = main.groupby(['Contexto', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main.reset_index(inplace=True)

main['Real'] = main.groupby(['Contexto'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['Contexto'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaCobradaZonaPais'

#Formatear
main.reset_index(inplace=True)
main = main[['Contexto', 'Variable', 'Fecha', 'Real','Presupuesto','PorcentajeCumplimiento']]

venta_cobrada_zona_pais = main
main = None
venta_cobrada_zona_pais


# In[ ]:





# ## 36. VentaFacturadaPortafolioRenal

# In[224]:


# Aplica VC5


# In[225]:


area_calculo_sba_completo.head()


# In[226]:


df_renal_ambulatorio.head()


# In[ ]:





# In[227]:


7.384809+1044514+8.649572


# In[228]:


# Calcular

main = df_renal_ambulatorio.rename(columns={'PlanVentaTotal': 'Presupuesto', 'RealVentaTotal': 'Real'})

main['Contexto'] = main['CodigoEmpleado']

main = main.groupby(['Contexto', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main['Real'] = main.groupby(['Contexto'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['Contexto'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaFacturadaPortafolioRenal'


#Formatear
main.reset_index(inplace=True)
main = main[['Contexto', 'Variable', 'Fecha', 'Real','Presupuesto', 'PorcentajeCumplimiento']]


venta_facturada_portafolio_renal = main.copy()
main = None
venta_facturada_portafolio_renal


# ## 36.1 VentaFacturadaPortafolioRenalSub

# In[229]:


df_area_calculo_sba.head()


# In[230]:


renal_ambulatorio = df_renal_ambulatorio.copy()
renal_ambulatorio['Consecutivo'] = '1'
renal_ambulatorio


# In[231]:


# Calcular

main = renal_ambulatorio.rename(columns={'PlanVentaTotal': 'Presupuesto', 'RealVentaTotal': 'Real'})

main['Contexto'] = main['CodigoEmpleado'].str.cat(main['Consecutivo'],sep="_")

main = main.groupby(['Contexto', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main['Real'] = main.groupby(['Contexto'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['Contexto'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaFacturadaPortafolioRenalSub'


#Formatear
main.reset_index(inplace=True)
main = main[['Contexto', 'Variable', 'Fecha', 'Real','Presupuesto', 'PorcentajeCumplimiento']]


venta_facturada_portafolio_renal_sub = main.copy()
main = None
venta_facturada_portafolio_renal_sub


# ## 37. VentaFacturadaPortafolioZonaCluster

# In[232]:


venta_recaudo_presupuesto_original_ve_real_zona_cluster.head()


# In[233]:


venta_recaudo_presupuesto_original_ve_presupuesto_zona_cluster.head()


# In[234]:


# VentaRecaudoReal (venta)
# VentaRecaudoPresupuesto (venta)

# Preparar
# drop_cols = ['Recaudo', 'Division','CodigoEmpleado','GrupoProducto', 'Canal', 'SBA']
real = venta_recaudo_presupuesto_original_ve_real_zona_cluster.copy()
ppto = venta_recaudo_presupuesto_original_ve_presupuesto_zona_cluster.copy()

# Sumamos los registros que tienen el mismo conexto para que exista un solo registro
ppto = ppto.groupby(['AreaCalculo','Fecha', 'ZonaGeografica', 'Cluster'], as_index=True)[['Venta']].apply(sum)
real = real.groupby(['AreaCalculo', 'Fecha', 'ZonaGeografica', 'Cluster'], as_index=True)[['Venta']].apply(sum)
ppto.reset_index(inplace=True)
real.reset_index(inplace=True)

main = ppto.rename(columns={'Venta': 'Presupuesto'}).merge(
    real.rename(columns={'Venta': 'Real'}),
    on=['Fecha', 'ZonaGeografica', 'Cluster','AreaCalculo'],
    how='left'
)
main = main.merge(
    df_zonas_clusters_empleado.rename(columns={'Zona': 'ZonaGeografica'}),
    on=['ZonaGeografica', 'Cluster','AreaCalculo'],
    how='left'
)

# Traer áreas de cálculo y tipo de empleado de la maestra de empleados
empleados = df_empleados[['TipoEmpleado', 'CodigoEmpleado', 'AreaCalculo', 'FechaIngreso']].copy()
main = main.merge(empleados, on=['TipoEmpleado', 'CodigoEmpleado', 'AreaCalculo'], how='left')

main = main[main['Fecha'] >= main['FechaIngreso']]
main


# In[235]:


main['Real'].fillna(0, inplace=True)

# Calcular

main['Contexto'] = main['CodigoEmpleado']
# + '_' +  main['ZonaGeografica'].astype('str')
main
# main['Contexto']= 'VFPZ'


# In[236]:


# Calcular

# main['Contexto'] = main['CodigoEmpleado'] + '_' +  main['ZonaGeografica'].astype(int).astype('str')
# main['Contexto']= 'VFPZ'
main = main.groupby(['Contexto', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main['Real'] = main.groupby(['Contexto'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['Contexto'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaFacturadaPortafolioZonaCluster'


#Formatear
main.reset_index(inplace=True)
main = main[['Contexto', 'Variable', 'Fecha', 'Real','Presupuesto', 'PorcentajeCumplimiento']]


venta_facturada_portafolio_zona_cluster = main.copy()
main = None
venta_facturada_portafolio_zona_cluster


# In[237]:


venta_facturada_portafolio_zona_cluster[venta_facturada_portafolio_zona_cluster['Contexto'].str.contains('1060654227') ]


# In[238]:


venta_facturada_portafolio_zona_cluster[venta_facturada_portafolio_zona_cluster['Contexto'].str.contains('5041369') ]


# ## 37.1 VentaFacturadaPortafolioZonaClusterSub

# In[239]:


# VentaRecaudoReal (venta)
# VentaRecaudoPresupuesto (venta)

# Preparar
# drop_cols = ['Recaudo', 'Division','CodigoEmpleado','GrupoProducto', 'Canal', 'SBA']

ppto = venta_recaudo_presupuesto_original_ve_presupuesto_zona_cluster.copy()
real = venta_recaudo_presupuesto_original_ve_real_zona_cluster.copy()

# Sumamos los registros que tienen el mismo conexto para que exista un solo registro
ppto = ppto.groupby(['AreaCalculo','Fecha', 'ZonaGeografica', 'Cluster', 'Consecutivo'], as_index=True)[['Venta']].apply(sum)
real = real.groupby(['AreaCalculo', 'Fecha', 'ZonaGeografica', 'Cluster', 'Consecutivo'], as_index=True)[['Venta']].apply(sum)
ppto.reset_index(inplace=True)
real.reset_index(inplace=True)

main = ppto.rename(columns={'Venta': 'Presupuesto'}).merge(
    real.rename(columns={'Venta': 'Real'}),
    on=['Fecha', 'ZonaGeografica', 'Cluster','AreaCalculo', 'Consecutivo'],
    how='outer'
)

main = main.merge(
    df_zonas_clusters_empleado.rename(columns={'Zona': 'ZonaGeografica'}),
    on=['ZonaGeografica', 'Cluster','AreaCalculo'],
    how='left'
)

# Traer áreas de cálculo y tipo de empleado de la maestra de empleados
empleados = df_empleados[['TipoEmpleado', 'CodigoEmpleado', 'AreaCalculo', 'FechaIngreso']].copy()
main = main.merge(empleados, on=['TipoEmpleado', 'CodigoEmpleado', 'AreaCalculo'], how='left')

main = main[main['Fecha'] >= main['FechaIngreso']]
main['Real'].fillna(0, inplace=True)
main['Contexto']= main['CodigoEmpleado'] + '_' +  main['Consecutivo'].astype(int).astype('str')
main


# In[240]:


main[main['CodigoEmpleado'] == '1010166177']


# In[241]:


main = main.groupby(['Contexto', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main['Real'] = main.groupby(['Contexto'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['Contexto'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaFacturadaPortafolioZonaClusterSub'


#Formatear
main.reset_index(inplace=True)
main = main[['Contexto', 'Variable', 'Fecha', 'Real','Presupuesto', 'PorcentajeCumplimiento']]


venta_facturada_portafolio_zona_cluster_sub = main.copy()
main = None
venta_facturada_portafolio_zona_cluster_sub


# In[242]:


venta_facturada_portafolio_zona_cluster_sub[venta_facturada_portafolio_zona_cluster_sub['Contexto'] == '1010166177_1']


# ## 38. VentaFacturadaZonaPais

# In[243]:


# VentaRecaudoReal (venta)
# VentaRecaudoPresupuesto (venta)

# Preparar
drop_cols = ['Recaudo', 'Division', 'Cluster','GrupoProducto', 'Canal', 'SBA']
real = df_venta_recaudo_real.copy().drop(columns=drop_cols)
ppto = df_venta_recaudo_presupuesto.copy().drop(columns=drop_cols)

# Sumamos los registros que tienen el mismo conexto para que exista un solo registro
ppto = ppto.groupby(['Fecha', 'ZonaGeografica'], as_index=True)[['Venta']].apply(sum)
ppto.reset_index(inplace=True)
real = real.groupby(['Fecha', 'ZonaGeografica'], as_index=True)[['Venta']].apply(sum)
real.reset_index(inplace=True)

main = ppto.rename(columns={'Venta': 'Presupuesto'}).merge(
    real.rename(columns={'Venta': 'Real'}),
    on=['Fecha', 'ZonaGeografica'],
    how='left'
)    

main = main.merge(
    df_zonas_empleado.rename(columns={'Zona': 'ZonaGeografica'}),
    on=['ZonaGeografica'],
    how='left'
)
main['Real'].fillna(0, inplace=True)

# Calcular
main['Contexto']= main['CodigoEmpleado']

main = main.groupby(['Contexto', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main['Real'] = main.groupby(['Contexto'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['Contexto'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaFacturadaZonaPais'


#Formatear
main.reset_index(inplace=True)
main = main[['Contexto', 'Variable', 'Fecha','Real','Presupuesto','PorcentajeCumplimiento']]

venta_facturada_zona_pais = main.copy()
main = None
venta_facturada_zona_pais


# In[244]:


venta_facturada_zona_pais[venta_facturada_zona_pais['Contexto'] == '1111115']


# In[245]:


venta_facturada_zona_pais[venta_facturada_zona_pais['Contexto'] == '1111124']


# In[ ]:





# # Nuevos Rubros 2024

# ## 38.1 VentaFacturadaZonaPaisSub

# In[246]:


df_venta_recaudo_real


# In[247]:


area_calculo_sba_completo_gc = area_calculo_sba_completo[area_calculo_sba_completo['TipoEmpleado'] == 'GC']
area_calculo_sba_completo_gc_without_dup = area_calculo_sba_completo_gc[['TipoEmpleado','Consecutivo', 'Division']].drop_duplicates()
area_calculo_sba_completo_gc_without_dup.head()


# In[248]:


drop_cols = ['Recaudo'] #'GrupoProducto', 'SBA'
facturado_real_zona_gc = pd.merge(
    left=area_calculo_sba_completo_gc_without_dup,
    right=df_venta_recaudo_real,
    on=['Division'],
    how='left'
)
facturado_real_zona_gc = facturado_real_zona_gc.dropna(subset=['ZonaGeografica']).drop(columns=drop_cols)
facturado_real_zona_gc.rename(columns={'Venta': 'Real'}, inplace=True)
facturado_real_zona_gc


# In[249]:


facturado_real_zona_gc[
    (facturado_real_zona_gc['ZonaGeografica'] == 'Centro') &
    (facturado_real_zona_gc['Fecha'] == '2023-12-01') &
    (facturado_real_zona_gc['Consecutivo'] == 1)
]['Real'].sum()


# In[250]:


facturado_presupuesto_zona_gc = pd.merge(
    left=area_calculo_sba_completo_gc_without_dup,
    right=df_venta_recaudo_presupuesto,
    on=['Division'],
    how='left'
)

facturado_presupuesto_zona_gc = facturado_presupuesto_zona_gc.drop(columns=drop_cols).dropna(subset=['ZonaGeografica'])
facturado_presupuesto_zona_gc.rename(columns={'Venta': 'Presupuesto'}, inplace=True)
facturado_presupuesto_zona_gc


# In[251]:


facturado_presupuesto_zona_gc[
    (facturado_presupuesto_zona_gc['ZonaGeografica'] == 'Centro') &
    (facturado_presupuesto_zona_gc['Fecha'] == '2023 -12-01') &
    (facturado_presupuesto_zona_gc['Consecutivo'] == 1)
]['Presupuesto'].sum()


# In[252]:


main = pd.merge(
    left=facturado_presupuesto_zona_gc,
    right=facturado_real_zona_gc,
    on=['ZonaGeografica', 'Fecha', 'Consecutivo', 'Division', 'CodigoEmpleado', 'TipoEmpleado', 'Cluster', 'GrupoProducto', 'SBA', 'Canal', 'Moneda'],
    how='outer'
)
main = main.dropna(subset=['ZonaGeografica']).drop(columns=['CodigoEmpleado'])
main


# In[253]:


df_zonas_empleado_gc = df_zonas_empleado[
    (df_zonas_empleado['TipoEmpleado']== 'GC')
]
df_zonas_empleado_gc


# In[254]:


df_parrillas[df_parrillas['Variable'] == 'VentaFacturadaZonaPaisSub']


# In[255]:


main = main.merge(
    df_zonas_empleado_gc.rename(columns={'Zona': 'ZonaGeografica'}),
    on=['ZonaGeografica', 'TipoEmpleado'],
    how='left'
)
main['Real'].fillna(0.0, inplace=True)
main.dropna(subset=['CodigoEmpleado'])


# In[256]:


main[
    (main['ZonaGeografica'] == 'Centro') &
    (main['Fecha'] == '2023-12-01') &
    (main['Consecutivo'] == 1)
]['Real'].sum()


# In[257]:


main[
    (main['ZonaGeografica'] == 'Centro') &
    (main['Fecha'] == '2023-12-01') &
    (main['Consecutivo'] == 1)
]['Presupuesto'].sum()


# In[258]:


# venta_cobrada_zona_pais_sub_centro_costo = dataframe_loader.f_calculo_centro_costo_variable(main=main, variable='VentaFacturadaZonaPaisSub').copy()
# venta_cobrada_zona_pais_sub_centro_costo


# In[259]:


venta_facturada_zona_pais_sub = main.copy()
venta_facturada_zona_pais_sub['Consecutivo'] = venta_facturada_zona_pais_sub['Consecutivo'].astype(int).astype('str')
venta_facturada_zona_pais_sub['Contexto'] = venta_facturada_zona_pais_sub['CodigoEmpleado'].str.cat(venta_facturada_zona_pais_sub['Consecutivo'],sep="_")
venta_facturada_zona_pais_sub


# In[260]:


venta_facturada_zona_pais_sub[venta_facturada_zona_pais_sub['Contexto'] == '5002711_1']['Real'].sum()


# In[261]:


venta_facturada_zona_pais_sub[venta_facturada_zona_pais_sub['Contexto'] == '5002711_2']['Real'].sum()


# In[262]:


venta_facturada_zona_pais_sub[venta_facturada_zona_pais_sub['Contexto'] == '5002711_3']['Real'].sum()


# In[263]:


venta_facturada_zona_pais_sub[venta_facturada_zona_pais_sub['ZonaGeografica'] == 'Norte']['Presupuesto'].sum()


# In[264]:


venta_facturada_zona_pais_sub = venta_facturada_zona_pais_sub.groupby(['Fecha', 'Contexto', 'ZonaGeografica'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
venta_facturada_zona_pais_sub.reset_index(inplace=True)
venta_facturada_zona_pais_sub


# In[265]:


venta_facturada_zona_pais_sub[
    (venta_facturada_zona_pais_sub['ZonaGeografica'] == 'Norte')
]


# In[266]:


# venta_facturada_zona_pais_sub = venta_facturada_zona_pais_sub1.copy()


# In[267]:


# venta_facturada_zona_pais_sub1 = venta_facturada_zona_pais_sub
# venta_facturada_zona_pais_sub1.head()


# In[268]:


venta_facturada_zona_pais_sub['Real'].fillna(0.0, inplace=True)
venta_facturada_zona_pais_sub['Presupuesto'].fillna(0.0, inplace=True)

venta_facturada_zona_pais_sub['Presupuesto'] = venta_facturada_zona_pais_sub.groupby(['Contexto', 'ZonaGeografica'])['Presupuesto'].transform(pd.Series.cumsum)
venta_facturada_zona_pais_sub['Real'] = venta_facturada_zona_pais_sub.groupby(['Contexto', 'ZonaGeografica'])['Real'].transform(pd.Series.cumsum)

venta_facturada_zona_pais_sub.head()


# In[ ]:





# In[269]:


venta_facturada_zona_pais_sub[
    (venta_facturada_zona_pais_sub['ZonaGeografica'] == 'Norte') 
]


# In[270]:


venta_facturada_zona_pais_sub['PorcentajeCumplimiento'] = venta_facturada_zona_pais_sub['Real'] / venta_facturada_zona_pais_sub['Presupuesto']
venta_facturada_zona_pais_sub['PorcentajeCumplimiento'] = venta_facturada_zona_pais_sub['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
venta_facturada_zona_pais_sub['PorcentajeCumplimiento'].fillna(0.0, inplace=True)

# venta_facturada_zona_pais_sub = convertir_a_euros(venta_facturada_zona_pais_sub)

venta_facturada_zona_pais_sub['Variable'] = 'VentaFacturadaZonaPaisSub'

# venta_facturada_zona_pais_sub = venta_facturada_zona_pais_sub[colums_name]

venta_facturada_zona_pais_sub = venta_facturada_zona_pais_sub[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

venta_facturada_zona_pais_sub


# In[271]:


venta_facturada_zona_pais_sub[
    (venta_facturada_zona_pais_sub['Fecha'] == '2024-03-01')
]


# ## 39 VentaFacturadaBraunPaisSub

# In[272]:


df_venta_otras_companias


# In[273]:


df_recaudo_otras_companias


# In[274]:


area_calculo_sba_completo


# In[275]:


area_calculo_sba_completo_bbmco_bbmec = area_calculo_sba_completo[
    ((area_calculo_sba_completo['TipoEmpleado']=='AD4') | (area_calculo_sba_completo['TipoEmpleado']=='MK')) &
    ((area_calculo_sba_completo['Division']=='BBMCO') | (area_calculo_sba_completo['Division']=='BBMEC'))
]
area_calculo_sba_completo_bbmco_bbmec


# In[276]:


main = pd.merge(
    left=area_calculo_sba_completo_bbmco_bbmec,
    right=df_venta_otras_companias.rename(columns={'Compania': 'Division'}),
    on=['Division'],
    how='left'
)
main


# In[ ]:





# In[277]:


main = main.merge(df_empleados)
main['PorcentajeCumplimiento'] = main['Real'] / main['Presupuesto']
main['PorcentajeCumplimiento'] = main['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
main['Variable'] = 'VentaFacturadaBraunPaisSUB'
main['Contexto'] = main['CodigoEmpleado'].astype(str).str.cat(main['Consecutivo'].astype(str), sep="_")
main


# In[278]:


main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
venta_facturada_braun_pais_sub = main.copy()
main = None
venta_facturada_braun_pais_sub


# In[279]:


df_parrillas[df_parrillas['Variable'].str.contains('VentaFacturadaBraunPaisSUB')]


# ## 40 VentaFacturadaBraunPais

# In[280]:


# VentaRecaudoReal (venta)
# VentaRecaudoPresupuesto (venta)

# Preparar
drop_cols = ['Recaudo', 'Division', 'Cluster','CodigoEmpleado', 'ZonaGeografica','GrupoProducto', 'Canal', 'SBA']
real = df_venta_recaudo_real.copy().drop(columns=drop_cols)
ppto = df_venta_recaudo_presupuesto.copy().drop(columns=drop_cols)

# Sumamos los registros que tienen el mismo conexto para que exista un solo registro
ppto = ppto.groupby(['Fecha'], as_index=True)[['Venta']].apply(sum)
real = real.groupby(['Fecha'], as_index=True)[['Venta']].apply(sum)
ppto.reset_index(inplace=True)
real.reset_index(inplace=True)

main = ppto.rename(columns={'Venta': 'Presupuesto'}).merge(
    real.rename(columns={'Venta': 'Real'}),
    on=['Fecha'],
    how='left'
)
main['Real'].fillna(0, inplace=True)


# Calcular
main['Contexto']= 'BBMCO'
main = main.groupby(['Contexto', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main['Real'] = main.groupby(['Contexto'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['Contexto'])['Presupuesto'].transform(pd.Series.cumsum)
main['PorcentajeCumplimiento'] = (main['Real']/main['Presupuesto']).apply(lambda x: round(x, 2))
main['Variable'] = 'VentaFacturadaBraunPais'


#Formatear
main.reset_index(inplace=True)
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]


venta_facturada_braun_pais = main.copy()
main = None
venta_facturada_braun_pais


# ## 41 RentabilidadUnidadNegocioCM2COGsSUB

# In[281]:


df_rentabilidad.copy()


# In[282]:


rentabilidad_gu = df_rentabilidad.copy()
rentabilidad_gu['TipoEmpleado'] = 'GU'
rentabilidad_gu = rentabilidad_gu.merge(
    area_calculo_sba_completo[['TipoEmpleado', 'AreaCalculo', 'GrupoProducto', 'SBA']],
    on=['TipoEmpleado', 'GrupoProducto', 'SBA'],
    how='left'
)
rentabilidad_gu


# In[283]:


area_calculo_sba_completo_gu


# In[284]:


main_rentabilidad = rentabilidad_gu[
    rentabilidad_gu['ClaseRentabilidad'] == 'CM2COGSP'
]
main_rentabilidad.drop(columns='ClaseRentabilidad', inplace=True)

main_area_calculo = main_rentabilidad.merge(
    area_calculo_sba_completo_gu,
    how='left'
)

main = main_area_calculo.merge(
    df_empleados,
    how='left'
)
main.head()


# In[285]:


main['Consecutivo'].fillna(0.0, inplace=True)
main['Contexto'] = main['CodigoEmpleado'].str.lower().str.cat(main['Consecutivo'].astype(int).astype(str), sep="_")
main = main.groupby(
    ['Fecha', 'Contexto'],
    as_index=False
).agg({'Real': 'sum', 'Presupuesto': 'sum'})

main['Variable'] = 'RentabilidadUnidadNegocioCM2COGSSUB'
main['PorcentajeCumplimiento'] = main['Real'] / main['Presupuesto']
main['PorcentajeCumplimiento'] = main['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
main.head()


# In[286]:


# ### Cálculo de resultados cualitativos precargados
# main = dataframe_loader.sobreescribir_resultados_cualitativos(
#     main=main,
#     variable='RentabilidadUnidadNegocioCM2COGSSUB',
#     columnas_extra=['CodigoEmpleado', 'Consecutivo'],
#     columnas_extra_merge=['CodigoEmpleado', 'Consecutivo']
# )
# ###

# main


# In[287]:


rentabilidad_unidad_negocio_cm2_cogs_sub = main.copy()

rentabilidad_unidad_negocio_cm2_cogs_sub = rentabilidad_unidad_negocio_cm2_cogs_sub[
    ['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
rentabilidad_unidad_negocio_cm2_cogs_sub = rentabilidad_unidad_negocio_cm2_cogs_sub.drop_duplicates()
rentabilidad_unidad_negocio_cm2_cogs_sub.head()


# In[288]:


rentabilidad_unidad_negocio_cm2_cogs_sub


# In[289]:


df_parrillas[df_parrillas['Variable'] == 'RentabilidadUnidadNegocioCM2COGSSUB']


# ## 42 RentabilidadDivisionCM2COGsSub

# In[290]:


df_rentabilidad.rename(columns={'Compania': 'Division'})


# In[291]:


df_rentabilidad['Compania'].drop_duplicates()


# In[292]:


# df_recaudo_otras_companias.rename(columns={'Compania': 'Division'})


# In[293]:


df_area_calculo_sba[df_area_calculo_sba['TipoEmpleado'] == 'GD']


# In[294]:


area_calculo_sba_completo_gd.copy()


# In[295]:


area_calculo_sba_completo_gd[area_calculo_sba_completo_gd['Division'] == 'AE']


# In[296]:


area_calculo_sba_completo_gd_copy = area_calculo_sba_completo_gd.copy()
area_calculo_sba_completo_gd_copy['ClaseRentabilidad'] = 'CM2COGSP'
area_calculo_sba_completo_gd_copy


# In[297]:


area_calculo_sba_completo_gd_copy[area_calculo_sba_completo_gd_copy['Division'] == 'AE']


# In[298]:


df_parrillas[df_parrillas['Variable'] == 'RentabilidadDivisionCM2COGsSub'] 


# In[299]:


main = pd.merge(
    left=area_calculo_sba_completo_gd_copy.copy(),
    right=df_rentabilidad[df_rentabilidad['ClaseRentabilidad'] == 'CM2COGSP'].drop(columns=['Compania']),
    on=['GrupoProducto', 'ClaseRentabilidad', 'SBA'],
    how = 'right'
)
main


# In[300]:


# main = pd.merge(
#     left=area_calculo_sba_completo_gd.copy().drop(columns=['SBA']),
#     right=df_rentabilidad.copy().drop(columns=['ClaseRentabilidad', 'Compania']),
#     on=['GrupoProducto'],
#     how = 'left'
# )
# main


# In[301]:


main[main['Division'] == 'AE']


# In[302]:


main[main['Division'] == 'AE']['Real'].sum()


# In[303]:


main[main['Division'] == 'AE']['Presupuesto'].sum()


# In[304]:


main[main['Division'] == 'HC']['Real'].sum()


# In[305]:


main[main['Division'] == 'HC']['Presupuesto'].sum()


# In[306]:


main[main['Division'] == 'BA']['Real'].sum()


# In[307]:


main[main['Division'] == 'BA']['Presupuesto'].sum()


# In[308]:


df_empleados[df_empleados['AreaCalculo'] == 1]


# In[309]:


main = main.merge(
    df_empleados,
    how='left'
)
main


# In[310]:


main[
    (main['Division'] == 'AE') & (main['Fecha'] == '2023-12-01')
]


# In[311]:


main['CodigoEmpleado'].fillna(0, inplace=True)
main['Consecutivo'].fillna(0, inplace=True)
main


# In[312]:


main['Contexto'] = main['CodigoEmpleado'].str.lower() + '_'+ main['Consecutivo'].astype(int).astype('str')
# main['Contexto'] = main['Division']
main = main.groupby(['Contexto', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
main['Real'] = main.groupby(['Contexto'])['Real'].transform(pd.Series.cumsum)
main['Presupuesto'] = main.groupby(['Contexto'])['Presupuesto'].transform(pd.Series.cumsum)
main


# In[313]:


main['PorcentajeCumplimiento'] = main['Real']/main['Presupuesto']
main['PorcentajeCumplimiento'] = main['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
main['Variable'] = 'RentabilidadDivisionCM2COGsSub'

# main['Contexto'] = main['Division']
main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

rentabilidad_division_cm2_cogs_sub = main.copy()

main = None
rentabilidad_division_cm2_cogs_sub


# In[314]:


rentabilidad_division_cm2_cogs_sub[rentabilidad_division_cm2_cogs_sub['Fecha'] == '2023-12-01']


# In[315]:


df_parrillas[df_parrillas['Variable'] =='RentabilidadDivisionCM2COGsSub']


# In[316]:


df_empleados[df_empleados['CodigoEmpleado'] == '6474959']


# In[317]:


df_empleados[df_empleados['CodigoEmpleado'] == '5004470']


# In[318]:


# rentabilidad_division_cm2cogs_gd = pd.merge(
#     left=df_rentabilidad[(df_rentabilidad['ClaseRentabilidad']=='CM2COGSP')],
#     right=area_calculo_sba_completo_gd,
#     on=['GrupoProducto', 'SBA'],
#     how='left'
# )
# rentabilidad_division_cm2_cogs_sub = rentabilidad_division_cm2cogs_gd.groupby(
#     ['Division', 'Consecutivo', 'Fecha'],
#     as_index=False
# ).agg({'Real': 'sum', 'Presupuesto': 'sum'})
# rentabilidad_division_cm2_cogs_sub['PorcentajeCumplimiento'] = rentabilidad_division_cm2_cogs_sub['Real']/cumplimiento_rentabilidad_cm3_grupo_clientes_sub['Presupuesto']
# rentabilidad_division_cm2_cogs_sub['PorcentajeCumplimiento'] = rentabilidad_division_cm2_cogs_sub['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
# rentabilidad_division_cm2_cogs_sub


# In[319]:


# rentabilidad_division_cm2_cogs_sub['Contexto'] = 'gd_' + rentabilidad_division_cm2_cogs_sub['Division'].astype('str').str.cat(rentabilidad_division_cm2_cogs_sub['Consecutivo'].astype('str'), sep="_")
# rentabilidad_division_cm2_cogs_sub['Variable'] = 'RentabilidadDivisionCM2COGsSub'
# rentabilidad_division_cm2_cogs_sub = rentabilidad_division_cm2_cogs_sub[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

# rentabilidad_division_cm2_cogs_sub


# ## 43 EBITCOGSPais

# In[320]:


df_parrillas[df_parrillas['Contexto'] == 'EBITCOGSP']


# In[321]:


ebit_cogs_pais = df_rentabilidad[(df_rentabilidad['Compania']=='BBMCO')&(df_rentabilidad['ClaseRentabilidad']=='EBITCOGSP')]

ebit_cogs_pais = ebit_cogs_pais.groupby(
    ['Fecha'],
    as_index=False
).agg({'Real': 'sum', 'Presupuesto': 'sum'})

# cumplimiento_rentabilidad_cm5_nivel_pais['Presupuesto'] = cumplimiento_rentabilidad_cm5_nivel_pais['Presupuesto'].transform(pd.Series.cumsum)
# cumplimiento_rentabilidad_cm5_nivel_pais['Real'] = cumplimiento_rentabilidad_cm5_nivel_pais['Real'].transform(pd.Series.cumsum)

ebit_cogs_pais['PorcentajeCumplimiento'] = ebit_cogs_pais['Real'] / ebit_cogs_pais['Presupuesto']
ebit_cogs_pais['PorcentajeCumplimiento'] = ebit_cogs_pais['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
ebit_cogs_pais['Contexto'] = 'EBITCOGSP'
ebit_cogs_pais['Variable'] = 'EBITCOGSPais'

ebit_cogs_pais.head()


# ## 44 EBITCOGSPaisSUB

# In[ ]:





# In[322]:


area_calculo_sba_completo[area_calculo_sba_completo['Division'] == 'BBMCO']


# In[323]:


area_calculo_sba_completo[area_calculo_sba_completo['Division'] == 'BBMEC']


# In[324]:


ebit_cogs_pais_sub = df_rentabilidad[df_rentabilidad['ClaseRentabilidad'] == 'EBITCOGSP'].rename(columns={'Compania':'Division'}).merge(
    area_calculo_sba_completo.drop(columns=['GrupoProducto', 'SBA']),
    on=['Division']
)
ebit_cogs_pais_sub.drop(columns='ClaseRentabilidad', inplace=True)
ebit_cogs_pais_sub


# In[325]:


ebit_cogs_pais_sub = ebit_cogs_pais_sub.merge(
        df_parrillas[df_parrillas['Variable'] == 'EBITCOGSPaisSUB'].merge(
        df_empleados
    )
)
ebit_cogs_pais_sub


# In[326]:


ebit_cogs_pais_sub['Variable'] = 'EBITCOGSPaisSUB'
### Cálculo de resultados cualitativos precargados

ebit_cogs_pais_sub['PorcentajeCumplimiento'] = ebit_cogs_pais_sub['Real'] / ebit_cogs_pais_sub['Presupuesto']
ebit_cogs_pais_sub['PorcentajeCumplimiento'] = ebit_cogs_pais_sub['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))

# ebit_cogs_pais_sub = dataframe_loader.sobreescribir_resultados_cualitativos(
#     main=ebit_cogs_pais_sub,
#     variable='EBITCOGSPaisSUB',
#     columnas_extra=['TipoEmpleado', 'Consecutivo'],
#     columnas_extra_merge=['TipoEmpleado', 'Consecutivo'],
#     pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == 'AD4']
# )
# ###

ebit_cogs_pais_sub['Consecutivo'] = ebit_cogs_pais_sub['Consecutivo'].astype(int).astype('str')
ebit_cogs_pais_sub['Contexto'] = ebit_cogs_pais_sub['CodigoEmpleado'] + '_'+ ebit_cogs_pais_sub['Consecutivo']

ebit_cogs_pais_sub = ebit_cogs_pais_sub[
    ['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]

ebit_cogs_pais_sub.head()


# In[327]:


ebit_cogs_pais_sub[ebit_cogs_pais_sub['Contexto'] == '5036358_1' ]


# ## Cálculo de Maestra de Resultados

# In[328]:


maestra_resultados = pd.concat([
    cumplimiento_objetivos_cualitativos, # cumplimiento_cualitativos,
    cumplimiento_recaudo_por_zona, # cumplimiento_cobro_por_zona,
    cumplimiento_venta_individual,
    cumplimiento_venta_individual_sub,
    cumplimiento_recaudo_individual,
    cumplimiento_recaudo_individual_sub, # cumplimiento_cobro_empleado_consecutivo,
    cumplimiento_venta_por_area_calculo, # cumplimiento_venta_por_area_calculo,
    cumplimiento_venta_por_area_calculo_sub,
    cumplimiento_recaudo_por_area_calculo, # cumplimiento_cobro_por_area_calculo,
    cumplimiento_recaudo_por_area_calculo_sub,
    cumplimiento_recaudo_nivel_pais, # cumplimiento_cobro_pais,
    cumplimiento_venta_por_unidad_negocio_gu,
    cumplimiento_venta_por_unidad_negocio_gu_sub,
    cumplimiento_recaudo_por_unidad_negocio_gu, # cumplimiento_cobro_area_calculo_gu,
    cumplimiento_recaudo_por_unidad_negocio_gu_sub,
    cumplimiento_venta_por_unidad_negocio_mk,
    cumplimiento_venta_por_unidad_negocio_mk_sub,
    cumplimiento_recaudo_por_unidad_negocio_mk, # cumplimiento_cobro_area_calculo_mk,
    cumplimiento_recaudo_por_unidad_negocio_mk_sub, # cumplimiento_cobro_area_calculo_mk,
    cumplimiento_rentabilidad_por_unidad_negocio_cm2_gu,
    cumplimiento_rentabilidad_por_unidad_negocio_cm3_gu,
    cumplimiento_rentabilidad_por_unidad_negocio_cm2_mk,
    cumplimiento_rentabilidad_por_unidad_negocio_cm3_mk,
    cumplimiento_rentabilidad_por_unidad_negocio_cm2_gu_sub,
    cumplimiento_rentabilidad_por_unidad_negocio_cm3_gu_sub,
    cumplimiento_rentabilidad_por_unidad_negocio_cm2_mk_sub,
    cumplimiento_rentabilidad_por_unidad_negocio_cm3_mk_sub,
    cumplimiento_recaudo_por_division, # cumplimiento_cobro_division,
    cumplimiento_recaudo_por_division_sub_mk, # cumplimiento_cobro_division_sub,
    cumplimiento_recaudo_otras_companias_mk, # cumplimiento_cobro_compañias_mk,
    cumplimiento_recaudo_por_division_sub_gd, # cumplimiento_recaudo_otras_companias_gd, # cumplimiento_cobro_division_sub_gd,
    cumplimiento_recaudo_por_division_sub_ad, # cumplimiento_cobro_division_sub_ad,
    cumplimiento_recaudo_otras_companias_ad, # cumplimiento_cobro_compañias_ad,
    cumplimiento_recaudo_por_division_sub_ad1, # cumplimiento_cobro_division_sub_ad1,
    cumplimiento_recaudo_otras_companias_ad1, # cumplimiento_cobro_compañias_ad1,
    cumplimiento_recaudo_por_division_sub_ad3, # cumplimiento_cobro_division_sub_ad3,
    cumplimiento_recaudo_otras_companias_ad3, # cumplimiento_cobro_compañias_ad3,
    cumplimiento_recaudo_por_division_sub_ad4, cumplimiento_recaudo_otras_companias_ad4, # cumplimiento_cobro_compañias_ad4,
    cumplimiento_recaudo_por_division_sub_ad5, cumplimiento_recaudo_otras_companias_ad5, # cumplimiento_cobro_compañias_ad5,
    cumplimiento_rentabilidad_cm5_nivel_pais, # rentabilidad_antes_de_impuestos,
    cumplimiento_rentabilidad_cm3_grupo_clientes_sub,  # rentabilidad_division_consecutivo,
    cumplimiento_rentabilidad_cm3_grupo_clientes, # rentabilidad_division,
    cumplimiento_rentabilidad_cm2_grupo_clientes, # rentabilidad_kam,
    cumplimiento_rentabilidad_cm5_ad, # rentabilidad_antes_de_impuestos_ad,
    cumplimiento_rentabilidad_cm5_ad1, # rentabilidad_antes_de_impuestos_ad1,
    cumplimiento_rentabilidad_cm5_ad2, # rentabilidad_antes_de_impuestos_ad2,
    cumplimiento_rentabilidad_cm5_ad3, # rentabilidad_antes_de_impuestos_ad3,
    cumplimiento_rentabilidad_cm5_ad4, # rentabilidad_antes_de_impuestos_ad4,
    cumplimiento_rentabilidad_cm5_ad5, # rentabilidad_antes_de_impuestos_ad5,
    cumplimiento_venta_previo_notas_credito_tp, # venta_credito_tp,
    cumplimiento_venta_servicios_mtto,
    # 2022
    venta_cobrada_braun_cluster,
    venta_cobrada_portafolio_cluster_sub, 
    venta_facturada_portafolio_cluster, 
    venta_cobrada_portafolio_pais_ve,
    venta_facturada_portafolio_zona_sub_ve,
    venta_cobrada_portafolio_zona_ve,
    venta_cobrada_portafolio_cluster_ve,
    venta_facturada_portafolio_cluster_sub_ve,
    venta_cobrada_zona_hospitalario_sub_gc,
    venta_cobrada_zona_hospitalario_sub_gd,
    venta_facturada_pais_ch,
    venta_facturada_zona_ch,
    rentabilidad_zona_hospitalario_cm2,
    venta_facturada_braun_pais,
    rentabilidad_compania_cm3,
    venta_cobrada_grupo_clientes_kams_sub,
    venta_facturada_grupo_clientes_kams,
    kpis_surgical,
    #2023
    venta_cobrada_portafolio_zona_cluster_sub,
    venta_facturada_portafolio_zona_cluster,
    venta_facturada_zona_pais,
    venta_cobrada_zona_pais_sub,
    venta_facturada_portafolio_renal,
    #2024
    venta_facturada_portafolio_renal_sub,
    venta_facturada_portafolio_zona_cluster_sub,
    venta_facturada_zona_pais_sub,
    venta_facturada_grupo_clientes_kams_sub,
    venta_cobrada_portafolio_cluster,    #2024  VentaCobradaPortafolioClusterSub -> VentaCobradaPortafolioCluster
    venta_facturada_portafolio_cluster_sub, #2024 VentaFacturadaPortafolioCluster -> VentaFacturadaPortafolioClusterSub
    venta_cobrada_portafolio_zona_cluster,
    cumplimiento_facturada_por_division_sub_gd,
    venta_facturada_braun_pais_sub,
    rentabilidad_unidad_negocio_cm2_cogs_sub,
    ebit_cogs_pais,
    ebit_cogs_pais_sub,
    rentabilidad_division_cm2_cogs_sub
])
maestra_resultados['Contexto'] = maestra_resultados['Contexto'].astype('str')
maestra_resultados.drop_duplicates(['Contexto', 'Fecha', 'Variable'], inplace=True)
maestra_resultados.head()


# In[ ]:





# In[ ]:





# In[329]:


maestra_resultados[maestra_resultados['Contexto'].str.contains('1098612757')]


# In[330]:


maestra_resultados.to_excel('maestra_resultados_.xlsx')


# In[331]:


maestra_resultados[maestra_resultados['Variable'] == "RentabilidadUnidadNegocioCM2COGSSUB"]


# In[332]:


maestra_resultados[ maestra_resultados['Contexto'].str.contains('1111117')]


# In[333]:


maestra_resultados[ maestra_resultados['Contexto'].str.contains('gd_3_5')]


# In[334]:


maestra_resultados[
    (maestra_resultados['Contexto'] == '1111128')
].head()


# ### Cálculo de Maestra de Resultados Centro Costo

# In[335]:


venta_cobrada_zona_pais_sub_centro_costo.head()


# In[336]:


venta_cobrada_portafolio_cluster_sub_centro_costo.head()


# In[337]:


venta_cobrada_portafolio_zona_cluster_sub_centro_costo.head()


# In[338]:


cumplimiento_recaudo_por_division_sub_mk_centro_costo.info()


# In[339]:


maestra_resultados_centro_costo = pd.concat([
    #2023
    venta_cobrada_zona_pais_sub_centro_costo,
    cumplimiento_recaudo_por_division_sub_mk_centro_costo, 
    cumplimiento_recaudo_por_division_sub_gd_centro_costo,
    cumplimiento_recaudo_por_unidad_negocio_gu_sub_centro_costo, 
    cumplimiento_recaudo_por_unidad_negocio_mk_sub_centro_costo,
    cumplimiento_venta_por_unidad_negocio_gu_sub_centro_costo, 
    cumplimiento_venta_por_unidad_negocio_mk_sub_centro_costo,
    cumplimiento_rentabilidad_por_unidad_negocio_cm3_gu_centro_costo, 
    cumplimiento_rentabilidad_por_unidad_negocio_cm3_mk_centro_costo,
    cumplimiento_rentabilidad_por_unidad_negocio_cm2_gu_centro_costo, 
    cumplimiento_rentabilidad_por_unidad_negocio_cm2_mk_centro_costo,
    venta_cobrada_portafolio_cluster_sub_centro_costo,
    venta_cobrada_portafolio_zona_cluster_sub_centro_costo
])
maestra_resultados_centro_costo
maestra_resultados_centro_costo['PorcentajeCumplimiento'] = maestra_resultados_centro_costo['Real']/maestra_resultados_centro_costo['Presupuesto']
# maestra_resultados['Contexto'] = maestra_resultados['Contexto'].astype('str')
# maestra_resultados.drop_duplicates(['Contexto', 'Fecha', 'Variable'], inplace=True)
# maestra_resultados.head()


# In[340]:


df_parrillas[df_parrillas['Variable'] == '1032450706']


# In[341]:


maestra_resultados_centro_costo[maestra_resultados_centro_costo['Contexto'].str.contains('mk_9_3')]


# In[342]:


maestra_resultados_centro_costo.to_excel('maestra_resultados_centro_costo_.xlsx')


# In[343]:


maestra_resultados_centro_costo[
    
    pd.notna(maestra_resultados_centro_costo['AreaCalculo'])
]


# In[ ]:





# In[344]:


maestra_resultados_centro_costo[maestra_resultados_centro_costo['Contexto'].str.contains('1032450706')]


# In[345]:


maestra_resultados[maestra_resultados['Variable'] == 'EBITCOGSPaisSUB']


# In[346]:


df_parrillas[df_parrillas['Variable'] == 'EBITCOGSPaisSUB']


# In[ ]:





# In[ ]:





# In[ ]:




