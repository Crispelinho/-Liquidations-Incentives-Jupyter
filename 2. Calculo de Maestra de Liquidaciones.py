#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
import import_ipynb
from shared_functions import DataframeLoader, Shared
from libraries.DataframesUtils import CalculoDataframes
base_dir = 'C:\\Users\\Crisp\\Documents\\QualitySoftGroup\\Proyectos\\Liquidations-Incentives-Jupyter'
sub_direct = 'archivos-fuente-138(3)'
folder_bbraun_source = os.path.join(base_dir, 'source', sub_direct)


# In[ ]:


dataframe_loader = DataframeLoader(
    base_dir=base_dir,
    sub_direct=sub_direct
)
shared = Shared()
print(dataframe_loader.cargar_dataframes)


# In[ ]:


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


# In[ ]:


fecha_liquidacion


# In[ ]:


# maestra_resultados.to_excel('maestra_resultados_.xlsx')


# In[ ]:


maestra_resultados = pd.read_excel(
        io=os.path.join('maestra_resultados_.xlsx'),
        dtype={
            'Contexto': str,
            'Fecha': 'datetime64[ns]',
            'PorcentajeCumplimiento':float ,
            'Variable': str,
            'Real': float,
            'Presupuesto': float
        }
    )
maestra_resultados


# In[ ]:


# maestra_resultados_centro_costo.to_excel('maestra_resultados_centro_costo.xlsx')


# In[ ]:


maestra_resultados_centro_costo = pd.read_excel(
        io=os.path.join('maestra_resultados_centro_costo_.xlsx'),
        dtype={
            'Fecha': 'datetime64[ns]',
            'GrupoProducto': str,
            'Division': str,
            'CodigoCentroCosto': str,
            'Real': float,
            'Presupuesto': float ,
            'Variable': str,
            'AreaCalculo': str,
            'Consecutivo': str,            
            'TipoEmpleado': str
        }
    )

maestra_resultados_centro_costo


# In[ ]:


maestra_resultados_centro_costo['AreaCalculo'].fillna(0, inplace=True)
maestra_resultados_centro_costo['AreaCalculo'] = maestra_resultados_centro_costo['AreaCalculo'].astype(int)
maestra_resultados_centro_costo


# In[ ]:


maestra_resultados_centro_costo[maestra_resultados_centro_costo['Variable'] == 'RentabilidadUnidadNegocioCM2COGSSUB']


# ## Euros

# In[ ]:


def convertir_a_euros(main):
    # Definir las tasas de cambio
    trm_marzo_2024 = 4253.92
    trm_abril_2024 = 4226.47
    trm_mayo_2024 = 4217.04
    trm_promedio_2024 = 4652.35

    # Crear una copia del DataFrame para evitar el SettingWithCopyWarning
    main = main.copy()

    # Convertir los valores de 'Real' y 'Presupuesto' a euros
    main['RealEuros'] = main['Real'] / trm_mayo_2024
    main['PresupuestoEuros'] = main['Presupuesto'] / trm_promedio_2024
    main['PorcentajeCumplimientoEuros'] = (main['RealEuros']/main['PresupuestoEuros']).apply(lambda x: round(x, 2))

    # Renombrar las columnas originales 'Real' y 'Presupuesto'
    main = main.rename(columns={
        'Real': 'RealCOP', 'Presupuesto': 'PresupuestoCOP', 
        'PorcentajeCumplimiento': 'PorcentajeCumplimientoCOP',
        'PorcentajeCumplimientoEuros': 'PorcentajeCumplimiento'
    })

    # Renombrar las nuevas columnas para reflejar que están en euros
    main = main.rename(columns={'RealEuros': 'Real', 'PresupuestoEuros': 'Presupuesto'})
    return main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento', 'RealCOP', 'PresupuestoCOP', 'PorcentajeCumplimientoCOP']]


# In[ ]:


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


# variables_convertir_euros = ['VentaFacturadaBraunPais', 'VentaFacturadaZonaPaisSub', 'VentaFacturadaDivisionSub', 'VentaFacturadaUnidadNegocioSub', 'VentaFacturadaGrupoClientesKAMSSub']
# variables_convertir_euros


# In[ ]:


# maestra_resultados_aplicar_euros = maestra_resultados[maestra_resultados['Variable'].isin(variables_convertir_euros)]
# maestra_resultados_aplicar_euros


# In[ ]:


# maestra_resultados[maestra_resultados['Variable'] == 'VentaFacturadaBraunPais']


# In[ ]:


# maestra_resultados_convertido_euros = maestra_resultados.copy()

# maestra_resultados_convertido_euros['RealCOP'] = 0
# maestra_resultados_convertido_euros['PresupuestoCOP'] = 0
# maestra_resultados_convertido_euros['PorcentajeCumplimientoCOP'] = 0

# maestra_resultados_convertido_euros.update(convertir_a_euros(maestra_resultados_aplicar_euros))
# maestra_resultados_convertido_euros


# In[ ]:


# maestra_resultados_convertido_euros[maestra_resultados_convertido_euros['Variable'] == 'VentaFacturadaBraunPais']


# In[ ]:


# # Renombrar las nuevas columnas para reflejar que están en euros
# main = main.rename(columns={'RealEuros': 'Real', 'PresupuestoEuros': 'Presupuesto'})


# In[ ]:


# # Renombrar las columnas originales 'Real' y 'Presupuesto'
# main = main.rename(columns={
#     'Real': 'RealCOP', 'Presupuesto': 'PresupuestoCOP', 
#     'PorcentajeCumplimiento': 'PorcentajeCumplimientoCOP',
#     'PorcentajeCumplimientoEuros': 'PorcentajeCumplimiento'
# })


# In[ ]:


# maestra_resultados_convertidos = maestra_resultados.drop(columns=['Unnamed: 0']).merge(
#     maestra_resultados_convertido_euros,
#     on = ['Contexto', 'Variable', 'Fecha'],
#     how='left'
# )
# maestra_resultados_convertidos


# In[ ]:


# maestra_resultados_convertidos[maestra_resultados_convertidos['Variable'] == 'VentaFacturadaBraunPais']


# In[ ]:


# maestra_resultados = maestra_resultados_convertido_euros.copy()
# maestra_resultados


# ## Se recalculan los porcentajes de cumplimiento para darle manejo a negativos

# In[ ]:


df_parrillas[df_parrillas['Variable'] == 'VentaCobradaBraunPais']


# In[ ]:


calculo_dataframe = CalculoDataframes()

variables_convertir_euros = ['VentaFacturadaBraunPais', 'VentaFacturadaZonaPaisSub', 'VentaFacturadaDivisionSub', 'VentaFacturadaUnidadNegocioSub', 'VentaFacturadaGrupoClientesKAMSSub']
# Convertir las variables a euros en 'maestra_resultados'
maestra_resultados = calculo_dataframe.convertir_valores_a_euros(maestra_resultados, variables_convertir_euros)

# Convertir las variables a euros en 'maestra_resultados_centro_costo'
maestra_resultados_centro_costo = calculo_dataframe.convertir_valores_a_euros(maestra_resultados_centro_costo, variables_convertir_euros)

# ## Se recalculan los porcentajes de cumplimiento para darle manejo a negativos


# In[ ]:


def recalcular_porcentajes_cumplimiento(row):
    response = row['PorcentajeCumplimiento']
    if not pd.isnull(row['Real']) and not pd.isnull(row['Presupuesto']):
        if row['Presupuesto'] == 0:
            response = 0
        elif row['Real'] - row['Presupuesto'] < 0:
            response = 1 + ((row['Real'] - row['Presupuesto']) / abs(row['Presupuesto']))
        elif row['Real'] - row['Presupuesto'] > 0:
            response = 1 + abs((row['Real'] - row['Presupuesto']) / abs(row['Presupuesto']))
    return round(response, 2)

maestra_resultados['PorcentajeCumplimiento'] = maestra_resultados.apply(recalcular_porcentajes_cumplimiento, axis=1)
maestra_resultados


# ### Se calculan los porcentajes de cumplimiento para Maestra de Centro de Costos

# In[ ]:


# maestra_resultados_centro_costo['PorcentajeCumplimiento'] = 0
maestra_resultados_centro_costo['PorcentajeCumplimiento'] = maestra_resultados_centro_costo.apply(recalcular_porcentajes_cumplimiento, axis=1)
# maestra_resultados_centro_costo.drop(columns={'AreaCalculo', 'Consecutivo', 'TipoEmpleado'})


# ## Se une la maestra de resultados con la parrilla

# In[ ]:


maestra_resultados[
     (maestra_resultados['Fecha']=='2024-05-01') &
    (maestra_resultados['Contexto']=='1111117')
]


# In[ ]:


# df_parrillas[df_parrillas['Variable'] == 'RentabilidadDivisionCM2COGsSub']


# In[ ]:


df_parrillas_tipos_calculos.head()


# In[ ]:


df_parrillas[df_parrillas['CodigoEmpleado'] == '5004523']


# In[ ]:





# In[ ]:


maestra_resultados[maestra_resultados['Variable'] == 'VentaCobradaBraunPais']


# In[ ]:


maestra_resultados_centro_costo[maestra_resultados_centro_costo['Variable'] == 'VentaFacturadaPortafolioClusterSub']


# In[ ]:


df_parrillas[
     (df_parrillas['Variable'] == 'RentabilidadUnidadNegocioCM2COGSSUB') 
#     (df_parrillas['CodigoEmpleado'] == '5036358')
]


# In[ ]:


maestra_resultados[maestra_resultados['Contexto'] == '5002711_1'].merge(
    df_parrillas[df_parrillas['Contexto'] == '5002711_1'],
    on=['Variable', 'Contexto'],
    how='left'
)


# ### Maestra de Resultados por Empleado

# In[ ]:


maestra_resultados_por_empleado = maestra_resultados.merge(
    df_parrillas,
    how='left',
    indicator=True
)
maestra_resultados_por_empleado


# In[ ]:


maestra_resultados_por_empleado[
     (maestra_resultados_por_empleado['Fecha']=='2024-05-01') &
    (maestra_resultados_por_empleado['Contexto']=='1111117')
]


# In[ ]:


df_empleados[df_empleados['CodigoEmpleado'] == '1111117']


# In[ ]:


maestra_resultados_por_empleado[maestra_resultados_por_empleado['Variable'] == 'VentaFacturadaGrupoClientesKAMSSub']


# In[ ]:


maestra_resultados_por_empleado = maestra_resultados_por_empleado.merge(
    df_empleados[['CodigoEmpleado', 'TipoEmpleado', 'AreaCalculo', 'Apellidos', 'Nombre', 'FechaIngreso']],
    on='CodigoEmpleado',
    how='left'
)
maestra_resultados_por_empleado


# In[ ]:


maestra_resultados_por_empleado[
    (maestra_resultados_por_empleado['Variable'] == 'ObjetivosCualitativos') &
    (maestra_resultados_por_empleado['Contexto'] == '5004293')
]


# In[ ]:


# Se añaden líneas para filtrar registros vacíos ('Fecha' y 'CodigoEmpleado')
maestra_resultados_por_empleado.dropna(subset=[
        'Fecha'
    ], inplace=True)
maestra_resultados_por_empleado['CodigoEmpleado'] = maestra_resultados_por_empleado['CodigoEmpleado'].astype(str)
maestra_resultados_por_empleado


# In[ ]:


df_salarios_variables[df_salarios_variables['CodigoEmpleado'] == '16659046']


# In[ ]:


maestra_resultados_por_empleado[maestra_resultados_por_empleado['CodigoEmpleado'] == '16659046']


# In[ ]:


# TODO Hacer el merge con la tabla de promedio variable
maestra_resultados_por_empleado = maestra_resultados_por_empleado.merge(
    df_salarios_variables[['CodigoEmpleado', 'Fecha', 'PromedioSalarioVariable']].rename(
        columns={'PromedioSalarioVariable': 'SalarioVariable'}
    ),
    on=['CodigoEmpleado', 'Fecha'],
    how='left'
)
maestra_resultados_por_empleado


# In[ ]:


maestra_resultados_por_empleado[
     (maestra_resultados_por_empleado['Fecha']=='2024-05-01') &
    (maestra_resultados_por_empleado['Contexto']=='1111117')
]


# ### Maestra de Resultados por Empleado y Centro de Costos

# In[ ]:


maestra_resultados_centro_costo.head()


# In[ ]:


df_parrillas


# In[ ]:


maestra_resultados_por_empleado_centro_costo = maestra_resultados_centro_costo.drop(columns={'AreaCalculo', 'Consecutivo', 'TipoEmpleado', 'Unnamed: 0'})


# In[ ]:


maestra_resultados_por_empleado_centro_costo = maestra_resultados_por_empleado_centro_costo.merge(
    df_parrillas,
    on=['Contexto', 'Variable'],
    how='left'
)
maestra_resultados_por_empleado_centro_costo.head()


# In[ ]:


maestra_resultados_por_empleado_centro_costo[
    (maestra_resultados_por_empleado_centro_costo['CodigoEmpleado'] == '5002899')
]   


# In[ ]:


maestra_resultados_por_empleado_centro_costo[
    (maestra_resultados_por_empleado_centro_costo['CodigoEmpleado'] == '1032450706') &
    (maestra_resultados_por_empleado_centro_costo['Fecha'] == '2023-01-01')
]


# In[ ]:


# maestra_resultados_por_empleado_centro_costo = maestra_resultados_centro_costo.drop(columns=['Unnamed: 0' , 'Consecutivo']).merge(
#     df_parrillas_tipos_calculos,
#     on=['TipoEmpleado', 'AreaCalculo'],
#     how='left'
# )
# maestra_resultados_por_empleado_centro_costo.head()


# In[ ]:


# maestra_resultados_por_empleado_centro_costo = maestra_resultados_por_empleado_centro_costo.merge(
#     df_empleados_centros_costos.rename(columns={'PorcentajeAsignacion': 'Porcentaje'})
# )


# In[ ]:


# maestra_resultados_por_empleado_centro_costo[maestra_resultados_por_empleado_centro_costo['CodigoEmpleado'] == '1032450706']


# In[ ]:


df_empleados[['CodigoEmpleado', 'TipoEmpleado', 'AreaCalculo', 'Apellidos', 'Nombre', 'FechaIngreso', 'FechaRetiro']][df_empleados['CodigoEmpleado'] == '5005463']


# In[ ]:


df_empleados[['CodigoEmpleado', 'TipoEmpleado', 'AreaCalculo', 'Apellidos', 'Nombre', 'FechaIngreso', 'FechaRetiro']][df_empleados['CodigoEmpleado'] == '1098612757']


# In[ ]:


df_empleados_inactivos[['CodigoEmpleado', 'TipoEmpleado', 'AreaCalculo', 'Apellidos', 'Nombre', 'FechaIngreso', 'FechaRetiro']][df_empleados_inactivos['CodigoEmpleado'] == '1032450706']


# In[ ]:


df_empleados_inactivos[['CodigoEmpleado', 'TipoEmpleado', 'AreaCalculo', 'Apellidos', 'Nombre', 'FechaIngreso', 'FechaRetiro']][df_empleados_inactivos['CodigoEmpleado'] == '5005463']


# In[ ]:


df_empleados_totales = pd.concat([df_empleados, df_empleados_inactivos])


# In[ ]:


maestra_resultados_por_empleado_centro_costo = maestra_resultados_por_empleado_centro_costo.merge(
    df_empleados_totales[['CodigoEmpleado', 'TipoEmpleado', 'AreaCalculo', 'Apellidos', 'Nombre', 'FechaIngreso']],
    on=['CodigoEmpleado'],
    how='left'
)
maestra_resultados_por_empleado_centro_costo.head()


# In[ ]:


# Se añaden líneas para filtrar registros vacíos ('Fecha' y 'CodigoEmpleado')
maestra_resultados_por_empleado_centro_costo.dropna(subset=[
        'Fecha'
    ], inplace=True)
maestra_resultados_por_empleado_centro_costo['CodigoEmpleado'] = maestra_resultados_por_empleado_centro_costo['CodigoEmpleado'].astype(str)
maestra_resultados_por_empleado_centro_costo.head()


# In[ ]:


# TODO Hacer el merge con la tabla de promedio variable
maestra_resultados_por_empleado_centro_costo = maestra_resultados_por_empleado_centro_costo.merge(
    df_salarios_variables[['CodigoEmpleado', 'Fecha', 'PromedioSalarioVariable']].rename(
        columns={'PromedioSalarioVariable': 'SalarioVariable'}
    ),
    on=['CodigoEmpleado', 'Fecha'],
    how='left'
)
maestra_resultados_por_empleado_centro_costo.head()


# In[ ]:


maestra_resultados_por_empleado[
     (maestra_resultados_por_empleado['Fecha']=='2024-05-01') &
    (maestra_resultados_por_empleado['Contexto']=='1111117') &
    (maestra_resultados_por_empleado['Variable']=='ObjetivosCualitativos')
]


# In[ ]:


maestra_resultados_por_empleado_centro_costo[maestra_resultados_por_empleado_centro_costo['CodigoEmpleado'] == '1032450706']


# ### Calculo sobre maestra resultados por empleado

# In[ ]:


maestra_resultados_por_empleado = maestra_resultados_por_empleado.merge(
    df_factores_liquidacion,
    on=['TipoEmpleado', 'PorcentajeCumplimiento'],
    how='left'
)

maestra_resultados_por_empleado['FactorIncentivo'] = np.where(
    ((maestra_resultados_por_empleado['Fecha'] >= '2022-08-01') & 
     (maestra_resultados_por_empleado['PorcentajeCumplimiento'] == 0) & 
     (maestra_resultados_por_empleado['Presupuesto'] == 0) & 
     (maestra_resultados_por_empleado['Real'] == 0)),
    0.25,
    maestra_resultados_por_empleado['FactorIncentivo']
)
maestra_resultados_por_empleado['FactorIncentivo'] = np.where(
    ((maestra_resultados_por_empleado['Fecha'] >= '2022-08-01') & 
     (maestra_resultados_por_empleado['PorcentajeCumplimiento'] == 0) & 
     (maestra_resultados_por_empleado['Presupuesto'] == 0) & 
     (maestra_resultados_por_empleado['Real'] > 0)),
    0.5,
    maestra_resultados_por_empleado['FactorIncentivo']
)

##
_excepciones_porcentaje_cumplimiento = [
    # {
    #     'TipoEmpleado': 'VC',
    #     'AreaCalculo': 7,
    #     'Variable': 'VentaFacturadaZonaCargoIndividualSub',
    #     'Consecutivo': 4,
    #     'FechaInicial': '2021-02-01',
    #     'FechaFinal': '2021-06-01',
    #     'PorcentajeCumplimiento': 1,
    #     'FactorIncentivo': 1,
    #     'Codigos': ['5003527']
    # }
]

for excepcion in _excepciones_porcentaje_cumplimiento:
    maestra_resultados_por_empleado['PorcentajeCumplimiento'] = np.where(
        (
            (maestra_resultados_por_empleado['CodigoEmpleado'].isin(excepcion['Codigos'])) &
            (maestra_resultados_por_empleado['Consecutivo'] == excepcion['Consecutivo']) &
            (maestra_resultados_por_empleado['Variable'] == excepcion['Variable']) &
            (maestra_resultados_por_empleado['Fecha'] >= excepcion['FechaInicial']) &
            (maestra_resultados_por_empleado['Fecha'] <= excepcion['FechaFinal'])
        ),
        excepcion['PorcentajeCumplimiento'],
        maestra_resultados_por_empleado['PorcentajeCumplimiento']
    )
    maestra_resultados_por_empleado['FactorIncentivo'] = np.where(
        (
            (maestra_resultados_por_empleado['CodigoEmpleado'].isin(excepcion['Codigos'])) &
            (maestra_resultados_por_empleado['Consecutivo'] == excepcion['Consecutivo']) &
            (maestra_resultados_por_empleado['Variable'] == excepcion['Variable']) &
            (maestra_resultados_por_empleado['Fecha'] >= excepcion['FechaInicial']) &
            (maestra_resultados_por_empleado['Fecha'] <= excepcion['FechaFinal'])
        ),
        excepcion['FactorIncentivo'],
        maestra_resultados_por_empleado['FactorIncentivo']
    )
##

maestra_resultados_por_empleado['FactorIncentivo'] = np.where(
    (maestra_resultados_por_empleado['PorcentajeCumplimiento']<0.9) & (maestra_resultados_por_empleado['FactorIncentivo'].isnull()),
    0,
    maestra_resultados_por_empleado['FactorIncentivo']
)
maestra_resultados_por_empleado['FactorIncentivo'] = np.where(
    (maestra_resultados_por_empleado['PorcentajeCumplimiento']>1.06) & (maestra_resultados_por_empleado['FactorIncentivo'].isnull()),
    2.5,
    maestra_resultados_por_empleado['FactorIncentivo']
)
maestra_resultados_por_empleado = maestra_resultados_por_empleado[maestra_resultados_por_empleado['Fecha'].notnull()]
maestra_resultados_por_empleado['FactorMes'] = pd.DatetimeIndex(maestra_resultados_por_empleado['Fecha']).month
maestra_resultados_por_empleado['FactorMes'] = maestra_resultados_por_empleado['FactorMes'].astype('int')
maestra_resultados_por_empleado['Liquidado'] = maestra_resultados_por_empleado['SalarioVariable']*maestra_resultados_por_empleado['Porcentaje']
maestra_resultados_por_empleado['Liquidado'] = maestra_resultados_por_empleado['Liquidado']*maestra_resultados_por_empleado['FactorIncentivo']

try:
    maestra_resultados_por_empleado['DifMonths'] = ((maestra_resultados_por_empleado.Fecha - maestra_resultados_por_empleado.FechaIngreso)/np.timedelta64(1, 'M'))
except:
    maestra_resultados_por_empleado['DifMonths'] = 0
maestra_resultados_por_empleado['DifMonths'].fillna(0, inplace=True)
maestra_resultados_por_empleado['DifMonths'] = maestra_resultados_por_empleado['DifMonths'].apply(lambda x: round(x, 0))
maestra_resultados_por_empleado['DifMonths'] = maestra_resultados_por_empleado['DifMonths'].astype('int')

maestra_resultados_por_empleado['DifMonths'] = maestra_resultados_por_empleado.groupby(['CodigoEmpleado'])['DifMonths'].transform(min)
maestra_resultados_por_empleado['DifMonths'] = np.where(
    maestra_resultados_por_empleado['DifMonths']>0, 0, maestra_resultados_por_empleado['DifMonths']
)
maestra_resultados_por_empleado['NuevoFactor'] = maestra_resultados_por_empleado['FactorMes'] + maestra_resultados_por_empleado['DifMonths']
maestra_resultados_por_empleado['NuevoFactor'] = maestra_resultados_por_empleado['NuevoFactor'].apply(lambda x: 0 if x<0 else x)

###
_excepciones_factor_mes = [
    # Garantizados para VEs y VCs
    {
        'TipoEmpleado': ['VC', 'VE'],
        'FechaInicial': '2022-03-01',
        'FechaFinal': '2022-12-01',
        'SumFactorMes': -2
    }
]
for excepcion in _excepciones_factor_mes:
    maestra_resultados_por_empleado['NuevoFactor'] = np.where(
        (
            (maestra_resultados_por_empleado['TipoEmpleado'].isin(excepcion['TipoEmpleado'])) &
            (maestra_resultados_por_empleado['Fecha'] >= excepcion['FechaInicial']) &
            (maestra_resultados_por_empleado['Fecha'] <= excepcion['FechaFinal'])
        ),
        maestra_resultados_por_empleado['NuevoFactor'] + excepcion['SumFactorMes'],
        maestra_resultados_por_empleado['NuevoFactor']
    )

maestra_resultados_por_empleado['NuevoFactor'] = np.where(
    maestra_resultados_por_empleado['NuevoFactor'] < 1,
    1,
    maestra_resultados_por_empleado['NuevoFactor']
)
maestra_resultados_por_empleado['NuevoFactorBK'] = maestra_resultados_por_empleado['NuevoFactor']
###

maestra_resultados_por_empleado['Liquidado'] = maestra_resultados_por_empleado['Liquidado']*maestra_resultados_por_empleado['NuevoFactor']

maestra_resultados_por_empleado = maestra_resultados_por_empleado[maestra_resultados_por_empleado['CodigoEmpleado'].notnull()]


# In[ ]:


maestra_resultados_por_empleado


# In[ ]:


maestra_resultados_por_empleado[
     (maestra_resultados_por_empleado['Fecha']=='2024-05-01') &
    (maestra_resultados_por_empleado['Contexto']=='291169471')
]['Liquidado']


# ### Calculo sobre maestra resultados por empleado centro costo

# In[ ]:





# In[ ]:


maestra_resultados_por_empleado_centro_costo_final = maestra_resultados_por_empleado_centro_costo.merge(
    df_factores_liquidacion,
    on=['TipoEmpleado', 'PorcentajeCumplimiento'],
    how='left'
)

maestra_resultados_por_empleado_centro_costo_final['FactorIncentivo'] = np.where(
    ((maestra_resultados_por_empleado_centro_costo_final['Fecha'] >= '2022-08-01') & 
     (maestra_resultados_por_empleado_centro_costo_final['PorcentajeCumplimiento'] == 0) & 
     (maestra_resultados_por_empleado_centro_costo_final['Presupuesto'] == 0) & 
     (maestra_resultados_por_empleado_centro_costo_final['Real'] == 0)),
    0.25,
    maestra_resultados_por_empleado_centro_costo_final['FactorIncentivo']
)
maestra_resultados_por_empleado_centro_costo_final['FactorIncentivo'] = np.where(
    ((maestra_resultados_por_empleado_centro_costo_final['Fecha'] >= '2022-08-01') & 
     (maestra_resultados_por_empleado_centro_costo_final['PorcentajeCumplimiento'] == 0) & 
     (maestra_resultados_por_empleado_centro_costo_final['Presupuesto'] == 0) & 
     (maestra_resultados_por_empleado_centro_costo_final['Real'] > 0)),
    0.5,
    maestra_resultados_por_empleado_centro_costo_final['FactorIncentivo']
)

##
_excepciones_porcentaje_cumplimiento = [
    # {
    #     'TipoEmpleado': 'VC',
    #     'AreaCalculo': 7,
    #     'Variable': 'VentaFacturadaZonaCargoIndividualSub',
    #     'Consecutivo': 4,
    #     'FechaInicial': '2021-02-01',
    #     'FechaFinal': '2021-06-01',
    #     'PorcentajeCumplimiento': 1,
    #     'FactorIncentivo': 1,
    #     'Codigos': ['5003527']
    # }
]

for excepcion in _excepciones_porcentaje_cumplimiento:
    maestra_resultados_por_empleado_centro_costo_final['PorcentajeCumplimiento'] = np.where(
        (
            (maestra_resultados_por_empleado_centro_costo_final['CodigoEmpleado'].isin(excepcion['Codigos'])) &
            (maestra_resultados_por_empleado_centro_costo_final['Consecutivo'] == excepcion['Consecutivo']) &
            (maestra_resultados_por_empleado_centro_costo_final['Variable'] == excepcion['Variable']) &
            (maestra_resultados_por_empleado_centro_costo_final['Fecha'] >= excepcion['FechaInicial']) &
            (maestra_resultados_por_empleado_centro_costo_final['Fecha'] <= excepcion['FechaFinal'])
        ),
        excepcion['PorcentajeCumplimiento'],
        maestra_resultados_por_empleado_centro_costo_final['PorcentajeCumplimiento']
    )
    maestra_resultados_por_empleado_centro_costo_final['FactorIncentivo'] = np.where(
        (
            (maestra_resultados_por_empleado_centro_costo_final['CodigoEmpleado'].isin(excepcion['Codigos'])) &
            (maestra_resultados_por_empleado_centro_costo_final['Consecutivo'] == excepcion['Consecutivo']) &
            (maestra_resultados_por_empleado_centro_costo_final['Variable'] == excepcion['Variable']) &
            (maestra_resultados_por_empleado_centro_costo_final['Fecha'] >= excepcion['FechaInicial']) &
            (maestra_resultados_por_empleado_centro_costo_final['Fecha'] <= excepcion['FechaFinal'])
        ),
        excepcion['FactorIncentivo'],
        maestra_resultados_por_empleado_centro_costo_final['FactorIncentivo']
    )
##

maestra_resultados_por_empleado_centro_costo_final['FactorIncentivo'] = np.where(
    (maestra_resultados_por_empleado_centro_costo_final['PorcentajeCumplimiento']<0.9) & (maestra_resultados_por_empleado_centro_costo_final['FactorIncentivo'].isnull()),
    0,
    maestra_resultados_por_empleado_centro_costo_final['FactorIncentivo']
)
maestra_resultados_por_empleado_centro_costo_final['FactorIncentivo'] = np.where(
    (maestra_resultados_por_empleado_centro_costo_final['PorcentajeCumplimiento']>1.06) & (maestra_resultados_por_empleado_centro_costo_final['FactorIncentivo'].isnull()),
    2.5,
    maestra_resultados_por_empleado_centro_costo_final['FactorIncentivo']
)
maestra_resultados_por_empleado_centro_costo_final = maestra_resultados_por_empleado_centro_costo_final[maestra_resultados_por_empleado_centro_costo_final['Fecha'].notnull()]
maestra_resultados_por_empleado_centro_costo_final['FactorMes'] = pd.DatetimeIndex(maestra_resultados_por_empleado_centro_costo_final['Fecha']).month
maestra_resultados_por_empleado_centro_costo_final['FactorMes'] = maestra_resultados_por_empleado_centro_costo_final['FactorMes'].astype('int')
maestra_resultados_por_empleado_centro_costo_final['Liquidado'] = maestra_resultados_por_empleado_centro_costo_final['SalarioVariable']*maestra_resultados_por_empleado_centro_costo_final['Porcentaje']
maestra_resultados_por_empleado_centro_costo_final['Liquidado'] = maestra_resultados_por_empleado_centro_costo_final['Liquidado']*maestra_resultados_por_empleado_centro_costo_final['FactorIncentivo']

try:
    maestra_resultados_por_empleado_centro_costo_final['DifMonths'] = ((maestra_resultados_por_empleado_centro_costo_final.Fecha - maestra_resultados_por_empleado_centro_costo_final.FechaIngreso)/np.timedelta64(1, 'M'))
except:
    maestra_resultados_por_empleado_centro_costo_final['DifMonths'] = 0
maestra_resultados_por_empleado_centro_costo_final['DifMonths'].fillna(0, inplace=True)
maestra_resultados_por_empleado_centro_costo_final['DifMonths'] = maestra_resultados_por_empleado_centro_costo_final['DifMonths'].apply(lambda x: round(x, 0))
maestra_resultados_por_empleado_centro_costo_final['DifMonths'] = maestra_resultados_por_empleado_centro_costo_final['DifMonths'].astype('int')

maestra_resultados_por_empleado_centro_costo_final['DifMonths'] = maestra_resultados_por_empleado_centro_costo_final.groupby(['CodigoEmpleado'])['DifMonths'].transform(min)
maestra_resultados_por_empleado_centro_costo_final['DifMonths'] = np.where(
    maestra_resultados_por_empleado_centro_costo_final['DifMonths']>0, 0, maestra_resultados_por_empleado_centro_costo_final['DifMonths']
)
maestra_resultados_por_empleado_centro_costo_final['NuevoFactor'] = maestra_resultados_por_empleado_centro_costo_final['FactorMes'] + maestra_resultados_por_empleado_centro_costo_final['DifMonths']
maestra_resultados_por_empleado_centro_costo_final['NuevoFactor'] = maestra_resultados_por_empleado_centro_costo_final['NuevoFactor'].apply(lambda x: 0 if x<0 else x)

###
_excepciones_factor_mes = [
    # Garantizados para VEs y VCs
    {
        'TipoEmpleado': ['VC', 'VE'],
        'FechaInicial': '2022-03-01',
        'FechaFinal': '2022-12-01',
        'SumFactorMes': -2
    }
]
for excepcion in _excepciones_factor_mes:
    maestra_resultados_por_empleado_centro_costo_final['NuevoFactor'] = np.where(
        (
            (maestra_resultados_por_empleado_centro_costo_final['TipoEmpleado'].isin(excepcion['TipoEmpleado'])) &
            (maestra_resultados_por_empleado_centro_costo_final['Fecha'] >= excepcion['FechaInicial']) &
            (maestra_resultados_por_empleado_centro_costo_final['Fecha'] <= excepcion['FechaFinal'])
        ),
        maestra_resultados_por_empleado_centro_costo_final['NuevoFactor'] + excepcion['SumFactorMes'],
        maestra_resultados_por_empleado_centro_costo_final['NuevoFactor']
    )

maestra_resultados_por_empleado_centro_costo_final['NuevoFactor'] = np.where(
    maestra_resultados_por_empleado_centro_costo_final['NuevoFactor'] < 1,
    1,
    maestra_resultados_por_empleado_centro_costo_final['NuevoFactor']
)
maestra_resultados_por_empleado_centro_costo_final['NuevoFactorBK'] = maestra_resultados_por_empleado_centro_costo_final['NuevoFactor']
###

maestra_resultados_por_empleado_centro_costo_final['Liquidado'] = maestra_resultados_por_empleado_centro_costo_final['Liquidado']*maestra_resultados_por_empleado_centro_costo_final['NuevoFactor']

maestra_resultados_por_empleado_centro_costo_final = maestra_resultados_por_empleado_centro_costo_final[maestra_resultados_por_empleado_centro_costo_final['CodigoEmpleado'].notnull()]


# In[ ]:


maestra_resultados_por_empleado_centro_costo_final.head()


# In[ ]:


maestra_resultados_por_empleado_centro_costo_final[maestra_resultados_por_empleado_centro_costo_final['CodigoEmpleado'] == '1098612757']


# ## Se calculan los resultados por mes

# In[ ]:


lista_variables_rentabilidad_acumuladas = [
    'RentabilidadBbraunPaisAntesDeImpuestos',
    'RentabilidadDivisionGrupoClientesCM3',
    'RentabilidadDivisionGrupoClientesCM3Sub',
    'RentabilidadUnidadNegocioCM2',
    'RentabilidadUnidadNegocioCM2Sub',
    'RentabilidadUnidadNegocioCM3',
    'RentabilidadUnidadNegocioCM3Sub',
    'TotalRentabilidadBraunAntesDeImpuestosCM5',
    'VentaFacturadaPrevioNotasCreditoTP',
    # 2022
    'RentabilidadCompaniaCM3',
    'RentabilidadZonaHospitalarioCM2',
    # 2024
    'RentabilidadUnidadNegocioCM2COGSSUB',
    'RentabilidadDivisionCM2COGsSub',
    'EBITCOGSPais',
    'EBITCOGSPaisSUB'
]

def calcular_resultado_mes(group):
    valor_previo_real = 0
    valor_previo_presupuesto = 0
    for index, item in group.iterrows():
        if item['Variable'] in lista_variables_rentabilidad_acumuladas:
            resultado_real_mes = item['Real']
            resultado_presupuesto_mes = item['Presupuesto']
        else:
            resultado_real_mes = item['Real'] - valor_previo_real
            resultado_presupuesto_mes = item['Presupuesto'] - valor_previo_presupuesto
        
        group.at[index, 'ResultadoRealMes'] = resultado_real_mes
        group.at[index, 'ResultadoPresupuesto'] = resultado_presupuesto_mes

        porcentaje_cumplimiento_mes = 0
        if resultado_presupuesto_mes == 0:
            porcentaje_cumplimiento_mes = 0
        elif resultado_real_mes - resultado_presupuesto_mes < 0:
            porcentaje_cumplimiento_mes = 1 + ((resultado_real_mes - resultado_presupuesto_mes) / abs(resultado_presupuesto_mes))
        elif resultado_real_mes - resultado_presupuesto_mes > 0:
            porcentaje_cumplimiento_mes = 1 + abs((resultado_real_mes - resultado_presupuesto_mes) / abs(resultado_presupuesto_mes))
        else:
            porcentaje_cumplimiento_mes = resultado_real_mes / resultado_presupuesto_mes

        group.at[index, 'PorcentajeCumplimientoMes'] = round(porcentaje_cumplimiento_mes, 2)
        
        valor_previo_real = item['Real']
        valor_previo_presupuesto = item['Presupuesto']
    return group


# ### Maestra Resultados por Empleado vs Resultado Detallado Histórico

# In[ ]:


maestra_resultados_por_empleado['Real'].fillna(0,inplace=True)
maestra_resultados_por_empleado['Presupuesto'].fillna(0,inplace=True)
maestra_resultados_por_empleado['ResultadoPresupuesto'] = 0
maestra_resultados_por_empleado['ResultadoRealMes'] = 0
maestra_resultados_por_empleado['PorcentajeCumplimientoMes'] = 0
maestra_resultados_por_empleado['PorcentajeCumplimientoMes'] = maestra_resultados_por_empleado['PorcentajeCumplimientoMes'].astype('float')
maestra_resultados_por_empleado = maestra_resultados_por_empleado.groupby(['CodigoEmpleado', 'Variable', 'Contexto'], group_keys=False).apply(calcular_resultado_mes)

maestra_resultados_por_empleado


# In[ ]:


maestra_resultados_por_empleado[
    (maestra_resultados_por_empleado['Contexto']=='52875549')
]


# In[ ]:


df_resultado_detallado_previo[df_resultado_detallado_previo['CodigoEmpleado'] == '52875549.0']


# In[ ]:


df_resultado_detallado_previo[
    (df_resultado_detallado_previo['Contexto']=='1111117') &
    (df_resultado_detallado_previo['Variable']=='ObjetivosCualitativos')
]


# In[ ]:


# Se une el resultadio histórico con el actual, de tal forma que al histórico se le agregan únicamenet 
# los resultados correspondientes al mes que se está liquidando
resultado_detallado_historico = df_resultado_detallado_previo.copy()
# Filtrar el DataFrame para encontrar las filas donde 'CodigoEmpleado' termina con '.0'
resultado_detallado_historico[resultado_detallado_historico['CodigoEmpleado'].astype(str).str.endswith('.0')].head()
resultado_detallado_historico['CodigoEmpleado'] = resultado_detallado_historico['CodigoEmpleado'].astype(str).str.replace(r'(\d+)\.0$', r'\1', regex=True)
resultado_detallado_historico


# In[ ]:


# resultado_detallado_historico.info()
# resultado_detallado_historico.to_excel('resultado_detallado_historico.xlsx')


# In[ ]:


resultado_detallado_historico[
    (resultado_detallado_historico['CodigoEmpleado']== '1111135.0')
]


# In[ ]:


last_date_historico = resultado_detallado_historico['Fecha'].max()
last_date_historico = None if pd.isnull(last_date_historico) else last_date_historico

try:
    maestra_resultados_por_empleado = maestra_resultados_por_empleado[
        (maestra_resultados_por_empleado['Fecha']<=fecha_liquidacion)
    ]
    if last_date_historico:
        maestra_resultados_por_empleado = maestra_resultados_por_empleado[
            maestra_resultados_por_empleado['Fecha']>last_date_historico
        ]
except:
    raise


# In[ ]:


maestra_resultados_por_empleado[
    (maestra_resultados_por_empleado['Contexto']=='1111117') &
    (maestra_resultados_por_empleado['Variable']=='ObjetivosCualitativos')
]


# In[ ]:


maestra_resultados_por_empleado = pd.concat([resultado_detallado_historico, maestra_resultados_por_empleado])
maestra_resultados_por_empleado = maestra_resultados_por_empleado.sort_values(by=['CodigoEmpleado', 'Fecha'])
maestra_resultados_por_empleado


# ### Maestra Resultados por Empleado Centro Costo vs Resultado Detallado Histórico

# In[ ]:


maestra_resultados_por_empleado_centro_costo_final['Real'].fillna(0,inplace=True)
maestra_resultados_por_empleado_centro_costo_final['Presupuesto'].fillna(0,inplace=True)
maestra_resultados_por_empleado_centro_costo_final['ResultadoPresupuesto'] = 0
maestra_resultados_por_empleado_centro_costo_final['ResultadoRealMes'] = 0
maestra_resultados_por_empleado_centro_costo_final['PorcentajeCumplimientoMes'] = 0
maestra_resultados_por_empleado_centro_costo_final['PorcentajeCumplimientoMes'] = maestra_resultados_por_empleado_centro_costo_final['PorcentajeCumplimientoMes'].astype('float')
maestra_resultados_por_empleado_centro_costo_final = maestra_resultados_por_empleado_centro_costo_final.groupby(['CodigoEmpleado', 'Variable', 'Contexto'], group_keys=False).apply(calcular_resultado_mes)

maestra_resultados_por_empleado_centro_costo_final.head()


# In[ ]:


df_resultado_detallado_previo.copy().merge(
    df_area_calculo_sba_centros_costos.rename(columns={'ConsecutivoParrilla':'Consecutivo'}),
    on=['AreaCalculo', 'Consecutivo','TipoEmpleado'],
    how='left'    
)

# Se une el resultadio histórico con el actual, de tal forma que al histórico se le agregan únicamenet 
# los resultados correspondientes al mes que se está liquidando
resultado_detallado_historico = df_resultado_detallado_previo.copy()
last_date_historico = resultado_detallado_historico['Fecha'].max()
last_date_historico = None if pd.isnull(last_date_historico) else last_date_historico

try:
    maestra_resultados_por_empleado_centro_costo_final = maestra_resultados_por_empleado_centro_costo_final[
        (maestra_resultados_por_empleado_centro_costo_final['Fecha']<=fecha_liquidacion)
    ]
    if last_date_historico:
        maestra_resultados_por_empleado_centro_costo_final = maestra_resultados_por_empleado_centro_costo_final[
            maestra_resultados_por_empleado_centro_costo_final['Fecha']>last_date_historico
        ]
except:
    raise

maestra_resultados_por_empleado_centro_costo_final = pd.concat([resultado_detallado_historico, maestra_resultados_por_empleado_centro_costo_final])
maestra_resultados_por_empleado_centro_costo_final = maestra_resultados_por_empleado_centro_costo_final.sort_values(by=['CodigoEmpleado', 'Fecha'])
maestra_resultados_por_empleado_centro_costo_final


# In[ ]:


maestra_resultados_por_empleado_centro_costo_final[maestra_resultados_por_empleado_centro_costo_final['CodigoEmpleado'] == '1098612757']


# In[ ]:


maestra_resultados_por_empleado[maestra_resultados_por_empleado['Variable'] == 'VentaFacturadaGrupoClientesKAMSSub']


# In[ ]:


maestra_resultados_por_empleado.to_excel('resultados_detallados.xlsx')


# In[ ]:


maestra_resultados_por_empleado[
    (maestra_resultados_por_empleado['CodigoEmpleado'] == '1111117') &
     (maestra_resultados_por_empleado['Variable'] == 'ObjetivosCualitativos')
]


# In[ ]:





# In[ ]:


maestra_resultados_por_empleado[
    (maestra_resultados_por_empleado['TipoEmpleado'] == 'VC') |
    (maestra_resultados_por_empleado['TipoEmpleado'] == 'VE')
]['Variable'].drop_duplicates()


# In[ ]:


maestra_resultados_por_empleado[maestra_resultados_por_empleado['Variable'] == 'RentabilidadUnidadNegocioCM2COGS']


# In[ ]:





# In[ ]:


maestra_resultados_por_empleado[
    (maestra_resultados_por_empleado['Variable'] == 'VentaFacturadaPortafolioRenalSub')
]


# ## Se obtienen las liquidaciones

# ### Creación de resultado_liquidaciones por maestra_resultados_por_empleado

# In[ ]:


excepciones_pagos_acumulados = {}

excepciones_diferencias = {}


# In[ ]:


maestra_resultados_por_empleado[
    (maestra_resultados_por_empleado['CodigoEmpleado'] == '1111135') &
    (maestra_resultados_por_empleado['Fecha'] == '2024-05-01')
][['Contexto', 'CodigoEmpleado', 'SalarioVariable', 'Fecha', 'NuevoFactorBK', 'Liquidado']]


# In[ ]:


maestra_resultados_por_empleado[
    (maestra_resultados_por_empleado['CodigoEmpleado'] == '16659046') &
    (maestra_resultados_por_empleado['Fecha'] == '2024-05-01')
][['Contexto', 'CodigoEmpleado', 'SalarioVariable', 'Fecha', 'NuevoFactorBK', 'Liquidado']]['Liquidado'].sum()


# In[ ]:


maestra_resultados_por_empleado[
    (maestra_resultados_por_empleado['CodigoEmpleado'] == '1111135') &
    (maestra_resultados_por_empleado['Fecha'] == '2024-05-01')
][['Contexto', 'CodigoEmpleado', 'SalarioVariable', 'Fecha', 'NuevoFactorBK', 'Liquidado']]['Liquidado'].sum()


# In[ ]:


df_incentivos_por_empleado[df_incentivos_por_empleado['CodigoEmpleado'] == '16659046']


# In[ ]:


df_incentivos_por_empleado[df_incentivos_por_empleado['CodigoEmpleado'] == '1111135']


# In[ ]:


df_incentivos_por_empleado[df_incentivos_por_empleado['CodigoEmpleado'] == '1111134']


# In[ ]:


df_incentivos_por_empleado[df_incentivos_por_empleado['CodigoEmpleado'] == '5005980']


# In[ ]:


resultado_liquidaciones = maestra_resultados_por_empleado.groupby(
    ['CodigoEmpleado', 'SalarioVariable', 'Fecha', 'NuevoFactorBK'],
    as_index=False
).Liquidado.agg({'Liquidado': 'sum'})
resultado_liquidaciones.rename(columns={'NuevoFactorBK': 'NuevoFactor'}, inplace=True)
resultado_liquidaciones.head()
resultado_liquidaciones = resultado_liquidaciones.merge(
    df_incentivos_por_empleado.rename(columns={'Valor': 'PagoReal'}),
    on = ['CodigoEmpleado', 'Fecha'],
    how = 'right'
)
resultado_liquidaciones.head()


# In[ ]:


resultado_liquidaciones[resultado_liquidaciones['CodigoEmpleado'] == '79539822']


# In[ ]:


resultado_liquidaciones[
    (resultado_liquidaciones['CodigoEmpleado'] == '16659046')
]


# In[ ]:


resultado_liquidaciones[
    (resultado_liquidaciones['CodigoEmpleado'] == '1111135')
]


# In[ ]:


resultado_liquidaciones = pd.merge(
    left=resultado_liquidaciones,
    right=df_empleados[['CodigoEmpleado', 'FechaIngreso']].drop_duplicates(['CodigoEmpleado']),
    how='left',
    on=['CodigoEmpleado']
)

# Filtro resultado liquidaciones SIN GARANTIZADO
resultado_liquidaciones = resultado_liquidaciones[resultado_liquidaciones['Fecha'] >= resultado_liquidaciones['FechaIngreso']]
resultado_liquidaciones.drop(columns=['FechaIngreso'], inplace=True)
resultado_liquidaciones.head()

# Cálculo de garantizado (100%) para VEs y VCs, Anticipos (85%) para el resto de empleados
resultado_liquidaciones = resultado_liquidaciones.merge(
    df_empleados[['CodigoEmpleado', 'TipoEmpleado']],
    on='CodigoEmpleado',
    how='left'
)

def calcular_liquidacion_mes(g):
    pago_acumulado = 0
    for i, row in g.iterrows():
        print("mes actual:", row['Fecha'].month)
        if row['Fecha'].month in meses_incentivos:
            print("Pago Real", g.at[i, 'PagoReal'])
            if g.at[i, 'PagoReal'] == 0:
                print("Pago Real es 0", g.at[i, 'PagoReal'])
                if i == 0:
                    print("i es igual a 0")
                    g.at[i, 'PagoReal'] = g.at[i, 'Liquidado']
                else:
                    print("i no es igual a 0")
                    g.at[i, 'PagoReal'] = g.at[i, 'Liquidado'] - pago_acumulado
                print("Pago Real actualizado", g.at[i, 'PagoReal'])
            if g.at[i, 'PagoReal'] < 0:
                g.at[i, 'PagoReal'] = 0.0
        print("pago_acumuladol_antes:", pago_acumulado)
        pago_acumulado += g.at[i, 'PagoReal']
        print("pago_acumuladol_después:", pago_acumulado)
        
        excepcion = excepciones_pagos_acumulados.get(row['CodigoEmpleado'], None)
        if excepcion and row['Fecha'] == excepcion['Fecha']:
            pago_acumulado = excepcion['ValorPagoAcumulado']
        
        excepcion_diferencia = excepciones_diferencias.get(row['CodigoEmpleado'], None)
        if excepcion_diferencia and row['Fecha'] == excepcion_diferencia['Fecha']:
            pago_acumulado = pago_acumulado + excepcion_diferencia['ValorDiferencia']
        
    return g
resultado_liquidaciones = resultado_liquidaciones.sort_values(by=['CodigoEmpleado', 'Fecha'])
resultado_liquidaciones.reset_index(inplace=True, drop=True)
resultado_liquidaciones['PagoReal'].fillna(0.0, inplace=True)
resultado_liquidaciones['Liquidado'].fillna(0.0, inplace=True)
resultado_liquidaciones


# In[ ]:


resultado_liquidaciones[resultado_liquidaciones['CodigoEmpleado'] == '16659046']


# In[ ]:


resultado_liquidaciones[
    (resultado_liquidaciones['CodigoEmpleado'] == '1111135')
]


# In[ ]:


# resultado_liquidaciones_copy = resultado_liquidaciones[
#     (resultado_liquidaciones['CodigoEmpleado'] == '16659046')
# ].groupby(['CodigoEmpleado'], group_keys=False).apply(calcular_liquidacion_mes)
# resultado_liquidaciones_copy[resultado_liquidaciones_copy['CodigoEmpleado'] == '16659046']


# In[ ]:


# resultado_liquidaciones_copy = resultado_liquidaciones[
#     (resultado_liquidaciones['CodigoEmpleado'] == '1111135') &  (resultado_liquidaciones['PagoGarantizado'] == 'NO')
# ].groupby(['CodigoEmpleado'], group_keys=False).apply(calcular_liquidacion_mes)
# resultado_liquidaciones_copy[resultado_liquidaciones_copy['CodigoEmpleado'] == '1111135']


# In[ ]:


# resultado_liquidaciones = resultado_liquidaciones.groupby(['CodigoEmpleado'], group_keys=False).apply(calcular_liquidacion_mes)
resultado_liquidaciones.rename(columns={'NuevoFactor': 'FactorMes'}, inplace=True)
resultado_liquidaciones['PagoGarantizado'] = 'NO'

resultado_liquidaciones['PagoReal'] = np.where(
    (
        (resultado_liquidaciones['Fecha'] >= '2022-01-01') &
        (resultado_liquidaciones['Fecha'] <= '2022-02-01') &
        (resultado_liquidaciones['TipoEmpleado'].isin(['VE', 'VC']))
    ),
    resultado_liquidaciones['SalarioVariable'],
    resultado_liquidaciones['PagoReal']
)
resultado_liquidaciones['PagoReal'] = np.where(
    (
        (resultado_liquidaciones['Fecha'] >= '2022-01-01') &
        (resultado_liquidaciones['Fecha'] <= '2022-02-01') &
        (resultado_liquidaciones['TipoEmpleado'].isin(['AD', 'AD1', 'AD2', 'AD3', 'AD4', 'AD5', 'GC', 'GD', 'GU', 'KAM', 'MK']))
    ),
    resultado_liquidaciones['SalarioVariable'] * 0.85,
    resultado_liquidaciones['PagoReal']
)
resultado_liquidaciones.drop(columns=['TipoEmpleado'], inplace=True)

resultado_liquidaciones.head(12)


# In[ ]:


resultado_liquidaciones[resultado_liquidaciones['CodigoEmpleado'] == '16659046']


# In[ ]:


resultado_liquidaciones[resultado_liquidaciones['CodigoEmpleado'] == '1111135']


# In[ ]:


resultado_liquidaciones[resultado_liquidaciones['CodigoEmpleado'] == '79539822']


# ### Creación de resultado_liquidaciones por maestra_resultados_por_empleado_centro_costo

# In[ ]:


# resultado_liquidaciones_centro_costo = maestra_resultados_por_empleado_centro_costo_final.groupby(
#     ['CodigoEmpleado', 'CodigoCentroCosto','SalarioVariable', 'Fecha', 'NuevoFactorBK'],
#     as_index=False
# ).Liquidado.agg({'Liquidado': 'sum'})
# resultado_liquidaciones_centro_costo.rename(columns={'NuevoFactorBK': 'NuevoFactor'}, inplace=True)
# resultado_liquidaciones_centro_costo.head()
# resultado_liquidaciones_centro_costo = resultado_liquidaciones_centro_costo.merge(
#     df_incentivos_por_empleado.rename(columns={'Valor': 'PagoReal'}),
#     on = ['CodigoEmpleado', 'Fecha'],
#     how = 'right'
# )

# resultado_liquidaciones_centro_costo = pd.merge(
#     left=resultado_liquidaciones_centro_costo,
#     right=df_empleados[['CodigoEmpleado', 'FechaIngreso']].drop_duplicates(['CodigoEmpleado']),
#     how='left',
#     on=['CodigoEmpleado']
# )

# resultado_liquidaciones_centro_costo.head()


# In[ ]:


# resultado_liquidaciones_centro_costo = resultado_liquidaciones_centro_costo[resultado_liquidaciones_centro_costo['Fecha'] >= resultado_liquidaciones_centro_costo['FechaIngreso']]
# resultado_liquidaciones_centro_costo.drop(columns=['FechaIngreso'], inplace=True)
# resultado_liquidaciones_centro_costo.head()

# # Cálculo de garantizado (100%) para VEs y VCs, Anticipos (85%) para el resto de empleados
# resultado_liquidaciones_centro_costo = resultado_liquidaciones_centro_costo.merge(
#     df_empleados[['CodigoEmpleado', 'TipoEmpleado']],
#     on='CodigoEmpleado',
#     how='left'
# )

# def calcular_liquidacion_mes(g):
#     pago_acumulado = 0
#     for i, row in g.iterrows():
#         if row['Fecha'].month in meses_incentivos:
#             if g.at[i, 'PagoReal'] == 0:
#                 if i == 0:
#                     g.at[i, 'PagoReal'] = g.at[i, 'Liquidado']
#                 else:
#                     g.at[i, 'PagoReal'] = g.at[i, 'Liquidado'] - pago_acumulado
            
#             if g.at[i, 'PagoReal'] < 0:
#                 g.at[i, 'PagoReal'] = 0.0
#         pago_acumulado += g.at[i, 'PagoReal']
        
#         excepcion = excepciones_pagos_acumulados.get(row['CodigoEmpleado'], None)
#         if excepcion and row['Fecha'] == excepcion['Fecha']:
#             pago_acumulado = excepcion['ValorPagoAcumulado']
        
#         excepcion_diferencia = excepciones_diferencias.get(row['CodigoEmpleado'], None)
#         if excepcion_diferencia and row['Fecha'] == excepcion_diferencia['Fecha']:
#             pago_acumulado = pago_acumulado + excepcion_diferencia['ValorDiferencia']
        
#     return g
# resultado_liquidaciones_centro_costo = resultado_liquidaciones_centro_costo.sort_values(by=['CodigoEmpleado', 'Fecha'])
# resultado_liquidaciones_centro_costo.reset_index(inplace=True, drop=True)
# resultado_liquidaciones_centro_costo[a'PagoReal'].fillna(0.0, inplace=True)
# resultado_liquidaciones_centro_costo['Liquidado'].fillna(0.0, inplace=True)
# resultado_liquidaciones_centro_costo['PagoGarantizado'] = 'NO'
# resultado_liquidaciones_centro_costo


# In[ ]:


# resultado_liquidaciones_centro_costo = resultado_liquidaciones_centro_costo.groupby(['CodigoEmpleado'], group_keys=False).apply(calcular_liquidacion_mes)
# resultado_liquidaciones_centro_costo.rename(columns={'NuevoFactor': 'FactorMes'}, inplace=True)
# resultado_liquidaciones_centro_costo['PagoGarantizado'] = 'NO'

# resultado_liquidaciones_centro_costo['PagoReal'] = np.where(
#     (
#         (resultado_liquidaciones_centro_costo['Fecha'] >= '2022-01-01') &
#         (resultado_liquidaciones_centro_costo['Fecha'] <= '2022-02-01') &
#         (resultado_liquidaciones_centro_costo['TipoEmpleado'].isin(['VE', 'VC']))
#     ),
#     resultado_liquidaciones_centro_costo['SalarioVariable'],
#     resultado_liquidaciones_centro_costo['PagoReal']
# )
# resultado_liquidaciones_centro_costo['PagoReal'] = np.where(
#     (
#         (resultado_liquidaciones_centro_costo['Fecha'] >= '2022-01-01') &
#         (resultado_liquidaciones_centro_costo['Fecha'] <= '2022-02-01') &
#         (resultado_liquidaciones_centro_costo['TipoEmpleado'].isin(['AD', 'AD1', 'AD2', 'AD3', 'AD4', 'AD5', 'GC', 'GD', 'GU', 'KAM', 'MK']))
#     ),
#     resultado_liquidaciones_centro_costo['SalarioVariable'] * 0.85,
#     resultado_liquidaciones_centro_costo['PagoReal']
# )
# resultado_liquidaciones_centro_costo.drop(columns=['TipoEmpleado'], inplace=True)

# resultado_liquidaciones_centro_costo.head(12)


# ## Se obtienen los garantizados

# ### Sin centro de costo

# In[ ]:


empleados_garantizado = df_empleados[df_empleados['FechaIngreso'] > fecha_liquidacion][['CodigoEmpleado', 'SalarioVariable', 'FechaIngreso']]
empleados_garantizado


# In[ ]:


fechas_resultado_liquidaciones = resultado_liquidaciones[['Fecha']].drop_duplicates()


# In[ ]:


empleados_garantizado = df_empleados[df_empleados['FechaIngreso'] > fecha_liquidacion][['CodigoEmpleado', 'SalarioVariable', 'FechaIngreso']]
fechas_resultado_liquidaciones = resultado_liquidaciones[['Fecha']].drop_duplicates()

empleados_garantizado['Key'] = 1
fechas_resultado_liquidaciones['Key'] = 1

liquidaciones_garantizados = pd.merge(
    left=fechas_resultado_liquidaciones.sort_values(['Fecha']),
    right=empleados_garantizado,
    how='outer',
    on=['Key']
).drop(columns=['Key'])

liquidaciones_garantizados = liquidaciones_garantizados[liquidaciones_garantizados['CodigoEmpleado'].notnull()]

liquidaciones_garantizados['DifMonths'] = ((liquidaciones_garantizados.FechaIngreso - liquidaciones_garantizados.Fecha)/np.timedelta64(1, 'M'))
liquidaciones_garantizados['DifMonths'] = liquidaciones_garantizados['DifMonths'].apply(lambda x: round(x, 0))
liquidaciones_garantizados['DifMonths'] = liquidaciones_garantizados['DifMonths'].astype('int')
liquidaciones_garantizados = liquidaciones_garantizados[liquidaciones_garantizados['DifMonths'] <= 4]

liquidaciones_garantizados['FactorMes'] = 0
liquidaciones_garantizados['Liquidado'] = liquidaciones_garantizados['SalarioVariable']
liquidaciones_garantizados['PagoReal'] = liquidaciones_garantizados['SalarioVariable']
liquidaciones_garantizados['PagoGarantizado'] = 'SI'

liquidaciones_garantizados.drop(columns=['DifMonths', 'FechaIngreso'], inplace=True)

liquidaciones_garantizados.head()


# In[ ]:


liquidaciones_garantizados[liquidaciones_garantizados['CodigoEmpleado'] == '16659046']


# In[ ]:


liquidaciones_garantizados[liquidaciones_garantizados['CodigoEmpleado'] == '1111135']


# ### Con centro de costo

# In[ ]:


# empleados_garantizado = df_empleados[df_empleados['FechaIngreso'] > fecha_liquidacion][['CodigoEmpleado', 'SalarioVariable', 'FechaIngreso']]
# fechas_resultado_liquidaciones_centro_costo = resultado_liquidaciones_centro_costo[['Fecha']].drop_duplicates()

# empleados_garantizado['Key'] = 1
# fechas_resultado_liquidaciones_centro_costo['Key'] = 1

# liquidaciones_garantizados_centro_costo = pd.merge(
#     left=fechas_resultado_liquidaciones_centro_costo.sort_values(['Fecha']),
#     right=empleados_garantizado,
#     how='outer',
#     on=['Key']
# ).drop(columns=['Key'])

# liquidaciones_garantizados_centro_costo = liquidaciones_garantizados_centro_costo[liquidaciones_garantizados_centro_costo['CodigoEmpleado'].notnull()]

# liquidaciones_garantizados_centro_costo['DifMonths'] = ((liquidaciones_garantizados_centro_costo.FechaIngreso - liquidaciones_garantizados_centro_costo.Fecha)/np.timedelta64(1, 'M'))
# liquidaciones_garantizados_centro_costo['DifMonths'] = liquidaciones_garantizados_centro_costo['DifMonths'].apply(lambda x: round(x, 0))
# liquidaciones_garantizados_centro_costo['DifMonths'] = liquidaciones_garantizados_centro_costo['DifMonths'].astype('int')
# liquidaciones_garantizados_centro_costo = liquidaciones_garantizados_centro_costo[liquidaciones_garantizados_centro_costo['DifMonths'] <= 4]

# liquidaciones_garantizados_centro_costo['FactorMes'] = 0
# liquidaciones_garantizados_centro_costo['Liquidado'] = liquidaciones_garantizados_centro_costo['SalarioVariable']
# liquidaciones_garantizados_centro_costo['PagoReal'] = liquidaciones_garantizados_centro_costo['SalarioVariable']
# liquidaciones_garantizados_centro_costo['PagoGarantizado'] = 'SI'

# liquidaciones_garantizados_centro_costo.drop(columns=['DifMonths', 'FechaIngreso'], inplace=True)

# liquidaciones_garantizados_centro_costo.head()


# ## Se unen las liquidaciones de garantizados con las demás

# ### Sin centro de costo

# In[ ]:


resultado_liquidaciones = pd.concat([resultado_liquidaciones, liquidaciones_garantizados])
resultado_liquidaciones.reset_index(drop=True, inplace=True)
resultado_liquidaciones


# In[ ]:


resultado_liquidaciones[resultado_liquidaciones['CodigoEmpleado'] == '1111135']


# In[ ]:


resultado_liquidaciones[resultado_liquidaciones['CodigoEmpleado'] == '16659046']


# ### Con centro de costo

# In[ ]:


# resultado_liquidaciones_centro_costo = pd.concat([resultado_liquidaciones_centro_costo, liquidaciones_garantizados_centro_costo])
# resultado_liquidaciones_centro_costo.reset_index(drop=True, inplace=True)
# resultado_liquidaciones_centro_costo


# ## Se conserva la información del histórico y se le añade la nueva información hasta la fecha de liquidación

# ### Sin centro de costo

# In[ ]:


df_resultado_liquidaciones_previo[df_resultado_liquidaciones_previo['CodigoEmpleado'] == '79539822']


# In[ ]:


df_resultado_liquidaciones_previo[df_resultado_liquidaciones_previo['CodigoEmpleado'] == '16659046']


# In[ ]:


df_resultado_liquidaciones_previo[df_resultado_liquidaciones_previo['CodigoEmpleado'] == '5005980']


# In[ ]:


meses_incentivos


# In[ ]:


resultado_liquidaciones[resultado_liquidaciones['CodigoEmpleado'] == '1111135']


# In[ ]:


# # Función para combinar valores de columnas _x y _y
# def combinar_valores(row, columna_base):
#     valor_x = row[f'{columna_base}_x']
#     valor_y = row[f'{columna_base}_y']
    
#     if valor_x != 0.0:
#         return valor_x
#     elif valor_y != 0.0:
#         return valor_y
#     else:
#         return 0


# In[ ]:


# resultado_liquidaciones[resultado_liquidaciones['CodigoEmpleado'] == '1111135'].merge(
#     resultado_liquidaciones_historico[resultado_liquidaciones_historico['CodigoEmpleado'] == '1111135'],
#     on=['CodigoEmpleado', 'Fecha'],
#     how='outer'
# ).fillna(0)


# In[ ]:


resultado_liquidaciones.columns


# In[ ]:


# prueba = resultado_liquidaciones[resultado_liquidaciones['CodigoEmpleado'] == '1111135'].merge(
#     resultado_liquidaciones_historico[resultado_liquidaciones_historico['CodigoEmpleado'] == '1111135'],
#     on=['CodigoEmpleado', 'Fecha'],
#     how='outer'
# ).fillna(0).sort_values(by=['CodigoEmpleado', 'Fecha'])

# prueba['SalarioVariable'] = prueba.apply(lambda row: combinar_valores(row, 'SalarioVariable'), axis=1)
# prueba['Liquidado'] = prueba.apply(lambda row: combinar_valores(row, 'Liquidado'), axis=1)
# prueba['FactorMes'] = prueba.apply(lambda row: combinar_valores(row, 'FactorMes'), axis=1)
# prueba['PagoReal'] = prueba.apply(lambda row: combinar_valores(row, 'PagoReal'), axis=1)
# prueba['PagoGarantizado'] = prueba.apply(lambda row: combinar_valores(row, 'PagoGarantizado'), axis=1)
# prueba = prueba[resultado_liquidaciones.columns]
# prueba


# In[ ]:


# prueba = resultado_liquidaciones.merge(
#     resultado_liquidaciones_historico,
#     on=['CodigoEmpleado', 'Fecha'],
#     how='outer'
# ).fillna(0).sort_values(by=['CodigoEmpleado', 'Fecha'])

# prueba['SalarioVariable'] = prueba.apply(lambda row: combinar_valores(row, 'SalarioVariable'), axis=1)
# prueba['Liquidado'] = prueba.apply(lambda row: combinar_valores(row, 'Liquidado'), axis=1)
# prueba['FactorMes'] = prueba.apply(lambda row: combinar_valores(row, 'FactorMes'), axis=1)
# prueba['PagoReal'] = prueba.apply(lambda row: combinar_valores(row, 'PagoReal'), axis=1)
# prueba['PagoGarantizado'] = prueba.apply(lambda row: combinar_valores(row, 'PagoGarantizado'), axis=1)
# prueba = prueba[resultado_liquidaciones.columns]

# resultado_liquidaciones = prueba.copy()
# resultado_liquidaciones


# In[ ]:


# def calcular_liquidacion_mes(g):
#     pago_acumulado = 0
#     suma_anticipos = 0
#     for i, row in g.iterrows():
# #         print(row)
# #         print(f"Liquidado: {row['Liquidado']} PagoReal: {row['PagoReal']} PagadoFecha:{row['PagadoFecha']}")
# #         print("mes actual:", row['Fecha'].month)
#         if row['Fecha'].month in meses_incentivos:
# #             print("Pago Real", g.at[i, 'PagoReal'])
#             if g.at[i, 'PagoReal'] == 0:
# #                 print("Pago Real es 0", g.at[i, 'PagoReal'])
#                 if i == 0:
# #                     print("i es igual a 0")
#                     g.at[i, 'PagoReal'] = g.at[i, 'Liquidado']
# #                     print(f"Pago Real {g.at[i, 'PagoReal']} igual a liquidado = {g.at[i, 'Liquidado']}")
#                 else:
# #                     print("i no es igual a 0")
#                     g.at[i, 'PagoReal'] = g.at[i, 'Liquidado'] - pago_acumulado
# #                     print(f"Pago Real {g.at[i, 'PagoReal']} igual a liquidado = {g.at[i, 'Liquidado']} - pago acumulado {pago_acumulado}")
# #                 print("Pago Real actualizado", g.at[i, 'PagoReal'])
#             if g.at[i, 'PagoReal'] < 0:
#                 g.at[i, 'PagoReal'] = 0.0
                
# #         print(row['Fecha'].month, type(row['Fecha'].month), g.at[i, 'PagoReal'])
#         if row['Fecha'].month not in meses_incentivos:
#             if g.at[i, 'PagoGarantizado'] == "NO":
# #                 print("Pago Real", g.at[i, 'PagoReal'])
#                 suma_anticipos += g.at[i, 'PagoReal']
        
#         if row['Fecha'].month == 4:
# #             print("Abril", suma_anticipos)
#             g.at[i, 'PagoReal'] = suma_anticipos + g.at[i, 'PagoReal']
        
# #         print("pago_acumuladol_antes:", pago_acumulado)
#         pago_acumulado += g.at[i, 'PagoReal']
# #         print("pago_acumuladol_después:", pago_acumulado)
        
#         g.at[i, 'PagadoFecha'] = pago_acumulado
        
#         excepcion = excepciones_pagos_acumulados.get(row['CodigoEmpleado'], None)
#         if excepcion and row['Fecha'] == excepcion['Fecha']:
#             pago_acumulado = excepcion['ValorPagoAcumulado']
        
#         excepcion_diferencia = excepciones_diferencias.get(row['CodigoEmpleado'], None)
#         if excepcion_diferencia and row['Fecha'] == excepcion_diferencia['Fecha']:
#             pago_acumulado = pago_acumulado + excepcion_diferencia['ValorDiferencia']
# #         print("----+++++++++++++++++++++++++++++-----")
#     return g


# In[ ]:


def calcular_liquidacion_mes_corregido(g):
    pago_acumulado = 0
    suma_anticipos = 0
    for i, row in g.iterrows():
        if row['Fecha'].month in meses_incentivos:
            if g.at[i, 'PagoReal'] == 0:
                if i == 0:

                    g.at[i, 'PagoReal'] = g.at[i, 'Liquidado']
                else:
                    g.at[i, 'PagoReal'] = g.at[i, 'Liquidado'] - pago_acumulado
            if g.at[i, 'PagoReal'] < 0:
                g.at[i, 'PagoReal'] = 0.0
                    
#         if row['Fecha'].month not in meses_incentivos:
#             if g.at[i, 'PagoGarantizado'] == "NO":
#                 suma_anticipos = suma_anticipos + g.at[i, 'PagoReal']

#         if row['Fecha'].month == 4:
#             g.at[i, 'PagoReal'] = suma_anticipos + g.at[i, 'PagoReal']
        

        
        try:
            if g.at[i, 'PagoGarantizado'] == "NO":
                pago_acumulado += g.at[i, 'PagoReal']
        except Exception as e:
            print(row)
            print("+++++++++++++++++++++++")
            print(g.at[i, 'PagoGarantizado'])
            print("-----------")
            print(type(g.at[i, 'PagoGarantizado']))
            print(e)
        g.at[i, 'PagadoFecha'] = pago_acumulado
        excepcion = excepciones_pagos_acumulados.get(row['CodigoEmpleado'], None)
        if excepcion and row['Fecha'] == excepcion['Fecha']:
            pago_acumulado = excepcion['ValorPagoAcumulado']
        
        excepcion_diferencia = excepciones_diferencias.get(row['CodigoEmpleado'], None)
        if excepcion_diferencia and row['Fecha'] == excepcion_diferencia['Fecha']:
            pago_acumulado = pago_acumulado + excepcion_diferencia['ValorDiferencia']
    return g


# In[ ]:


resultado_liquidaciones_copy = resultado_liquidaciones.copy()
resultado_liquidaciones = resultado_liquidaciones_copy
resultado_liquidaciones


# In[ ]:


resultado_liquidaciones[resultado_liquidaciones['CodigoEmpleado'] == '1060654227']


# In[ ]:


resultado_liquidaciones_historico = df_resultado_liquidaciones_previo.copy()
resultado_liquidaciones_historico['PagoGarantizado'] = resultado_liquidaciones_historico['PagoGarantizado'].fillna('NO')
last_date_historico = resultado_liquidaciones_historico['Fecha'].max()
last_date_historico = None if pd.isnull(last_date_historico) else last_date_historico

try:
    resultado_liquidaciones = resultado_liquidaciones[
        resultado_liquidaciones['Fecha']<=fecha_liquidacion
    ]
    if last_date_historico:
        resultado_liquidaciones = resultado_liquidaciones[
            resultado_liquidaciones['Fecha']>last_date_historico
        ]
except:
    pass

resultado_liquidaciones = pd.concat([resultado_liquidaciones_historico, resultado_liquidaciones]).reset_index(drop=True)
resultado_liquidaciones = resultado_liquidaciones.sort_values(by=['CodigoEmpleado', 'Fecha'])

for codigo_empleado_excepcion in excepciones_pagos_acumulados:
    excepcion = excepciones_pagos_acumulados[codigo_empleado_excepcion]
    resultado_liquidaciones['PagoReal'] = np.where(
        (
            (resultado_liquidaciones['CodigoEmpleado'] == codigo_empleado_excepcion) &
            (resultado_liquidaciones['Fecha'] <= excepcion['Fecha'])
        ),
        excepcion['ValorPagoAcumulado'],
        resultado_liquidaciones['PagoReal']
    )

for codigo_empleado_excepcion in excepciones_diferencias:
    excepcion = excepciones_diferencias[codigo_empleado_excepcion]
    resultado_liquidaciones['PagoReal'] = np.where(
        (
            (resultado_liquidaciones['CodigoEmpleado'] == codigo_empleado_excepcion) &
            (resultado_liquidaciones['Fecha'] == excepcion['Fecha'])
        ),
        resultado_liquidaciones['PagoReal'] + excepcion['ValorDiferencia'],
        resultado_liquidaciones['PagoReal']
    )
    
resultado_liquidaciones['PagadoFecha'] = 0

resultado_liquidaciones = resultado_liquidaciones.groupby(['CodigoEmpleado'], group_keys=False).apply(calcular_liquidacion_mes_corregido)
resultado_liquidaciones.head()


# In[ ]:


resultado_liquidaciones[
    (resultado_liquidaciones['CodigoEmpleado'] == '16659046') | 
    (resultado_liquidaciones['CodigoEmpleado'] == '1111135')|
     (resultado_liquidaciones['CodigoEmpleado'] == '79539822')
]


# In[ ]:





# In[ ]:


resultado_liquidaciones['PagadoFecha'] = resultado_liquidaciones.apply(
    lambda row: row['PagadoFecha'] - row['PagoReal'] if row['PagoGarantizado'] == 'NO' else row['PagadoFecha'], axis=1
)


# In[ ]:


resultado_liquidaciones[
#     (resultado_liquidaciones['CodigoEmpleado'] == '16659046') | 
    (resultado_liquidaciones['CodigoEmpleado'] == '1111135') |
#     (resultado_liquidaciones['CodigoEmpleado'] == '1018451704') |
    (resultado_liquidaciones['CodigoEmpleado'] == '79539822')
]


# In[ ]:


resultado_liquidaciones.drop_duplicates(['CodigoEmpleado', 'Fecha'], inplace=True)

resultado_final = pd.merge(
    left=resultado_liquidaciones[['CodigoEmpleado', 'Fecha', 'PagoGarantizado', 'Liquidado', 'PagoReal', 'PagadoFecha', 'FactorMes']],
    right=df_empleados,
    on=['CodigoEmpleado'],
    how='left'
)
resultado_final = resultado_final[['Identificacion', 'Nombre', 'Apellidos', 'CodigoEmpleado', 'TipoEmpleado', 'AreaCalculo', 'Fecha', 'SalarioVariable', 'PagoGarantizado', 'FactorMes', 'Liquidado', 'PagoReal', 'PagadoFecha']]
resultado_final = resultado_final.sort_values(by=['TipoEmpleado', 'AreaCalculo', 'CodigoEmpleado', 'Fecha'])

resultado_final.rename(columns={
    'Identificacion': 'Cédula',
    'CodigoEmpleado': 'Código',
    'TipoEmpleado': 'Tipo Empleado',
    'AreaCalculo': 'Área de cálculo',
    'Liquidado': 'Valor liquidado',
    'PagoReal': 'Valor neto a pagar',
    'PagadoFecha': 'Pagado a la fecha',
    'SalarioVariable': 'Salario variable',
    'FactorMes': 'Factor mes',
    'PagoGarantizado': '¿Pago garantizado?'
}, inplace=True)
resultado_final.head()


# ### Con centro de costo

# In[ ]:


# resultado_liquidaciones_historico_centro_costo = df_resultado_liquidaciones_previo.copy()
# resultado_liquidaciones_historico_centro_costo['PagoGarantizado'] = resultado_liquidaciones_historico_centro_costo['PagoGarantizado'].fillna('NO')
# last_date_historico = resultado_liquidaciones_historico_centro_costo['Fecha'].max()
# last_date_historico = None if pd.isnull(last_date_historico) else last_date_historico

# try:
#     resultado_liquidaciones_centro_costo = resultado_liquidaciones_centro_costo[
#         resultado_liquidaciones['Fecha']<=fecha_liquidacion
#     ]
#     if last_date_historico:
#         resultado_liquidaciones_centro_costo = resultado_liquidaciones_centro_costo[
#             resultado_liquidaciones_centro_costo['Fecha']>last_date_historico
#         ]
# except:
#     pass

# resultado_liquidaciones_centro_costo = pd.concat([resultado_liquidaciones_historico_centro_costo, resultado_liquidaciones_centro_costo])
# resultado_liquidaciones_centro_costo = resultado_liquidaciones_centro_costo.sort_values(by=['CodigoEmpleado', 'Fecha'])

# for codigo_empleado_excepcion in excepciones_pagos_acumulados:
#     excepcion = excepciones_pagos_acumulados[codigo_empleado_excepcion]
#     resultado_liquidaciones_centro_costo['PagoReal'] = np.where(
#         (
#             (resultado_liquidaciones_centro_costo['CodigoEmpleado'] == codigo_empleado_excepcion) &
#             (resultado_liquidaciones_centro_costo['Fecha'] <= excepcion['Fecha'])
#         ),
#         excepcion['ValorPagoAcumulado'],
#         resultado_liquidaciones_centro_costo['PagoReal']
#     )

# for codigo_empleado_excepcion in excepciones_diferencias:
#     excepcion = excepciones_diferencias[codigo_empleado_excepcion]
#     resultado_liquidaciones_centro_costo['PagoReal'] = np.where(
#         (
#             (resultado_liquidaciones_centro_costo['CodigoEmpleado'] == codigo_empleado_excepcion) &
#             (resultado_liquidaciones_centro_costo['Fecha'] == excepcion['Fecha'])
#         ),
#         resultado_liquidaciones_centro_costo['PagoReal'] + excepcion['ValorDiferencia'],
#         resultado_liquidaciones_centro_costo['PagoReal']
#     )

# resultado_liquidaciones_centro_costo['PagadoFecha'] = resultado_liquidaciones_centro_costo.groupby(['CodigoEmpleado'])['PagoReal'].transform(pd.Series.cumsum)
# resultado_liquidaciones_centro_costo['PagadoFecha'] = resultado_liquidaciones_centro_costo['PagadoFecha'] - resultado_liquidaciones_centro_costo['PagoReal']
# resultado_liquidaciones_centro_costo


# In[ ]:


# resultado_liquidaciones_centro_costo.drop_duplicates(['CodigoEmpleado', 'Fecha'], inplace=True)

# resultado_final_centro_costo = pd.merge(
#     left=resultado_liquidaciones_centro_costo[['CodigoEmpleado', 'Fecha', 'PagoGarantizado', 'Liquidado', 'PagoReal','PagadoFecha', 'FactorMes', 'CodigoCentroCosto']],
#     right=df_empleados,
#     on=['CodigoEmpleado'],
#     how='left'
# )
# resultado_final_centro_costo = resultado_final_centro_costo[['Identificacion', 'Nombre', 'Apellidos', 'CodigoEmpleado', 'TipoEmpleado', 'AreaCalculo', 'Fecha', 'SalarioVariable', 'PagoGarantizado', 'FactorMes', 'Liquidado', 'PagoReal', 'PagadoFecha', 'CodigoCentroCosto']]
# resultado_final_centro_costo = resultado_final_centro_costo.sort_values(by=['TipoEmpleado', 'AreaCalculo', 'CodigoEmpleado', 'Fecha', 'CodigoCentroCosto'])

# resultado_final_centro_costo.rename(columns={
#     'Identificacion': 'Cédula',
#     'CodigoEmpleado': 'Código',
#     'TipoEmpleado': 'Tipo Empleado',
#     'AreaCalculo': 'Área de cálculo',
#     'Liquidado': 'Valor liquidado',
#     'PagoReal': 'Valor neto a pagar',
#     'PagadoFecha': 'Pagado a la fecha',
#     'SalarioVariable': 'Salario variable',
#     'FactorMes': 'Factor mes',
#     'PagoGarantizado': '¿Pago garantizado?'
# }, inplace=True)
# resultado_final_centro_costo.head()


# In[ ]:


resultado_liquidaciones[resultado_liquidaciones['CodigoEmpleado'] == '16659046']


# In[ ]:


resultado_liquidaciones[resultado_liquidaciones['CodigoEmpleado'] == '1111135'] 


# In[ ]:


resultado_liquidaciones[resultado_liquidaciones['CodigoEmpleado'] == '79539822'] 


# In[ ]:


resultado_liquidaciones_mayo = resultado_liquidaciones[resultado_liquidaciones['Fecha'] == '2024-05-01']


# In[ ]:


# resultado_final_centro_costo.to_excel(os.path.join(OUTPUT_FOLDER, 'resultado_final_centro_costo.xlsx'))
maestra_resultados_por_empleado.to_excel(os.path.join(OUTPUT_FOLDER, 'maestra_resultados_por_empleado.xlsx'))
# maestra_resultados_por_empleado_centro_costo.to_excel(os.path.join(OUTPUT_FOLDER, 'maestra_resultados_por_empleado_centro_costo.xlsx'))
resultado_liquidaciones.to_excel(os.path.join(OUTPUT_FOLDER, 'resultado_liquidaciones.xlsx'))
resultado_final.to_excel(os.path.join(OUTPUT_FOLDER, 'resultado_final.xlsx'))
maestra_resultados.to_excel(os.path.join(OUTPUT_FOLDER, 'maestra_resultados.xlsx'))


# In[ ]:


# resultado_liquidaciones = resultado_liquidaciones.reset_index()
# resultados_comparacion_nomina = resultados_comparacion_nomina.reset_index()


# In[ ]:


resultados_comparacion_nomina = pd.read_excel(
        io=os.path.join('C:\\Users\\Crisp\\Documents\\QualitySoftGroup\\Empresas\\Complemento360\\NR - Cambio de Parrillas','Relacion Compara datos de liquidacion 29jun2024 vs reportados a Nomina.xlsx'),
        dtype={
            'Código': str,
            'Fecha': 'datetime64[ns]',
            '¿Pago garantizado?': str,
            'Valor liquidado reportado a Nomina':float ,
            'Valor Neto a Pagar Reportado a Nomina': float,
            'Pagado a la fecha Reportado a Nomina': float,
        }
    )
resultados_comparacion_nomina.rename(columns={'Código':'CodigoEmpleado'}, inplace=True)


# In[ ]:


resultados_comparacion_nomina = resultados_comparacion_nomina.merge(
    resultado_liquidaciones_mayo,
    on=['CodigoEmpleado', 'Fecha'],
    how='left'
)


# In[ ]:


resultados_comparacion_nomina.columns


# In[ ]:


# resultados_comparacion_nomina['Valor liquidado'] = resultados_comparacion_nomina['Liquidado']
# resultados_comparacion_nomina['Valor neto a pagar'] = resultados_comparacion_nomina['PagoReal']
# resultados_comparacion_nomina['Pagado a la fecha'] = resultados_comparacion_nomina['PagadoFecha']


# In[ ]:


resultados_comparacion_nomina['Nueva Diferencia Valor Liquidado'] = resultados_comparacion_nomina['Valor liquidado reportado a Nomina'] - resultados_comparacion_nomina['Liquidado']
resultados_comparacion_nomina['Nueva Diferencia Valor Neto a Pagar'] = resultados_comparacion_nomina['Valor Neto a Pagar Reportado a Nomina'] - resultados_comparacion_nomina['PagoReal']
resultados_comparacion_nomina['Nueva Diferencia Pagado a la fecha'] = resultados_comparacion_nomina['Pagado a la fecha Reportado a Nomina'] - resultados_comparacion_nomina['PagadoFecha']
resultados_comparacion_nomina


# In[ ]:


resultados_comparacion_nomina[resultados_comparacion_nomina['CodigoEmpleado'] == '1111135']


# In[ ]:


resultados_comparacion_nomina.to_excel(os.path.join(OUTPUT_FOLDER, 'resultados_comparacion_nomina.xlsx'))


# In[ ]:


resultados_comparacion_nomina[resultados_comparacion_nomina['Nueva Diferencia Valor Liquidado'] > 1]


# In[ ]:


resultados_comparacion_nomina[resultados_comparacion_nomina['Nueva Diferencia Valor Liquidado'] < -1]


# ## Generación de PDFs

# In[ ]:


def get_dataframes(
        df_resultado_detallado,
        df_resultado_liquidaciones
):
    resultado_liquidaciones = df_resultado_liquidaciones.set_index(['CodigoEmpleado', 'Fecha'])
    resultado_liquidaciones.fillna(value={
        'PagoReal': 0,
        'Liquidacion': 0
    }, inplace=True)

    resultado_detallado = df_resultado_detallado.fillna(value={
        'FactorIncentivo': 0,
        'PorcentajeCumplimiento': 0,
        'Presupuesto': 0,
        'Real': 0,
        'Liquidado': 0,
        'ResultadoPresupuesto': 0,
        'ResultadoRealMes': 0,
        'PorcentajeCumplimientoMes': 0
    })

    return resultado_detallado, resultado_liquidaciones


# In[ ]:


df_detalles_para_pdf, df_liquidaciones_para_pdf = get_dataframes(
    df_resultado_detallado=maestra_resultados_por_empleado,
    df_resultado_liquidaciones=resultado_liquidaciones
)


# In[ ]:





# In[ ]:




