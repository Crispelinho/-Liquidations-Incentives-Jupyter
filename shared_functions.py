import os
import sys
import pandas as pd
import numpy as np

class Shared:
    
    def dataframe_search_filter(self, main, columns_dict_search):
        for (k, v) in zip(columns_dict_search.keys(), columns_dict_search.values()):
            if k in main.columns:
                main = main[main[k] == v]
        return main


class DataframeLoader:
    
    fecha_liquidacion = None
    meses_incentivos = None
    df_resultado_detallado_previo = None
    df_resultado_liquidaciones_previo = None
    df_empleados = None
    df_empleados_inactivos = None
    df_tipos_empleados = None
    df_clusters_empleado = None
    df_zonas_empleado = None
    df_salarios_variables = None
    df_parrillas = None
    df_venta_recaudo_real = None
    df_venta_recaudo_presupuesto = None
    df_venta_por_zona = None
    df_recaudo_por_zona = None
    df_rentabilidad = None
    df_venta_otras_companias = None
    df_recaudo_otras_companias = None
    df_venta_recaudo_kams = None
    df_rentabilidad_kam = None
    df_venta_servicios_mtto = None
    df_objetivos_cualitativos = None
    df_incentivos_por_empleado = None
    df_area_calculo_sba = None
    df_factores_liquidacion = None
    df_cluster_plan_real_recaudo = None
    df_zona_plan_real_venta_ch = None
    df_rentabilidad_zona = None
    df_resultados_variables_cualitativas = None
    df_zonas_clusters_empleado = None
    df_centros_costos = None
    df_empleados_centros_costos = None
    df_centros_costos_grupos_productos_divisiones = None
    df_parrillas_tipos_calculos = None
    df_area_calculo_sba_centros_costos = None
    df_renal_ambulatorio = None
    
    def __init__(self, base_dir, sub_direct):
        self.base_dir = base_dir
        self.folder_bbraun_source = os.path.join(base_dir, 'source', sub_direct)

    def cargar_dataframes(self, base_dir: str, folder_bbraun_source: str):
        if base_dir not in sys.path:
            sys.path.append(base_dir)
        if 'OUTPUT_FOLDER' not in locals():
            OUTPUT_FOLDER = os.path.join(base_dir, 'output')
            self.OUTPUT_FOLDER = OUTPUT_FOLDER

        if 'fecha_liquidacion' not in locals():
            fecha_liquidacion = '2024-05-01'
            self.fecha_liquidacion = fecha_liquidacion
            
        if 'meses_incentivos' not in locals():
            meses_incentivos = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
            self.meses_incentivos = meses_incentivos
        if 'df_resultado_detallado_previo' not in locals():
            df_resultado_detallado_previo = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'ResultadosDetalladosPrevios.xlsx'),
                dtype={
                    'Contexto': str,
                    'Fecha': 'datetime64[ns]',
                    'Variable': str,
                    'PorcentajeCumplimiento': float,
                    'Real': float,
                    'Presupuesto': float,
                    'CodigoEmpleado': str,
                    'Porcentaje': float,
                    'Consecutivo': int,
                    'TipoEmpleado': str,
                    'SalarioVariable': float,
                    'AreaCalculo': int,
                    'Apellidos': str,
                    'Nombre': str,
                    'FechaIngreso': 'datetime64[ns]',
                    'FactorIncentivo': float,
                    'FactorMes': int,
                    'Liquidado': float,
                    'DifMonths': int,
                    'NuevoFactor': int,
                    'ResultadoPresupuesto': float,
                    'ResultadoRealMes': float,
                    'PorcentajeCumplimientoMes': float
                }
            )
            self.df_resultado_detallado_previo = df_resultado_detallado_previo
        if 'df_resultado_liquidaciones_previo' not in locals():
            df_resultado_liquidaciones_previo = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'ResultadosLiquidacionesPrevios.xlsx'),
                dtype={
                    'CodigoEmpleado': str,
                    'SalarioVariable': float,
                    'Fecha': 'datetime64[ns]',
                    'FactorMes':int ,
                    'PagoGarantizado': str,
                    'Liquidado': float,
                    'PagoReal': float
                }
            )
            self.df_resultado_liquidaciones_previo = df_resultado_liquidaciones_previo
        if 'df_empleados' not in locals():
            df_empleados = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'Empleados.xlsx'),
                dtype={
                    'TipoEmpleado': str,
                    'CodigoEmpleado': str,
                    'Identificacion': str,
                    'CorreoElectronico': str,
                    'Nombre': str,
                    'Apellidos': str,
                    'SalarioVariable': float,
                    'FechaIngreso': 'datetime64[ns]',
                    'FechaRetiro': 'datetime64[ns]',
                    # 'Zona': str,
                    'AreaCalculo': int
                }
            )
            self.df_empleados = df_empleados
            
        if 'df_empleados_inactivos' not in locals():
            df_empleados_inactivos = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'EmpleadosInactivos.xlsx'),
                dtype={
                    'TipoEmpleado': str,
                    'CodigoEmpleado': str,
                    'Identificacion': str,
                    'CorreoElectronico': str,
                    'Nombre': str,
                    'Apellidos': str,
                    'SalarioVariable': float,
                    'FechaIngreso': 'datetime64[ns]',
                    'FechaRetiro': 'datetime64[ns]',
                    # 'Zona': str,
                    'AreaCalculo': int
                }
            )
            self.df_empleados_inactivos = df_empleados_inactivos
            
        if 'df_tipos_empleados' not in locals():
            df_tipos_empleados = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'TiposEmpleados.xlsx'),
                dtype={
                    'CodigoTipoEmpleado': str,
                    'CategoriaTipoEmpleado': str,
                    'TextoPDF': str
                }
            )
            self.df_tipos_empleados = df_tipos_empleados

        if 'df_clusters_empleado' not in locals():
            df_clusters_empleado = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'ClustersEmpleados.xlsx'),
                dtype={
                    'Cluster': str,
                    'CodigoEmpleado': str,
                    'TipoEmpleado': str,
                    'AreaCalculo': int
                }
            )
            self.df_clusters_empleado = df_clusters_empleado
        if 'df_zonas_empleado' not in locals():
            df_zonas_empleado = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'ZonasEmpleados.xlsx'),
                dtype={
                    'Zona': str,
                    'CodigoEmpleado': str,
                    'TipoEmpleado': str,
                    'AreaCalculo': int
                }
            )
            self.df_zonas_empleado = df_zonas_empleado
        if 'df_salarios_variables' not in locals():
            df_salarios_variables = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'SalariosVariables.xlsx'),
                dtype={
                    'CodigoEmpleado': str,
                    'Fecha': 'datetime64[ns]',
                    'SalarioVariable': float,
                    'PromedioSalarioVariable': float
                }
            )
        if 'df_parrillas' not in locals():
            df_parrillas = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'Parrillas.xlsx'),
                dtype={
                    'CodigoEmpleado': str,
                    'Variable': str,
                    'Contexto': str,
                    'Porcentaje': float,
                    'Consecutivo': int
                }
            )
            self.df_parrillas = df_parrillas
        if 'df_venta_recaudo_real' not in locals():
            df_venta_recaudo_real = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'VentaRecaudoReal.xlsx'),
                dtype={
                    'CodigoEmpleado': str,
                    'Division': str,
                    'GrupoProducto': str,
                    'SBA': str,
                    'Fecha': 'datetime64[ns]',
                    'Cluster': str,
                    'ZonaGeografica': str,
                    'Canal': str,
                    'Venta': float,
                    'Recaudo': float,
                    'Moneda': str,
                }
            )
            self.df_venta_recaudo_real = df_venta_recaudo_real
        if 'df_venta_recaudo_presupuesto' not in locals():
            df_venta_recaudo_presupuesto = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'VentaRecaudoPresupuesto.xlsx'),
                dtype={
                    'CodigoEmpleado': str,
                    'Division': str,
                    'GrupoProducto': str,
                    'SBA': str,
                    'Fecha': 'datetime64[ns]',
                    'Cluster': str,
                    'ZonaGeografica': str,
                    'Canal': str,
                    'Venta': float,
                    'Recaudo': float,
                    'Moneda': str,
                }
            )
            self.df_venta_recaudo_presupuesto = df_venta_recaudo_presupuesto
        if 'df_venta_por_zona' not in locals():
            df_venta_por_zona = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'VentaPorZona.xlsx'),
                dtype={
                    'Zona': str,
                    # 'Fecha': np.datetime64,
                    'Real': float,
                    'Presupuesto': float
                }
            )
        if 'df_recaudo_por_zona' not in locals():
            df_recaudo_por_zona = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'RecaudoPorZona.xlsx'),
                dtype={
                    'Zona': str,
                    'Fecha': 'datetime64[ns]',
                    'Real': float,
                    'Presupuesto': float
                }
            )
            self.df_recaudo_por_zona = df_recaudo_por_zona
        if 'df_rentabilidad' not in locals():
            df_rentabilidad = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'Rentabilidad.xlsx'),
                dtype={
                    'Compania': str,
                    'GrupoProducto': str,
                    'SBA': str,
                    'ClaseRentabilidad': str,
                    'Fecha': 'datetime64[ns]',
                    'Real': float,
                    'Presupuesto': float
                }
            )
            self.df_rentabilidad = df_rentabilidad
        if 'df_venta_otras_companias' not in locals():
            df_venta_otras_companias = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'VentaOtrasCompanias.xlsx'),
                dtype={
                    'Compania': str,
                    'Fecha': 'datetime64[ns]',
                    'Real': float,
                    'Presupuesto': float
                }
            )
            self.df_venta_otras_companias = df_venta_otras_companias
        if 'df_recaudo_otras_companias' not in locals():
            df_recaudo_otras_companias = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'RecaudoOtrasCompanias.xlsx'),
                dtype={
                    'Compania': str,
                    'Fecha': 'datetime64[ns]',
                    'Real': float,
                    'Presupuesto': float
                }
            )
            self.df_recaudo_otras_companias = df_recaudo_otras_companias

        if 'df_venta_recaudo_kams' not in locals():
            df_venta_recaudo_kams = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'VentaRecaudoKAMS.xlsx'),
                dtype={
                    'TipoEmpleado': str,
                    'AreaCalculo': int,
                    'Consecutivo': int,
                    'Fecha': 'datetime64[ns]',
                    'VentaReal': float,
                    'VentaPresupuesto': float,
                    'RecaudoReal': float,
                    'RecaudoPresupuesto': float
                }
            )
            self.df_venta_recaudo_kams = df_venta_recaudo_kams
        if 'df_rentabilidad_kam' not in locals():
            df_rentabilidad_kam = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'RentabilidadKAM.xlsx'),
                dtype={
                    'Division': str,
                    'Fecha': 'datetime64[ns]',
                    'Real': float,
                    'Presupuesto': float
                }
            )
            self.df_rentabilidad_kam = df_rentabilidad_kam
        if 'df_venta_servicios_mtto' not in locals():
            df_venta_servicios_mtto = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'VentaServiciosMtto.xlsx'),
                dtype={
                    'Fecha': 'datetime64[ns]',
                    'Real': float,
                    'Presupuesto': float
                }
            )
            self.df_venta_servicios_mtto = df_venta_servicios_mtto
            
        if 'df_objetivos_cualitativos' not in locals():
            df_objetivos_cualitativos = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'ObjetivosCualitativos.xlsx'),
                dtype={
                    # 'Fecha': np.datetime64,
                    'CodigoEmpleado': str,
                    'PorcentajeCumplimiento': float
                }
            )
            self.df_objetivos_cualitativos = df_objetivos_cualitativos
            
        if 'df_incentivos_por_empleado' not in locals():
            df_incentivos_por_empleado = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'IncentivosPorEmpleado.xlsx'),
                dtype={
                    'CodigoEmpleado': str,
                    'Fecha': 'datetime64[ns]',
                    'Valor': float
                }
            )
            self.df_incentivos_por_empleado = df_incentivos_por_empleado
            
        if 'df_area_calculo_sba' not in locals():
            df_area_calculo_sba = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'AreaCalculoSBA.xlsx'),
                dtype={
                    'TipoEmpleado': str,
                    'AreaCalculo': int,
                    'Consecutivo': int,
                    'Division': str,
                    'GrupoProducto': str,
                    'SBA': str
                }
            )
            self.df_area_calculo_sba = df_area_calculo_sba
            
        if 'df_factores_liquidacion' not in locals():
            df_factores_liquidacion = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'FactoresLiquidacion.xlsx'),
                dtype={
                    'TipoEmpleado': str,
                    'PorcentajeCumplimiento': float,
                    'FactorIncentivo': float
                }
            )
            self.df_factores_liquidacion = df_factores_liquidacion
            
        if 'df_cluster_plan_real_recaudo' not in locals():
            df_cluster_plan_real_recaudo = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'ClusterPlanRealRecaudo.xlsx'),
                dtype={
                    'Cluster': str,
                    'Fecha': 'datetime64[ns]',
                    'Real': float,
                    'Presupuesto': float
                }
            )
            self.df_cluster_plan_real_recaudo = df_cluster_plan_real_recaudo
            
        if 'df_zona_plan_real_venta_ch' not in locals():
            df_zona_plan_real_venta_ch = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'ZonaPlanRealVentaCH.xlsx'),
                dtype={
                    'Zona': str,
                    'Fecha': 'datetime64[ns]',
                    'Real': float,
                    'Presupuesto': float
                }
            )
            self.df_zona_plan_real_venta_ch = df_zona_plan_real_venta_ch
            
        if 'df_rentabilidad_zona' not in locals():
            df_rentabilidad_zona = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'RentabilidadZona.xlsx'),
                dtype={
                    'Zona': str,
                    'Fecha': 'datetime64[ns]',
                    'Real': float,
                    'Presupuesto': float
                }
            )
            self.df_rentabilidad_zona = df_rentabilidad_zona
            
        if 'df_resultados_variables_cualitativas' not in locals():
            df_resultados_variables_cualitativas = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'ResultadosVariablesCualitativas.xlsx'),
                dtype={
                    'Variable': str,
                    'CodigoEmpleado': str,
                    'TipoEmpleado': str,
                    'AreaCalculo': int,
                    'Consecutivo': int,
                    'PorcentajeCumplimiento': float,
                    'Fecha': 'datetime64[ns]'
                }
            )
            self.df_resultados_variables_cualitativas = df_resultados_variables_cualitativas

        if 'df_zonas_clusters_empleado' not in locals():
            df_zonas_clusters_empleado = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'ZonasClustersEmpleados.xlsx'),
                dtype={
                    'CodigoEmpleado': str,
                    'TipoEmpleado': str,
                    'AreaCalculo': int,
                    'Zona': str,
                    'Cluster': str,
                }
            )
            self.df_zonas_clusters_empleado = df_zonas_clusters_empleado
            
        if 'df_centros_costos' not in locals():
            df_centros_costos = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'CentrosCostos.xlsx'),
                dtype={
                    'CodigoCentroCosto': str,
                    'NombreCentroCosto': str,
                }
            )
            self.df_centros_costos = df_centros_costos

        if 'df_empleados_centros_costos' not in locals():
            df_empleados_centros_costos = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'EmpleadosCentrosCostos.xlsx'),
                dtype={
                    'CodigoEmpleado': str,
                    'CodigoCentroCosto': str,
                    'PorcentajeAsignacion': float,
                }
            )
            self.df_empleados_centros_costos = df_empleados_centros_costos

        if 'df_centros_costos_grupos_productos_divisiones' not in locals():
            
            df_centros_costos_grupos_productos_divisiones = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'CentroCostoGrupoProductoDivision.xlsx'),
                dtype={
                    'Divison': str,
                    'GrupoProducto': str,
                    'CodigoCentroCosto': str
                }
            )
            self.df_centros_costos_grupos_productos_divisiones= df_centros_costos_grupos_productos_divisiones

        if 'df_parrillas_tipos_calculos' not in locals():
            df_parrillas_tipos_calculos = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'ParrillaTipoCalculo.xlsx'),
                dtype={
                    'TipoEmpleado': str,
                    'AreaCalculo': int,
                    'CodigoRubro': str,
                    'NombreRubro': str,
                    'TipoCalculo': str,
                }
            )
            self.df_parrillas_tipos_calculos = df_parrillas_tipos_calculos

        if 'df_area_calculo_sba_centros_costos' not in locals():
            df_area_calculo_sba_centros_costos = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'AreaCalculoSBACentrosCostos.xlsx'),
                dtype={
                    'TipoEmpleado': str,
                    'AreaCalculo': int,
                    'Consecutivo': int,
                    'Division': str,
                    'GrupoProducto': str,
                    'SBA': str
                }
            )
            self.df_area_calculo_sba_centros_costos = df_area_calculo_sba_centros_costos

        # if 'df_gd_canales' not in locals():
        #     df_gd_canales = pd.read_excel(
        #         io=os.path.join(folder_bbraun_source, 'ResultadosGDCanales.xlsx'),
        #         dtype={
        #             'CodigoEmpleado': str,
        #             'Fecha': 'datetime64[ns]',
        #             'Consecutivo': int,
        #             'PlanVentaFacturada': float,
        #             'RealVentaTotal': float,
        #             'PlanVentaCobrada': float,
        #             'RealVentaCobrada': float,
        #         }
        #     )

        if 'df_renal_ambulatorio' not in locals():
            df_renal_ambulatorio = pd.read_excel(
                io=os.path.join(folder_bbraun_source, 'ResultadosVCRenalAmbulatorio.xlsx'),
                dtype={
                    'CodigoEmpleado': str,
                    'Fecha': 'datetime64[ns]',
                    'PlanVentaTotal': float,
                    'RealVentaTotal': float,
                    'PlanVentaCobradaRenal': float,
                    'RealVentaCobrada Renal': float,
                }
            )
            self.df_renal_ambulatorio = df_renal_ambulatorio

        if 'codigos_ve_vc' not in locals():
            codigos_ve_vc = ['5002700']
            self.codigos_ve_vc = codigos_ve_vc

        _dataframes_entrada = [
            {
                'df': df_empleados,
                'name': 'Empleados.xlsx'
            },
            {
                'df': df_clusters_empleado,
                'name': 'ClustersEmpleado.xlsx'
            },
            {
                'df': df_zonas_empleado,
                'name': 'ZonasEmpleado.xlsx'
            },
            {
                'df': df_zonas_clusters_empleado,
                'name': 'ZonasClustersEmpleado.xlsx'
            },
            {
                'df': df_salarios_variables,
                'name': 'SalariosVariables.xlsx'
            },
            {
                'df': df_parrillas,
                'name': 'Parrillas.xlsx'
            },
            {
                'df': df_venta_recaudo_real,
                'name': 'VentaRecaudoReal.xlsx'
            },
            {
                'df': df_venta_recaudo_presupuesto,
                'name': 'VentaRecaudoPresupuesto.xlsx'
            },
            {
                'df': df_venta_por_zona,
                'name': 'VentaPorZona.xlsx'
            },
            {
                'df': df_recaudo_por_zona,
                'name': 'RecaudoPorZona.xlsx'
            },
            {
                'df': df_rentabilidad,
                'name': 'Rentabilidad.xlsx'
            },
            {
                'df': df_venta_otras_companias,
                'name': 'VentaOtrasCompanias.xlsx'
            },
            {
                'df': df_recaudo_otras_companias,
                'name': 'RecaudoOtrasCompanias.xlsx'
            },
            {
                'df': df_venta_recaudo_kams,
                'name': 'VentaRecaudoKAMS.xlsx'
            },
            {
                'df': df_rentabilidad_kam,
                'name': 'RentabilidadKAM.xlsx'
            },
            {
                'df': df_objetivos_cualitativos,
                'name': 'ObjetivosCualitativos.xlsx',
            },
            {
                'df': df_venta_servicios_mtto,
                'name': 'VentaServiciosMtto.xlsx',
            },
            {
                'df': df_incentivos_por_empleado,
                'name': 'IncentivosPorEmpleado.xlsx'
            },
            {
                'df': df_area_calculo_sba,
                'name': 'AreaCalculoSBA.xlsx'
            },
            {
                'df': df_factores_liquidacion,
                'name': 'FactoresLiquidacion.xlsx'
            },
            {
                'df': df_cluster_plan_real_recaudo,
                'name': 'ClusterPlanRealRecaudo.xlsx'
            },
            {
                'df': df_zona_plan_real_venta_ch,
                'name': 'ZonaPlanRealVentaCH.xlsx'
            },
            {
                'df': df_rentabilidad_zona,
                'name': 'RentabilidadZona.xlsx'
            },
            {
                'df': df_resultados_variables_cualitativas,
                'name': 'ResultadosVariablesCualitativas.xlsx'
            },
        #     {
        #         'df': df_gd_canales,
        #         'name': 'ResultadosGDCanales.xlsx'
        #     },
        #     {
        #         'df': df_renal_ambulatorio,
        #         'name': 'ResultadosVCRenalAmbulatorio.xlsx'
        #     },
        ]
        self._dataframes_entrada = _dataframes_entrada

        return (
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
        )

    def sobreescribir_resultados_cualitativos(
        self,
        main,
        variable, 
        columnas_extra, 
        columnas_extra_merge, 
        pre_merge_lambda=None
    ):
        print(columnas_extra, columnas_extra_merge)
        resultados_cualitativos_variable = self.df_resultados_variables_cualitativas[columnas_extra + ['Variable', 'Fecha',
                                                                                                  'PorcentajeCumplimiento']]
        resultados_cualitativos_variable = resultados_cualitativos_variable[resultados_cualitativos_variable['Variable'] == variable]
        if pre_merge_lambda:
            resultados_cualitativos_variable = pre_merge_lambda(resultados_cualitativos_variable)
        main_df = pd.merge(
            left=main,
            right=resultados_cualitativos_variable.rename(columns={'PorcentajeCumplimiento': 'PorcentajeCumplimientoPrecargado'}),
            on=columnas_extra_merge+['Fecha', 'Variable'],
            how='outer'
        )
        main_df['Real'] = np.where(
            main_df['PorcentajeCumplimientoPrecargado'].isnull(),
            main_df['Real'],
            np.nan
        )
        main_df['Presupuesto'] = np.where(
            main_df['PorcentajeCumplimientoPrecargado'].isnull(),
            main_df['Presupuesto'],
            np.nan
        )
        main_df['PorcentajeCumplimiento'] = np.where(
            main_df['PorcentajeCumplimientoPrecargado'].isnull(),
            main_df['PorcentajeCumplimiento'],
            main_df['PorcentajeCumplimientoPrecargado']
        )
        main_df.drop(columns=['PorcentajeCumplimientoPrecargado'], inplace=True)

        return main_df


    def f_calculo_centro_costo_variable(
        self,
        main,
        variable
    ):
        if 'CodigoCentroCosto' not in main.columns:
            main = main.merge(
                self.df_centros_costos_grupos_productos_divisiones.rename(columns={'ConsecutivoParrilla':'Consecutivo'}),
                on=['Division', 'GrupoProducto'],
                how='left'
            )
        main['CodigoEmpleado'] = main['CodigoEmpleado'].astype(str).fillna('')
        main['Consecutivo'] = main['Consecutivo'].astype(str).fillna('')
        main['Consecutivo'] = main['Consecutivo'].str.replace('.0', '')
        main['Contexto'] = main['CodigoEmpleado'] + '_' + main['Consecutivo']
        main = main.groupby(
            ['Contexto', 'Fecha', 'GrupoProducto', 'Division', 'CodigoCentroCosto'], as_index=False)[['Real', 'Presupuesto']].apply(sum)
        main['Real'] = main.groupby(['Contexto', 'GrupoProducto', 'Division', 'CodigoCentroCosto'])['Real'].transform(pd.Series.cumsum)
        main['Presupuesto'] = main.groupby(
            ['Contexto', 'GrupoProducto', 'Division', 'CodigoCentroCosto'])['Presupuesto'].transform(pd.Series.cumsum)
        #     main['RealTotal'] = main.groupby(['Contexto', 'Fecha'], as_index=False)[['Real']].transform(pd.Series.sum)
        #     main['Porcentaje Aplicado'] = main['Real'] / main['RealTotal']
        main['Variable'] = variable
        return main

    def f_calculo_centro_costo_variable_tipo_empleado(
        self,
        main,
        variable,
        tipo_empleado
    ):
    #     df_area_calculo_sba_centros_costos_tipo_empleado = (
    #         df_area_calculo_sba_centros_costos[df_area_calculo_sba_centros_costos['TipoEmpleado'] == tipo_empleado]
    #     )    

        main = main.merge(
            self.df_centros_costos_grupos_productos_divisiones.rename(columns={'ConsecutivoParrilla':'Consecutivo'}),
            on=['Division', 'GrupoProducto'],
            how='left'
        )
        main['AreaCalculo'] = main['AreaCalculo'].astype(str)
        main['Consecutivo'] = main['Consecutivo'].astype(str)
        main['Contexto'] = tipo_empleado.lower() +'_'+ main['AreaCalculo'].str.cat(main['Consecutivo'],sep="_")
        main = main.groupby(
            ['Contexto', 'Fecha', 'GrupoProducto', 'Division', 'CodigoCentroCosto'], as_index=False)[['Real', 'Presupuesto']].apply(sum)

    #     main['Real'] = main.groupby(['Contexto', 'CentroCosto'])['Real'].transform(pd.Series.sum)
    #     main['Presupuesto'] = main.groupby(['Contexto', 'CentroCosto'])['Presupuesto'].transform(pd.Series.sum)

        main['Real'] = main.groupby(['Contexto', 'GrupoProducto', 'Division', 'CodigoCentroCosto'])['Real'].transform(pd.Series.cumsum)
        main['Presupuesto'] = main.groupby(
            ['Contexto', 'GrupoProducto', 'Division', 'CodigoCentroCosto'])['Presupuesto'].transform(pd.Series.cumsum)
        main['Variable'] = variable
        return main

    def validar_calculo_centro_costo(
        self,
        main,
        contexto,
        fecha
    ):

        real = main[
            (main['Contexto'] == contexto) &
            (main['Fecha'] == fecha)
        ]['Real'].sum()

        presupuesto = main[
            (main['Contexto'] == contexto) &
            (main['Fecha'] == fecha)
        ]['Presupuesto'].sum()

        return real, presupuesto

    def validar_calculo_centro_costo_division_sub(self, main, contexto, fecha):

        real = main[
            (main['Contexto'] == contexto) &
            (main['Fecha'] == fecha)
        ].groupby(['AreaCalculo', 'Consecutivo', 'Variable'])['Real'].transform(pd.Series.cumsum)

        presupuesto = main[
            (main['Contexto'] == contexto) &
            (main['Fecha'] == fecha)
        ].groupby(['AreaCalculo', 'Consecutivo', 'Variable'])['Presupuesto'].transform(pd.Series.cumsum)

        return real, presupuesto

    def f_calculo_centro_costo_variable_rentabilidad(
        self,
        main, 
        variable, 
        clase_rentabilidad, 
        tipo_empleado
    ):
        main = main.merge(
            self.df_centros_costos_grupos_productos_divisiones.rename(columns={'ConsecutivoParrilla':'Consecutivo'}),
            on=['Division', 'GrupoProducto'],
            how='left'
        )
        main['Consecutivo'] = main['Consecutivo'].astype(int).astype('str')
        main['AreaCalculo'] = main['AreaCalculo'].astype(int).astype('str')
        main['Contexto'] = tipo_empleado.lower()+'_' + main['AreaCalculo'] + '_' +main['Consecutivo']
        main = main.groupby(
            ['TipoEmpleado', 'ClaseRentabilidad', 'Fecha', 'AreaCalculo', 'Consecutivo', 'CodigoCentroCosto', 'Contexto'],
            as_index=False
        ).agg({'Real': 'sum', 'Presupuesto': 'sum'})
        main = main[main['ClaseRentabilidad']==clase_rentabilidad]
        main.drop(columns='ClaseRentabilidad', inplace=True)
        main['Variable'] = variable
        return main
    
    def f_calculo_centro_costo_variable_division_sub(
        self,
        main,
        variable,
        tipo_empleado
    ):
    #     df_area_calculo_sba_centros_costos_tipo_empleado = (
    #         df_area_calculo_sba_centros_costos[df_area_calculo_sba_centros_costos['TipoEmpleado'] == tipo_empleado]
    #     )    

        main = main.merge(
            self.df_centros_costos_grupos_productos_divisiones.rename(columns={'ConsecutivoParrilla':'Consecutivo'}),
            on=['Division', 'GrupoProducto'],
            how='left'
        )
        main['AreaCalculo'] = main['AreaCalculo'].astype(str)
        main['Consecutivo'] = main['Consecutivo'].astype(str)
        main['Contexto'] = tipo_empleado.lower() +'_'+ main['AreaCalculo'].str.cat(main['Consecutivo'],sep="_")
    #     main = main.groupby(['Contexto', 'Fecha', 'GrupoProducto', 'Division', 'CodigoCentroCosto'], as_index=False)[['Real', 'Presupuesto']].apply(sum)

    #     main['Real'] = main.groupby(['Contexto', 'CentroCosto'])['Real'].transform(pd.Series.sum)
    #     main['Presupuesto'] = main.groupby(['Contexto', 'CentroCosto'])['Presupuesto'].transform(pd.Series.sum)

        main['Real'] = main.groupby(['Contexto', 'GrupoProducto', 'Division', 'CodigoCentroCosto'])['Real'].transform(pd.Series.cumsum)
        main['Presupuesto'] = main.groupby(
            ['Contexto', 'GrupoProducto', 'Division', 'CodigoCentroCosto'])['Presupuesto'].transform(pd.Series.cumsum)
        main['Variable'] = variable
        return main


    # def f_calculo_centro_costo_variable_tipo_empleado(main, variable, tipo_empleado):
    #     df_area_calculo_sba_centros_costos_tipo_empleado = (
    #         df_area_calculo_sba_centros_costos[df_area_calculo_sba_centros_costos['TipoEmpleado'] == tipo_empleado]
    #     )    

    #     main = main.merge(
    #         df_area_calculo_sba_centros_costos_tipo_empleado.rename(columns={'ConsecutivoParrilla':'Consecutivo'}),
    #         on=['AreaCalculo','Division', 'Consecutivo', 'GrupoProducto'],
    #         how='left'
    #     )
    #     main['AreaCalculo'] = main['AreaCalculo'].astype(str)
    #     main['Consecutivo'] = main['Consecutivo'].astype(str)
    #     main['Contexto'] = tipo_empleado.lower() +'_'+ main['AreaCalculo'].str.cat(main['Consecutivo'],sep="_")
    #     main = main.groupby(['AreaCalculo', 'Consecutivo', 'Fecha'], as_index=False)[['Real', 'Presupuesto']].apply(sum)

    # #     main['Real'] = main.groupby(['Contexto', 'CentroCosto'])['Real'].transform(pd.Series.sum)
    # #     main['Presupuesto'] = main.groupby(['Contexto', 'CentroCosto'])['Presupuesto'].transform(pd.Series.sum)

    #     main['Real'] = main.groupby(['AreaCalculo', 'Consecutivo'])['Real'].transform(pd.Series.cumsum)
    #     main['Presupuesto'] = main.groupby(['AreaCalculo', 'Consecutivo'])['Presupuesto'].transform(pd.Series.cumsum)
    #     main['Variable'] = variable
    #     return main
    
    def f_calculo_centro_costo_variable_rentabilidad(self, main, variable, clase_rentabilidad, tipo_empleado):
        main = main.merge(
            self.df_centros_costos_grupos_productos_divisiones.rename(columns={'ConsecutivoParrilla':'Consecutivo'}),
            on=['Division', 'GrupoProducto'],
            how='left'
        )
        main['Consecutivo'] = main['Consecutivo'].astype(int).astype('str')
        main['AreaCalculo'] = main['AreaCalculo'].astype(int).astype('str')
        main['Contexto'] = tipo_empleado.lower()+'_' + main['AreaCalculo'] + '_' +main['Consecutivo']
        main = main.groupby(
            ['TipoEmpleado', 'ClaseRentabilidad', 'Fecha', 'AreaCalculo', 'Consecutivo', 'CodigoCentroCosto', 'Contexto'],
            as_index=False
        ).agg({'Real': 'sum', 'Presupuesto': 'sum'})
        main = main[main['ClaseRentabilidad']==clase_rentabilidad]
        main.drop(columns='ClaseRentabilidad', inplace=True)
        main['Variable'] = variable
        return main

    def calculo_porcentaje_cumplimiento(self, main, variable, tipo_empleado):

        main = main.groupby(
            ['TipoEmpleado', 'Fecha', 'AreaCalculo', 'Consecutivo', 'Variable'],
            as_index=False
        ).agg({'Real': 'sum', 'Presupuesto': 'sum'})

        main['PorcentajeCumplimiento'] = (
            main['Real']/
            main['Presupuesto'])
        main['PorcentajeCumplimiento'] = (
            main['PorcentajeCumplimiento'].apply(lambda x: round(x, 2))
        )
        ### Cálculo de resultados cualitativos precargados
        main = self.sobreescribir_resultados_cualitativos(
            main=main,
            variable=variable,
            columnas_extra=['TipoEmpleado', 'AreaCalculo'], 
            columnas_extra_merge=['TipoEmpleado', 'AreaCalculo'],
            pre_merge_lambda=lambda x: x[x['TipoEmpleado'] == tipo_empleado]
        )
        ###
        main['Consecutivo'] = main['Consecutivo'].astype(int).astype('str')
        main['AreaCalculo'] = main['AreaCalculo'].astype(int).astype('str')

        main['Contexto'] = tipo_empleado.lower()+'_' + main['AreaCalculo'] + '_' +main['Consecutivo']
        main = main[['Contexto', 'Variable', 'Fecha', 'Real', 'Presupuesto', 'PorcentajeCumplimiento']]
        return main