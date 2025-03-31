import pandas as pd
import matplotlib.pyplot as plt

# Año de referencia para el último año a analizar
ANIO_REFERENCIA = 2025

def procesar_datos(csv_file):
    df = pd.read_csv(csv_file, sep=';', encoding='utf-8')
    df['fecha_extraccion'] = pd.to_datetime(df['fecha_extraccion'])
    df['anio'] = df['fecha_extraccion'].dt.year
    df['anio-mes'] = df['fecha_extraccion'].dt.to_period('M').astype(str)
    # Resumen anual
    resumen_anual = df.groupby('anio').agg(
        esquemas=('esquema', 'nunique'),
        tablas=('nombre', lambda x: x[df['tipo'] == 'table'].nunique()),
        vistas=('nombre', lambda x: x[df['tipo'].str.contains('view')].nunique()),
        registros=('registros', 'sum')
    ).reset_index()
    resumen_anual.to_csv('resumen_datalake_anual_ult_2.csv', sep=';', index=False, encoding='utf-8')
    # Resumen mensual del último año específico
    df_ultimo_anio = df[df['anio'] == ANIO_REFERENCIA]
    resumen_mensual_ultimo_anio = df_ultimo_anio.groupby('anio-mes').agg(
        esquemas=('esquema', 'nunique'),
        tablas=('nombre', lambda x: x[df_ultimo_anio['tipo'] == 'table'].nunique()),
        vistas=('nombre', lambda x: x[df_ultimo_anio['tipo'].str.contains('view')].nunique()),
        registros=('registros', 'sum')
    ).reset_index()
    resumen_mensual_ultimo_anio.to_csv('resumen_datalake_mensual_ultimo_anio.csv', sep=';', index=False, encoding='utf-8')
    return resumen_anual, resumen_mensual_ultimo_anio

def generar_graficos(resumen_anual, resumen_mensual_ultimo_anio):
    fig, axes = plt.subplots(2, 4, figsize=(16, 8))
    fig.suptitle('Evolución del uso del Datalake', fontsize=16)
    # Gráficos mensuales del último año
    resumen_mensual_ultimo_anio['anio-mes'] = resumen_mensual_ultimo_anio['anio-mes'].astype(str)
    axes[0, 0].plot(resumen_mensual_ultimo_anio['anio-mes'], resumen_mensual_ultimo_anio['esquemas'], marker='o', linestyle='-', color='b')
    axes[0, 0].set_title('Esquemas - Último Año')
    axes[0, 0].tick_params(axis='x', rotation=45)
    axes[0, 1].plot(resumen_mensual_ultimo_anio['anio-mes'], resumen_mensual_ultimo_anio['tablas'], marker='s', linestyle='-', color='g')
    axes[0, 1].set_title('Tablas - Último Año')
    axes[0, 1].tick_params(axis='x', rotation=45)
    axes[0, 2].plot(resumen_mensual_ultimo_anio['anio-mes'], resumen_mensual_ultimo_anio['vistas'], marker='^', linestyle='-', color='r')
    axes[0, 2].set_title('Vistas - Último Año')
    axes[0, 2].tick_params(axis='x', rotation=45)
    axes[0, 3].plot(resumen_mensual_ultimo_anio['anio-mes'], resumen_mensual_ultimo_anio['registros'], marker='d', linestyle='-', color='m')
    axes[0, 3].set_title('Registros - Último Año')
    axes[0, 3].tick_params(axis='x', rotation=45)
    # Gráficos anuales
    resumen_anual['anio'] = resumen_anual['anio'].astype(str)
    axes[1, 0].plot(resumen_anual['anio'], resumen_anual['esquemas'], marker='o', linestyle='-', color='b')
    axes[1, 0].set_title('Esquemas - Anual')
    axes[1, 1].plot(resumen_anual['anio'], resumen_anual['tablas'], marker='s', linestyle='-', color='g')
    axes[1, 1].set_title('Tablas - Anual')
    axes[1, 2].plot(resumen_anual['anio'], resumen_anual['vistas'], marker='^', linestyle='-', color='r')
    axes[1, 2].set_title('Vistas - Anual')
    axes[1, 3].plot(resumen_anual['anio'], resumen_anual['registros'], marker='d', linestyle='-', color='m')
    axes[1, 3].set_title('Registros - Anual')
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.savefig('evolucion_datalake_ult_2.png')
    plt.show()

if __name__ == '__main__':
    archivo_csv = 'contenido_bd_2.csv'
    resumen_anual, resumen_mensual_ultimo_anio = procesar_datos(archivo_csv)
    generar_graficos(resumen_anual, resumen_mensual_ultimo_anio)
