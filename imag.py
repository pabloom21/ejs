import pandas as pd
import matplotlib.pyplot as plt

# Datos de contenido_bd_2 inventados para comprobar funcionamiento
def procesar_datos(csv_file):
    # cargo el CSV y proceso los datos para obtener valores máximos por mes.
    df = pd.read_csv(csv_file, sep=';', encoding='utf-8')
    df['fecha_extraccion'] = pd.to_datetime(df['fecha_extraccion'])
    df['mes'] = df['fecha_extraccion'].dt.to_period('M')
    resumen = df.groupby('mes').agg(
        esquemas=('esquema', 'nunique'),
        tablas=('nombre', lambda x: x[df['tipo'] == 'table'].nunique()),
        vistas=('nombre', lambda x: x[df['tipo'].str.contains('view')].nunique()),
        registros=('registros', 'sum')
    ).reset_index()
    resumen.to_csv('resumen_datalake_2.csv', sep=';', index=False, encoding='utf-8')
    return resumen

def generar_graficos(resumen):
    #Genero y guardo los gráficos en un archivo PNG.
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    resumen['mes'] = resumen['mes'].astype(str)
    fig.suptitle('Evolución del uso del Datalake', fontsize=16)
    # Grafico los esquemas
    axes[0, 0].plot(resumen['mes'], resumen['esquemas'], marker='o', linestyle='-', color='b')
    axes[0, 0].set_title('Evolución de Esquemas')
    axes[0, 0].set_xticklabels(resumen['mes'], rotation=45)
    # Grafico las tablas
    axes[0, 1].plot(resumen['mes'], resumen['tablas'], marker='s', linestyle='-', color='g')
    axes[0, 1].set_title('Evolución de Tablas')
    axes[0, 1].set_xticklabels(resumen['mes'], rotation=45)
    # Grafico las vistas
    axes[1, 0].plot(resumen['mes'], resumen['vistas'], marker='^', linestyle='-', color='r')
    axes[1, 0].set_title('Evolución de Vistas')
    axes[1, 0].set_xticklabels(resumen['mes'], rotation=45)
    # Grafico los registros
    axes[1, 1].plot(resumen['mes'], resumen['registros'], marker='d', linestyle='-', color='m')
    axes[1, 1].set_title('Evolución de Registros')
    axes[1, 1].set_xticklabels(resumen['mes'], rotation=45)
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.savefig('evolucion_datalake_2.png')
    plt.show()

def generar_graficos_por_esquema(csv_file):
    # Cargo el CSV y agrupo los datos por mes y esquema
    df = pd.read_csv(csv_file, sep=';', encoding='utf-8')
    df['fecha_extraccion'] = pd.to_datetime(df['fecha_extraccion'])
    df['mes'] = df['fecha_extraccion'].dt.to_period('M')
    resumen_esquema = df.groupby(['mes', 'esquema']).agg(
        tablas=('nombre', lambda x: x[df['tipo'] == 'table'].nunique()),
        vistas=('nombre', lambda x: x[df['tipo'].str.contains('view')].nunique()),
        registros=('registros', 'sum')
    ).reset_index()
    resumen_esquema.to_csv('resumen_datalake_por_esquema_2.csv', sep=';', index=False, encoding='utf-8')
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    fig.suptitle('Evolución del uso del Datalake por Esquema', fontsize=16)
    # Grafico los esquemas totales (igual que antes)
    resumen_total = df.groupby('mes').agg(esquemas=('esquema', 'nunique')).reset_index()
    resumen_total['mes'] = resumen_total['mes'].astype(str)
    axes[0, 0].plot(resumen_total['mes'], resumen_total['esquemas'], marker='o', linestyle='-', color='b')
    axes[0, 0].set_title('Evolución de Esquemas')
    axes[0, 0].set_xticklabels(resumen_total['mes'], rotation=45)
    # Grafico la evolución de tablas por esquema
    for esquema in resumen_esquema['esquema'].unique():
        datos_esquema = resumen_esquema[resumen_esquema['esquema'] == esquema]
        axes[0, 1].plot(datos_esquema['mes'].astype(str), datos_esquema['tablas'], marker='s', linestyle='-', label=esquema)
    axes[0, 1].set_title('Evolución de Tablas por Esquema')
    axes[0, 1].set_xticklabels(resumen_total['mes'], rotation=45)
    axes[0, 1].legend()
    # Grafico la evolución de vistas por esquema
    for esquema in resumen_esquema['esquema'].unique():
        datos_esquema = resumen_esquema[resumen_esquema['esquema'] == esquema]
        axes[1, 0].plot(datos_esquema['mes'].astype(str), datos_esquema['vistas'], marker='^', linestyle='-', label=esquema)
    axes[1, 0].set_title('Evolución de Vistas por Esquema')
    axes[1, 0].set_xticklabels(resumen_total['mes'], rotation=45)
    axes[1, 0].legend()
    # Grafico la evolución de registros por esquema
    for esquema in resumen_esquema['esquema'].unique():
        datos_esquema = resumen_esquema[resumen_esquema['esquema'] == esquema]
        axes[1, 1].plot(datos_esquema['mes'].astype(str), datos_esquema['registros'], marker='d', linestyle='-', label=esquema)
    axes[1, 1].set_title('Evolución de Registros por Esquema')
    axes[1, 1].set_xticklabels(resumen_total['mes'], rotation=45)
    axes[1, 1].legend()
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.savefig('evolucion_datalake_por_esquema.png')
    plt.show()

def procesar_datos_por_dia(csv_file):
    # Cargo el CSV y proceso los datos para obtener valores máximos por día
    df = pd.read_csv(csv_file, sep=';', encoding='utf-8')
    df['fecha_extraccion'] = pd.to_datetime(df['fecha_extraccion'])
    df['anio'] = df['fecha_extraccion'].dt.year
    df['anio-mes'] = df['fecha_extraccion'].dt.to_period('M').astype(str)
    df['anio-mes-dia'] = df['fecha_extraccion'].dt.to_period('D').astype(str)
    
    resumen_dia = df.groupby(['anio', 'anio-mes', 'anio-mes-dia']).agg(
        esquemas=('esquema', 'nunique'),
        tablas=('nombre', lambda x: x[df['tipo'] == 'table'].nunique()),
        vistas=('nombre', lambda x: x[df['tipo'].str.contains('view')].nunique()),
        registros=('registros', 'sum')
    ).reset_index()
    resumen_dia.to_csv('resumen_datalake_2_por_dia.csv', sep=';', index=False, encoding='utf-8')
    return resumen_dia



if __name__ == '__main__':
    archivo_csv = 'contenido_bd_2.csv'
    datos_procesados = procesar_datos(archivo_csv)
    generar_graficos(datos_procesados)
    generar_graficos_por_esquema(archivo_csv)
    datos_por_día = procesar_datos_por_dia(archivo_csv)
