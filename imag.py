import pandas as pd
import matplotlib.pyplot as plt

# Código realizado usando un csv llamado contenido_bd_2 inventado, el real es contenido_bd
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

if __name__ == '__main__':
    archivo_csv = 'contenido_bd_2.csv'
    datos_procesados = procesar_datos(archivo_csv)
    generar_graficos(datos_procesados)
