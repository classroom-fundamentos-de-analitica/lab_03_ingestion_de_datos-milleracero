"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():
    """
    Función para leer y procesar datos desde un archivo de ancho fijo (clusters_report.txt).
    """
    # Leer el archivo con anchos de columna fijos
    datos = pd.read_fwf("clusters_report.txt", widths=[9, 16, 16, 77])
    
    # Ajustar los nombres de las columnas
    datos.columns = datos.columns + " " + list(datos.iloc[0])
    datos.columns = [col.replace(" nan", "").replace(" ", "_").lower() for col in datos.columns]
    
    # Eliminar las dos primeras filas (cabeceras innecesarias)
    datos = datos.iloc[2:]
    datos.reset_index(inplace=True, drop=True)
    
    # Corregir la fila específica de la columna de palabras clave principales
    datos.iloc[23, 3] = datos.iloc[23, 3] + '.'
    columna_palabras = list(datos.iloc[:, 3]).copy()
    lista_palabras = []
    acumulado = ""

    for elemento in columna_palabras:
        if acumulado:  # Añadir el elemento actual a la cadena temporal
            acumulado += ' ' + elemento
        else:
            acumulado = elemento
        
        # Si el elemento actual termina en un punto, añadir a la nueva lista y resetear la cadena temporal
        if elemento.endswith('.'):
            lista_palabras.append(acumulado)
            acumulado = ""  # Reiniciar para el próximo grupo

    # Filtrar las filas donde la columna 'cluster' no sea NaN
    datos = datos[datos['cluster'].notna()]
    datos['principales_palabras_clave'] = lista_palabras
    
    # Eliminar dobles espacios y caracteres innecesarios
    datos['principales_palabras_clave'] = (
        datos['principales_palabras_clave']
        .str.replace('    ', ' ')
        .str.replace('   ', ' ')
        .str.replace('  ', ' ')
        .str.replace(',,', ',')
        .str.replace('.', '')
    )
    datos.reset_index(inplace=True, drop=True)
    
    # Convertir las columnas 'cluster' y 'cantidad_de_palabras_clave' a tipo numérico
    datos['cluster'] = datos['cluster'].astype(int)
    datos['cantidad_de_palabras_clave'] = datos['cantidad_de_palabras_clave'].astype(int)
    
    # Remover el símbolo de porcentaje y convertir 'porcentaje_de_palabras_clave' a tipo float
    datos['porcentaje_de_palabras_clave'] = (
        datos['porcentaje_de_palabras_clave']
        .str.replace(' %', '')
        .str.replace(",", ".")
        .astype(float)
    )
    
    return datos
