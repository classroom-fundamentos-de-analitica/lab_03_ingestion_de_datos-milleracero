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
    Función para leer y procesar datos desde un archivo de texto fijo (clusters_report.txt).
    Transforma el contenido en un DataFrame de pandas con columnas específicas.
    """
    # Leer el archivo de texto con anchos de columna fijos
    data_frame = pd.read_fwf(
        "clusters_report.txt",
        widths=[9, 16, 16, 80],  # Anchos de las columnas
        header=None,
        names=[
            "cluster",  # Número del clúster
            "num_palabras_clave",  # Cantidad de palabras clave
            "porcentaje_palabras",  # Porcentaje de palabras clave
            "texto_largo",  # Columna con texto largo
        ],
        skip_blank_lines=False,
        converters={
            "porcentaje_palabras": lambda x: x.rstrip(" %").replace(",", ".")  # Convertir porcentaje a float
        },
    ).drop([0, 1, 2, 3], axis=0)  # Eliminar las primeras 4 filas (metadatos)

    texto_largo_col = data_frame["texto_largo"]  # Guardar la columna de texto largo
    data_frame = data_frame[data_frame["cluster"].notna()].drop("texto_largo", axis=1)  # Eliminar columna de texto largo
    data_frame = data_frame.astype(
        {
            "cluster": int,
            "num_palabras_clave": int,
            "porcentaje_palabras": float,
        }
    )

    palabras_clave_list = []  # Lista para almacenar las palabras clave principales
    texto_acumulado = ""
    for linea in texto_largo_col:
        if isinstance(linea, str):
            texto_acumulado += linea + " "  # Acumular texto si es una cadena de caracteres
        else:
            # Procesar texto acumulado eliminando espacios en blanco extras
            texto_acumulado = ", ".join([" ".join(x.split()) for x in texto_acumulado.split(",")])
            palabras_clave_list.append(texto_acumulado.rstrip("."))  # Agregar el texto procesado a la lista
            texto_acumulado = ""  # Reiniciar el acumulador
            continue

    # Añadir la columna de palabras clave principales al DataFrame
    data_frame["palabras_clave_principales"] = palabras_clave_list
    return data_frame


print(ingest_data())
