"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""

import pandas as pd
import re as re


def load_data(filename):
    data = pd.read_fwf(
        filename,
        skiprows=4,
        widths=[7, 6, 19, 87],
        header=None,
        index_col=False,
        names=[
            "cluster",
            "cantidad_de_palabras_clave",
            "porcentaje_de_palabras_clave",
            "principales_palabras_clave",
        ],
        decimal=".",
    )
    return data


def clean_cluster_column(data):
    data = data.copy()
    data["cluster"] = data["cluster"].ffill()
    data["principales_palabras_clave"] = data.groupby(["cluster"])[
        "principales_palabras_clave"
    ].transform(lambda x: " ".join(x))
    data = data.drop_duplicates(subset=["cluster"])
    data = data.reset_index(drop=True)
    data["cluster"] = data["cluster"].astype(int)

    return data


def clean_principales_palabras_clave_column(data):
    data = data.copy()
    data["principales_palabras_clave"] = data["principales_palabras_clave"].str.strip(
        "."
    )
    data["principales_palabras_clave"] = data["principales_palabras_clave"].replace(
        r"\s+", " ", regex=True
    )
    return data


def clean_cantidad_de_palabras_clave_column(data):
    data = data.copy()
    data["cantidad_de_palabras_clave"] = data["cantidad_de_palabras_clave"].astype(int)

    return data


def clean_porcentaje_de_palabras_clave_column(data):
    data = data.copy()
    data["porcentaje_de_palabras_clave"] = data[
        "porcentaje_de_palabras_clave"
    ].str.strip("%")
    data["porcentaje_de_palabras_clave"] = data[
        "porcentaje_de_palabras_clave"
    ].str.replace(",", ".")
    data["porcentaje_de_palabras_clave"] = data["porcentaje_de_palabras_clave"].astype(
        float
    )

    return data


def ingest_data():
    data = load_data("./clusters_report.txt")
    data = clean_cluster_column(data)
    data = clean_principales_palabras_clave_column(data)
    data = clean_cantidad_de_palabras_clave_column(data)
    data = clean_porcentaje_de_palabras_clave_column(data)

    return data
