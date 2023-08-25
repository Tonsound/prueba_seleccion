from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
import pyspark.sql.functions as f
import os



class TestRipley:
    def __init__(self, files_path):
        self.files_path = files_path
        self.registries = []
        self.counter = 0
        self.spark = SparkSession.builder.appName("Ripley_Test").getOrCreate()

    # Funcion para limpiar la data
    def clean_data(self, df):
        df_clean = df.filter(col("Price").cast("double").isNotNull())
        df_clean = df_clean.dropDuplicates(["Index"])
        return df_clean
    
    # Test para checkear que los valores distintos al precio no se hayan actualizado
    def just_price_update_test(self, df1, df2, df3, df_final):
        intersection_df1_df2_df3 =  df1.join(df2.select("Index"), on="Index", how="inner").join(df3.select("Index"), on="Index", how="inner")        
        if df_final.join(intersection_df1_df2_df3.select("Index"), on="Index", how="inner").select("Amount").exceptAll(intersection_df1_df2_df3.select("Amount")).isEmpty():
            log = "Al añadir los archivos, se ha modificado solo el precio"
            print(log)
            self.registries.append(log)  

    # Test para checkear que el precio se haya actualizado correctamente
    def price_update_test(self, df1, df2, df3, df_final):
        index_1 = df1.select("Index")
        index_2 = df2.select("Index")
        index_3 = df3.select("Index")
        # Existen 3 casos, el primeroes que hay index y valor no nulo del 1 archivo, pero no del archivo 2 y 3. En ese caso el valor del price del archivo 1 debe ser igual al archivo final
        df1_sin_df2_df3 =  df1.join(index_1.exceptAll(index_3).exceptAll(index_2), on="Index", how="inner")
        intersecting_indices = df_final.join(df1_sin_df2_df3.select("Index"), on="Index", how="inner")
        if intersecting_indices.select("Index", "Price").orderBy("Index").exceptAll(df1_sin_df2_df3.select("Index", "Price").orderBy("Index")).isEmpty():
            log = "Los precios del primer archivo estan actualizados en el precio final"
            print(log)
            self.registries.append(log)
        # El segundo es que hay index y valor no nulo del 2 archivo, pero no del archivo 3. En ese caso el valor del price del archivo 2 debe ser igual al archivo final
        df2_sin_3 =  df2.join(index_2.exceptAll(index_3), on="Index", how="inner")
        intersecting_indices = df_final.join(df2_sin_3.select("Index"), on="Index", how="inner")
        if intersecting_indices.select("Index", "Price").orderBy("Index").exceptAll(df2_sin_3.select("Index", "Price").orderBy("Index")).isEmpty():
            log = "Los precios del segundo archivo estan actualizados en el precio final"
            print(log)
            self.registries.append(log)
        # El tercero es que hay index y valor no nulo del 3 archivo, en ese caso debe ser igual al archivo final
        intersecting_indices = df_final.join(df3.select("Index"), on="Index", how="inner")
        if intersecting_indices.select("Index", "Price").orderBy("Index").exceptAll(df3.select("Index", "Price").orderBy("Index")).isEmpty():
            log = "Los precios del tercer archivo estan actualizados en el precio final"
            print(log)
            self.registries.append(log)



    # Test para checkear que no existan duplicados en el archivo final 
    def duplicates_test(self, df):
        length_ini = df.count()
        df = df.dropDuplicates(["Index"])
        length_fin = df.count()
        log = f"Existian {length_ini - length_fin} registros duplicados"
        print(log)
        self.registries.append(log)

    # Test para checkear que los valores en precio no sean nulos y sean numericos 
    def no_null_numeric_price_test(self, df):
        length_ini = df.count()
        df = df.filter(col('Price').cast("double").isNotNull())
        length_fin = df.count()
        log = f"Se encontraron {length_ini - length_fin} registros con precio nulo o no numerico"
        print(log)
        self.registries.append(log)

    # Test para checkear que los datos eliminados incluyan su fecha de eliminación
    def delete_date_ok(self, df_final, df_delete):
        df_filtered = df_final.join(df_delete.select("Index"), on="Index", how="inner").select("Index", "delete_time").orderBy("Index")
        if df_filtered.exceptAll(df_delete).isEmpty():
            log = "Todos los registros borrados están añadidos al registro final"
            print(log)
            self.registries.append(log)

    # Test para contabilizar el monto existente en la columna precio
    def count_price(self, df):
        sum_df = df.withColumn("Price", col("Price").cast("double")).agg(sum("Price")).collect()[0][0]
        log = f"La suma de todos los valores de la columna precio es: {sum_df}."
        print(log)
        self.registries.append(log)


    # Función para guardar logs
    def save_logs(self, lista_logs):
        with open("logs/testing_logs.txt", "w") as file:
            for string in lista_logs:
                file.write(string + "\n")

    # Función General que lleva a cabo todo el proceso 
    def process(self, file_init, file_0, file_1, file_delete, file_result):
        df1 = self.spark.read.parquet(files_path + file_init)
        df2 = self.spark.read.parquet(files_path + file_0)
        df3 = self.spark.read.parquet(files_path + file_1)
        df_delete = self.spark.read.parquet(files_path + file_delete)
        df_final = self.spark.read.parquet(files_path + file_result)
        df1 = self.clean_data(df1)
        df2 = self.clean_data(df2)
        df3 = self.clean_data(df3)
        self.just_price_update_test( df1, df2, df3, df_final)
        self.delete_date_ok(df_final, df_delete)
        self.duplicates_test( df_final)
        self.no_null_numeric_price_test(df_final)
        self.price_update_test(df1, df2, df3, df_final)
        self.count_price(df_final)
        self.save_logs(self.registries)
        self.spark.stop()
        

if __name__ == "__main__":
    files_path = os.getcwd()
    # Archivos
    file_init = "/input/house_prices_20230801.parquet"
    file_0 = "/input/house_prices_20230901.parquet"
    file_1 = "/input/house_prices_20231001.parquet"
    file_delete = "/input/house_prices_deletes.parquet"
    file_result = "/output/house_prices_final.parquet"
    # Función
    data_processor = TestRipley(files_path)
    data_processor.process(file_init, file_0, file_1, file_delete, file_result)