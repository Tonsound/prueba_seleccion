from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
import pyspark.sql.functions as f
import os



class DataRipley:
    def __init__(self, files_path):
        self.files_path = files_path
        self.registries = []
        self.counter = 0
        self.spark = SparkSession.builder.appName("Ripley_Test").getOrCreate()
    
    # Elimina las files con valores nulos o no numerales en el precio y los duplicados en base a la columna Index
    def clean_data(self, filename):
        df = self.spark.read.parquet(self.files_path + filename)
        df_clean = df.filter(col("Price").cast("double").isNotNull())
        df_clean = df_clean.dropDuplicates(["Index"])
        length_ini = df.count()
        length_fin = df_clean.count()
        log = f"Limpiando {filename} se eliminaron {length_ini - length_fin} registros"
        print(log)
        self.registries.append(log)
        return df_clean
    
        # Test para contabilizar el monto existente en la columna precio
    def count_price(self, df):
        sum_df = df.withColumn("Price", col("Price").cast("double")).agg(sum("Price")).collect()[0][0]
        log = f"La suma de todos los valores de la columna precio es: {sum_df}."
        print(log)
        self.registries.append(log)

    
    # Esta función une 2 Dataframes, actualizando el valor de la columna Price en caso de que tengan la misma llave primaria (Index)
    def merge_data(self, df1, df2):
        df_difference = df1.select("Index").exceptAll(df2.select("Index"))
        df_difference = df1.join(df_difference, on="Index", how="inner")
        df_difference2 = df2.select("Index").exceptAll(df1.select("Index"))
        df_difference2 = df2.join(df_difference2, on="Index", how="inner")
        intersecting_indices = df1.select("Index").intersect(df2.select("Index"))
        interescting_df = df1.join(intersecting_indices, on="Index", how="inner")
        df2_just_price = df2.select("Index", "Price")
        df_interescted_no_price = interescting_df.drop("Price")
        df_intersected_final = df_interescted_no_price.join(df2_just_price, on ="Index", how="left").select(df1.columns)
        combined_df = df_difference.union(df_difference2).union(df_intersected_final)
        registros_añadidos = df_difference2.count()
        registros_actualizados = intersecting_indices.count()
        self.counter += 1
        log = f"Al agregar el archivo {str(self.counter)} añadieron {registros_añadidos} registros y se actualizaron {registros_actualizados} quedando un total de {combined_df.count()} registros"
        print(log)
        self.registries.append(log)
        return combined_df

    
    # Esta función añade la fecha de eliminación si es que esta existe en la data entregada
    def add_delete_time(self, df1, df_delete):
        updated_df = df1.join(df_delete, on ="Index", how="left")
        log = f"Se han borrado {df_delete.count()} propiedades, la fecha de eliminación fué añadida al archivo"
        print(log)
        self.registries.append(log)
        return updated_df

    # Función para guardar logs
    def save_logs(self, lista_logs):
        with open("logs/logs.txt", "w") as file:
            for string in lista_logs:
                file.write(string + "\n")

    # Función para guardar el archivo
    def save_file(self, df, file_path, nombre_archivo):
        updated_df_single_partition = df.repartition(1).coalesce(1)
        updated_df_single_partition.write.mode("append").parquet(file_path + nombre_archivo, mode="overwrite")
        parquet_files = [file for file in os.listdir("output") if file.endswith(".parquet")] #analizar buscar el -1
        print(parquet_files[0])
        os.rename( os.getcwd() + "/output/" + str(parquet_files[0]), os.getcwd() +  "/output/house_prices_final.parquet")
        log = "Se ha guardado el archivo final en la carpeta output bajo el nombre de house_prices_final.parquet"
        self.registries.append(log)

    # Función General que lleva a cabo todo el proceso 
    def process(self, file_init, file_0, file_1, file_delete):
        df_clean1 = self.clean_data(file_init)
        df_clean2 = self.clean_data(file_0)
        df_clean3 = self.clean_data(file_1)
        clean1_2 = self.merge_data(df_clean1, df_clean2)
        clean1_2_3 = self.merge_data(clean1_2 , df_clean3)
        df_delete = self.spark.read.parquet(self.files_path + file_delete)
        final_df = self.add_delete_time(clean1_2_3, df_delete)
        log_archivo_final = f"El archivo final tiene {final_df.count()} filas de datos"
        self.registries.append(log_archivo_final)
        self.count_price(final_df)
        # Guardando registros y archivo
        self.save_file(final_df, "output/", "")
        self.save_logs(self.registries)
        self.spark.stop()

if __name__ == "__main__":
    files_path = os.path.join(os.getcwd(), "input/") 
    # Archivos
    file_init = "house_prices_20230801.parquet"
    file_0 = "house_prices_20230901.parquet"
    file_1 = "house_prices_20231001.parquet"
    file_delete = "house_prices_deletes.parquet"
    # Función
    data_processor = DataRipley(files_path)
    data_processor.process(file_init, file_0, file_1, file_delete)