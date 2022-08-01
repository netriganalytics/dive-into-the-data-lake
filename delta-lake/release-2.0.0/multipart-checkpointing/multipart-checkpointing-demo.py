import pyspark
from delta import *
import random


class DeltaLakeMultipartCheckpointing:
    """Delta Lake protocol allows splitting the checkpoint into multiple Parquet files. By default, each checkpoint file
    is written as a single Parquet file. However, by  splitting the checkpoint into multiple files, we can parallelize
    and speed up the checkpoint write."""
    TABLE_NAME = 'default.airbnb_nyc'

    def __init__(self):
        builder = pyspark.sql.SparkSession.builder.appName("DeltaLakeMultipartCheckpointing") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()

        # To use this feature, we set the SQL configuration `spark.databricks.delta.checkpoint.partSize=n`,
        # where `n` is the max number of actions (e.g. AddFile) per checkpoint file. In other words,
        # Delta Lake will attempt to write a maximum of this many actions per checkpoint file in parallel.
        self.spark.sql("SET spark.databricks.delta.checkpoint.partSize=10")  # 10 actions per checkpoint file
        #self.spark.sql("SET spark.databricks.delta.checkpoint.partSize=1")  # 1 actions per checkpoint file

    def create_airbnb_delta_table(self):
        """
        Creates a Delta Table using the new TableBuilder API.
        """
        DeltaTable.createIfNotExists(self.spark) \
            .tableName(self.TABLE_NAME) \
            .addColumn("id", "INT") \
            .addColumn("name", "STRING") \
            .addColumn("host_id", "INT") \
            .addColumn("host_name", "STRING") \
            .addColumn("neighbourhood_group", "STRING") \
            .addColumn("neighbourhood", "STRING") \
            .addColumn("latitude", "FLOAT") \
            .addColumn("longitude", "FLOAT") \
            .addColumn("room_type", "STRING") \
            .addColumn("price", "INT") \
            .addColumn("minimum_nights", "INT") \
            .addColumn("number_of_reviews", "INT") \
            .addColumn("last_review", "STRING") \
            .addColumn("reviews_per_month", "FLOAT") \
            .addColumn("calculated_host_listings_count", "INT") \
            .addColumn("availability_365", "INT") \
            .execute()

    def randomly_insert_data(self):
        """
        NYC Airbnb open dataset:
        https://www.kaggle.com/datasets/dgomonov/new-york-city-airbnb-open-data?select=AB_NYC_2019.csv
        """
        airbnb_data = [[
            (2539,'Clean & quiet apt home by the park',2787,'John','Brooklyn','Kensington',40.64749,-73.97237,'Private room',149,1,9,'2018-10-19',0.21,6,365),
            (2595,'Skylit Midtown Castle',2845,'Jennifer','Manhattan','Midtown',40.75362,-73.98377,'Entire home/apt',225,1,45,'2019-05-21',0.38,2,355),
            (3647,'THE VILLAGE OF HARLEM....NEW YORK !',4632,'Elisabeth','Manhattan','Harlem',40.80902,-73.9419,'Private room',150,3,0,'2019-05-21',0,1,365),
            (3831,'Cozy Entire Floor of Brownstone',4869,'LisaRoxanne','Brooklyn','Clinton Hill',40.68514,-73.95976,'Entire home/apt',89,1,270,'2019-07-05',4.64,1,194),
            (5022,'Entire Apt: Spacious Studio/Loft by central park',7192,'Laura','Manhattan','East Harlem',40.79851,-73.94399,'Entire home/apt',80,10,9,'2018-11-19',0.10,1,0)
        ],[
            (5099,'Large Cozy 1 BR Apartment In Midtown East',7322,'Chris','Manhattan','Murray Hill',40.74767,-73.975,'Entire home/apt',200,3,74,'2019-06-22',0.59,1,129),
            (5121,'BlissArtsSpace!',7356,'Garon','Brooklyn','Bedford-Stuyvesant',40.68688,-73.95596,'Private room',60,45,49,'2017-10-05',0.40,1,0),
            (5178,'Large Furnished Room Near Bway' ,8967,'Shunichi','Manhattan','Hells Kitchen',40.76489,-73.98493,'Private room',79,2,430,'2019-06-24',3.47,1,220),
            (5203,'Cozy Clean Guest Room - Family Apt',7490,'MaryEllen','Manhattan','Upper West Side',40.80178,-73.96723,'Private room',79,2,118,'2017-07-21',0.99,1,0),
            (5238,'Cute & Cozy Lower East Side 1 bdrm',7549,'Ben','Manhattan','Chinatown',40.71344,-73.99037,'Entire home/apt',150,1,160,'2019-06-09',1.33,4,188)
        ],[
            (5295,'Beautiful 1br on Upper West Side',7702,'Lena','Manhattan','Upper West Side',40.80316,-73.96545,'Entire home/apt',135,5,53,'2019-06-22',0.43,1,6),
            (5441,'Central Manhattan/near Broadway',7989,'Kate','Manhattan','Hells Kitchen',40.76076,-73.98867,'Private room',85,2,188,'2019-06-23',1.50,1,39),
            (5803,'Lovely Room 1, Garden, Best Area, Legal rental',9744,'Laurie','Brooklyn','South Slope',40.66829,-73.98779,'Private room',89,4,167,'2019-06-24',1.34,3,314),
            (6021,'Wonderful Guest Bedroom in Manhattan for SINGLES',11528,'Claudio','Manhattan','Upper West Side',40.79826,-73.96113,'Private room',85,2,113,'2019-07-05',0.91,1,333),
            (6090,'West Village Nest - Superhost',11975,'Alina','Manhattan','West Village',40.7353,-74.00525,'Entire home/apt',120,90,27,'2018-10-31',0.22,1,0)
        ]]
        random_index = random.choice(range(0, len(airbnb_data) - 1))
        schema = 'id int, name string, host_id int, host_name string, neighbourhood_group string, ' \
                 'neighbourhood string, latitude float, longitude float, room_type string, price int, ' \
                 'minimum_nights int, number_of_reviews int, last_review string, reviews_per_month float, ' \
                 'calculated_host_listings_count int, availability_365 int'
        new_data_df = self.spark.createDataFrame(airbnb_data[random_index], schema)
        new_data_df.write.format("delta").mode("append").saveAsTable(self.TABLE_NAME)


if __name__ == '__main__':
    demo = DeltaLakeMultipartCheckpointing()
    demo.create_airbnb_delta_table()
    # Delta Lake will write a checkpoint file for every 10 commits to the transaction log
    for i in range(0, 10):
        print('Appending new data to Delta Lake table...')
        demo.randomly_insert_data()