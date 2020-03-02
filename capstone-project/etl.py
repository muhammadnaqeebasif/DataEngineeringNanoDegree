# Importing the libraries
from aws_configuration_parser import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as fn

import os

os.environ['AWS_ACCESS_KEY_ID']= ACCESS_KEY
os.environ['AWS_SECRET_ACCESS_KEY']=SECRET_KEY


def create_spark_session():
    """ Creates Spark Session

    Returns
    -------
    pyspark.sql.session.SparkSession
        SparkSession
    """
    packages = ['com.qubole.spark/spark-sql-kinesis_2.11/1.1.3-spark_2.4',
                'com.databricks:spark-redshift_2.11:2.0.1',
                'org.apache.hadoop:hadoop-aws:2.7.0']
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", ','.join(packages)) \
        .getOrCreate()
    return spark

def process_batch_data(spark,input_prefix, output_prefix):
    """ Processes the batch data stored in S3 bucket with key prefix specified by input_prefix and writes the result in
        S3 bucket with key prefix specified by output_prefix

        Parameters
        ----------
        spark : pyspark.sql.session.SparkSession
            Spark Session
        input_prefix : str
            The path in the S3 Bucket where the data is stored
        output_prefix : str
            The path is the S3 Bucket to where result is to be written
        Returns
        -------
        None
        """
    # Get forces data
    forces = spark.read.json(f"s3a://{S3['BUCKET']}/{input_prefix}/forces.json")

    # Get forces description
    forces_description = spark.read.json(f"s3a://{S3['BUCKET']}/{input_prefix}/forces_description.json")

    # Extracting the data from forces_description making it simple
    forces_description = forces_description.select('id', 'name', 'telephone', 'url',
                                                   fn.explode('engagement_methods').alias('engagement_method')) \
                                           .selectExpr('id AS force_id', 'name AS force_name', 'telephone', 'url',
                                                       'engagement_method.description AS engagement_method_description',
                                                       'engagement_method.title AS engagement_method_title',
                                                       'engagement_method.url AS engagement_method_url')

    # Creating dimension table for the forces by joining forces and forces_descriptions together
    dim_forces = forces.join(forces_description,
                             [forces.id == forces_description.force_id, forces.name == forces_description.force_name],
                             how='left')

    # dropping the duplicate columns
    dim_forces = dim_forces.drop('force_id', 'force_name')

    # Writing the dimension table according to output prefix specified
    dim_forces.write.mode('overwrite').json(f"s3a://{S3['Bucket']}/{output_prefix}/dim_forces")

    # Reading the senior officers data
    senior_officers = spark.read.json(f"s3a://{S3['BUCKET']}/{input_prefix}/senior_officers.json")

    # Writing the table according to the output prefix specified
    senior_officers.selectExpr('force_id', 'name', 'rank', 'bio',
                               'contact_details.email AS email',
                               'contact_details.telephone AS telephone',
                               'contact_details.twitter AS twitter',
                               'contact_details.website AS website'
                               )\
                    .write.mode('overwrite')\
                    .json(f"s3a://{S3['Bucket']}/{output_prefix}/senior_officers")

    # Reading the neighborhood data
    neighborhoods = spark.read.json(f"s3a://{S3['BUCKET']}/{input_prefix}/neighborhoods.json")

    # Reading the data containing the description of the neighborhood
    specific_neighborhoods = spark.read.json(f"s3a://{S3['BUCKET']}/{input_prefix}/specific_neighborhood.json")

    # Extracting the police stations locations in the specific neighborhood
    neighborhood_locations = specific_neighborhoods.select(fn.col('id'),
                                                              fn.explode('locations').alias('location')) \
                                                   .selectExpr('id AS neighborhood_id',
                                                               'location.address AS address',
                                                               'location.description AS description',
                                                               'location.latitude AS latitude',
                                                               'location.longitude AS longitude',
                                                               'location.name AS location_name',
                                                               'location.postcode AS postcode',
                                                               'location.type AS type') \
                                                   .filter("""
                                                            address IS NOT NULL OR
                                                            description IS NOT NULL OR
                                                            latitude IS NOT NULL OR
                                                            longitude IS NOT NULL OR
                                                            location_name IS NOT NULL OR
                                                            postcode IS NOT NULL OR
                                                            type IS NOT NULL
                                                        """)
    neighborhood_locations.write.mode('overwrite')\
                    .json(f"s3a://{S3['Bucket']}/{output_prefix}/neighborhood_locations")

    # dropping locations column from the specific neighborhood
    specific_neighborhoods = specific_neighborhoods.drop('locations')

    # Extracting the required columns from the spcific neighborhood
    specific_neighborhoods = specific_neighborhoods.selectExpr('id AS neighborhood_id',
                                                               'name AS neighborhood_name',
                                                               'population',
                                                               'contact_details.address AS address',
                                                               'contact_details.`e-messaging` AS e_messaging',
                                                               'contact_details.email AS email',
                                                               'contact_details.facebook AS facebook',
                                                               'contact_details.fax AS fax',
                                                               'contact_details.rss AS rss',
                                                               'contact_details.telephone AS telephone',
                                                               'contact_details.twitter AS twitter',
                                                               'contact_details.website AS website',
                                                               'contact_details.youtube AS youtube',
                                                               'centre.latitude AS centre_latitude',
                                                               'centre.longitude AS centre_longitude')

    # Creating dimension table for the neighborhoods by joining neighborhoods and specific_neighborhoods together
    dim_neighborhoods = neighborhoods.join(specific_neighborhoods,
                                           on=[neighborhoods.id == specific_neighborhoods.neighborhood_id,
                                               neighborhoods.name == specific_neighborhoods.neighborhood_name ],
                                           how='left').drop('neighborhood_id','neighborhood_name')

    # Writing the neighborhood dimension table according to the output prefix provided
    dim_neighborhoods.write.mode('overwrite').json(f"s3a://{S3['Bucket']}/{output_prefix}/dim_neighborhoods")

    # Reading data for the boundaries for each neighborhoods
    neighborhood_boundaries = spark.read.json(f"s3a://{S3['BUCKET']}/{input_prefix}/neighborhood_boundaries.json")

    # Extracting required columns from the specific neighborhood
    neighborhood_boundaries= neighborhood_boundaries.select('neighborhood_id',
                                                            fn.explode('boundaries').alias('boundaries')) \
                                                    .selectExpr('neighborhood_id',
                                                                'boundaries.latitude AS latitude',
                                                                'boundaries.longitude AS longitude')

    # Writing the neighborhood boundaries table according to the output prefix provided
    neighborhood_boundaries.write.mode('overwrite').json(f"s3a://{S3['Bucket']}/{output_prefix}/neighborhood_boundaries")




def main():
    """ Main function to call

    Returns
    -------
    None
    """
    spark = create_spark_session()

    process_batch_data(spark,S3['batched_key'],S3['batched_processed_key'])

if __name__ == '__main__':
    main()
