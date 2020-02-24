from helpers import *
import os
import pandas as pd
from aws_configuration_parser import *

if __name__ == '__main__':

    if not os.path.exists('data'):
        os.makedirs('data')

    # Downloading the forces related data from the API
    # Getting the forces data from the API
    forces = create_static_data("data/forces.json",
                                "https://data.police.uk/api/forces")

    # Getting the description of the forces
    forces_description = create_static_data('data/forces_description.json',
                                            lambda force: f"https://data.police.uk/api/forces/{force['id']}",
                                            forces)

    # Getting the information about senior officers of each force
    senior_officers = create_static_data('data/senior_officers.json',
                                         lambda force: f"https://data.police.uk/api/forces/{force['id']}/people",
                                         forces,
                                         lambda force, d: dict(d, **{'force_id': force['id']}))

    # Downloading the Neighborhood Related data from the API
    counties = ['avon-and-somerset', 'bedfordshire', 'btp', 'cambridgeshire',
                'cheshire', 'city-of-london', 'cleveland', 'cumbria', 'derbyshire',
                'devon-and-cornwall', 'dorset', 'durham', 'dyfed-powys', 'essex',
                'gloucestershire', 'greater-manchester', 'gwent', 'hampshire',
                'hertfordshire', 'humberside', 'kent', 'lancashire', 'leicestershire',
                'lincolnshire', 'merseyside', 'metropolitan', 'norfolk', 'north-wales',
                'north-yorkshire', 'northamptonshire', 'northern-ireland', 'northumbria',
                'nottinghamshire', 'south-wales', 'south-yorkshire', 'staffordshire',
                'suffolk', 'surrey', 'sussex', 'thames-valley', 'warwickshire', 'west-mercia',
                'west-midlands', 'west-yorkshire', 'wiltshire']

    # Getting the neighborhoods from the API
    neighborhoods = create_static_data('data/neighborhoods.json',
                                       lambda county: f'https://data.police.uk/api/{county}/neighbourhoods',
                                       counties,
                                       lambda county, d: dict(d, **{'county': county}))

    # Getting the information about specific neighborhoods from the API
    specific_neighborhoods = create_static_data('data/specific_neighborhood.json',
                                                lambda neighborhood: f"https://data.police.uk/api/{neighborhood['county']}/{neighborhood['id']}",
                                                neighborhoods)

    # Getting the boundaries of neighborhood from the API
    neighborhood_boundaries = create_static_data('data/neighborhood_boundaries.json',
                                                 lambda
                                                     neighborhood: f"https://data.police.uk/api/{neighborhood['county']}/{neighborhood['id']}/boundary",
                                                 neighborhoods,
                                                 lambda neighborhood, b: {'neighborhood_id': neighborhood['id'],
                                                                          'boundaries': b},
                                                 convert_list=True)

    # Getting the neighborhood teams from the API
    neighborhood_teams = create_static_data('data/neighborhood_teams.json',
                                            lambda neighborhood: f"https://data.police.uk/api/{neighborhood['county']}/{neighborhood['id']}/people",
                                            neighborhoods,
                                            lambda neighborhood, d: dict(d, **{'neighbour_id': neighborhood['id']}))

    # uploading the files to the S3 bucket
    for file in [file for file in os.listdir('data') if '.json' in file]:
        upload_to_s3(f'data/{file}',
                     S3['BUCKET'],
                     f'static_data/{file}',
                     ACCESS_KEY,
                     SECRET_KEY,
                     REGION)