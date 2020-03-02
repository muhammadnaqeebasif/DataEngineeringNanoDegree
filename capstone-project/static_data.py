from data_create_helpers import *
import os
from plugins.aws_configuration_parser import *

if __name__ == '__main__':

    # creating aws configuration object
    aws_configs = AwsConfigs('credentials/credentials.csv', 'credentials/resources.cfg')

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

    # Getting the neighborhoods from the API
    neighborhoods = create_static_data('data/neighborhoods.json',
                                       lambda force: f"https://data.police.uk/api/{force['id']}/neighbourhoods",
                                       forces,
                                       lambda force, d: dict(d, **{'force_id': force['id']}))

    # Getting the information about specific neighborhoods from the API
    specific_neighborhoods = create_static_data('data/specific_neighborhood.json',
                                                lambda neighborhood: f"https://data.police.uk/api/{neighborhood['force_id']}/{neighborhood['id']}",
                                                neighborhoods)

    # Getting the boundaries of neighborhood from the API
    neighborhood_boundaries = create_static_data('data/neighborhood_boundaries.json',
                                                 lambda
                                                     neighborhood: f"https://data.police.uk/api/{neighborhood['force_id']}/{neighborhood['id']}/boundary",
                                                 neighborhoods,
                                                 lambda neighborhood, b: {'neighborhood_id': neighborhood['id'],
                                                                          'boundaries': b},
                                                 convert_list=True)

    # Getting the neighborhood teams from the API
    neighborhood_teams = create_static_data('data/neighborhood_teams.json',
                                            lambda neighborhood: f"https://data.police.uk/api/{neighborhood['force_id']}/{neighborhood['id']}/people",
                                            neighborhoods,
                                            lambda neighborhood, d: dict(d, **{'neighbour_id': neighborhood['id']}))

    # uploading the files to the S3 bucket
    for file in [file for file in os.listdir('data') if '.json' in file]:
        upload_to_s3(f'data/{file}',
                     aws_configs.S3['BUCKET'],
                     f"{aws_configs.S3['batched_key']}/{file}",
                     aws_configs.ACCESS_KEY,
                     aws_configs.SECRET_KEY,
                     aws_configs.REGION)