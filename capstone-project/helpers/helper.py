import requests
import time
import json
import os
from typing import Callable, Union
import boto3

def upload_to_s3(filename,Bucket,Key,ACCESS_KEY,SECRET_KEY,region):
    """Uploads the file to S3 bucket

    Parameters
    ----------
    filename: str
        File which we want to upload
    Bucket: str
        Bucket to which file needs to be uploaded
    Key : str
        The key to which file needs to be uploaded
    ACCESS_KEY: str
        Access key to access the resource
    SECRET_KEY: str
        Secret key to access the resource
    region: str
        The region to which the S3 resource is present
    Returns
    -------
    None

    """
    s3 = boto3.resource('s3',
                       region_name=region,
                       aws_access_key_id=ACCESS_KEY,
                       aws_secret_access_key=SECRET_KEY
                   )

    s3.meta.client.upload_file(filename,Bucket,Key)

def write_json_local(json_data:dict, path:str, mode:str='w') -> None:
    """Writes the json data to the path specified.

    Parameters
    ----------
    json_data : dict
        json data that needs to be written
    path : str
        path of the file to which the data needs to be written
    mode : str, optional
        The mode by which the file is to be opened for writing the data.
        The accepted values are 'a' and 'w', by default 'w'

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If mode is not in ['a','w']

    """
    with open(path, 'a') as f:
        for data in json_data:
            json.dump(data, f)
            f.write('\n')
    # if mode not in ['a','w']:
    #     raise ValueError(f"mode should be in ['a','w']")
    # with open(path,mode) as f:
    #     if len(json_data) <= 1:
    #         json.dump(json_data,f)
    #     else:
    #
    #     if mode =='a':
    #         f.write('\n')

def read_json_local(path:str) -> list:
    """Reads the json data from the path specified

    Parameters
    ----------
    path:str
        Path of the file from which the data is to be read

    Returns
    -------
    list
        The json data stored in the form of a list

    """
    json_data = None
    num_lines = sum(1 for line in open(path))
    with open(path,'r') as f:
        if num_lines <= 1:
            json_data = json.load(f)
        else:
            json_data = []
            for line in f:
                json_data.append(json.loads(line))
    return json_data

def read_from_url(url:str,func:Callable[[dict], dict]=None,
                  convert_list:bool=False) -> list:
    """Reads the data from the specified url.

    Parameters
    ----------
    url : str
        API url from which data is to be read
    func : Callable[[dict], dict], optional
        Function to apply a transformation on the data, by default None.
    convert_list : bool
        If True Converts the data into the list before applying the transformation,
        by default False.

    Returns
    -------
    list
        The data read from the API url
    """
    data = requests.get(url).json()
    if (not isinstance(data,list)) or convert_list:
        data = [data]
    if func:
        data = list(map(func,data))
    time.sleep(0.08)
    return data

def create_static_data(path:str,
                       create_url:Union[str,Callable[[str], str]],
                       elements:list=None,
                       transform_func:Callable[[dict,dict], dict]=None,
                       convert_list:bool=False) :
    """Creates and returns the data by reading the data from API url.

    Parameters
    ----------
    path : str
        path of the file to which the data needs to be written
    create_url: Union[str,Callable[[str], str]]
        creates url for the API to read from. Can accept string or function.
    elements : list, optional
        If passed the data is fetched by iterating over the elements,
        by default None.
    transform_func: Callable[[dict,dict], dict], optional
        If sa function is passed then a transformation is applied to the data.
    convert_list : bool
        If True Converts the data into the list before applying the transformation,
        by default False.

    Returns
    -------
    list
        The data read from the API
    """
    if not os.path.exists(path):
        if elements is None:
            results = read_from_url(create_url)
            write_json_local(results,path)
        else:
            results = []
            for element in elements:
                try:
                    data = read_from_url(create_url(element),convert_list=convert_list)
                    if transform_func:
                        data = [transform_func(element,d) for d in data]
                    write_json_local(data,path,'a')
                    results = results + data
                except:
                    continue

        return results
    else:
        return read_json_local(path)
