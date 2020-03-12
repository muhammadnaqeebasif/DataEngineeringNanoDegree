## Udacity Data Engineering Capstone-Project
### Project Description
UK Police has a great dataset that provides open data about police, crimes and outcomes in England, Wales and 
Northern Ireland and is available on [data.police.uk](data.police.uk). We can also filter the data based on regions, cities, neighborhoods, 
coordinates, officer names, crime category etc. inside the United Kingdom. 

The purpose of this project is to the fetch the data about the crimes from the api offered by 
UK Police. ETL pipeline is then built which extracts the data from the fetched data,
stages the data in Redshift and transforms the data into a set of dimensional tables. The database and ETL pipeline will
then be tested by running queries.

Further documentation about the API can be found [here](https://data.police.uk/docs/)

### Data Sources
* **Forces**: A list of all the police forces available via the API except the British Transport Police, which is 
excluded from the list returned. Further documentation of the data can be found 
[here](https://data.police.uk/docs/method/forces/)
* **Specific Force**: A list of forces and the description of the forces available via the API. Further documentation
of the data can be found [here](https://data.police.uk/docs/method/force/)
* **Senior Officers**: A list of all the senior officers of the specific force availabe via the API. Its documentation can
be found [here](https://data.police.uk/docs/method/senior-officers/)
* **Neighborhoods**: A list of all the neighborhoods for a force available via the API. Further the documentation of 
the data can be found [here](https://data.police.uk/docs/method/neighbourhoods/)
* **Specific neighbourhood**: A list of neighborhoods and the descriptions of the neighborhoods available via the API. 
Further documentation of the data can be found [here](https://data.police.uk/docs/method/neighbourhood/)
* **Neighbourhood boundary**: A list of latitude/longitude pairs that make up the boundary of a neighbourhood 
available via the API. Further documentation of the data can be found
 [here](https://data.police.uk/docs/method/neighbourhood-boundary/)
* **Street-level crimes**: Crimes at street-level; either within a 1 mile radius of a single point,
 or within a custom area available via the API. Further documentation of the data can be found
 [here](https://data.police.uk/docs/method/crime-street/)
* **Outcomes for a specific crime**: Returns the outcomes (case history) for the specified crime. Crime ID is 
64-character identifier, as returned by other API methods. Further documentation can be found 
[here](https://data.police.uk/docs/method/outcomes-for-crime/)

### Exploration and Assessing the data
