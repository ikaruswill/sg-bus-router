# sg-bus-router
A better way to bus-surf in Singapore.

Finds the shortest bus-only path between two locations defined by GPS coordinates or 5-digit bus stop codes.

Uses a modified multi-origin A* algorithm with a custom cost function.


## Usage overview
1. Request an API key from LTA:
    - Application is instantaneous and free
    - [Click here to apply](https://www.mytransport.sg/content/mytransport/home/dataMall/request-for-api.html)
2. Save api key as python variable in keys.py in project root
    - api_key = 'paste_api_key_here'
3. Download most up-to-date dataset
    - Run update_dataset.sh
4. Run route.py



## Route.py usage
Use the command line argument _-h_ or _--help_ at any point to display the help message.

usage: python route.py [-v] [-t TRANSFER_PENALTY] {coords,codes}


## Modes
* __coords__: Find the shortest bus route between 2 GPS coordinates

* __codes__: Find the shortest bus route between 2 bus stop codes



## Global optional arguments

* __-v__: set verbose flag

* __-t TRANSFER_PENALTY__: distance in km you would rather travel on the bus as opposed to spending the time & effort to make a transfer to another bus service



## Mode: coords
usage: python route.py coords [-h] [-o LAT LON] [-g LAT LON] [-r RADIUS]

* __-o LAT LON__: origin lattitude and longitude in degrees

* __-g LAT LON__: goal lattitude and longitude in degrees

#### Optional arguments

* __-r RADIUS__: radius from both origin and goal you are willing to walk to/from a bus stop


## Mode: codes
usage: python route.py codes [-h] [-o ORIGIN [ORIGIN ...]] [-g GOAL]

* __-o ORIGIN [ORIGIN ...]__: possible origin bus stop codes separated by spaces

* __-g GOAL__: destination bus stop code
