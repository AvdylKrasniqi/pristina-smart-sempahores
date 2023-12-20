This project uses Nominatim API to get address name from reverse geocoding API. On docker-compose there is PBF_URL defined for Kosovo data.
if you start it for the first time, it would take around 10 to 20 minutes to import the data on postgres db. After that, there is a PHP web server exposed by default on port 8080 where you can actually make api requests.

Copy 1.csv to this project folder, then

To run the project: 
```sh
docker-compose build
docker-compose up
pip install -r requirements.txt
python3 preprocess.py  
python3 process.py
```
