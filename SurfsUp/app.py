# Import the dependencies.
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

import numpy as np
import pandas as pd
import datetime as dt

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine('sqlite:///Resources/hawaii.sqlite')

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
station = Base.classes.station
measurement = Base.classes.measurement

# Create our session (link) from Python to the DB
session = Session(bind=engine)


#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################
@app.route("/")
def homepage():
    #List all the available routes.
    return(
        f"Welcome to the Hawaii Climate API!<br/>"
        f"<br/>"
        f"Please refer to the following for the available routes:<br/>"
        f"<br/>"
        f"1.<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"Precipitation data for the most recent year available.<br/>"
        f"<br/>"
        f"2.<br/>"
        f"/api/v1.0/stations<br/>"
        f"Information about weather stations in Hawaii.<br/>"
        f"<br/>"
        f"3.<br/>"
        f"/api/v1.0/tobs<br/>"
        f"Temperature observations of station USC00519281 for the most recent year available.<br/>"
        f"<br/>"
        f"4.<br/>"
        f"/api/v1.0/start<br/>"
        f"Return the average, maximum, and minimum temperatures from the given start date to the end of the dataset, if a start date is provided.<br/>"
        f"<br/>"
        f"5.<br/>"
        f"/api/v1.0/start/end<br/>"
        f"Return the average, maximum, and minimum temperatures from the given start date to the given end date, if both dates are provided.<br/>"
        f"<br/>"
        f"Note: For the start date and end date, replace 'start' and 'end' with your desired dates in the format 'YYYY/MM/DD'."
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    # The last date in the dataset was obtained in the previous part.
    last_date = dt.date(2017, 8, 23)
    one_year_ago = last_date - dt.timedelta(days=365)
    
    # Query and retrieve the past one year precipitation data, and sort the result by date.
    one_year_prcp = session.query(measurement.date, measurement.prcp).\
        filter(measurement.date >= one_year_ago).order_by(measurement.date).all()
    
    # Convert the query result to a dictionary with the date as the key and the precipitation as the value.
    # And append it to a list called 'prcp_list'.
    prcp_list = []
    for date, prcp in one_year_prcp:
        prcp_dict = {date:prcp}
        prcp_list.append(prcp_dict)
    
    return jsonify(prcp_list)
        

@app.route("/api/v1.0/stations")
def stations():
    # Query and retrieve the data in the 'station' table.
    station_data = session.query(station).all()

     # Convert the query result to a dictionary and append it to a list called 'station_list'.
    station_list=[]
    for record in station_data:
        station_dict = {}
        station_dict['id'] = record.id
        station_dict['station'] = record.station
        station_dict['name'] = record.name
        station_dict['latitude'] = record.latitude
        station_dict['longitude'] = record.longitude
        station_dict['elevation'] = record.elevation
        station_list.append(station_dict)

    return jsonify(station_list)


@app.route("/api/v1.0/tobs")
def tobs():
    # Query the stations and their observation counts in descending order in order to obtain the most-active staion.
    station_active_list = session.query(measurement.station, func.count(measurement.station)).\
        group_by(measurement.station).order_by(func.count(measurement.station).desc()).all()
    most_active_station = station_active_list[0][0]
    
    # Query and retrieve the past one year temperature data of the most-active station.
    last_date = dt.date(2017, 8, 23)
    one_year_ago = last_date - dt.timedelta(days=365)
    one_year_tobs = session.query(measurement.date, measurement.tobs).\
        filter(measurement.station == most_active_station).filter(measurement.date >= one_year_ago).all()
    
    # Convert the query result to a dictionary with the date as the key and the tobs as the value.
    tobs_list = []
    for date, tobs in one_year_tobs:
        tobs_dict = {date:tobs}
        tobs_list.append(tobs_dict)
    
    return jsonify(tobs_list)   
    

@app.route("/api/v1.0/<start>")
def start(start):
    # Convert the user-entered start date to a date object.
    start_date = func.strftime("%Y-%m-%d", start)
    
    # Create a list collecting functions to calculate minimum, average, and maximum temperature.
    sel = [func.min(measurement.tobs), func.avg(measurement.tobs), func.max(measurement.tobs)]

    # Pass in the 'sel' list to the query and retrieve the data for the period from the given start date to the last date in the dataset.
    start_result = session.query(*sel).filter(measurement.date >= start_date).all()
    
    # Convert the query result to a dictionary and append it to a list called 'start_list'.
    start_list = []
    for min, avg, max in start_result:
        start_dict = {}
        start_dict['TMIN'] = min
        start_dict['TMAX'] = max
        start_dict['TAVG'] = avg
        start_list.append(start_dict)

    return jsonify(start_list)


@app.route("/api/v1.0/<start>/<end>")
def start_end(start, end):
    # Convert the user-entered start date and end date to date objects.
    start_date = func.strftime("%Y-%m-%d", start)
    end_date = func.strftime("%Y-%m-%d", end)

    # Create a list collecting functions to calculate minimum, average, and maximum temperature.
    sel = [func.min(measurement.tobs), func.avg(measurement.tobs), func.max(measurement.tobs)]

    # Pass in the 'sel' list and retrieve the data for the period from the given start date to the given end date.
    start_end_result = session.query(*sel).filter(measurement.date >= start_date).filter(measurement.date <= end_date).all()
    
    # Convert the query result to a dictionary and append it to the 'start_end_list' list.
    start_end_list = []
    for min, avg, max in start_end_result:
        start_end_dict = {}
        start_end_dict['TMIN'] = min
        start_end_dict['TMAX'] = max
        start_end_dict['TAVG'] = avg
        start_end_list.append(start_end_dict)

    return jsonify(start_end_list)


session.close()


if __name__ == '__main__':
    app.run(debug=True)