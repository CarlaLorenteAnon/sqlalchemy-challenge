import numpy as np
import datetime as dt
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

engine = create_engine("sqlite:///hawaii.sqlite")
Base = automap_base()
Base.prepare(engine, reflect=True)

# Base.metadata.tables # Check tables, not much useful
# Base.classes.keys() # Get the table names

Measurement = Base.classes.measurement
Station = Base.classes.station

from flask import Flask, jsonify

app = Flask(__name__)

@app.route("/")
def home():
    return(
        f"Welcome to the main page!<br/>"
        f"The available routes are:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"Temperatures from the start date (yyyy-mm-dd):   /api/v1.0/yyyy-mm-dd<br/>"
        f"Temperatures from the start date until end date:   /api/v1.0/yyyy-mm-dd/yyyy-mm-dd<br/>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    session = Session(engine)
    selection = [Measurement.date, Measurement.prcp]
    precipitation_info = session.query(*selection)
    session.close()

    precipitation_list = []
    date_list = []
    for date, prcp in precipitation_info:
        precipitation_list.append(prcp)
        date_list.append(date)
    ziped = zip(date_list, precipitation_list)
    precipitation_dict = dict(ziped)
    return jsonify(precipitation_dict)

@app.route("/api/v1.0/stations")
def stations():
    session = Session(engine)
    selection = [Station.station]
    stations_info = session.query(*selection)
    session.close()

    stations_list = []
    for station in stations_info:
        stations_list.append(station)
    return jsonify(stations_list)


@app.route("/api/v1.0/tobs")
def tobs():
    session = Session(engine)

    #find what day it was one year ago
    most_recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    one_year_ago = dt.datetime.strptime(most_recent_date[0],"%Y-%m-%d") - dt.timedelta(days=365)

    #find most active station
    selection = [Measurement.station,func.count(Measurement.station)]
    active_stations = session.query(*selection).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).all()
    active_stations_df = pd.DataFrame(active_stations, columns = ["station", "Count"])
    max_count = active_stations_df["Count"].max()
    most_active_station = active_stations_df[active_stations_df["Count"] == max_count]
    id_most_active_station = most_active_station["station"][0]

    #select dates and temperaturefor the last year of data
    selection = [Measurement.station, Measurement.date, Measurement.tobs]
    temperatures_for_most_active_station = session.query(*selection).filter(Measurement.date >= one_year_ago)

    #Filter also for the most active station 
    temperatures_for_most_active_station_df = pd.DataFrame(temperatures_for_most_active_station, columns = ["Station","Date", "Temperature"])
    final_df = temperatures_for_most_active_station_df[temperatures_for_most_active_station_df["Station"] == most_active_station["station"][0]]
    final_df = final_df[["Date", "Temperature"]]
    #Create a dictionary
    tbos_dict = dict(final_df.values)

    session.close()

    return jsonify(tbos_dict)

@app.route("/api/v1.0/<start>")
def given_start(start):
    session = Session(engine)

    selection = [func.max(Measurement.tobs), func.min(Measurement.tobs), func.avg(Measurement.tobs)]
    prcp_and_dates = session.query(*selection).filter(Measurement.date >= start).all()

    final_list = []
    temp_dict = {}
    for max, min, avg in prcp_and_dates:
        temp_dict["Max"] = max
        temp_dict["Min"] = min
        temp_dict["Avg"] = avg
        final_list.append(temp_dict)

    session.close()

    return jsonify(final_list)

@app.route("/api/v1.0/<start>/<end>")
def given_start_and_end(start, end):
    session = Session(engine)

    selection = [func.max(Measurement.tobs), func.min(Measurement.tobs), func.avg(Measurement.tobs)]
    prcp_and_dates = session.query(*selection).filter(Measurement.date >= start).filter(Measurement.date <= end).all()

    final_list = []
    temp_dict = {}
    for max, min, avg in prcp_and_dates:
        temp_dict["Max"] = max
        temp_dict["Min"] = min
        temp_dict["Avg"] = avg
        final_list.append(temp_dict)

    session.close()

    return jsonify(final_list)

if __name__ == "__main__":
    app.run(debug=True)