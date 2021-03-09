
import numpy as np
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func


from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(engine, reflect=True)
print()

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def Welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start_date/<br/>"
        f"/api/v1.0/start_date/end_date/<br/>"
        f"/<br>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    print("Server requested climate app precipitation page...")

    # Create a session link from Python to the db
    session = Session(engine)

    # perform a query to retrieve all the date and precipitation values 
    prcp_date = session.query(Measurement.date, Measurement.prcp).\
                order_by(Measurement.date).all()

    # close the session 
    session.close()

    # convert the query results into a dictionary using date as the key and prcp as the value 
    prcp_dict = {} 
    for date, prcp in prcp_date:
        prcp_dict[date] = prcp
    
    # Return the JSON representation of dictionary.
    return jsonify(prcp_dict)



@app.route("/api/v1.0/stations")
def stations():
    print("Server requested climate app station data...")

    # create a session from Python to the database 
    session = Session(engine)
    
    # perform a query to retrieve all the station data 
    results = session.query(Station.id, Station.station, Station.name).all()

    # close the session 
    session.close()

    # create a list of dictionaries with station info using for loop
    list_stations = []

    for st in results:
        station_dict = {}

        station_dict["id"] = st[0]
        station_dict["station"] = st[1]
        station_dict["name"] = st[2]

        list_stations.append(station_dict)

    # Return a JSON list of stations from the dataset.
    return jsonify(list_stations)


@app.route("/api/v1.0/tobs")
def tobs():
    print("Server reuested climate app temp observation data ...")

    # create a session from Python to the database 
    session = Session(engine)
    
    #  Query the dates and temperature observations of the most active station for the last year of data.
    
    most_active_station = session.query(Measurement.station, func.count(Measurement.station)).\
                                        order_by(func.count(Measurement.station).desc()).\
                                        group_by(Measurement.station).\
                                        first()[0]
                
    # identify the last date, convert to datetime and calculate the start date (12 months from the last date) 
    last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    format_str = '%Y-%m-%d'
    last_dt = dt.datetime.strptime(last_date, format_str)
    date_one_yearago = last_dt - dt.timedelta(days=365)
    
    # build query for tobs with above conditions 
    most_active_tobs = session.query(Measurement.date, Measurement.tobs).\
                                    filter((Measurement.station == most_active_station)\
                                            & (Measurement.date >= date_one_yearago)\
                                            & (Measurement.date <= last_dt)).all()

   
    # close the session 
    session.close()
    
    # Return a JSON list of temperature observations (TOBS) for last year.
    return jsonify(most_active_tobs)


@app.route("/api/v1.0/<start>")
def temp_range_start(start):
    """TMIN, TAVG, and TMAX per date starting from a starting date.
    
    Args:
        start (string): A date string in the format %Y-%m-%d
        
    Returns:
        TMIN, TAVE, and TMAX
    """

    # Create session link from Python to the DB
    session = Session(engine)

    return_list = []
    
    #String format as desired
    format_str = '%Y-%m-%d'
    start_dt = dt.datetime.strptime(start, format_str)

    results =   session.query(  Measurement.date,\
                                func.min(Measurement.tobs), \
                                func.avg(Measurement.tobs), \
                                func.max(Measurement.tobs)).\
                        filter(Measurement.date >= start_dt).\
                        group_by(Measurement.date).all()

    for date, min, avg, max in results:
        new_dict = {}
        new_dict["Date"] = date
        new_dict["TMIN"] = min
        new_dict["TAVG"] = avg
        new_dict["TMAX"] = max
        return_list.append(new_dict)

    session.close()    

    return jsonify(return_list)


@app.route("/api/v1.0/<start>/<end>")
def temp_range_start_end(start,end):
    """TMIN, TAVG, and TMAX per date for a date range.
    
    Args:
        start (string): A date string in the format %Y-%m-%d
        end (string): A date string in the format %Y-%m-%d
        
    Returns:
        TMIN, TAVE, and TMAX
    """

    # Create session link from Python to the DB
    session = Session(engine)

    return_list = []
    
    #String format as desired
    format_str = '%Y-%m-%d'
    start_dt = dt.datetime.strptime(start, format_str)
    end_dt = dt.datetime.strptime(end, format_str)

    results =   session.query(  Measurement.date,\
                                func.min(Measurement.tobs), \
                                func.avg(Measurement.tobs), \
                                func.max(Measurement.tobs)).\
                        filter((Measurement.date >= start_dt) & (Measurement.date <= end_dt)).\
                        group_by(Measurement.date).all()

    for date, min, avg, max in results:
        new_dict = {}
        new_dict["Date"] = date
        new_dict["TMIN"] = min
        new_dict["TAVG"] = avg
        new_dict["TMAX"] = max
        return_list.append(new_dict)

    session.close()    

    return jsonify(return_list)

if __name__ == '__main__':
    app.run(debug=True)













