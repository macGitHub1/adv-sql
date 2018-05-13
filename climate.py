#imports
from flask import Flask, jsonify
import numpy as np
import pandas as pd
from datetime import date

from sqlalchemy import Table, select, MetaData,create_engine, Column, Integer, String, Numeric, Text, Float
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base



#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

### Globals **
engine_ = create_engine("sqlite:///Resources/hawaii.sqlite")
metadata_ = MetaData()
mTable= Table('measurements', metadata_,autoload=True, autoload_with=engine_)
sTable = Table('stations', metadata_,autoload=True, autoload_with=engine_)

@app.route("/")
def home():
    print("Server received request for 'Home' page...")
    return "Welcome to my 'Home' page!"



@app.route("/api/v1.0/precipitation")
def precipitation():
    out_dict = {}
    stmnt = select([mTable.c.date,mTable.c.tobs]).where( mTable.c.date > "2016-8-23")
    out = pd.read_sql(stmnt, engine_)

    for index, row in out.iterrows():
      out_dict[row[0]] = row[1]

    
    return( jsonify(out_dict))



@app.route("/api/v1.0/stations")
def get_stations():
     out_List = [ ]
     stmnt = select([sTable.c.name])
     out = pd.read_sql(stmnt, engine_)
    
     for index, row in out.iterrows():
       out_List.append( row[0] )

    
     return( jsonify(out_List))



@app.route("/api/v1.0/tobs")
def get_tobs():
     out_dict = {}
     stmnt = select([mTable.c.date,mTable.c.tobs]).where( mTable.c.date > "2016-8-23")
     out = pd.read_sql(stmnt, engine_)

     
     for index, row in out.iterrows():
       out_dict[row[0]] = row[1]

    
     return( jsonify(out_dict))


#execute
if __name__ == "__main__":
    app.run(debug=True)

