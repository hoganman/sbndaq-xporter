#!/usr/bin/env python3
# import psycopg2 # Get database functions

import dbvariables  # get variable names
import safe


def fill_db(timestr):
    """ function to fill db from xml file """
    # Already connected to database in main program
    # dbvariables.conn = database connection
    # dbvariables.cur = cursor

    # database file name:
    # TODO This will need to change for SBN-FD
    dbtableXML = "sbnd_xml_database"

    # make a new database table

    # the following line is for testing only!
    # dbvariables.cur.execute("TRUNCATE TABLE "+dbtableXML+";")
    goodrun = True
    psqlcmd = "INSERT INTO " + dbtableXML + \
              "(runnumber, subrun, runstarttimesec) VALUES (" + \
              safe.run + ", " + safe.subrun + ", " + timestr + ");"
    # print(psqlcmd)
    dbvariables.cur.execute(psqlcmd)
    dbvariables.conn.commit()
    return goodrun
