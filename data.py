from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

QUERY_FLIGHT_BY_ID = "SELECT flights.*, airlines.airline, flights.ID as FLIGHT_ID, flights.DEPARTURE_DELAY as DELAY FROM flights JOIN airlines ON flights.airline = airlines.id WHERE flights.ID = :id"
QUERY_FLIGHT_BY_DATE = "SELECT f.id, f.ORIGIN_AIRPORT, f.DESTINATION_AIRPORT, a.AIRLINE, f.DEPARTURE_DELAY AS DELAY FROM airlines AS a JOIN flights AS f ON	a.ID = f.AIRLINE WHERE COALESCE(f.DEPARTURE_DELAY, 0) AND f.DAY = :day AND f.MONTH = :month AND f.YEAR = :year AND f.DEPARTURE_DELAY >= 20 ORDER BY DEPARTURE_DELAY DESC"
QUERY_FLIGHT_BY_AIRLINE = "SELECT f.id, f.ORIGIN_AIRPORT, f.DESTINATION_AIRPORT, a.AIRLINE, f.DEPARTURE_DELAY AS DELAY FROM airlines AS a JOIN flights AS f ON a.ID = f.AIRLINE WHERE COALESCE(f.DEPARTURE_DELAY, 0) AND f.DEPARTURE_DELAY >= 20 AND lower(a.AIRLINE) LIKE lower(:airline) ORDER BY DEPARTURE_DELAY DESC"
QUERY_FLIGHT_BY_ORIGIN_AIRPORT = "SELECT f.id, f.ORIGIN_AIRPORT, f.DESTINATION_AIRPORT, a.AIRLINE, f.DEPARTURE_DELAY AS DELAY FROM airlines AS a JOIN flights AS f ON a.ID = f.AIRLINE WHERE COALESCE(f.DEPARTURE_DELAY, 0) AND f.DEPARTURE_DELAY >= 20 AND f.ORIGIN_AIRPORT = :origin_airport ORDER BY DEPARTURE_DELAY DESC"
QUERY_AVG_PERCENTAGE_DELAYED_FLIGHTS = "SELECT ORIGIN_AIRPORT, DESTINATION_AIRPORT, AVG(DELAYED_FLIGHTS * 100.0 / TOTAL_FLIGHTS) AS PERCENT_DELAYED FROM ( SELECT ORIGIN_AIRPORT, DESTINATION_AIRPORT, COUNT(CASE WHEN DEPARTURE_DELAY >= 20 THEN 0 END) AS DELAYED_FLIGHTS, COUNT(*) AS TOTAL_FLIGHTS FROM flights GROUP BY ORIGIN_AIRPORT, DESTINATION_AIRPORT ) AS ROUTE_STATS WHERE (ORIGIN_AIRPORT = :origin AND DESTINATION_AIRPORT = :destination) OR (ORIGIN_AIRPORT = :origin_vv AND DESTINATION_AIRPORT = :destination_vv) GROUP BY ORIGIN_AIRPORT, DESTINATION_AIRPORT"
QUERY_LONG_LAT = "SELECT IATA_CODE, LATITUDE, LONGITUDE FROM airports WHERE IATA_CODE = :origin OR IATA_CODE = :destination"


class FlightData:
    """
    The FlightData class is a Data Access Layer (DAL) object that provides an
    interface to the flight data in the SQLITE database. When the object is created,
    the class forms connection to the sqlite database file, which remains active
    until the object is destroyed.
    """

    def __init__(self, db_uri):
        """
        Initialize a new engine using the given database URI
        """
        self._engine = create_engine(db_uri)

    def _execute_query(self, query, params):
        """
        Execute an SQL query with the params provided in a dictionary,
        and returns a list of records (dictionary-like objects).
        If an exception was raised, print the error, and return an empty list.
        """
        try:
            with self._engine.connect() as connection:
                query = text(query)
                results = connection.execute(query, params)
                return results.fetchall()
        except SQLAlchemyError as e:
            print(f"SQLAlchemy Error: {e}")
            return []
        except Exception as e:
            print(f"Unexpected Error: {e}")
            return []

    def get_flight_by_id(self, flight_id):
        """
        Searches for flight details using flight ID.
        If the flight was found, returns a list with a single record.
        """
        params = {'id': flight_id}
        return self._execute_query(QUERY_FLIGHT_BY_ID, params)

    def get_flights_by_date(self, day, month, year):
        params = {'day': day, 'month': month, 'year': year}
        return self._execute_query(QUERY_FLIGHT_BY_DATE, params)

    def get_delayed_flights_by_airline(self, airline: str):
        params = {'airline': "%" + airline + "%"}
        return self._execute_query(QUERY_FLIGHT_BY_AIRLINE, params)

    def get_delayed_flights_by_airport(self, airport_input: str):
        params = {'origin_airport': airport_input}
        return self._execute_query(QUERY_FLIGHT_BY_ORIGIN_AIRPORT, params)

    def __del__(self):
        """
        Closes the connection to the databse when the object is about to be destroyed
        """
        self._engine.dispose()
