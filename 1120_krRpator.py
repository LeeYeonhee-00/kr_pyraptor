import logging
import partridge as ptg

# capture logs in notebook
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logging.debug("test")

# load a GTFS of AC Transit
# path = 'gtfs.zip'
path = 'kr_gtfs.zip'
_date, service_ids = ptg.read_busiest_date(path)
view = {'trips.txt': {'service_id': service_ids}}
feed = ptg.load_feed(path, view)
