import argparse
import json


class ToListAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if values:
            setattr(namespace, self.dest, json.loads(values))


class ToLatLon(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, "lon", values[0])
        setattr(namespace, "lat", values[1])


class ToXY(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, "x", values[0])
        setattr(namespace, "y", values[1])

##################
## Service parsers
##################

service_parser = argparse.ArgumentParser(add_help=False)

# coordinate group (mutual)

coord_group_m = service_parser.add_mutually_exclusive_group(required=True)
coord_group_m.add_argument("--lonlat", nargs=2, type=float, action=ToLatLon,
                           metavar=("LON", "LAT"),
                           help="Longitude and latitude, use to run a single tile")
coord_group_m.add_argument("--bounds", type=str, action=ToListAction,
                           metavar=[["minLON", "minLAT"], ["maxLON", "maxLAT"]],
                           help="Bounding box in lat/lon")
coord_group_m.add_argument("--xy", nargs=2, type=int,  # action=ToXY,
                           metavar=("X", "Y"),
                           help="X/Y tile index (z is always set to 12), use  to run a single tile")
coord_group_m.add_argument("--tile_bounds", type=str, action=ToListAction,
                           metavar=[["minX", "minY"], ["maxX", "maxY"]],
                           help="Bounding box for x/y tiles")

# Cluster group
cluster_group = service_parser.add_argument_group("Cluster settings", "Configure the cluster.")

cluster_group.add_argument("-w", "--width", dest="width",
                           help="Gaussian width in cluster algorithm", type=int, default=5)

cluster_group.add_argument("-c", "--min_count", dest="min_count",
                           help="Minimum number of alerts in a cluster", type=int, default=25)
cluster_group.add_argument("-i", "--iterations", dest="iterations",
                           help="Number of times to iterate when finding clusters", type=int, default=25)

# Date group
date_group = service_parser.add_argument_group("Dates", "Set start and end date.")

date_group.add_argument("--start_date", dest="start_date", type=str,
                        metavar="YYYY-MM-DD", help="Start date")
date_group.add_argument("--end_date", dest="end_date", type=str,
                        metavar="YYYY-MM-DD", help="End date (optional), default today")

################
## Save parser
################

save_parser = argparse.ArgumentParser(add_help=False)

# Save group
save_group = save_parser.add_argument_group("Save settings", "Save data.")

save_group.add_argument("-f", "--file", dest="filename", type=str,
                        help="CSV filename")
save_group.add_argument("--local", dest="local", action="store_true",
                        help="If set, save file locally")
save_group.add_argument("--bucket", dest="bucket", type=str,
                        help="S3 bucket in which CSV file will be saved (optional)")
save_group.add_argument("--temp_dir", dest="temp_dir", type=str,
                        help="Temp directory.")


#############
## Export Parser
#############

export_parser = argparse.ArgumentParser(add_help=False)

export_group = export_parser.add_argument_group("Export settings", "Export data.")
export_group.add_argument("--format", dest="format", choices=["PG"],
                          help="Export format (default PG)", default="PG")
export_group.add_argument("--pg_table", dest="pg_table", type=str,
                          help="PostgreSQL table name")
export_group.add_argument("--pg_schema", dest="pg_schema", type=str,
                          help="PostgreSQL schema name")
export_group.add_argument("--pg_dbname", dest="pg_dbname", type=str,
                          required=True, help="PostgreSQL database name")
export_group.add_argument("--pg_host", dest="pg_host", type=str,
                          help="PostgreSQL host")
export_group.add_argument("--pg_port", dest="pg_port", type=str,
                          help="PostgreSQL port")
export_group.add_argument("--pg_user", dest="pg_user", type=str,
                          required=True, help="PostgreSQL user")
export_group.add_argument("--pg_password", dest="pg_password", type=str,
                          required=True, help="PostgreSQL password")
export_group.add_argument("--concave", dest="concave", type=int,
                          help="Target percent of area for concave hull. Integers between 0 and 100." \
                               "When set to 100, area is equal to convex hull")
export_group.add_argument("--temp_dir", dest="temp_dir", type=str,
                          help="Temp directory. Make sure this directory is accessible for postgres user")
export_group.add_argument("--overwrite", dest="overwrite", action='store_true',
                          help="Overwrite existing table")
