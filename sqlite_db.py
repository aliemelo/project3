import sys
import argparse
import logging
import os
import util.loggerinitializer as utl
from db_util import dbmanip as db


# Initialize log object
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
utl.initialize_logger(os.getcwd(), logger)


def main():
    parser = argparse.ArgumentParser(description="A tool to manipulate an sqlite DB")

    subparsers = parser.add_subparsers(title='actions',
                                       description='valid actions',
                                       help='Use sqlite-python.py {action} -h for help with each action',
                                       dest='command')

    # create db and tables
    parser_index = subparsers.add_parser('createdb', help="Create database and tables")

    parser_index.add_argument("--db", dest='db', default=None, action="store", help="The DB name", required=True)

    # insert data on the table
    parser_insert = subparsers.add_parser('insert', help='Insert data on tables')

    parser_insert.add_argument("--db", default=None, action="store", help="The DB name", required=True)

    parser_insert.add_argument("--file", default=None, action="store", help="TSV file with the data to be inserted", 
                               required=True) 


    # select specific fields
    parser_select = subparsers.add_parser('select', help='Select fields from the DB')

    parser_select.add_argument("--db", default=None, action="store", help="The DB name",
                               required=True)

    parser_select.add_argument("--celltype", action="store_true", help="Select all cell types",
                               required=False, default=False)

    parser_select.add_argument("--tracks", action="store", help="Select all information about tracks belonging to an "
                                                                "specific assay", required=False, default=False)

    parser_select.add_argument("--trackname", action="store",
                               help="Select all track name associated with a specific assay_track_name",
                               required=False, default=False)

    parser_select.add_argument("--ca", action="store", help="Select all cell_types associated with a assay",
                               required=False, default=False)


    # update data
    parser_update = subparsers.add_parser('update', help='Update a field in a DB')

    parser_update.add_argument("--db", default=None, action="store", help="The DB name",
                               required=True)

    parser_update.add_argument("--assay", default=False, action="store",
                               help="Name of the assay to be updated",
                               required=False)

    parser_update.add_argument("--assaynew", default=False, action="store", required=False,
                               help="Name of the new assay")

    parser_update.add_argument("--donor", default=False, action="store",
                               help="Name of the donor to be updated",
                               required=False)

    parser_update.add_argument("--donornew", default=False, action="store",
                               help="Name of the new donor",
                               required=False)


    # delete records
    parser_delete = subparsers.add_parser('delete', help='Delete rows from the DB')

    parser_delete.add_argument("--db", default=None, action="store", help="The DB name", required=True)

    parser_delete.add_argument("--trackname", dest="del_track",default=False, action="store",
                               help="Delete all records from a given track_name", required=False)

    args = parser.parse_args()



    # connect to db
    conn = db.connect_db(args.db, logger)


    if args.command == 'createdb':
        db.create_table(conn, logger)

    elif args.command == "insert":
        list_of_data = []

        with open(args.file, 'r') as f:
            for line in f:

                # reset dictionary
                line_dict = dict()

                # Skip empty lines
                if not line.strip():
                    continue
                elif line.startswith(","):
                    continue

                # split line
                values = line.strip().split(',')

                # put each field in a dict
                line_dict['cell_type_category'] = values[0]
                line_dict['cell_type'] = values[1]
                line_dict['cell_type_track_name'] = values[2]
                line_dict['cell_type_short'] = values[3]
                line_dict['assay_category'] = values[4]
                line_dict['assay'] = values[5]
                line_dict['assay_track_name'] = values[6]
                line_dict['assay_short'] = values[7]
                line_dict['donor'] = values[8]
                line_dict['time_point'] = values[9]
                line_dict['view'] = values[10]
                line_dict['track_name'] = values[11]
                line_dict['track_type'] = values[12]
                line_dict['track_density'] = values[13]
                line_dict['provider_institution'] = values[14]
                line_dict['source_server'] = values[15]
                line_dict['source_path_to_file'] = values[16]
                line_dict['server'] = values[17]
                line_dict['path_to_file'] = values[18]
                line_dict['new_file_name'] = values[19]

                #append the dict to a list
                list_of_data.append(line_dict)

        db.insert_data(conn, list_of_data, logger)

    elif args.command == "select" and args.celltype is not False:
        all_cell_types = db.select_cell_type(conn, logger)

        for cell_type in all_cell_types:
            print(cell_type[0])

    elif args.command == "select" and args.tracks is not False:
        all_tracks = db.select_tracks(conn, args.tracks, logger)

        print("\n| track_name\t| track_type\t| track_density")
        for track in all_tracks:
            print("|", "\t| ".join(track))

    elif args.command == "select" and args.trackname is not False:
        all_tracknames = db.select_trackname(conn, args.trackname, logger)

        print("\n| Track Name")
        for track in all_tracknames:
            print("|", "\t| ".join(track))

    elif args.command == "select" and args.ca is not False:
        all_cell_types = db.select_cell_type(conn, logger, args.ca)

        print("\n| Cell Type")
        for assay in all_cell_types:
            print("|", "\t| ".join(assay))

    elif args.command == "update" and args.assay is not False and args.assaynew is not False:
        db.update_assay(conn, args.assay, args.assaynew, logger)

    elif args.command == "update" and args.donor is not False and args.donornew is not False:
        db.update_donor(conn, args.donor, args.donornew, logger)

    elif args.command == "delete":
        db.delete_track_name(conn, args.del_track, logger)

if __name__ == '__main__':
    main()