import sqlite3



def connect_db(db_name, logger):
    try:
        conn = sqlite3.connect(db_name + '.db')
        logger.info(f'Connection established with DB: {db_name}.db')

        return conn


    except sqlite3.OperationalError:
        logger.error(f'Could not connect with {db_name}.db. Make sure the DB name is right')


def create_table(conn, logger):
    c = conn.cursor()

    try:
        c.execute('CREATE TABLE IF NOT EXISTS ChipSeqTable(cell_type_category TEXT NOT NULL, cell_type TEXT NOT NULL, '
                  'cell_type_track_name TEXT NOT NULL, cell_type_short TEXT NOT NULL, assay_category TEXT NOT NULL, '
                  'assay TEXT NOT NULL, assay_track_name TEXT NOT NULL, assay_short TEXT NOT NULL, donor TEXT NOT NULL,'
                  ' time_point TEXT NOT NULL, view TEXT NOT NULL, track_name TEXT NOT NULL, track_type TEXT NOT NULL,'
                  ' track_density TEXT NOT NULL, provider_institution TEXT NOT NULL, source_server TEXT NOT NULL,'
                  ' source_path_to_file TEXT NOT NULL, server TEXT NOT NULL, path_to_file TEXT NOT NULL, '
                  'new_file_name TEXT NOT NULL);')
        logger.info('Table project was created')

    except sqlite3.OperationalError:
        logger.error('Table project could not be created')


def insert_data(conn, list_of_data, logger):
    c = conn.cursor()

    try:
        with conn:
            for data in list_of_data:
                c.execute("INSERT INTO ChipSeqTable VALUES(:cell_type_category, :cell_type, :cell_type_track_name, :cell_type_short, :assay_category, :assay, :assay_track_name, :assay_short, :donor, :time_point, :view, :track_name, :track_type, :track_density, :provider_institution, :source_server, :source_path_to_file, :server, :path_to_file, :new_file_name)", data)
            logger.info('Data was inserted on the DB')

    except sqlite3.OperationalError:
        logger.error('Data could not be inserted')


def select_cell_type(conn, logger, assay=None):

    c = conn.cursor()

    try:
        with conn:
            if not assay:
                c.execute("SELECT DISTINCT cell_type FROM ChipSeqTable")
                all_cell_types = c.fetchall()

                logger.info(f'Selected cell types')
                return all_cell_types

            else:
                c.execute("SELECT DISTINCT cell_type FROM ChipSeqTable WHERE assay = :assay", {"assay": assay})
                all_cell_types = c.fetchall()

                logger.info(f'Selected cell types associated with assay: {assay}')
                return all_cell_types

    except sqlite3.OperationalError:
        logger.error(f'Could not select cell types. Check if the table exists.')


def select_tracks(conn, assay, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute("SELECT DISTINCT track_name, track_type, track_density FROM "
                      "ChipSeqTable WHERE assay "
                      "= :assay", {"assay": assay})
            all_tracks = c.fetchall()

            logger.info(f'Selected all information about tracks from assay: {assay}')

            return all_tracks

    except sqlite3.OperationalError:
        logger.error(f'Could not select assay. Check if the table exists.')

def select_trackname(conn, assay_track_name, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute(
                "SELECT DISTINCT track_name FROM "
                "ChipSeqTable WHERE assay_track_name "
                "= :assay_track_name", {"assay_track_name": assay_track_name})
            all_tracks = c.fetchall()

            logger.info(f'Selected all track names associated with assay: {assay_track_name}')

            return all_tracks

    except sqlite3.OperationalError:
        logger.error(f'Could not select assay. Check if the table exists.')


def update_assay(conn, assay, assaynew, logger):

    c = conn.cursor()

    try:
        with conn:

            c.execute("UPDATE ChipSeqTable SET assay = :assaynew WHERE assay = :assay", {'assaynew': assaynew,
                                                                                     'assay': assay})
            logger.info(f'Assay: {assay} was updated for assay: {assaynew}')

    except sqlite3.OperationalError:
        logger.error(f'COULD NOT UPDATE assay: {assay} for assay: {assaynew}')


def update_donor(conn, donor, donornew, logger):
    c = conn.cursor()

    try:
        with conn:

            c.execute("UPDATE ChipSeqTable SET donor = :donornew WHERE donor = :donor", {'donornew': donornew,
                                                                                         'donor': donornew})
            logger.info(f'Donor: {donor} was updated for donor: {donornew}')

    except sqlite3.OperationalError:
        logger.error(f'COULD NOT UPDATE donor: {donor} for donor: {donornew}')


def delete_track_name(conn, track_name, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute("DELETE FROM ChipSeqTable WHERE track_name = :track_name", {"track_name": track_name})

            logger.info(f'Rows where track name is: "{track_name}" were deleted')

    except sqlite3.OperationalError:
        logger.error(f'Could not delete {track_name}')