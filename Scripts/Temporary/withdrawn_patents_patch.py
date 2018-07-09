import MySQLdb


def create_temp_patent_table(host, username, password,
                             database, withdrawn_patents):
    '''
    Create a temporary table of withdrawn patents, then merge with
    a temporary copy of the patent table to create a 1/0 flag for
    withdrawn patents.
    '''

    db = MySQLdb.connect(host=host, user=username,
                           passwd=password, db=database)
    cursor = db.cursor()

    # Create table of withdrawn patents
    cursor.execute("DROP TABLE IF EXISTS temp_withdrawn_patents;")
    cursor.execute(
        """
        CREATE TABLE temp_withdrawn_patents (
            `id` varchar(20) COLLATE utf8_unicode_ci NOT NULL,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
        """
    )
    print("Table created: temp_withdrawn_patents")

    # Insert withrawn patents
    insert_statement = "INSERT INTO temp_withdrawn_patents (id) VALUES (%s)"
    cursor.executemany(insert_statement, withdrawn_patents)

    # Create clone of patent table
    cursor.execute("DROP TABLE IF EXISTS temp_patent_update")
    cursor.execute("CREATE TABLE temp_patent_update LIKE patent")
    cursor.execute("ALTER TABLE temp_patent_update ADD COLUMN withdrawn BOOLEAN DEFAULT NULL")
    print("Table created: temp_patent_update")

    # Insert patent information and withdrawn status to temp_patent_update
    cursor.execute(
        """
        INSERT INTO temp_patent_update
            SELECT p.*,
            IF(w.id, 1, 0) withdrawn
        FROM patent p
        LEFT JOIN temp_withdrawn_patents w ON p.id = w.id
       """
    )
    print("Table updated: temp_patent_update")

    # Drop the temporary table with withdrawn patent numbers
    # cursor.execute("DROP TABLE temp_withdrawn_patents")
    # print("Table dropped: temp_withdrawn_patents")

    db.commit()
    print("Changes committed.")


def parse_withdrawn_patents(filepath):
    ''' Return a list of patent numbers from the withdrawn patents txt file '''

    with open(filepath) as f:
        withdrawn_patents = [patent_number for patent_number in
                             f.read().split('\r\n') if patent_number != '']

    for i, patent_number in enumerate(withdrawn_patents):
        withdrawn_patents[i] = parse_patent_number(patent_number)

    print("Sample patent numbers: ")
    print(withdrawn_patents[:10])
    print(withdrawn_patents[-10:])
    return withdrawn_patents

def parse_patent_number(patent_number):
    ''' Strip leading 0s from patent numbers, maintaining letter prefixes '''
    # TODO: Resolve patent number parsing issues
    # Patent Numbers that start with H0 either have 7 or 8 characters;
    # trimming excess leading 0s is therefore ambiguous in these cases
    prefix = ''.join([c for c in patent_number if c.isalpha()])
    suffix = ''.join([c for c in patent_number if c.isdigit()]).lstrip('0')

    # For patents that don't start with 'PP', add zeroes to get total length 7
    if prefix != 'PP':
        suffix = suffix.zfill(7 - len(prefix))

    return prefix + suffix


if __name__ == '__main__':

    import sys

    if len(sys.argv) != 6:
        print("Usage: python withdrawn_patents_patch.py <HOST> <USERNAME> "
              "<PASSWORD> <DATABASE> <WITHDRAWN_PATENTS_FILE>")
        sys.exit(1)

    [host, username, password, database, withdrawn_patent_file] = sys.argv[1:]
    withdrawn_patents = parse_withdrawn_patents(withdrawn_patent_file)

    create_temp_patent_table(host=host, username=username,
                             password=password, database=database,
                             withdrawn_patents=withdrawn_patents)
