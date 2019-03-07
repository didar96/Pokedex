
import sqlite3

import const


def get_con_cur(db_filename):
    """Returns an open connection and cursor associated with the sqlite
    database associated with db_filename.

    Args:
        db_filename: (str) the filename of the db to which to connect

    Returns: a tuple of:
        -an open connection to the sqlite database
        -an open cursor associated with the connection
    """
    con = sqlite3.connect(db_filename)
    cur = con.cursor()

    return (con, cur)


def close_con_cur(con, cur):
    """Commits changes and closes the given cursor and connection to a sqlite
    database.

    Args:
        con: an open sqlite3 connection to a database
        cur: a cursor associated with con

    Returns:
        None
    """
    cur.close()
    con.commit()
    con.close()


def table_exists(cur):
    """Returns whether the pokemon table already exists in the database and
    whether it is non-empty

    Args:
        cur: an open sqlite3 cursor created from a connection to the pokemon db
    """
    query = 'SELECT * FROM pokemon'
    try:
        cur.execute(query)
    except sqlite3.OperationalError:
        return False
    return cur.fetchone() is not None


def create_table(csv_filename, con, cur):
    """(Re-)creates the pokemon table in the database

    In the SQLite cursor cur, drops the pokemon table if it already exists, and
    re-creates it. Fills it with the information contained in the CSV file
    denoted by csv_filename. Afterwards, commits the changes through the given
    connection con.

    Implicitly converts all strs in the loaded data to lower-case before
    insertion into the database.

    Args:
        csv_filename: (str) the filename of the CSV file containing the pokemon
        information
        con: an open connection to the sqlite database
        cur: an open cursor associated with the connection

    Returns:
        None
    """

    cur.execute('DROP TABLE IF EXISTS pokemon')
    cur.execute(
        'CREATE TABLE pokemon (name TEXT, species_id INTEGER, height REAL, '
        'weight REAL, type_1 TEXT, type_2 TEXT, url_image TEXT, '
        'generation_id INTEGER, evolves_from_species_id TEXT)')


    query = ('INSERT INTO pokemon VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)')
    with open(csv_filename) as f:
        header_dict = parse_header(f)
        for line in f:
            line = line.strip().split(const.SEP)
            pokemon = line[header_dict['pokemon']]
            pokemon = pokemon.lower()
            species_id = int(line[header_dict['species_id']])
            height = float(line[header_dict['height']])
            weight = float(line[header_dict['weight']])
            type_1 = line[header_dict['type_1']]
            type_1 = type_1.lower()
            type_2 = line[header_dict['type_2']]
            type_2 = type_2.lower()
            url_image = line[header_dict['url_image']]
            url_image = url_image.lower()
            generation_id = int(line[header_dict['generation_id']])
            evolves_from_species_id = str(line[header_dict[
                'evolves_from_species_id']])
            evolves_from_species_id = evolves_from_species_id.lower()
            cur.execute(
                query, [pokemon, species_id, height, weight, type_1, type_2,
                        url_image, generation_id, evolves_from_species_id])


    con.commit()


def get_pokemon_names(cur):
    """Returns a list of pokemon names in the database (as strs) sorted in
    alphabetical order

    Args:
        cur: an open sqlite3 cursor created from a connection to the pokemon db
    """

    new_list = []
    query = ('SELECT name FROM pokemon')
    cur.execute(query)
    data = cur.fetchall()
    for element in data:
        new_list.append(element[0])
    return sorted(new_list)


def get_stats_by_name(name, cur):
    """Returns the stats of the pokemon with the given name as stored in the
    database.

    Args:
        name: the pokemon's name
        cur: an open sqlite3 cursor created from a connection to the pokemon db

    Returns: a tuple of
        -the pokemon's name (str)
        -the pokemon's species id (int)
        -the pokemon's height (float)
        -the pokemon's weight (float)
        -the pokemon's type 1 (str)
        -the pokemon's type 2 (str)
        -the pokemon's url image (str)
        -the pokemon's generation (int)
        -the species id from which the pokemon evolves (str)
    """

    query = (
        'SELECT name, species_id, height, weight, type_1, type_2, url_image, '
        'generation_id, evolves_from_species_id FROM pokemon WHERE name = (?)')
    cur.execute(query, (name,))
    data = cur.fetchall()

    return data[0]


def get_pokemon_ids(cur):
    """Returns a list of pokemon (species) ids (as ints) sorted in increasing
    order as stored in the database.

    Args:
        cur: an open sqlite3 cursor created from a connection to a pokemon db
    """

    new_list = []
    query = ('SELECT species_id FROM pokemon')
    cur.execute(query)
    data = cur.fetchall()

    for element in data:
        new_list.append(element[0])

    return sorted(new_list)


def get_stats_by_id(species_id, cur):
    """Returns the stats of the pokemon with the given species id as stored in
    the database.

    Args:
        species_id: the pokemon's species id (int)
        cur: an open sqlite3 cursor created from a connection to the pokemon db

    Returns: a tuple of
        -the pokemon's name (str)
        -the pokemon's species id (int)
        -the pokemon's height (float)
        -the pokemon's weight (float)
        -the pokemon's type 1 (str)
        -the pokemon's type 2 (str)
        -the pokemon's url image (str)
        -the pokemon's generation (int)
        -the species id from which the pokemon evolves (str)
    """

    query = (
        'SELECT name, species_id, height, weight, type_1, type_2, url_image, '
        'generation_id, evolves_from_species_id FROM pokemon '
        'WHERE species_id = (?)')
    cur.execute(query, (species_id,))
    data = cur.fetchall()

    return data[0]


def unique_and_sort(ell):
    """Returns a copy of ell which contains all unique elements of ell sorted
    in ascending order.

    Args:
        ell: a list that can be sorted
    """

    return sorted(set(ell))


def get_pokemon_types(cur):
    """Returns a list of distinct pokemon types (strs) sorted in alphabetical
    order.

    Both type_1 and type_2 are treated as types.

    Args:
        cur: an open sqlite3 cursor created from a connection to the pokemon db
    """
    new_list = []
    query = ('SELECT type_1, type_2 FROM pokemon')
    cur.execute(query)
    data = cur.fetchall()

    for types in data:
        for element in types:
            if element not in new_list:
                new_list.append(element)

    return unique_and_sort(new_list)


def get_pokemon_by_type(pokemon_type, cur):
    """Returns a list of pokemon names (strs) of all pokemon of the given type,
    where the list is sorted in alphabetical order.

    Args:
        pokemon_type: the pokemon type (which may be a type_1 or type_2) (str)
        cur: an open sqlite3 cursor created from a connection to the pokemon db
    """

    new_list = []
    query = ('SELECT name FROM pokemon WHERE type_1 = (?) OR type_2 = (?)')
    cur.execute(query, (pokemon_type, pokemon_type))
    data = cur.fetchall()

    for element in data:
        new_list.append(element[0])

    return unique_and_sort(new_list)


def parse_header(f):
    """Parses the header and builds a dict mapping column name to index

    Args:
        f: a freshly opened file in the format of pokemon.csv

    Returns:
        a dict where:
            -each key is one of:
                'pokemon', 'species_id', 'height', 'weight', 'type_1',
                'type_2', 'url_image', 'generation_id',
                'evolves_from_species_id'
            -each value is the index of the corresponding key in the CSV file
                starting from column 0.
                eg. If 'pokemon' is in the second column, then its index will
                be 1. If 'species_id' is the third column, then its index will
                be 2.
    """
    columns = ['pokemon', 'species_id', 'height', 'weight', 'type_1', 'type_2',
               'url_image', 'generation_id', 'evolves_from_species_id']

    header = f.readline().strip().split(const.SEP)
    result = {}
    for column in columns:
        result[column] = header.index(column)
    return result
