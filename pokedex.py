
import math

import backend
import const


# DON'T MODIFY THE CODE IN THE SECTION BELOW ----------------------------------
if const.SHOW_IMAGES:
    # In case you're curious, this block of code attempts to import matplotlib.
    # If it succeeds, good!
    # If it fails, then it uses a Python tool named PIP to install the missing
    # modules into your Python library, and then again imports matplotlib
    try:
        from matplotlib import pyplot as plt
        from matplotlib import image as mpimg
    except ModuleNotFoundError:
        print('You didn\'t have matplotlib installed but you set '
              'const.SHOW_IMAGES to True. Attempting to automatically '
              'install...')
        # This try-except statement handles the case where different versions
        # of pip provide different functions to call
        try:
            from pip import main as pipmain
        except:
            from pip._internal import main as pipmain
        # The following line installs matplotlib
        pipmain(['install', 'matplotlib', '--user'])
        from matplotlib import pyplot as plt
        from matplotlib import image as mpimg
        print('Installation successful')
# DON'T MODIFY THE CODE IN THE SECTION ABOVE ----------------------------------


def main():
    """The main function of the Pokedex program.

    Args:
        None

    Returns:
        None
    """
    con, cur = backend.get_con_cur(const.DB_FILENAME)

    if not backend.table_exists(cur):
        backend.create_table(const.CSV_FILENAME, con, cur)

    while True:
        response = get_main_choice()
        if response == '0':
            print('Goodbye!')
            backend.close_con_cur(con, cur)
            break
        elif response == '1':

            name_chosen = get_pokemon_name(backend.get_pokemon_names(cur))
            display_stats(backend.get_stats_by_name(name_chosen, cur))

        elif response == '2':

            number_chosen = get_pokemon_id(backend.get_pokemon_ids(cur))
            display_stats(backend.get_stats_by_id(number_chosen, cur))

        elif response == '3':

            type_chosen = get_pokemon_type(backend.get_pokemon_types(cur))
            display_pokemon_by_type(
                backend.get_pokemon_by_type(type_chosen, cur))

        elif response == '4':
            # Don't remove or edit this this!
            print('(Re-)creating database...')

            backend.create_table(const.CSV_FILENAME, con, cur)
        else:
            raise ValueError('A choice was made which wasn\'t 0, 1, 2, 3, 4')


def get_main_choice():
    """Asks the user for a choice of how they'd like to use the pokedex.

    If the user enters an invalid choice, repeatedly prompts them until they
    enter a correct one.

    Args:
        None

    Returns:
        The user's choice as a str, which can be '0', '1', '2', '3', or '4'
    """
    # This is the prompt you'll display to the user to ask for their choice.
    # Don't modify it!
    prompt = ('Choose an option (0 - 3):\n'
              '0: quit program\n'
              '1: find pokemon by name\n'
              '2: find pokemon by number\n'
              '3: list pokemon by type\n'
              '4: reload data\n')
    # This is the prompt you'll display to the user if they enter something
    # invalid. Don't modify it!
    warning = 'Invalid choice. Please try again.'

    choice = input(prompt)
    condition = True
    while condition:
        if choice not in ['0', '1', '2', '3', '4']:
            print(warning)
            choice = input(prompt)
        else:
            condition = False
    return choice


def get_pokemon_name(pokemon_names):
    """Asks the user for a pokemon name for which they'd like the stats.

    Implicitly converts the name entered by the user to lower case.

    If the user enters a name of a pokemon which is not in pokemon_names, asks
    the user if they meant <name>, where <name> is the most similar pokemon
    name in pokemon_names according to the Levenshtein distance. Then, repeats
    until the user enters a valid pokemon name

    Args:
        pokemon_names: a list of pokemon names (strs) sorted alphabetically

    Returns:
        The pokemon name chosen by the user (as a str)
    """
    # This is the prompt you'll display to the user to ask for their choice.
    # Don't modify it!
    prompt = 'Enter the name of a pokemon: '
    # This is the prompt you'll display to the user if they enter something
    # invalid. Don't modify it! However, you'll be required to fill in the
    # placeholders with the appropriate values.
    warning = 'I don\'t recognize the name {0}. Did you mean {1}?'

    choice = input(prompt)
    choice = choice.lower()
    condition = True
    while condition:
        if choice in pokemon_names:
            condition = False
        else:
            print(warning.format(choice, get_closest_word(
                choice, pokemon_names)))
            choice = input(prompt)
            choice = choice.lower()
    return choice


def get_closest_word(word_0, words):
    """Finds the closest word in the list to word_0 as measured by the
    Levenshtein distance.

    Args:
        word_0: a str
        words: a list of str

    Returns:
        The closest word in words to word_0 as a str.
    """

    new_list = []
    for word in words:
        distance = levenshtein_distance(word_0, word)
        new_list.append(distance)

    # lowest_distance_index is the index of the lowest distance in new_list
    lowest_distance_index = new_list.index(min(new_list))
    return words[lowest_distance_index]


def levenshtein_distance(s1, s2):
    """Returns the Levenshtein distance between strs s1 and s2

    Args:
        s1: a str
        s2: a str
    """
    # Source of the implementation:
    # https://stackoverflow.com/questions/2460177/edit-distance-in-python
    # If you'd like to know more about this algorithm, you can study it in
    # CSCC73 Algorithms. It applies an advanced technique called dynamic
    # programming.
    # For more information:
    # https://en.wikipedia.org/wiki/Levenshtein_distance
    # https://en.wikipedia.org/wiki/Dynamic_programming
    if len(s1) > len(s2):
        s1, s2 = s2, s1

    distances = range(len(s1) + 1)
    for i2, c2 in enumerate(s2):
        distances_ = [i2+1]
        for i1, c1 in enumerate(s1):
            if c1 == c2:
                distances_.append(distances[i1])
            else:
                distances_.append(1 + min((distances[i1], distances[i1 + 1],
                                           distances_[-1])))
        distances = distances_
    return distances[-1]


def get_pokemon_id(species_ids):
    """Asks the user for a pokemon number for which they'd like the stats.

    If the user enters something invalid (ie. anything that's not a valid
    number of a pokemon), warns the user and repeats until they enter a valid
    pokemon number.

    Args:
        species_ids: a list of species ids (ints) sorted in increasing order

    Returns:
        the id chosen by the user as an int
    """
    max_pokemon_id = max(species_ids)
    # This is the prompt you'll display to the user to ask for their choice.
    # Don't modify it!
    prompt = 'Enter a pokemon number (1 - {0}): '.format(max_pokemon_id)
    # This is the prompt you'll display to the user if they enter something
    # invalid. Don't modify it! However, you'll be required to fill in the
    # placeholders with the appropriate values.
    warning = 'Please enter a number between 1 and {0}'.format(max_pokemon_id)

    while True:
        species_id = input(prompt)
        if species_id.isdigit():
            species_id = int(species_id)
            if species_id in species_ids:
                break
        print(warning)
    return species_id


def display_stats(stats):
    """Prints the stats of a pokemon to the user.

    If const.SHOW_IMAGES is set to True, displays the image of the pokemon to
    the user.

    Args:
        stats: a tuple of:
            -pokemon name (str)
            -species_id (int)
            -height (float)
            -weight (float)
            -type_1 (str)
            -type_2 (str)
            -url_image (str)
            -generation_id (int)
            -evolves_from_species_id (str)

    Returns:
        None
    """
    # This function has already been implemented for you. You don't need to do
    # anything with it except call it in the appropriate location!
    template = ('Pokemon name: {0}\n'
                'Pokemon number: {1}\n'
                'Height (in m): {2}\n'
                'Weight (in kg): {3}\n'
                'Type 1: {4}\n'
                'Type 2: {5}\n'
                'Generation: {6}\n'
                'Evolves from: {7}\n')
    text = template.format(stats[0], stats[1], stats[2], stats[3], stats[4],
                           stats[5], stats[7], stats[8])
    print(text, end='')
    if const.SHOW_IMAGES:
        img_filename = stats[6]
        if img_filename.endswith('.png'):
            image = mpimg.imread(const.IMAGES_DIR + img_filename)
            plt.clf()
            plt.imshow(image)
            plt.show()
        else:
            print('No image for this Pokemon available')


def get_pokemon_type(pokemon_types):
    """Asks the user for a type of pokemon and displays the names of all
    pokemon with that type.

    Implicitly converts the type entered by the user to lower case.

    If the user enters an invalid type, warns the user and repeats until they
    enter a valid one.

    Args:
        pokemon_types: a list of pokemon types sorted in alphabetic order

    Returns:
        the pokemon type chosen by the user
    """
    # This is the prompt you'll display to the user to ask for their choice.
    # Don't modify it!
    prompt = ('enter a type from one of the following: \n{0}\n'.format(
        ', '.join(pokemon_types)))
    # This is the prompt you'll display to the user if they enter something
    # invalid. Don't modify it!
    warning = 'Unrecognized type'


    choice = input(prompt)
    choice = choice.lower()
    condition = True

    while condition:
        if choice in pokemon_types:
            condition = False
        else:
            print(warning)
            choice = input(prompt)
            choice = choice.lower()

    return choice


def display_pokemon_by_type(pokemon_names):
    """Displays the names of the pokemon of the chosen type.

    Args:
        pokemon_names: a list of pokemon names to display sorted in alphabetic
        order

    Returns:
        None
    """
    # This function has already been implemented for you. You don't need to do
    # anything with it except call it in the appropriate location!
    curr_index = 0
    while curr_index < len(pokemon_names):
        try:
            curr_pokemon_names = pokemon_names[curr_index:curr_index +
                                               const.NAMES_PER_LINE]
        except IndexError:
            curr_pokemon_names = pokemon_names[curr_index:]
            curr_index = len(pokemon_names)
        curr_index += const.NAMES_PER_LINE
        print(', '.join(curr_pokemon_names))
    print()


if __name__ == '__main__':
    main()
