import mysql.connector as mysql
import csv
import ast


def createdb(user: str, password: str):
    try:
        with mysql.connect(host="localhost", user=user, passwd=password) as db_conn:
            with db_conn.cursor() as cursor:
                cursor.execute("SHOW DATABASES LIKE 'popular_videogames'")
                database_exists = cursor.fetchone()

                if not database_exists:
                    cursor.execute("CREATE DATABASE popular_videogames ")
                    print("Database created successfully.")
                else:
                    print("Database exists already.")

    except mysql.Error as err:
        print(f"Error: {err}")


def create_tables(user: str, password: str):
    try:
        with mysql.connect(host="localhost", user=user, passwd=password, database="popular_videogames") as db_conn:
            with db_conn.cursor() as cursor:
                videogames = (
                    "CREATE TABLE IF NOT EXISTS videogames("
                    "game_id INT PRIMARY KEY,"
                    "game_title VARCHAR(100) NOT NULL,"
                    "release_date DATE,"
                    "rating INT,"
                    "times_listed INT,"
                    "number_of_reviews INT,"
                    "summary VARCHAR(4000),"
                    "plays INT,"
                    "playing INT,"
                    "backlogs INT,"
                    "wishlist INT);"
                )

                developers = "CREATE TABLE IF NOT EXISTS developers(" "name VARCHAR(100) PRIMARY KEY);"

                genre = "CREATE TABLE IF NOT EXISTS genre(" "name VARCHAR(100) PRIMARY KEY);"

                reviews = (
                    "CREATE TABLE IF NOT EXISTS reviews("
                    "id INT PRIMARY KEY AUTO_INCREMENT,"
                    "content VARCHAR(8000) NOT NULL,"
                    "game_id INT NOT NULL,"
                    "FOREIGN KEY (game_id) REFERENCES videogames(game_id) ON DELETE CASCADE);"
                )

                developed_by = (
                    "CREATE TABLE IF NOT EXISTS developed_by("
                    "developer VARCHAR(100) NOT NULL,"
                    "game_id INT NOT NULL,"
                    "PRIMARY KEY (developer, game_id),"
                    "FOREIGN KEY (developer) REFERENCES developers(name) ON DELETE CASCADE,"
                    "FOREIGN KEY (game_id) REFERENCES videogames(game_id) ON DELETE CASCADE);"
                )

                genre_is = (
                    "CREATE TABLE IF NOT EXISTS genre_is("
                    "game_id INT NOT NULL,"
                    "genre_name VARCHAR(100) NOT NULL,"
                    "PRIMARY KEY (game_id, genre_name),"
                    "FOREIGN KEY (game_id) REFERENCES videogames(game_id) ON DELETE CASCADE,"
                    "FOREIGN KEY (genre_name) REFERENCES genre(name) ON DELETE CASCADE);"
                )

                cursor.execute(videogames)
                cursor.execute(developers)
                cursor.execute(genre)
                cursor.execute(reviews)
                cursor.execute(developed_by)
                cursor.execute(genre_is)

                print("Tables have been created successfully.")

    except mysql.Error as err:
        print(f"Error: {err}")


def date_convert(date: str) -> str:
    if date == "" or date[0:3] == "rel":
        return None
    month_dict = {
        "Jan": "01",
        "Feb": "02",
        "Mar": "03",
        "Apr": "04",
        "May": "05",
        "Jun": "06",
        "Jul": "07",
        "Aug": "08",
        "Sep": "09",
        "Oct": "10",
        "Nov": "11",
        "Dec": "12",
    }
    day = date[4:6]
    month = month_dict[date[0:3]]
    year = date[-4:]
    return f"{year}-{month}-{day}"


def num_variable_processing(number: str):
    if number == "":
        return None
    elif "K" in number:
        return int(float(number[:-1]) * 1000)
    elif "." in number:
        return float(number)
    else:
        return int(number)


def multivalued_processing(original_string: str):
    if original_string != "":
        return ast.literal_eval(original_string)
    else:
        return []


def insert_data(user: str, password: str):
    try:
        with mysql.connect(host="localhost", user=user, passwd=password, database="popular_videogames") as db_conn:
            with db_conn.cursor() as cursor:
                with open("./games.csv") as file:
                    dataset = csv.reader(file, delimiter=",")
                    next(dataset)
                    rows_read, rows_inserted = 0, 0
                    skipped_rows = 0
                    all_game_titles = set()
                    for row in dataset:
                        rows_read += 1
                        skip_to_next_row = False
                        # Primary Key
                        game_id = int(row[0])

                        # String Values
                        game_title = row[1]
                        summary = row[8]

                        # Date
                        release_date = date_convert(row[2])

                        # Numerical Values
                        rating = num_variable_processing(row[4])
                        times_listed = num_variable_processing(row[5])
                        num_reviews = num_variable_processing(row[6])
                        plays = num_variable_processing(row[10])
                        playing = num_variable_processing(row[11])
                        backlogs = num_variable_processing(row[12])
                        wishlist = num_variable_processing(row[13])

                        # (Potential) Multivalued Attributes
                        developers = multivalued_processing(row[3])
                        genres = multivalued_processing(row[7])
                        reviews = multivalued_processing(row[9])

                        if game_title in all_game_titles:
                            compare_summary = "SELECT summary, game_id " "FROM videogames " "WHERE game_title = %s;"
                            compare_summary_data = (game_title,)
                            cursor.execute(compare_summary, compare_summary_data)
                            result = cursor.fetchall()
                            for row in result:
                                summary_from_database = row[0]
                                game_id_from_database = row[1]
                                if summary_from_database == summary:
                                    skip_to_next_row = True
                                    for review in reviews:
                                        try:
                                            insert_review = "INSERT INTO reviews(content, game_id) VALUES (%s, %s)"
                                            review_data = (review, game_id_from_database)
                                            cursor.execute(insert_review, review_data)
                                        except mysql.Error as err:
                                            if err.errno != 1062:
                                                print(f"Error: {err}")

                        if skip_to_next_row:
                            skipped_rows += 1
                            continue

                        insert_videogame = (
                            "INSERT INTO videogames(game_id, game_title, release_date, rating, times_listed, number_of_reviews, summary, plays, playing, backlogs, wishlist)"
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        )
                        videogame_data = (
                            game_id,
                            game_title,
                            release_date,
                            rating,
                            times_listed,
                            num_reviews,
                            summary,
                            plays,
                            playing,
                            backlogs,
                            wishlist,
                        )
                        cursor.execute(insert_videogame, videogame_data)
                        all_game_titles.add(game_title)

                        for developer in developers:
                            try:
                                insert_developer = "INSERT INTO developers(name) VALUES (%s)"
                                dev_data = (developer,)
                                cursor.execute(insert_developer, dev_data)
                            except mysql.Error as err:
                                if err.errno != 1062:
                                    print(f"Error: {err}")

                            relate_dev_to_game = "INSERT INTO developed_by(developer, game_id) VALUES (%s, %s)"
                            dev_to_game_data = (developer, game_id)
                            cursor.execute(relate_dev_to_game, dev_to_game_data)

                        for genre in genres:
                            try:
                                insert_genre = "INSERT INTO genre(name) VALUES (%s)"
                                genre_data = (genre,)
                                cursor.execute(insert_genre, genre_data)
                            except mysql.Error as err:
                                if err.errno != 1062:
                                    print(f"Error: {err}")

                            relate_genre_to_game = "INSERT INTO genre_is(game_id, genre_name) VALUES (%s, %s)"
                            genre_to_game_data = (game_id, genre)
                            cursor.execute(relate_genre_to_game, genre_to_game_data)

                        for review in reviews:
                            insert_review = "INSERT INTO reviews(content, game_id) VALUES (%s, %s)"
                            review_data = (review, game_id)
                            cursor.execute(insert_review, review_data)

                        rows_inserted += 1

                    print(f"{rows_read} row(s) have been read successfully.")
                    print(f"{rows_inserted} distinct row(s) have been inserted successfully.")
                    print(f"{skipped_rows} row(s) have been skipped because of duplicated data.")

            db_conn.commit()

    except mysql.Error as err:
        print(f"Error: {err}")


def resetdb(user: str, password: str):
    try:
        with mysql.connect(host="localhost", user=user, passwd=password, database="popular_videogames") as db_conn:
            with db_conn.cursor() as cursor:
                cursor.execute("DROP DATABASE popular_videogames")
    except mysql.Error as err:
        if err.errno != 1049:
            print(f"Error: {err}")
