import mysql.connector as mysql
import pandas as pd

queries: dict[str, dict[str, str]] = {
    "database": {
        "Total database size": """
            SELECT 
                table_schema "Database Name", 
                SUM(data_length + index_length) / (1024 * 1024) "Database Size (MB)"
            FROM information_schema.tables
            WHERE table_schema = 'popular_videogames';""",
        "Number of tables": """
            SELECT COUNT(*) AS "Number of Tables"
            FROM information_schema.tables
            WHERE table_schema = 'popular_videogames';""",
        "Number of rows per table": """
            SELECT table_name, table_rows
            FROM information_schema.tables
            WHERE table_schema = 'popular_videogames'""",
        "List tables in the database": "SHOW TABLES;",
        "Columns in each table": """
            SELECT
                table_name,
                GROUP_CONCAT(column_name ORDER BY ordinal_position) AS COLUMNS
            FROM information_schema.columns
            WHERE table_schema = 'popular_videogames'
            GROUP BY table_name;""",
        "Indexes information": """
            SELECT table_name, index_name, column_name
            FROM information_schema.statistics
            WHERE table_schema = 'popular_videogames';""",
        "Show the videogames table": """
            SELECT
                game_id,
                game_title,
                release_date,
                rating,
                times_listed,
                number_of_reviews,
                LEFT(summary, 30) AS truncated_summary,
                plays,
                playing,
                backlogs,
                wishlist
            FROM videogames
            LIMIT 10;""",
        "Show the developers table": """
            SELECT *
            FROM developers
            LIMIT 10;""",
        "Show the genre table": """
            SELECT *
            FROM genre
            LIMIT 10;""",
    },
    "videogames": {
        "Number of videogames per decade": """
            SELECT
                CONCAT(LEFT(release_date, 3), '0s') AS decade,
                COUNT(*) AS number_of_games
            FROM videogames
            GROUP BY decade
            ORDER BY decade;""",
        "Top 10 videogames by rating": """
            SELECT game_title, rating
            FROM videogames
            ORDER BY rating DESC
            LIMIT 10;""",
        "Top 10 most reviewed videogames (by registered reviews)": """
            SELECT v.game_title, COUNT(r.game_id) AS review_count
            FROM videogames v
            JOIN reviews r ON v.game_id = r.game_id
            GROUP BY v.game_title
            ORDER BY review_count DESC
            LIMIT 10;""",
        "Top 10 most played videogames": """
            SELECT game_title, plays
            FROM videogames
            ORDER BY plays DESC
            LIMIT 10;""",
        "Top 10 most wishlisted videogames": """
            SELECT game_title, wishlist
            FROM videogames
            ORDER BY wishlist DESC
            LIMIT 10;""",
        "Top 10 videogames by active players": """
            SELECT game_title, playing
            FROM videogames
            ORDER BY playing DESC
            LIMIT 10;""",
        "Videogames without reviews": """
            SELECT game_title
            FROM videogames
            WHERE game_id NOT IN (
                SELECT DISTINCT game_id FROM reviews);""",
        "Videogames without a rating number": """
            SELECT game_title
            FROM videogames
            WHERE rating IS NULL;""",
        "Videogames without an announced release date": """
            SELECT game_title
            FROM videogames
            WHERE release_date IS NULL;""",
    },
    "developers": {
        "Top 10 developers by number of developed games": """
            SELECT developer, COUNT(*) as num_of_games
            FROM developed_by
            GROUP BY developer
            ORDER BY num_of_games DESC
            LIMIT 10;""",
        "Top 10 developers by the average rating of their games": """
            SELECT d.developer, AVG(v.rating) as avg_rating
            FROM developed_by d
            JOIN videogames v ON d.game_id = v.game_id
            GROUP BY d.developer
            ORDER BY avg_rating DESC
            LIMIT 10;""",
        "Top 5 developers with most reviews": """
            SELECT d.developer, COUNT(r.game_id) as total_reviews
            FROM developed_by d
            JOIN reviews r ON d.game_id = r.game_id
            GROUP BY d.developer
            ORDER BY total_reviews DESC
            LIMIT 10;""",
        "Developers who have developed for more than 10 genres": """
            SELECT d.developer, COUNT(DISTINCT gi.genre_name) as num_genres
            FROM developed_by d
            JOIN genre_is gi ON d.game_id = gi.game_id
            GROUP BY d.developer
            HAVING num_genres > 10
            ORDER BY num_genres DESC;""",
    },
    "genres": {
        "List of all genres": """
            SELECT name
            FROM genre;""",
        "Number of videogames per genre": """
            SELECT genre_name, COUNT(*) as num_of_games
            FROM genre_is
            GROUP BY genre_name
            ORDER BY num_of_games DESC;""",
        "Average rating of games per genre": """
            SELECT g.genre_name, AVG(v.rating) as avg_rating
            FROM genre_is g
            JOIN videogames v ON g.game_id = v.game_id
            GROUP BY g.genre_name
            ORDER BY avg_rating DESC;""",
        "Genres with the fewest videogames": """
            SELECT g.genre_name, COUNT(*) as num_of_games
            FROM genre_is g
            GROUP BY g.genre_name
            ORDER BY num_of_games ASC
            LIMIT 5;""",
    },
}
categories = ["database", "videogames", "developers", "genres"]


def query_categories() -> list:
    return categories


def queries_inside(category: str) -> list:
    queries_list = list(queries[category].keys())
    queries_list.sort()
    return queries_list


def execute_query(user: str, password: str, category: str, query: str):
    try:
        with mysql.connect(
            host="localhost", user=user, passwd=password, database="popular_videogames"
        ) as db_conn:
            with db_conn.cursor() as cursor:
                query = queries[category][query]
                cursor.execute(query)
                output = pd.DataFrame(
                    cursor.fetchall(), columns=[desc[0] for desc in cursor.description]
                )
                print(output.to_string(index=False))
    except mysql.Error as err:
        print(f"Error: {err}")
