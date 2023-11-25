from getpass import getpass
import mysql.connector as mysql
import dbhandling, queries


def print_text(level: int, text: str):
    if level == 1:
        print("\n\n\n\n\n" + "=" * 51)
        print(text)
        print("=" * 51)
    elif level == 2:
        print("\n\n\n\n\n===", text, "===")
    elif level == 3:
        print("\n\n\n---", text, "---")
    elif level == 4:
        print("\n>>>", text)
    elif level == 5:
        print(">", text, "<")
    else:
        print("\n", text)


def select_category() -> str:
    print_text(2, "Database Querying")
    query_categories = queries.query_categories()
    print_text(3, "Query Categories")
    for idx, category in enumerate(query_categories):
        print(f"{idx+1}.   Queries about the {category}")
    print(f"0.   Close program")
    category_choice = int(
        input("\n> Please indicate the number of the category you want to explore: ")
    )
    if category_choice == 0:
        return 0
    return query_categories[category_choice - 1]


def select_query(category: str) -> str:
    print_text(3, f"Queries About The {category.capitalize()}")
    query_options = queries.queries_inside(category)
    for idx, query in enumerate(query_options):
        print(f"{idx+1}.   {query}")
    print(f"0.   Go back")
    query_choice = int(input("\n> Please indicate the number of the query to execute: "))
    if query_choice == 0:
        return 0
    return query_options[query_choice - 1]


def main():
    print_text(1, "Welcome to the Popular Videogames 1980-2023 Dataset")
    print_text(3, "Created by")
    group_members = {
        "Shefik Memedi",
        "Yassir El Arrag",
        "Martina Fagnani",
        "Leonardo Farfan",
        "Edoardo Brown",
    }
    for member in group_members:
        print(f"• {member}")

    print_text(2, "Initialization")

    initialize = False
    while initialize not in ("Y", "N"):
        initialize = input(
            "\n> Would you like to initialize the database from zero? (Y/N): "
        ).capitalize()

    user, password = False, False
    while user == False or password == False:
        user = input("\n> Please type your MySQL server username: ")
        password = getpass("\n> Please type your MySQL server password: ")
        try:
            db_conn = mysql.connect(host="localhost", user=user, passwd=password)
            cursor = db_conn.cursor()

        except mysql.Error as err:
            if err.errno == 1045:
                print(f"\nAuthentication Failed: {err}")
                user, password = False, False
            else:
                print(f"\nError: {err}")

    if initialize == "N":
        cursor.execute("SHOW DATABASES LIKE 'popular_videogames'")
        database_exists = cursor.fetchone()
        if not database_exists:
            print("\nDatabase doesn't exist.")
            initialize = "Y"

    if initialize == "Y":
        dbhandling.resetdb(user, password)
        print_text(4, "Creating database...")
        dbhandling.createdb(user, password)
        print_text(4, "Creating tables...")
        dbhandling.create_tables(user, password)
        print_text(4, "Inserting data...")
        dbhandling.insert_data(user, password)

    while True:
        cat_choice = select_category()
        if cat_choice == 0:
            print_text(4, "Closing program...\n")
            break
        query_choice = select_query(cat_choice)
        if query_choice == 0:
            continue
        print_text(4, "Executing query...\n")
        queries.execute_query(user, password, cat_choice, query_choice)
        print_text(3, "End of Query")
        retry = input("\n> Do you want to execute another query? (Y/N): ").capitalize()
        if retry != "Y":
            print_text(4, "Closing program...\n")
            break

    db_conn.close()
    cursor.close()


if __name__ == "__main__":
    main()
