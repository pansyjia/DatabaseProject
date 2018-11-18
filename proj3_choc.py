import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

def init_db_choc():
    try:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
    except:
        print("Fail to connect to the database.")

    # drop the tables if exist
    statement = '''
        DROP TABLE IF EXISTS 'Bars';
    '''
    cur.execute(statement)
    conn.commit()

    statement = '''
        DROP TABLE IF EXISTS 'Countries';
    '''
    cur.execute(statement)
    conn.commit()

    # create two tables
    statement = '''
        CREATE TABLE 'Bars' (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Company' TEXT,
            'SpecificBeanBarName' TEXT,
            'REF' TEXT,
            'ReviewDate' TEXT,
            'CocoaPercent' REAL,
            'CompanyLocation' TEXT,
            'CompanyLocationId' INTEGER,
            'Rating' REAL,
            'BeanType' TEXT,
            'BroadBeanOrigin' TEXT,
            'BroadBeanOriginId' INTEGER
        );
    '''
    try:
        cur.execute(statement)
    except:
        print("Fail to create table Bars.")
    conn.commit()


    statement = '''
        CREATE TABLE 'Countries' (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Alpha2' TEXT,
            'Alpha3' TEXT,
            'EnglishName' TEXT,
            'Region' TEXT,
            'Subregion' TEXT,
            'Population' INTEGER,
            'Area' REAL
        );
    '''
    try:
        cur.execute(statement)
    except:
        print("Fail to create table Countries.")
    conn.commit()

    #close db connection
    conn.close()

# insert file data into the db
def insert_csv(FNAME):
    # connect to the db
    try:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
    except:
        print("Fail to connect to the database")

    #read data from csv
    with open(FNAME, 'r') as csv_file:
        csv_data = csv.reader(csv_file)

        #skip the first row
        next(csv_data)

        for row in csv_data:
            insert_statement = '''
                INSERT OR IGNORE INTO "Bars"
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            values = (None, row[0], row[1], row[2], row[3], float(str(row[4]).strip('%')), row[5], None, row[6], row[7], row[8], None)
            cur.execute(insert_statement, values)
            conn.commit()
            # conn.close()


def insert_json(FILENAME):
    # connect to the db
    try:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
    except:
        print("Fail to connect to the database")

    json_file = open(FILENAME, 'r')
    json_data = json_file.read()
    json_dict = json.loads(json_data)

    for row in json_dict:
        insert_statement = '''
            INSERT OR IGNORE INTO "Countries"
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        '''
        values = (None, row["alpha2Code"], row["alpha3Code"], row["name"], row["region"], row["subregion"], row["population"], row["area"])
        cur.execute(insert_statement, values)
        conn.commit()
        # conn.close()


## join tables and insert foreign keys
def update_tables():
    # connect to the db
    try:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
    except:
        print("Fail to connect to the database")

    # set keys
    update_CompanyLocationId = '''
        UPDATE Bars
        SET (CompanyLocationId) = (SELECT c.ID FROM Countries AS c WHERE Bars.CompanyLocation = c.EnglishName)
    '''

    update_BroadBeanOriginId = '''
        UPDATE Bars
        SET (BroadBeanOriginId) = (SELECT c.ID FROM Countries AS c WHERE Bars.BroadBeanOrigin = c.EnglishName)
    '''

    cur.execute(update_CompanyLocationId)
    cur.execute(update_BroadBeanOriginId)
    conn.commit()



# Part 2: Implement logic to process user commands
def bars_command(specification="", keyword="", criteria="ratings", sorting="top", limit="10"):
    #connect to Database
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    # select outputs
    if "C1" in specification:
        statement = "SELECT SpecificBeanBarName, Company, CompanyLocation,     Rating, CocoaPercent, BroadBeanOrigin "
        statement += "FROM Bars "
        statement += "JOIN Countries AS C1 ON Bars.CompanyLocationId = C1.Id "
    elif "C2" in specification:
        statement = "SELECT SpecificBeanBarName, Company, CompanyLocation, Rating, CocoaPercent, BroadBeanOrigin "
        statement += "FROM Bars  "
        statement += "JOIN Countries AS C2 ON Bars.BroadBeanOriginId = C2.Id "
    else:
        statement = "SELECT SpecificBeanBarName, Company, CompanyLocation, Rating, CocoaPercent, BroadBeanOrigin "
        statement += "FROM Bars "

    # format statement
    if specification != "":
        if "Alpha2" in specification:
            keyword = keyword.upper()
        try:
            statement += "WHERE {} = '{}' ".format(specification, keyword)
        except:
            print("Fail to specify bars command.")

    # ratings/cocoa
    if criteria == "ratings":
        statement += "ORDER BY {} ".format("Rating")
    elif criteria == "cocoa":
        statement += "ORDER BY {} ".format("CocoaPercent")

    #top or bottom
    if sorting == "top":
        statement += "{} ".format("DESC")
    elif sorting == "bottom":
        statement += "{} ".format("ASC")

    # limit
    statement += "LIMIT {} ".format(limit)

    #return a list of tuples
    results = []
    rows = cur.execute(statement).fetchall()
    for row in rows:
        results.append(row)
    conn.commit()

    return results


def companies_command(specification="", keyword="", criteria="ratings", sorting="top", limit="10"):
    #connect to Database
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    #select outputs
    if criteria == "ratings":
        statement = "SELECT Company, CompanyLocation, AVG(Rating) "
    elif criteria == "cocoa":
        statement = "SELECT Company, CompanyLocation, AVG(CocoaPercent) "
    elif criteria == "bars_sold":
        statement = "SELECT Company, CompanyLocation,COUNT(SpecificBeanBarName) "

    statement += "FROM Bars "

    # format statement
    if "C1.Alpha2" in specification:
        statement += "JOIN Countries AS C1 ON Bars.CompanyLocationId = C1.Id "
        statement += "GROUP BY Company "
        statement += "HAVING COUNT(SpecificBeanBarName) > 4 "
    elif "C2.Alpha2" in specification:
        statement += "JOIN Countries AS C2 ON Bars.BroadBeanOriginId = C2.Id "
        statement += "GROUP BY Company "
        statement += "HAVING COUNT(SpecificBeanBarName) > 4 "
    elif specification == "Alpha2" or specification == "Region":
        statement += "JOIN Countries ON Bars.CompanyLocation = Countries.EnglishName "
        statement += "GROUP BY Company "
        statement += "HAVING COUNT(SpecificBeanBarName) > 4 "
    else:
        statement += "GROUP BY Company "
        statement += "HAVING COUNT(SpecificBeanBarName) > 4 "

    if specification != "":
        if "Alpha2" in specification:
            keyword = keyword.upper()
        try:
            statement += "AND {} = '{}' ".format(specification, keyword)
        except:
            print("Fail to specify companies command.")

    # ratings/cocoa
    if criteria == "ratings":
        statement += "ORDER BY {} ".format("AVG(Rating)")
    elif criteria == "cocoa":
        statement += "ORDER BY {} ".format("AVG(CocoaPercent)")
    elif criteria == "bars_sold":
        statement += "ORDER BY {} ".format("COUNT(SpecificBeanBarName)")

    # top or bottom
    if sorting == "top":
        statement += "{} ".format("DESC")
    elif sorting == "bottom":
        statement += "{} ".format("ASC")

    # limit
    statement += "LIMIT {}".format(limit)

    # return a list of tuples
    results = []
    # print(statement)
    rows = cur.execute(statement).fetchall()
    for row in rows:
        results.append(row)
    conn.commit()

    return results


def countries_command(specification="", keyword="", criteria="ratings", sorting="top", limit="10", sources="sellers"):
    ##connect to Database
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    #select outputs
    statement = "SELECT EnglishName, Region, "

    if criteria == "ratings":
        statement += "AVG(Rating) "
    elif criteria == "cocoa":
        statement += "AVG(CocoaPercent) "
    elif criteria == "bars_sold":
        statement += "COUNT(SpecificBeanBarName) "

    statement += "FROM Countries "

    if sources == "sellers":
        statement += "JOIN Bars ON Countries.Id = Bars.CompanyLocationId "
    elif sources == "sources":
        statement += "JOIN Bars ON Countries.Id = Bars.BroadBeanOriginId "

    statement += "GROUP BY EnglishName "
    statement += "HAVING COUNT(SpecificBeanBarName) > 4 "

    if specification != "":
        if "Region" in specification:
            keyword = keyword.title()
        try:
            statement += "AND {} = '{}' ".format(specification, keyword)
        except:
            print("Fail to specify countries command.")

    # ratings/cocoa
    if criteria == "ratings":
        statement += "ORDER BY {} ".format("AVG(Rating)")
    elif criteria == "cocoa":
        statement += "ORDER BY {} ".format("AVG(CocoaPercent)")
    elif criteria == "bars_sold":
        statement += "ORDER BY {} ".format("COUNT(SpecificBeanBarName)")

    # top or bottom
    if sorting == "top":
        statement += "{} ".format("DESC")
    elif sorting == "bottom":
        statement += "{} ".format("ASC")

    # limit result number
    statement += "LIMIT {} ".format(limit)

    # return a list of tuples
    results = []
    rows = cur.execute(statement).fetchall()
    for row in rows:
        results.append(row)
    conn.commit()

    return results



def regions_command(specification="", keyword="", criteria="ratings", sorting="top", limit="10", sources="sellers"):
    #connect to Database
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    #select outputs
    statement = "SELECT Region, "

    if criteria == "ratings":
        statement += "AVG(Rating) "
    elif criteria == "cocoa":
        statement += "AVG(CocoaPercent) "
    elif criteria == "bars_sold":
        statement += "COUNT(SpecificBeanBarName) "

    statement += "FROM Countries "

    if sources == "sellers":
        statement += "JOIN Bars ON Countries.Id = Bars.CompanyLocationId "
    elif sources == "sources":
        statement += "JOIN Bars ON Countries.Id = Bars.BroadBeanOriginId "

    statement += "GROUP BY Region "
    statement += "HAVING COUNT(SpecificBeanBarName) > 4 "

    # ratings/cocoa
    if criteria == "ratings":
        statement += "ORDER BY {} ".format("AVG(Rating)")
    elif criteria == "cocoa":
        statement += "ORDER BY {} ".format("AVG(CocoaPercent)")
    elif criteria == "bars_sold":
        statement += "ORDER BY {} ".format("COUNT(SpecificBeanBarName)")

    # top or bottom
    if sorting == "top":
        statement += "{} ".format("DESC")
    elif sorting == "bottom":
        statement += "{} ".format("ASC")

    # limit result number
    statement += "LIMIT {} ".format(limit)

    # return a list of tuples
    results = []
    rows = cur.execute(statement).fetchall()
    for row in rows:
        results.append(row)
    conn.commit()

    return results


# formate outputs
def str_output(str_result):
    if len(str_result) > 12:
        formatted_output = str_result[:12] + "..."
    else:
        formatted_output = str_result
    return formatted_output

def perc_output(cocoa):
    formatted_output = str(cocoa).replace(".0", "%")
    return formatted_output

def digi_output(rating):
    formatted_output = "{0:.1f}".format(rating, 1)
    return formatted_output


#process commands
def process_command(command):
    command_list = command.lower().split()

    command_dict = {
        "specification":"",
        "keyword":"",
        "criteria": "ratings",
        "sorting":"top",
        "limit":"10",
        "sources": "sellers"
    }

    command_type_list = ["bars", "companies", "countries", "regions"]
    criteria_list = ["ratings", "cocoa", "bars_sold"]
    sorting_list = ["top", "bottom"]
    sources_list = ["sellers", "sources"]
    specification_list = ["sellcountry", "sourcecountry", "sellregion", "sourceregion", "country", "region", "sellers", "sources"]

    implement = True

    for command in command_list:
        if command in command_type_list:
            command_dict["command_type"] = command
        elif command in criteria_list:
            command_dict["criteria"] = command
        elif command in sources_list:
            command_dict["sources"] = command
        elif "=" in command:
            spec_list = command.split("=")
            for item in spec_list:
                if item in sorting_list:
                    command_dict["sorting"] = spec_list[0]
                    command_dict["limit"] = spec_list[1]
                elif item in specification_list:
                    if spec_list[0] == "sellcountry":
                        command_dict["specification"] = "C1.Alpha2"
                    elif spec_list[0] == "sourcecountry":
                        command_dict["specification"] = "C2.Alpha2"
                    elif spec_list[0] == "sellregion":
                        command_dict["specification"] = "C1.Region"
                    elif spec_list[0] == "sourceregion":
                        command_dict["specification"] = "C2.Region"
                    elif spec_list[0] == "country":
                        command_dict["specification"] = "Alpha2"
                    else:
                        command_dict["specification"] = spec_list[0].title()
                    command_dict["keyword"] = spec_list[1].title()
        else:
            implement = False
            print("Invalid Input or Exit. ")

    results = []
    # execute bars_command
    if command_dict["command_type"] == "bars" and implement == True:
        results = bars_command(command_dict["specification"], command_dict["keyword"], command_dict["criteria"], command_dict["sorting"], command_dict["limit"])

        # 6-tuple: 'SpecificBeanBarName','Company', 'CompanyLocation', 'Rating', 'CocoaPercent', 'BroadBeanOrigin'
        row_format = "{0:16} {1:16} {2:16} {3:6} {4:6} {5:16}"
        for row in results:
            print(row_format.format(str_output(row[0]), str_output(row[1]), str_output(row[2]), digi_output(row[3]),perc_output(row[4]), str_output(row[5])))

        return results

    elif command_dict["command_type"] == "companies" and implement == True:
        results = companies_command(command_dict["specification"], command_dict["keyword"], command_dict["criteria"], command_dict["sorting"], command_dict["limit"])

        # 3-tuptle: 'Company', 'CompanyLocation', <agg> (i.e., average rating or cocoa percent, or number of bars sold)
        row_format = "{0:16} {1:16} {2:16}"
        for row in results:
            agg = ""
            if command_dict["criteria"] == "ratings":
                agg = digi_output(row[2])
            elif command_dict["criteria"] == "cocoa":
                agg = perc_output(row[2])
            elif command_dict["criteria"] == "bars_sold":
                agg = row[2]

            # print(row[0])
            print(row_format.format(str_output(row[0]), str_output(row[1]),agg))

        return results

    elif command_dict["command_type"] == "countries" and implement == True:
        results = countries_command(command_dict["specification"], command_dict["keyword"], command_dict["criteria"], command_dict["sorting"], command_dict["limit"], command_dict["sources"])

        # 3-tuptle: 'Country', 'Region', <agg> (i.e., average rating or cocoa percent, or number of bars sold)
        row_format = "{0:16} {1:16} {2:16}"
        for row in results:
            (ct, r, agg) = row
            if command_dict["criteria"] == "ratings":
                agg = digi_output(agg)
            elif command_dict["criteria"] == "cocoa":
                agg = perc_output(agg)

            print(row_format.format(str_output(ct), str_output(r),agg))

        return results

    elif command_dict["command_type"] == "regions" and implement == True:
        results = regions_command(command_dict["specification"], command_dict["keyword"], command_dict["criteria"], command_dict["sorting"], command_dict["limit"], command_dict["sources"])

        # 2-tuptle: 'Region', <agg> (i.e., average rating or cocoa percent, or number of bars sold)
        row_format = "{0:16} {1:16}"
        for row in results:
            (r, agg) = row
            if command_dict["criteria"] == "ratings":
                agg = digi_output(agg)
            elif command_dict["criteria"] == "cocoa":
                agg = perc_output(agg)

            print(row_format.format(str_output(r), agg))

        return results



def load_help_text():
    with open('help.txt') as f:
        return f.read()


# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')

        try:
            results = process_command(response)
        except:
            continue

        if response == 'help':
            print(help_text)
            continue



# # Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    init_db_choc()
    insert_csv(BARSCSV)
    insert_json(COUNTRIESJSON)
    update_tables()
    interactive_prompt()
