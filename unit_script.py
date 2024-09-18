import psycopg2
import pandas as pd
from datetime import datetime

# Generate current date and time in the specified format
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
def BuildUnitQuery(id,name, description,SubjectId ,topicsMinutesAggregates,prerequisites,index,quizQuestionsNumber,quizDefaultMinutes,quizQuestionMark,isDeleted):
    # create query to create new unit with the given data
    query = f"INSERT INTO public.\"Units\"(\"Id\",\"Name\", \"Description\", \"SubjectId\", \"TopicsMinutesAggregates\", \"Prerequisites\", \"Index\", \"QuizQuestionsNumber\", \"QuizDefaultMinutes\", \"QuizQuestionMark\", \"IsDeleted\", \"DeletedAt\",  \"CreatedAt\") VALUES ('{id}','{name}', '{description}', '{SubjectId}', {topicsMinutesAggregates}, '{prerequisites}', {index}, {quizQuestionsNumber}, {quizDefaultMinutes}, {quizQuestionMark}, {isDeleted}, '-infinity' ,'{current_time}');"
    return query


# Connect to the database
connection = psycopg2.connect(database="MozakaraSystemDb", user="postgres", password="postgres", host="localhost", port=5432)
cursor = connection.cursor()

# Load the data
xls = pd.ExcelFile('Yemen -General Education -level 10.xlsx')
Units = pd.read_excel(xls,'Units')

print("Adding Units to the database...............................")
for i in range(0, len(Units)):
    finalQuery = BuildUnitQuery(Units.iloc[i]["Id"],
                                Units.iloc[i]["Name"],
                                Units.iloc[i]["Description"],
                                Units.iloc[i]["SubjectId"],
                                Units.iloc[i]["TopicsMinutesAggregates"],
                                Units.iloc[i]["Prerequisites"],
                                Units.iloc[i]["Index"],
                                Units.iloc[i]["QuizQuestionsNumber"],
                                Units.iloc[i]["QuizDefaultMinutes"],
                                Units.iloc[i]["QuizQuestionMark"],
                                Units.iloc[i]["IsDeleted"])
    try:
        cursor.execute(finalQuery)
        connection.commit()
        print(f"Unit {Units.iloc[i]['Name']} added successfully")
    except Exception as e:
        connection.rollback()
        print(f"Error in adding the unit query to the database: {e}")
        continue
    
cursor.close()
connection.close()