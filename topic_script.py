import psycopg2
import pandas as pd
from datetime import datetime

# Generate current date and time in the specified format
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

def BuildTopicQuery(id,name, description,UnitId ,minutesAggregate,prerequisites,index,quizQuestionsNumber,quizDefaultMinutes,quizQuestionMark,isDeleted):
    # create query to create new topic with the given data
    query = f"INSERT INTO public.\"Topics\"(\"Id\",\"Name\", \"Description\", \"UnitId\", \"MinutesAggregate\", \"Prerequisites\", \"Index\", \"QuizQuestionsNumber\", \"QuizDefaultMinutes\", \"QuizQuestionMark\", \"IsDeleted\", \"DeletedAt\",  \"CreatedAt\") VALUES ('{id}','{name}', '{description}', '{UnitId}', {minutesAggregate}, '{prerequisites}', {index}, {quizQuestionsNumber}, {quizDefaultMinutes}, {quizQuestionMark}, {isDeleted}, '-infinity' ,'{current_time}');"
    return query


# Connect to the database
connection = psycopg2.connect(database="MozakaraSystemDb", user="postgres", password="1234", host="localhost", port=5432)
cursor = connection.cursor()

# Load the data
xls = pd.ExcelFile('Subjects.xlsx')
Topics = pd.read_excel(xls,'Lessons')

print("Adding Topics to the database...............................")
for i in range(0, len(Topics)):
    finalQuery = BuildTopicQuery(Topics.iloc[i]["Id"],
                                Topics.iloc[i]["Name"],
                                Topics.iloc[i]["Description"],
                                Topics.iloc[i]["UnitId"],
                                Topics.iloc[i]["MinutesAggregate"],
                                Topics.iloc[i]["Prerequisites"],
                                Topics.iloc[i]["Index"],
                                Topics.iloc[i]["QuizQuestionsNumber"],
                                Topics.iloc[i]["QuizDefaultMinutes"],
                                Topics.iloc[i]["QuizQuestionMark"],
                                Topics.iloc[i]["IsDeleted"])
    try:
        cursor.execute(finalQuery)
        connection.commit()
        print(f"topic {Topics.iloc[i]['Name']} added successfully")
    except Exception as e:
        connection.rollback()
        print(f"Error in adding the topic query to the database: {e}")
        continue
    
cursor.close()
connection.close()