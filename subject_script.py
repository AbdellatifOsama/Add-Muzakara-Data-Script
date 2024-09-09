import psycopg2
import pandas as pd
from datetime import datetime

# Generate current date and time in the specified format
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

def BuildSubjectQuery(id,name, description,studyLevelId ,unitsMinutesAggregate,views,valid,quizQuestionsNumber,quizDefaultMinutes,quizQuestionMark,isDeleted):
    # create query to create new subject with the given data
    query = f"INSERT INTO public.\"Subjects\"(\"Id\",\"Name\", \"Description\", \"StudyLevelId\", \"UnitsMinutesAggregate\", \"Views\", \"Valid\", \"QuizQuestionsNumber\", \"QuizDefaultMinutes\", \"QuizQuestionMark\", \"IsDeleted\", \"DeletedAt\",  \"CreatedAt\") VALUES ('{id}','{name}', '{description}', '{studyLevelId}', {unitsMinutesAggregate}, {views}, {valid}, {quizQuestionsNumber}, {quizDefaultMinutes}, {quizQuestionMark}, {isDeleted}, '-infinity' ,'{current_time}');"
    return query

# Connect to the database
connection = psycopg2.connect(database="MozakaraSystemDb", user="postgres", password="postgres", host="localhost", port=5432)
cursor = connection.cursor()

# Load the data
xls = pd.ExcelFile('Yemen -General Education -level 11.xlsx')
Subjects = pd.read_excel(xls,'Subjects')


print("Adding Subjects to the database...............................")

for i in range(0, len(Subjects)):
    finalQuery = BuildSubjectQuery(Subjects.iloc[i]["Id"],
                                Subjects.iloc[i]["Name"],
                                Subjects.iloc[i]["Description"],
                                Subjects.iloc[i]["StudyLevelId"],
                                Subjects.iloc[i]["UnitsMinutesAggregate"],
                                Subjects.iloc[i]["Views"],
                                Subjects.iloc[i]["Valid"],
                                Subjects.iloc[i]["QuizQuestionsNumber"],
                                Subjects.iloc[i]["QuizDefaultMinutes"],
                                Subjects.iloc[i]["QuizQuestionMark"],
                                Subjects.iloc[i]["IsDeleted"])
    try:
        cursor.execute(finalQuery)
        connection.commit()
        print(f"subject {Subjects.iloc[i]['Name']} added successfully")
    except Exception as e:
        connection.rollback()
        print(f"Error in adding the subject query to the database: {e}")
        continue
    
cursor.close()
connection.close()