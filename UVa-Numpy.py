
# coding: utf-8

# # Uva API to CSV
# 
# UVa has an api end point at [Uhunt](http://uhunt.felix-halim.net/api). This python script will extract information from api end point and convert to CSV files for Data Analysis

# In[49]:

import pandas as pd
import requests as rq
import numpy as np
from IPython.display import HTML, display
import os.path as path
import time
import datetime
import shlex

def header(s):
    display( HTML('<h1 align="center">{}</h1>'.format(s)) )
    
def subheader(s):
    display( HTML('<h3 align="center">{}</h3>'.format(s)) )

def boolToStr (x):
    return 'true' if x else 'false'

def timeToStamp(x):
    t = type(x)
    if t == pd.tslib.Timestamp:
        x = x.to_pydatetime()

    if type(x) != datetime.datetime:
        raise ValueError("Please send datetime.datetime or pandas.Timestamp. You have sent {}".format(type(x)))
        
    return  int( x.timestamp() * 1000 )


# In[44]:

def toUUID(x):
    return "00000000-0000-0000-0000-{:012}".format(x)


# # UVa Problem List
# 
# First, lets grab all the problems on UVa

# In[3]:

if path.exists('problem.csv'):
    problemDF = pd.read_csv('problem.csv')
else: 
    # Grab it from API
    problemList = rq.get( 'http://uhunt.felix-halim.net/api/p' ).json()
    columns = [ 
        'id', 
        'number', 
        'title',
        'dacu',
        'best_runtime',
        'dummy',
        'best_memory',
        'no_verdict',
        'submission_error', 
        'cannot_judge',
        'in_queue',
        'CE', # Compilation Error
        'RF', # Restricted Function
        'RTE',
        'OLE',
        'TLE',
        'MLE',
        'WA',
        'PE',
        'AC',
        'runtime',
        'status'
    ]
    problemDF = pd.DataFrame.from_records(problemList, columns=columns)
    del problemDF['dummy']
    problemDF.to_csv('problem.csv', index=False)
    
problemDF = problemDF.to_records()
display(problemDF)

totalProblem = len(problemDF)
print ( "Total number of problems = {}".format(totalProblem))


# # Grab All Submissions from 2016
# 
# In order to grab all submissions in 2016, we will be using the following api: `/api/p/subs/{pids-csv}/{start-sbt}/{end-sbt}.` For this, we will be requiring problem-ids as csv and start-end time as unix time stamp

# In[6]:

# # Grab start and end time in unix time stamp
# # date -d "Jan 1 2016" +%s 
start2016 = 1451584800
end2016 = 1483120800

problemIdList = problemDF['id']

subProblemIdList = problemIdList
totalSubProblem = len(subProblemIdList) - 1

problemSubFolder = 'problem_sub_2016'

# for ind, problemId in enumerate ( subProblemIdList ) :
#     fileName = path.join( problemSubFolder, "{}.csv".format(problemId) )
#     if path.exists(fileName) :
#         if ind % 1000 == 0:
#             print ( "{:5}/{:<5} Done Processing Problem Id: {}. Already exists.".format(ind, totalSubProblem, problemId) )
#     else :
#         subAPI = "http://uhunt.felix-halim.net/api/p/subs/{}/{}/{}".format(problemId, start2016, end2016)
#         sub = rq.get(subAPI).json()
#         subDF = pd.DataFrame.from_records(sub)
#         subDF.to_csv(fileName, index=False )
#         if ind % 1000 == 0:
#             print ( "{:5}/{:<5} Done Processing Problem Id: {}".format(ind, totalSubProblem, problemId) )


# ### Finally Finshed!
# 
# Now, lets merge all these files into one file.
# 
# **Important** : Merging all problems into one files creates a csv file of size 120MB with over 5 million rows! So lets just merge a small amount of files.

# In[54]:

mergedFile = 'merged2016.csv'

if path.exists(mergedFile) is False:
    
    print ( "Merging CSV" )
    mergeSub = open( mergedFile, 'a' )

    for ind, problemId in enumerate ( subProblemIdList ):
        fileName = path.join( problemSubFolder, "{}.csv".format(problemId) )
        f = open(fileName)
        if ind > 0 : # Throw away the header file
            f.readline()
        for line in f:
            mergeSub.write(line)
        f.close()

    mergeSub.close()

submission = pd.read_csv(mergedFile)

vjudgeUser = [ x for x in submission['uname'].unique() if "vjudge" in x  ]
display(vjudgeUser)

print ( "Removing vjudge" )

submission = submission[~submission['uname'].isin(vjudgeUser)]

print ( "Vjudge Removed")

display(submission['ver'].unique())

submission = submission.to_records()

display(len(submission))


# # Convert it to Coding Submission History

# In[ ]:

codingHistory = pd.DataFrame()

verdicts = {
    0  : "Unknown",
    10 : "Submission error",
    15 : " Can't be judged",
    20 : "In queue",
    30 : "Compile error",
    35 : "Restricted function",
    40 : "Runtime error",
    45 : "Output limit",
    50 : "Time limit",
    60 : "Memory limit",
    70 : "Wrong answer",
    80 : "PresentationE",
    90 : "Accepted"
}

languages = {
    1 : "C",
    2 : "Java",
    3 : "C++",
    4 : "Pascal",
    5 : "C++11",
    6 : "Python"
}

codingHistory['user_email'] = ["{}@muktosoft.com".format(x) for x in submission['uname']]
codingHistory['time'] = [datetime.datetime.fromtimestamp(x) for x in submission['sbt']]
codingHistory['accepted'] = [ x == 90 for x in submission['ver']]
codingHistory['exercise_id'] = [toUUID(x) for x in submission['pid']]
codingHistory['language'] = [languages[x] for x in submission['lan']]
codingHistory['runtime'] = submission['run']
codingHistory['verdict'] = [verdicts[x] for x in submission['ver']]
codingHistory['sid'] = submission['sid']

codingHistory = codingHistory.sort_values(by="time").reset_index(drop=True)
codingHistory = codingHistory.to_records()

print ( "Total length of coding history rows {}".format(len(codingHistory)))
codingHistory


# In[45]:

def genUsgeLog( startU, endU):
    """Generate usage_log from codingHistory
    
    You can send parameters to slice users
    
    @startU: start of slice
    @endU: end of slice
    """
    usageLog = pd.DataFrame()

    # Usage log will have user emails for each submission
    usageLog['user_email'] = codingHistory['user_email']
    usageLog['event_type'] = 'view'
    usageLog['current_item_id'] = codingHistory['exercise_id']
    usageLog['current_item_type'] = 'lesson'
    usageLog['current_enter_time'] = codingHistory['time']
    usageLog['is_completed'] = False
    usageLog['is_first_view'] = False
    usageLog['session_id'] = ''
    usageLog['session_start_time'] = np.datetime64()
    usageLog['previous_item_id'] = toUUID(0)
    usageLog['previous_item_type'] = "unknown"
    usageLog['previous_enter_time'] = datetime.datetime.fromtimestamp(0)
    usageLog['sid'] = codingHistory['sid']
    
    usageLog = usageLog.to_records()
    
    users = pd.unique ( codingHistory['user_email'] )[startU:endU]
    total = len(users)
    
    newRows = [usageLog]
    timeDeltaMinus = pd.Timedelta(seconds=-1)
    timeDeltaPlus = pd.Timedelta(seconds=1)
    
    for unum, user in enumerate( users ):

        userSub = codingHistory[ codingHistory['user_email'] == user ]
        
        totalSub = len(userSub)
        if unum % 1000 == 0:
            print ( "Processing {:20} She has {} submissions. {:5}/{:<5}".format(user, totalSub, unum+1, total) )

        solved = set() # Contains solved problem by user
        seen = set()

        prevDate = datetime.date(2015,12,31)
        sessionId = toUUID(0)
        sessionStart = datetime.date(2015,12,31)
        prevItemId = toUUID(0)
        prevItemType = "unknown"
        prevEnterTime = datetime.datetime.fromtimestamp(0)
        prevIndex = -1
        
        loopInd = 0
        
        for sub in userSub:
            index = sub['index']
            loopInd = loopInd + 1
            #print("Processing Submissions {:5}/{:<5}".format(loopInd,totalSub) )

            eid = sub['exercise_id']
            today = pd.Timestamp( sub['time'] ).to_pydatetime().date()
            delta = today - prevDate

            if delta.days > 0: # A new day has started
                # Create a session end event for previous session
                if prevIndex > -1:
                    newRow = usageLog[prevIndex:prevIndex+1].copy()
                    newRow['event_type'] = 'end'
                    newRows.append(newRow)
                
                prevDate = today
                sessionId = toUUID(sub['sid'])
                sessionStart = sub['time'] + timeDeltaMinus
                prevItemId = toUUID(0)
                prevItemType = "unknown"
                prevEnterTime = datetime.datetime.fromtimestamp(0)
                
                 # Create a session start event for this session
                newRow = usageLog[index:index+1].copy()
                newRow['event_type'] = 'start'
                newRow['session_id'] = sessionId
                newRow['session_start_time'] = sessionStart
                newRow['current_enter_time'] = newRow['session_start_time']
                newRows.append(newRow)
                
        
            usageLog[index].session_id = sessionId
            usageLog[index].session_start_time = sessionStart
    
            usageLog[index].previous_item_id = prevItemId
            usageLog[index].previous_item_type = prevItemType
            usageLog[index].previous_enter_time = prevEnterTime

            prevItemId = eid
            prevItemType = 'lesson'
            prevEnterTime = sub['time']

            if eid in seen:
                usageLog[index].is_first_view = False
            else:
                seen.add(eid)
                usageLog[index].is_first_view = True

            if sub['verdict'] == 'Accepted' or eid in solved:
                solved.add(eid)
                usageLog[index].is_completed = True
            else:
                usageLog[index].is_completed = False
                
            prevIndex = index
    
    usageLog = np.concatenate(newRows)
    usageLog = pd.DataFrame(usageLog)
    usageLog.loc[usageLog['event_type'] == 'end', 'current_enter_time'] += timeDeltaPlus
    
    usageLog = usageLog.sort_values(by='current_enter_time').reset_index(drop=True)
    usageLog = usageLog[['user_email','session_id','session_start_time','event_type','current_item_id',
                        'current_item_type','current_enter_time','is_completed','is_first_view',
                        'previous_item_id','previous_item_type','previous_enter_time']]
    return usageLog


# In[18]:

usageLogCSV = 'usage_log.csv'
if path.exists(usageLogCSV):
    usageLog = pd.read_csv(usageLogCSV, parse_dates=['session_start_time',
                                                     'current_enter_time',
                                                     'previous_enter_time'] )
else:
    totalUsers = len ( np.unique( codingHistory['user_email'] ) )
    display(totalUsers)

    usageLog = genUsgeLog(0,totalUsers)
    usageLog.to_csv('usage_log.csv', index=False)


# # Time to insert into Database
# 
# Every thing is set. We have our coding submission history and usage log. Now, we just need to insert them in our database.

# In[11]:

from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect('celica_al_engine_db') # Connect to database and keyspace

for table in ['usage_log', 'user', 'lesson', 'coding_submission_history', 'lesson_exercise']:
    session.execute("TRUNCATE {}".format(table))


# # Insert Users

# In[81]:

def createUserTable():
    session.execute("""CREATE TABLE celica_al_engine_db.user ( email text PRIMARY KEY, username text );""")
    
def insertUsers( users ):
    total = len(users)
    for ind, user in enumerate( users ):
        if ind % 1000 == 0:
            print ( "Processing {} out of {}".format(ind,total) )
            
        command = """
        INSERT INTO celica_al_engine_db.user(
            email,
            username            
        ) VALUES (
            {!r},
            {!r}
        );
        """.format( user, user.split("@")[0] )

        session.execute(command)


# In[82]:

users = usageLog['user_email'].unique()
print ( "Total number of users: {}".format(len(users)) )

# createUserTable()
insertUsers(users)


# # Insert Lessons

# In[29]:

def createLessonTable():
    session.execute(
        """
        CREATE TABLE celica_al_engine_db.lesson (
            id uuid,
            created_at timestamp,
            body text,
            dependent_lessons list<uuid>,
            knowledge_components list<text>,
            name text,
            PRIMARY KEY (id, created_at)
        );
        """
    )

def insertLessons(lessons):
    total = len(lessons)
    
    for ind, lesson in enumerate(lessons):
        if ind % 1000 == 0:
            print ( "Processing {} out of {}".format(ind,total) )
            
        id = lesson[0]
        title = lesson[1].replace('\'', '').replace('"','')        

        command = """
        INSERT INTO celica_al_engine_db.lesson(
            id,
            created_at,
            body,
            dependent_lessons,
            knowledge_components,
            name
        ) VALUES (
            {!s},
            {!s},
            'Dummy Body',
            [{!s}],
            ['dummy'],
            {!r}
        );
        """.format(toUUID(id), int( datetime.datetime.now().timestamp() * 1000 ), toUUID(0),title)

        session.execute(command)
        
def insertLessonExercise(lessons):
    total = len(lessons)
    
    for ind, lesson in enumerate(lessons):
        if ind % 1000 == 0:
            print ( "Processing {} out of {}".format(ind,total) )
            
        id = lesson[0]
        title = lesson[1].replace('\'', '').replace('"','')        

        command = """
        INSERT INTO celica_al_engine_db.lesson_exercise(
            lesson_id,
            exercise_ids
        ) VALUES (
            {!s},
            [{!s}]
        );
        """.format(toUUID(id), toUUID(id))
        
        session.execute(command)


# In[30]:

# createLessonTable()

problemDF = pd.DataFrame(problemDF)

lessons = problemDF[['id', 'title']].values

print ( "Total number of lessons: {}".format(len(lessons)))

insertLessons(lessons)
insertLessonExercise(lessons)


# # Insert CodingHistory

# In[22]:

def createCodingHistory():
    session.execute(
        """
        CREATE TABLE celica_al_engine_db.coding_submission_history (
            user_email text,
            time timestamp,
            accepted boolean,
            duplicate boolean,
            exercise_id uuid,
            knowledge_components list<text>,
            language text,
            lesson_ids list<uuid>,
            memory_used int,
            point int,
            point_delta int,
            runtime int,
            verdict text,
            volume_ids list<uuid>,
            PRIMARY KEY (user_email, time)
        );
        """
    )

def insertCodingHistory( history ):
    total = len(history)
    for ind, h in enumerate( history ):
        
        if ind % 1000 == 0:
            print ( "Processing {} out of {}".format(ind,total) )
        
        command = """
        INSERT INTO celica_al_engine_db.coding_submission_history(
            user_email,
            time,
            accepted,
            duplicate,
            exercise_id,
            knowledge_components,
            language,
            lesson_ids,
            memory_used,
            point,
            point_delta,
            runtime,
            verdict,
            volume_ids
        ) VALUES (
            {!r},
            {!s},
            {!s},
            {!s},
            {!s},
            ['dummy'],
            {!r},
            [{!s}],
            0,
            {!s},
            0,
            {!s},
            {!r},
            [{!s}]
        );
        """.format(
            h.user_email,
            h.time.tolist() // 1000000,
            'true' if h.accepted else 'false',
            'false',
            h.exercise_id,
            h.language,
            h.exercise_id,
            10 if h.accepted else 0,
            h.runtime,
            h.verdict,
            toUUID(0)
        )
        session.execute(command)


# In[23]:

# createCodingHistory()

insertCodingHistory(codingHistory)


# # Insert Usage Log

# In[12]:

def createUsageLog():
    session.execute(
        """
        CREATE TABLE celica_al_engine_db.usage_log (
            user_email text,
            session_id uuid,
            session_start_time timestamp,
            event_type text,
            current_item_id uuid,
            current_item_type text,
            current_enter_time timestamp,
            is_completed boolean,
            is_first_view boolean,
            previous_item_id uuid,
            previous_item_type text,
            previous_enter_time timestamp,
            volume_id uuid,
            created_at timestamp,
            PRIMARY KEY (user_email, session_id, current_enter_time)
        );
        """
    )

def insertUsageLog( usageLog ):
    total = len(usageLog)
    for ind, use in usageLog.iterrows():
        
        if ind % 1000 == 0:
            print ( "Processing {} out of {}".format(ind,total) )
        
        command = """
        INSERT INTO celica_al_engine_db.usage_log (
            user_email,
            session_id, session_start_time,
            event_type,
            current_item_id, current_item_type, current_enter_time,
            is_completed, is_first_view,
            previous_item_id, previous_item_type, previous_enter_time,
            volume_id, created_at
        ) values (
            {!r},
            {!s}, {!s},
            {!r},
            {!s}, {!r}, {!s},
            {!s}, {!s},
            {!s}, {!r}, {!s},
            {!s}, {!s}
        );
        """.format(
            use.user_email,
            use.session_id, timeToStamp(use.session_start_time),
            use.event_type,
            use.current_item_id, use.current_item_type, timeToStamp(use.current_enter_time),
            boolToStr(use.is_completed), boolToStr(use.is_first_view),
            use.previous_item_id, use.previous_item_type, timeToStamp(use.previous_enter_time),
            toUUID(0), timeToStamp(use.current_enter_time)
        )
        
        session.execute(command)


# In[19]:

# createUsageLog()
insertUsageLog(usageLog)

print ( "Total usagelog {}".format(len(usageLog)))


# 
