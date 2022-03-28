import json
from time import sleep
import psycopg2
import boto3
import dateutil.parser
import requests

f = open('credentials.json')
data = json.load(f)
url = "https://your-domain.atlassian.net/rest/api/3/search"
headers = {
    "Accept": "application/json",
    "Content-Type": "application/json"
}

def insert_jira(issue_type, issue_number, issue_link, assignee, status, project, summary, description, timestamp):
    sql = "insert into table1 (issue_type, issue_number, issue_link, assignee, status, project, summary, description," \
          "timestamp) values (%s, %s, %s,%s, %s, %s,%s, %s, %s) returning id"
    conn = None
    try:
        conn = psycopg2.connect(...)
        cur = conn.cursor()
        cur.execute(sql,
                    (issue_type, issue_number, issue_link, assignee, status, project, summary, description, timestamp))
        id = cur.fetchone()[0]
        print(id, " is inserted")
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def getIssues(startAt, data):
    query = {
        'jql': 'project = YOURTEAM', "maxResults": 1000, "startAt": 100 * startAt
    }
    user = ...
    token = ...
    response = requests.get(url, headers=headers, params=query, auth=(user, token))
    data = response.json()
    issues = data["issues"]

    for issue in issues:
        issue_number = issue["key"]
        issue_fields = issue["fields"]
        issueType = issue_fields["issuetype"]["name"]
        issue_link = "https://your-domain.atlassian.net/browse/" + str(issue_number)
        try:
            assignee = issue_fields["assignee"]["displayName"]
        except:
            continue
        project = issue_fields["project"]["key"]
        summary = issue_fields["summary"]

        changeLog = requests.get(
            "https://your-domain.atlassian.net/rest/api/3/issue/" + str(issue_number) + "/changelog/",
            headers=headers, auth=(user, token))
        changeLogData = changeLog.json()

        for elem in changeLogData["values"]:
            for item in elem["items"]:
                status = ""
                description = ""
                timestamp = ""
                if item["field"] == "status":
                    d = dateutil.parser.parse(elem["created"])
                    status = item["toString"]
                    timestamp = int(d.timestamp())
                if item["field"] == "description":
                    description = item["toString"]
                if status == "":
                    continue

                insert_jira(issueType, issue_number, issue_link, assignee, status, project, summary, description,
                            timestamp)


count = 0
limit = 100 #go until 100*100 issue
while count < limit:
    getIssues(count, data)
    count += 1
