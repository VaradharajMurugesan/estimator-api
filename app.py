from flask import Flask,request,jsonify
import json
import mysql.connector

app = Flask(__name__)


def getConnection():
    con = mysql.connector.connect(host='emergerejobcareer-mysql.mysql.database.azure.com',
                                    database='estimator',
                                    user='emergere',
                                    password='password-1')   

    return con

@app.route('/EST_User_Posting',methods=['POST'])
def add_user1():
    try:
        projectName=request.json["projectName"]
        estimatorName=request.json["estimatorName"]
        dashBoardName=request.json["dashBoardName"]
        totalEfforts_inPersonHours=request.json["totalEfforts_inPersonHours"]
        retestingEfforts=request.json["retestingEfforts"]
        totalEfforts_inPersonDays=request.json["totalEfforts_inPersonDays"]
        taskGroup=request.json["taskGroup"]
        con = getConnection()
        cur = con.cursor()
        sql="""INSERT INTO estimator(projectName,estimatorName,dashBoardName,totalEfforts_inPersonHours,retestingEfforts,totalEfforts_inPersonDays)
            VALUES (%s,%s,%s,%s,%s,%s)"""
        cur.execute(sql,(projectName,estimatorName,dashBoardName,totalEfforts_inPersonHours,retestingEfforts,totalEfforts_inPersonDays))
        estimatorID=cur.lastrowid
        for lst in taskGroup:   
            cur.execute('INSERT INTO taskGroup(taskGroupName, estimatorID) VALUES (%s,%s)',
                       (lst['taskGroupName'], estimatorID))
            taskGroup_id=cur.lastrowid
            for tsklist in lst["tasks"]:
               cur.execute('INSERT INTO  tasks( taskName, totalNum, totalPerUnit, totalEffort, taskGroup_id) VALUES (%s,%s, %s,%s, %s)',
                           ( tsklist['taskName'], tsklist['totalNum'], tsklist['totalPerUnit'], tsklist['totalEffort'],taskGroup_id))    #row_id = cursor.lastrowid
        con.commit()
        con.close()
        values = request.get_json()
        return jsonify("Added Parameters: ",values)
    
    except Exception as e:
        return jsonify(e,"An ERROR occurred in table POST Method")
    
@app.route('/Est_Getall',methods=['GET'])
def Get_allEstID_tables():
  try:
      con = getConnection()
      cur = con.cursor()
      cur.execute  (""" SELECT JSON_ARRAYAGG(  
                            JSON_OBJECT(
                            'estimatorID', e.estimatorID,
                            'projectName', e.projectName,
                            'estimatorName', e.estimatorName,
                            'dashBoardName', e.dashBoardName,
                            'totalEfforts_inPersonHours', e.totalEfforts_inPersonHours,
                            'retestingEfforts', e.retestingEfforts,
                            'totalEfforts_inPersonDays', e.totalEfforts_inPersonDays,
                            'created_date',e.created_date,
                            'updated_date',e.updated_date,
                            'taskGroup', 
                        (SELECT JSON_ARRAYAGG(
                             JSON_OBJECT(
                                      'taskGroup_id', tg.taskGroup_id, 
                                      'taskGroupname', tg.taskGroupname,
                                      'estimatorID',tg.estimatorID,
                                      'created_date',tg.created_date,
                                      'updated_date',tg.updated_date,
                                      'tasks', 
                                    (SELECT JSON_ARRAYAGG(
                                         JSON_OBJECT(
                                              'task_id', t.task_id, 
                                              'taskName', t.taskName, 
                                              'totalNum', t.totalNum, 
                                              'totalPerUnit', t.totalPerUnit, 
                                              'totalEffort', t.totalEffort,
                                              'taskGroup_id',t.taskGroup_id,
                                              'created_date',t.created_date,
                                              'updated_date',t.updated_date
                      )
                 ) FROM tasks t where t.taskGroup_id =tg.taskGroup_id )
            )
       ) FROM taskGroup tg  WHERE tg.estimatorID = e.estimatorID )
      )
      )FROM estimator e """)
      rows = cur.fetchall()
      result_json_str=rows[0][0]
      result_json = json.loads(result_json_str)
      return jsonify(result_json)
  
  except Exception as e:
   return jsonify(e,"An ERROR occurred in table GET Method")

@app.route('/getbyID_Estimator/<int:estimatorID>', methods=['GET'])
def Get_byID_Estimator(estimatorID):
  try:
      con = getConnection()
      cur = con.cursor()
      cur.execute("SELECT * FROM estimator WHERE estimatorID = %s", [estimatorID])
      row = cur.fetchone()
      if row is None:
          return jsonify("Record not found"), 404
      rows = cur.execute("""SELECT JSON_OBJECT(
                              'estimatorID', e.estimatorID,
                              'projectName', e.projectName,
                              'estimatorName', e.estimatorName,
                              'dashBoardName', e.dashBoardName,
                              'totalEfforts_inPersonHours', e.totalEfforts_inPersonHours,
                              'retestingEfforts', e.retestingEfforts,
                              'totalEfforts_inPersonDays', e.totalEfforts_inPersonDays,
                              'created_date',e.created_date,
                              'updated_date',e.updated_date,
                              'taskGroup', 
                              (SELECT JSON_ARRAYAGG(
                                  JSON_OBJECT(
                                      'taskGroup_id', tg.taskGroup_id, 
                                      'taskGroupname', tg.taskGroupname,
                                      'estimatorID',tg.estimatorID,
                                      'created_date',tg.created_date,
                                      'updated_date',tg.updated_date,
                                      'tasks', 
                                      (SELECT JSON_ARRAYAGG(
                                          JSON_OBJECT(
                                              'task_id', t.task_id, 
                                              'taskName', t.taskName, 
                                              'totalNum', t.totalNum, 
                                              'totalPerUnit', t.totalPerUnit, 
                                              'totalEffort', t.totalEffort,
                                              'taskGroup_id',t.taskGroup_id,
                                              'created_date',t.created_date,
                                              'updated_date',t.updated_date
                                          )
                                      ) FROM tasks t WHERE t.taskGroup_id = tg.taskGroup_id)
                                  )
                                ) FROM taskGroup tg WHERE tg.estimatorID = e.estimatorID)
                            ) FROM estimator e WHERE e.estimatorID = %s""", (estimatorID,))
                            
      rows = cur.fetchall()
      if len(rows) == 0:
          return jsonify("please enter a valid estimatorID")
      con.close()
      result_json_str = rows[0][0]
      result_json = json.loads(result_json_str)
      return jsonify(f"Showing estimatorID : {estimatorID}", result_json)
  
  except Exception as e:
   return jsonify(e,"An ERROR found in getting single ID")

@app.route('/EST_bi_UPDATE_id',methods=['PUT'])
def update_user1(): 
    try: 
        request1= request.get_json()    
        for lst in request1:
            estimatorID=lst["estimatorID"]
            projectName=lst["projectName"]
            estimatorName=lst["estimatorName"]
            dashboardName=lst["dashBoardName"]
            totalEfforts_inPersonHours=lst["totalEfforts_inPersonHours"]
            retestingEfforts=lst["retestingEfforts"]
            totalEfforts_inPersonDays=lst["totalEfforts_inPersonDays"]
            updated_date=lst["updated_date"]
            taskGroup=lst["taskGroup"]
            con = getConnection()
            cur = con.cursor()
            cur.execute("SELECT * FROM estimator WHERE estimatorID = %s", [estimatorID])
            row = cur.fetchone()
            
            if row is None:
                return jsonify("ID not found"), 404
            
            sql = """UPDATE estimator SET
            projectName = %s,
            estimatorName = %s,
            dashBoardName = %s,
            totalEfforts_inPersonHours = %s,
            retestingEfforts = %s,
            totalEfforts_inPersonDays = %s,
            updated_date=%s
            WHERE estimatorID = %s"""

            cur.execute(sql,(projectName, estimatorName, dashboardName, totalEfforts_inPersonHours, retestingEfforts, totalEfforts_inPersonDays, updated_date,estimatorID))

            for lst in taskGroup:   
                cur.execute('UPDATE  taskGroup SET taskGroupName=%s,updated_date=%s WHERE taskGroup_id=%s',
                        (lst['taskGroupName'],lst['updated_date'],lst['taskGroup_id']))
                taskGroup_id=lst['taskGroup_id']
                for tsklist in lst["tasks"]:
                    cur.execute('UPDATE tasks SET taskName=%s, totalNum=%s, totalPerUnit=%s, totalEffort=%s,updated_date=%s WHERE task_id=%s',
                                ( tsklist['taskName'], tsklist['totalNum'], tsklist['totalPerUnit'], tsklist['totalEffort'],tsklist['updated_date'],tsklist['task_id']))    #row_id = cursor.lastrowid

            con.commit()
            con.close()
        values = request.get_json()
        return jsonify(values,"Data Successfully Updated")
    
    except Exception as e:
        return jsonify(e,"An ERROR occurred in table PUT Method")

@app.route('/delete_EST-Group',methods=['DELETE'])
def delete_user1():
    try:
        con = getConnection()
        cur = con.cursor()
        data = request.get_json()
        estimatorID = data.get("estimatorID")
        cur.execute("SELECT * FROM estimator WHERE estimatorID = %s", [estimatorID])
        row = cur.fetchone()
        if row is None:
            return jsonify("Record not found"), 404        
        cur.execute("DELETE FROM estimator WHERE estimatorID = %s", (estimatorID,))
        con.commit()
        con.close()
        return jsonify({"message": f"EstimatorID-{estimatorID} and associated task groups and tasks deleted successfully."})
    
    except Exception as e:
        return jsonify(e,"An ERROR occurred in table DELETE Method")

if __name__ == '__main__':
    app.run()