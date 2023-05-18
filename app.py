import os
import mysql.connector
import json
from data import DataBase
from flask import (Flask, redirect, render_template, request,
                   send_from_directory, url_for, jsonify)
from logging.config import dictConfig

dictConfig(
  {
    "version": 1,
    'formatters': {'default': {
    'format': '[%(asctime)s] %(levelname)s in %(module)s: %(funcName)s : %(lineno)d : %(message)s',
    }},
    "handlers": {
      "time-rotate": {
        "class": "logging.handlers.TimedRotatingFileHandler",
        "filename": r"Log/estimator_log.log",
        "when": "D",
        "interval": 1,
        "backupCount": 20,
        "formatter": "default",
      },
    },
    "root": {
      "level": "DEBUG",
      "handlers": ["time-rotate"],
    },
  }
)

app = Flask(__name__)


@app.route('/Estimator_add',methods=['POST'])
def add_Estimator():
    try:
        app.logger.info('Insert Process Starting')
        projectName=request.json["projectName"]
        estimatorName=request.json["estimatorName"]
        dashBoardName=request.json["dashBoardName"]
        totalEfforts_inPersonHours=request.json["totalEfforts_inPersonHours"]
        retestingEfforts=request.json["retestingEfforts"]
        totalEfforts_inPersonDays=request.json["totalEfforts_inPersonDays"]
        is_active=request.json["is_active"]
        taskGroup=request.json["taskGroup"]     
        app.logger.info('Data request received')
        con = DataBase.getConnection()
        cur = con.cursor()
        sql="""INSERT INTO estimator(projectName,estimatorName,dashBoardName,totalEfforts_inPersonHours,retestingEfforts,totalEfforts_inPersonDays,is_active)
            VALUES (%s,%s,%s,%s,%s,%s,%s)"""
        cur.execute(sql,(projectName,estimatorName,dashBoardName,totalEfforts_inPersonHours,retestingEfforts,totalEfforts_inPersonDays,is_active))
        estimatorID=cur.lastrowid
        app.logger.info('Estimator Insert request received Successfully')
        for lst in taskGroup:   
            cur.execute('INSERT INTO taskGroup(taskGroupName,is_active, estimatorID) VALUES (%s,%s,%s)',
                       (lst['taskGroupName'],lst["is_active"],estimatorID))
            taskGroup_id=cur.lastrowid
            app.logger.info('TaskGroup Insert request received Successfully')
            for tsklist in lst["tasks"]:
               cur.execute('INSERT INTO  tasks( taskName, totalNum, totalPerUnit, totalEffort,is_active, taskGroup_id) VALUES (%s,%s, %s,%s, %s,%s)',
                           ( tsklist['taskName'], tsklist['totalNum'], tsklist['totalPerUnit'], tsklist['totalEffort'],tsklist['is_active'],taskGroup_id)) 
               app.logger.info('Tasks Insert request received Successfully')
        con.commit()
        con.close()
        values = request.get_json()
        app.logger.info('Values Inserted Successfully')
        return jsonify(values,"Data Successfully Uploded")
    
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(e,"An ERROR occurred in table POST Method")
    
@app.route('/Est_Getall',methods=['GET'])
def Get_allEst_tables():
  try:
      app.logger.info('Get All Process Starting')
      con = DataBase.getConnection()
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
                            'is_active',e.is_active,
                            'taskGroup', 
                        (SELECT JSON_ARRAYAGG(
                             JSON_OBJECT(
                                      'taskGroup_id', tg.taskGroup_id, 
                                      'taskGroupName', tg.taskGroupName,
                                      'estimatorID',tg.estimatorID,
                                      'created_date',tg.created_date,
                                      'updated_date',tg.updated_date,
                                      'is_active',tg.is_active,
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
                                              'updated_date',t.updated_date,
                                              'is_active',t.is_active
                      )
                 ) FROM tasks t where t.taskGroup_id =tg.taskGroup_id and is_active=1)
            )
       ) FROM taskGroup tg  WHERE tg.estimatorID = e.estimatorID and is_active=1)
      )
      )FROM estimator e WHERE is_active=1""")
      rows = cur.fetchall()
      result_json_str=rows[0][0]
      result_json = json.loads(result_json_str)
      app.logger.info('Get All request received Successfully')
      return jsonify(result_json)
  
  except Exception as e:
   app.logger.error('An error occurred: %s', str(e))
   return jsonify(e,"An ERROR occurred in table GET Method")

@app.route('/Est_getbyid-Estimator/<int:estimatorID>', methods=['GET'])
def Get_ByID_Estimator(estimatorID):
  try:
      app.logger.info('Get By_ID Process Starting')
      con = DataBase.getConnection()
      cur = con.cursor()
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
                              'is_active',e.is_active,
                              'taskGroup', 
                              (SELECT JSON_ARRAYAGG(
                                  JSON_OBJECT(
                                      'taskGroup_id', tg.taskGroup_id, 
                                      'taskGroupName', tg.taskGroupName,
                                      'estimatorID',tg.estimatorID,
                                      'created_date',tg.created_date,
                                      'updated_date',tg.updated_date,
                                      'is_active',tg.is_active,
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
                                              'updated_date',t.updated_date,
                                              'is_active',t.is_active
                                          )
                                      ) FROM tasks t WHERE t.taskGroup_id = tg.taskGroup_id and is_active=1)
                                  )
                                ) FROM taskGroup tg WHERE tg.estimatorID = e.estimatorID and is_active=1)
                            ) FROM estimator e WHERE e.estimatorID = %s and is_active=1 """, (estimatorID,))
                            
      rows = cur.fetchall()
      if len(rows) == 0:
          app.logger.info('Record Not Found for this Specific Id')
          return jsonify("please enter a valid estimatorID")
      con.close()
      result_json_str = rows[0][0]
      result_json = json.loads(result_json_str)
      app.logger.info('Get By_ID request received Successfully')
      return jsonify(f"Showing estimatorID : {estimatorID}", result_json)
  
  except Exception as e:
   app.logger.error('An error occurred: %s', str(e))
   return jsonify(e,"An ERROR occurred in table GET_BY_ID Method")

@app.route('/Estimator_Update1',methods=['PUT'])
def update_Estimator(): 
    try: 
        app.logger.info('Update1 Method Starting')
        request1= request.get_json()    
        for lst in request1:
            estimatorID=lst["estimatorID"]
            projectName=lst["projectName"]
            estimatorName=lst["estimatorName"]
            dashBoardName=lst["dashBoardName"]
            totalEfforts_inPersonHours=lst["totalEfforts_inPersonHours"]
            retestingEfforts=lst["retestingEfforts"]
            totalEfforts_inPersonDays=lst["totalEfforts_inPersonDays"]
            updated_date=lst["updated_date"]
            taskGroup=lst["taskGroup"]
            is_active=lst["is_active"]
            app.logger.info('Data Update Request Received Successfully')
            con = DataBase.getConnection()
            cur = con.cursor()
            cur.execute("SELECT * FROM estimator WHERE estimatorID = %s", [estimatorID])
            row = cur.fetchone()
            if row is None:
                app.logger.info('Record Not Found to Update For This Specific ID')
                return jsonify("Record not found"), 404
            sql = """UPDATE estimator SET
            projectName = %s,
            estimatorName = %s,
            dashBoardName = %s,
            totalEfforts_inPersonHours = %s,
            retestingEfforts = %s,
            totalEfforts_inPersonDays = %s,
            updated_date=%s,
            is_active=%s
            WHERE estimatorID = %s"""

            cur.execute(sql,(projectName, estimatorName, dashBoardName, totalEfforts_inPersonHours, retestingEfforts, totalEfforts_inPersonDays, updated_date ,is_active, estimatorID))
            app.logger.info('Estimator Update1 Request Received Successfully')

            for lst in taskGroup:   
                cur.execute('UPDATE  taskGroup SET taskGroupName=%s,updated_date=%s,is_active=%s WHERE taskGroup_id=%s',
                        (lst['taskGroupName'],lst['updated_date'],lst['is_active'],lst['taskGroup_id']))
                taskGroup_id=lst['taskGroup_id']
                app.logger.info('TaskGroup Update1 Request Received Successfully')
                for tsklist in lst["tasks"]:
                    cur.execute('UPDATE tasks SET taskName=%s, totalNum=%s, totalPerUnit=%s, totalEffort=%s,updated_date=%s,is_active=%s WHERE task_id=%s',
                                ( tsklist['taskName'], tsklist['totalNum'], tsklist['totalPerUnit'], tsklist['totalEffort'],tsklist['updated_date'],tsklist['is_active'],tsklist['task_id']))    #row_id = cursor.lastrowid
                    app.logger.info('Tasks Update1 Request Received Successfully')
            con.commit()
            con.close()
        values = request.get_json()
        app.logger.info('Update1 Process Successfully Executed')
        return jsonify(values,"Data Successfully Updated")
    
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(e,"An ERROR occurred in table PUT Method")

@app.route('/EstimatorUpdate', methods=['PUT'])
def updateInsert_Estimator():
    try:
        app.logger.info('Update Method Starting')
        request1 = request.get_json()
        for lst in request1:
            estimatorID = lst.get("estimatorID")
            projectName = lst["projectName"]
            estimatorName = lst["estimatorName"]
            dashBoardName = lst["dashBoardName"]
            totalEfforts_inPersonHours = lst["totalEfforts_inPersonHours"]
            retestingEfforts = lst["retestingEfforts"]
            totalEfforts_inPersonDays = lst["totalEfforts_inPersonDays"]
            updated_date = lst["updated_date"]
            taskGroup = lst["taskGroup"]
            is_active=lst["is_active"]
            app.logger.info('Data update request received')
            con = DataBase.getConnection()
            cur = con.cursor()            
            if estimatorID is not None and estimatorID != "":
                sql = """UPDATE estimator SET projectName=%s, estimatorName=%s, dashBoardName=%s, 
                         totalEfforts_inPersonHours=%s, retestingEfforts=%s, totalEfforts_inPersonDays=%s,
                         updated_date=%s,is_active=%s WHERE estimatorID=%s"""
                cur.execute(sql, (projectName, estimatorName, dashBoardName, totalEfforts_inPersonHours,
                                  retestingEfforts, totalEfforts_inPersonDays, updated_date,is_active, estimatorID))
                app.logger.info("Estimator Data Updated Successfully")
            else:
                sql = """INSERT INTO estimator(projectName, estimatorName, dashBoardName, 
                         totalEfforts_inPersonHours, retestingEfforts, totalEfforts_inPersonDays,is_active)
                         VALUES (%s, %s, %s, %s, %s, %s,%s)"""
                cur.execute(sql, (projectName, estimatorName, dashBoardName, totalEfforts_inPersonHours,
                                  retestingEfforts, totalEfforts_inPersonDays,is_active))
                estimatorID = cur.lastrowid
                app.logger.info('Estimator Data Newly Inserted Successfully By PUT Method')
            for lst in taskGroup:
                taskGroup_id = lst.get("taskGroup_id")
                if taskGroup_id is not None and taskGroup_id != "":
                    cur.execute('UPDATE taskGroup SET taskGroupName=%s, updated_date=%s,is_active=%s WHERE taskGroup_id=%s',
                                (lst['taskGroupName'], lst['updated_date'],lst['is_active'], taskGroup_id))
                    app.logger.info("TaskGroup  Data Updated Successfully")
                else:
                    cur.execute('INSERT INTO taskGroup(taskGroupName,is_active, estimatorID) VALUES (%s, %s,%s)',
                                (lst['taskGroupName'],lst['is_active'], estimatorID))
                    taskGroup_id = cur.lastrowid
                    app.logger.info('TaskGroup Data Newly Inserted Successfully By PUT Method')
                for tsklist in lst["tasks"]:
                    task_id = tsklist.get("task_id")
                    if task_id is not None and task_id != "":
                        cur.execute('UPDATE tasks SET taskName=%s, totalNum=%s, totalPerUnit=%s, totalEffort=%s, '
                                    'updated_date=%s,is_active=%s WHERE task_id=%s',
                                    (tsklist['taskName'], tsklist['totalNum'], tsklist['totalPerUnit'],
                                     tsklist['totalEffort'], tsklist['updated_date'],tsklist['is_active'], task_id))
                        app.logger.info("Task Data Updated Successfully")
                   
                    else:
                        cur.execute('INSERT INTO tasks(taskName, totalNum, totalPerUnit, totalEffort, is_active, taskGroup_id) '
                                    'VALUES (%s, %s, %s, %s, %s,%s)',
                                    (tsklist['taskName'], tsklist['totalNum'], tsklist['totalPerUnit'],
                                     tsklist['totalEffort'], tsklist['is_active'], taskGroup_id))
                        app.logger.info('TaskGroup Data Newly Inserted Successfully By PUT Method')
            con.commit()
            con.close()
            values = request.get_json()
            app.logger.info('EstimatorUpdate Process Successfully Executed')
            return jsonify(values,"Data Successfully Updated")
        
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(e,"An ERROR occurred in table PUT Method")

@app.route('/Estimator_delete',methods=['DELETE'])
def delete_Esti_ByID():
    try:
        app.logger.info('Delete Method Starting')
        con = DataBase.getConnection()
        cur = con.cursor()
        data = request.get_json()
        estimatorID = data.get("estimatorID")
        cur.execute("SELECT * FROM estimator WHERE estimatorID = %s", [estimatorID])
        row = cur.fetchone()
        if row is None:
            app.logger.info('Record Not Found to Delete for this Specific ID')
            return jsonify("Record not found"), 404        
        cur.execute("DELETE FROM estimator WHERE estimatorID = %s", (estimatorID,))
        con.commit()
        con.close()
        app.logger.info('Estimator_Id Delete Process Successfully Executed')
        return jsonify({"message": f"EstimatorID-{estimatorID} and associated task groups and tasks deleted successfully."})
    
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(e,"An ERROR occurred in table DELETE Method")


if __name__ == '__main__':
   app.run()
