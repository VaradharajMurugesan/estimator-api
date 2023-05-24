from flask import Flask,request,jsonify
import json
from data import DataBase
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

con = DataBase.getConnection()
cur=con.cursor()



@app.route('/Estimator_add',methods=['POST'])
def add_Estimator():
    try:
        app.logger.info('BI Insert Process Starting')
        category_id=request.json["category_id"]
        projectName=request.json["projectName"]
        estimatorName=request.json["estimatorName"]
        BIName=request.json["BIName"]
        totalEfforts_inPersonHours=request.json["totalEfforts_inPersonHours"]
        retestingEfforts=request.json["retestingEfforts"]
        totalEfforts_inPersonDays=request.json["totalEfforts_inPersonDays"]
        is_active=request.json["is_active"]
        bi_taskgroup=request.json["bi_taskgroup"]     
        app.logger.info('Data request received')
        con = DataBase.getConnection()
        cur = con.cursor()
        sql="""INSERT INTO bi_estimator(category_id,projectName,estimatorName,BIName,totalEfforts_inPersonHours,retestingEfforts,totalEfforts_inPersonDays,is_active)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
        cur.execute(sql,(category_id,projectName,estimatorName,BIName,totalEfforts_inPersonHours,retestingEfforts,totalEfforts_inPersonDays,is_active))
        BI_estimator_ID=cur.lastrowid
        app.logger.info('bi_estimator Insert request received Successfully')
        for lst in bi_taskgroup:   
            cur.execute('INSERT INTO bi_taskgroup(is_active,taskgroup_id, BI_estimator_ID) VALUES (%s,%s,%s)',
                       (lst["is_active"],lst["taskgroup_id"],BI_estimator_ID))
            BI_taskGroup_id=cur.lastrowid
            app.logger.info('bi_taskgroup Insert request received Successfully')
            for tsklist in lst["bi_tasks"]:
               cur.execute('INSERT INTO  bi_tasks( taskName, totalNum, totalPerUnit, totalEffort,is_active, BI_taskGroup_id) VALUES (%s,%s,%s, %s,%s,%s)',
                           ( tsklist['taskName'], tsklist['totalNum'], tsklist['totalPerUnit'], tsklist['totalEffort'],tsklist['is_active'],BI_taskGroup_id)) 
               app.logger.info('bi_tasks Insert request received Successfully')
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
      app.logger.info('BI_Get All Process Starting')
      con = DataBase.getConnection()
      cur = con.cursor()
      cur.execute  (""" SELECT JSON_ARRAYAGG(  
                            JSON_OBJECT(
                            'categoryId',e.category_id,
                            'categoryName',c.category_name,
                            'BiEstimatorID', e.BI_estimator_ID,
                            'projectName', e.projectName,
                            'estimatorName', e.estimatorName,
                            'BIName', e.BIName,
                            'totalEffortsInPersonHours', e.totalEfforts_inPersonHours,
                            'retestingEfforts', e.retestingEfforts,
                            'totalEffortsInPersonDays', e.totalEfforts_inPersonDays,
                            'createdDate',e.created_date,
                            'updatedDate',e.updated_date,
                            'isActive',e.is_active,
                            'biTaskgroup', 
                        (SELECT JSON_ARRAYAGG(
                             JSON_OBJECT(
                                      'BiTaskgroupId', tg.BI_taskGroup_id,
                                      'taskgroupId',tg.taskgroup_id,
                                      'taskgroupName', tg1.taskgroup_name,
                                      'BiEstimator_ID',tg.BI_estimator_ID,
                                      'createdDate',tg.created_date,
                                      'updatedDate',tg.updated_date,
                                      'isActive',tg.is_active,
                                      'biTasks', 
                                    (SELECT JSON_ARRAYAGG(
                                         JSON_OBJECT(
                                              'BiTaskId', t.BI_task_id, 
                                              'BiTtaskGroupId',t.BI_taskGroup_id,
                                              'taskName', t.taskName, 
                                              'totalNum', t.totalNum, 
                                              'totalPerUnit', t.totalPerUnit, 
                                              'totalEffort', t.totalEffort,
                                              'createdDate',t.created_date,
                                              'updatedDate',t.updated_date,
                                              'isActive',t.is_active
                      )
                 ) FROM bi_tasks t where t.BI_taskGroup_id =tg.BI_taskGroup_id and is_active=1)
            )
       ) FROM bi_taskgroup tg inner join tbltaskgroup tg1 on tg1.taskgroup_id = tg.taskgroup_id WHERE tg.BI_estimator_ID = e.BI_estimator_ID and tg.is_active=1 and tg1.is_active= 1)
      )
      )FROM bi_estimator e  inner join category c on c.category_id = e.category_id WHERE e.is_active=1 and c.is_active=1""")
      rows = cur.fetchall()
      result_json_str=rows[0][0]
      result_json = json.loads(result_json_str)
      app.logger.info('BI_Get All request received Successfully')
      return jsonify(result_json)
  
  except Exception as e:
   app.logger.error('An error occurred: %s', str(e))
   return jsonify(e,"An ERROR occurred in table BI_GET Method")

@app.route('/Est_getbyid-Estimator/<int:BI_estimator_ID>', methods=['GET'])
def Get_ByID_Estimator(BI_estimator_ID):
  try:
      app.logger.info('BI_Get By_ID Process Starting')
      con = DataBase.getConnection()
      cur = con.cursor()
      rows = cur.execute("""SELECT JSON_OBJECT(
                              'categoryId',e.category_id,
                              'categoryName',c.category_name,
                              'BiEstimatorID', e.BI_estimator_ID,
                              'projectName', e.projectName,
                              'estimatorName', e.estimatorName,
                              'BIName', e.BIName,
                              'totalEffortsInPersonHours', e.totalEfforts_inPersonHours,
                              'retestingEfforts', e.retestingEfforts,
                              'totalEffortsInPersonDays', e.totalEfforts_inPersonDays,
                              'createdDate',e.created_date,
                              'updatedDate',e.updated_date,
                              'isActive',e.is_active,
                              'biTaskgroup', 
                              (SELECT JSON_ARRAYAGG(
                                  JSON_OBJECT(
                                      'BiTaskGroupId', tg.BI_taskGroup_id, 
                                      'taskgroupId', tg.taskgroup_id,
                                      'taskgroupName', tg1.taskgroup_name,
                                      'BiEstimatorID',tg.BI_estimator_ID,
                                      'createdDate',tg.created_date,
                                      'updatedDate',tg.updated_date,
                                      'isActive',tg.is_active,
                                      'biTasks', 
                                      (SELECT JSON_ARRAYAGG(
                                          JSON_OBJECT(
                                              'BiTaskId', t.BI_task_id,
                                              'BiTtaskGroupId',t.BI_taskGroup_id,
                                              'taskName', t.taskName, 
                                              'totalNum', t.totalNum, 
                                              'totalPerUnit', t.totalPerUnit, 
                                              'totalEffort', t.totalEffort,
                                              'createdDate',t.created_date,
                                              'updatedDate',t.updated_date,
                                              'isActive',t.is_active
                                          )
                                      ) FROM bi_tasks t WHERE t.BI_taskGroup_id = tg.BI_taskGroup_id and t.is_active=1)
                                  )
                                ) FROM bi_taskgroup tg inner join tbltaskgroup tg1 on tg1.taskgroup_id = tg.taskgroup_id WHERE tg.BI_estimator_ID = e.BI_estimator_ID and tg.is_active=1 and tg1.is_active= 1)

                            ) FROM bi_estimator e  inner join category c on c.category_id = e.category_id WHERE e.BI_estimator_ID = %s and e.is_active=1 and c.is_active=1""", (BI_estimator_ID,))
                            
      rows = cur.fetchall()
      if len(rows) == 0:
          app.logger.info('Record Not Found for this Specific BI_Id')
          return jsonify("please enter a valid BI_estimator_ID")
      con.close()
      result_json_str = rows[0][0]
      result_json = json.loads(result_json_str)
      app.logger.info('Get By_ID request received Successfully')
      return jsonify(f"Showing BI_estimator_ID : {BI_estimator_ID}", result_json)
  
  except Exception as e:
   app.logger.error('An error occurred: %s', str(e))
   return jsonify(e,"An ERROR occurred in table BI_GET_BY_ID Method")

@app.route('/Estimator_Update1',methods=['PUT'])
def update_Estimator(): 
    try: 
        app.logger.info('Update1 Method Starting')
        request1= request.get_json()    
        for lst in request1:
            BI_estimator_ID=lst["BI_estimator_ID"]
            projectName=lst["projectName"]
            estimatorName=lst["estimatorName"]
            BIName=lst["BIName"]
            totalEfforts_inPersonHours=lst["totalEfforts_inPersonHours"]
            retestingEfforts=lst["retestingEfforts"]
            totalEfforts_inPersonDays=lst["totalEfforts_inPersonDays"]
            updated_date=lst["updated_date"]
            is_active=lst["is_active"]
            bi_taskgroup=lst["bi_taskgroup"]
            app.logger.info('Data Update Request Received Successfully')
            con = DataBase.getConnection()
            cur = con.cursor()
            cur.execute("SELECT * FROM bi_estimator WHERE BI_estimator_ID = %s", [BI_estimator_ID])
            row = cur.fetchone()
            if row is None:
                app.logger.info('Record Not Found to Update For This Specific ID')
                return jsonify("Record not found"), 404
            sql = """UPDATE bi_estimator SET
            projectName = %s,
            estimatorName = %s,
            BIName = %s,
            totalEfforts_inPersonHours = %s,
            retestingEfforts = %s,
            totalEfforts_inPersonDays = %s,
            updated_date=%s,
            is_active=%s
            WHERE BI_estimator_ID = %s"""

            cur.execute(sql,(projectName, estimatorName, BIName, totalEfforts_inPersonHours, retestingEfforts, totalEfforts_inPersonDays, updated_date ,is_active, BI_estimator_ID))
            app.logger.info('bi_estimator Update1 Request Received Successfully')

            for lst in bi_taskgroup:   
                cur.execute('UPDATE  bi_taskgroup SET taskgroup_id=%s,updated_date=%s,is_active=%s WHERE BI_taskGroup_id=%s',
                        (lst['taskgroup_id'],lst['updated_date'],lst['is_active'],lst['BI_taskGroup_id']))
                app.logger.info('bi_taskgroup Update1 Request Received Successfully')
                for tsklist in lst["bi_tasks"]:                   
                    cur.execute('UPDATE bi_tasks SET taskName=%s, totalNum=%s, totalPerUnit=%s, totalEffort=%s,updated_date=%s,is_active=%s,BI_taskGroup_id=%s WHERE BI_task_id=%s',
                                ( tsklist['taskName'], tsklist['totalNum'], tsklist['totalPerUnit'], tsklist['totalEffort'],tsklist['updated_date'],tsklist['is_active'],tsklist['BI_taskGroup_id'],tsklist['BI_task_id']))   
                    app.logger.info('bi_tasks Update1 Request Received Successfully')
            con.commit()
            con.close()
        values = request.get_json()
        app.logger.info('Estimator_Update1 Process Successfully Executed')
        return jsonify(values,"Data Successfully Updated")
    
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(e,"An ERROR occurred in table BI_PUT Method")

@app.route('/EstimatorUpdate', methods=['PUT'])
def updateInsert_Estimator():
    try:
        app.logger.info('EstimatorUpdate Method Starting')
        request1 = request.get_json()
        for lst in request1:
            BI_estimator_ID = lst.get("BI_estimator_ID")
            category_id=lst["category_id"]
            projectName = lst["projectName"]
            estimatorName = lst["estimatorName"]
            BIName = lst["BIName"]
            totalEfforts_inPersonHours = lst["totalEfforts_inPersonHours"]
            retestingEfforts = lst["retestingEfforts"]
            totalEfforts_inPersonDays = lst["totalEfforts_inPersonDays"]
            updated_date = lst["updated_date"]
            bi_taskgroup = lst["bi_taskgroup"]
            is_active=lst["is_active"]
            app.logger.info('Data update request received')
            con = DataBase.getConnection()
            cur = con.cursor()            
            if BI_estimator_ID is not None and BI_estimator_ID != "":
                sql = """UPDATE bi_estimator SET projectName=%s, estimatorName=%s, BIName=%s, 
                         totalEfforts_inPersonHours=%s, retestingEfforts=%s, totalEfforts_inPersonDays=%s,
                         updated_date=%s,is_active=%s WHERE  BI_estimator_ID=%s """
                cur.execute(sql, (projectName, estimatorName, BIName, totalEfforts_inPersonHours,
                                  retestingEfforts, totalEfforts_inPersonDays, updated_date,is_active, BI_estimator_ID))
                app.logger.info("bi_estimator Data Updated Successfully")
            else:
                sql = """INSERT INTO bi_estimator(category_id,projectName, estimatorName, BIName, 
                         totalEfforts_inPersonHours, retestingEfforts, totalEfforts_inPersonDays,is_active)
                         VALUES (%s, %s, %s, %s, %s, %s,%s,%s)"""
                cur.execute(sql, (category_id,projectName, estimatorName, BIName, totalEfforts_inPersonHours,
                                  retestingEfforts, totalEfforts_inPersonDays,is_active))
                BI_estimator_ID = cur.lastrowid
                app.logger.info('bi_estimator Data Newly Inserted Successfully By PUT Method')
            for lst in bi_taskgroup:
                BI_taskGroup_id = lst.get("BI_taskGroup_id")
                if BI_taskGroup_id is not None and BI_taskGroup_id != "":
                    cur.execute('UPDATE bi_taskgroup SET taskgroup_id=%s,updated_date=%s,is_active=%s WHERE BI_taskGroup_id=%s',
                                (lst['taskgroup_id'],lst['updated_date'],lst['is_active'], BI_taskGroup_id))
                    app.logger.info("bi_taskgroup  Data Updated Successfully")
                else:
                    cur.execute('INSERT INTO bi_taskgroup(is_active,taskgroup_id, BI_estimator_ID) VALUES (%s, %s,%s)',
                                (lst['is_active'],lst["taskgroup_id"], BI_estimator_ID))
                    BI_taskGroup_id = cur.lastrowid
                    app.logger.info('bi_taskgroup Data Newly Inserted Successfully By PUT Method')
                for tsklist in lst["bi_tasks"]:
                    BI_task_id = tsklist.get("BI_task_id")
                    if BI_task_id is not None and BI_task_id != "":
                        cur.execute('UPDATE bi_tasks SET taskName=%s, totalNum=%s, totalPerUnit=%s, totalEffort=%s, '
                                    'updated_date=%s,is_active=%s WHERE BI_task_id=%s',
                                    (tsklist['taskName'], tsklist['totalNum'], tsklist['totalPerUnit'],
                                     tsklist['totalEffort'], tsklist['updated_date'],tsklist['is_active'], BI_task_id))
                        app.logger.info("bi_tasks Data Updated Successfully")
                    else:
                        cur.execute('INSERT INTO bi_tasks(taskName, totalNum, totalPerUnit, totalEffort, is_active,BI_taskGroup_id) '
                                    'VALUES (%s, %s, %s, %s,%s,%s)',
                                    (tsklist['taskName'], tsklist['totalNum'], tsklist['totalPerUnit'],
                                     tsklist['totalEffort'], tsklist['is_active'], BI_taskGroup_id))
                        app.logger.info('bi_tasks Data Newly Inserted Successfully By PUT Method')
            con.commit()
            con.close()
            values = request.get_json()
            app.logger.info('EstimatorUpdate Process Successfully Executed')
            return jsonify(values,"Data Successfully Updated")
        
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(e,"An ERROR occurred in table BI_PUT Method")

@app.route('/Estimator_delete',methods=['DELETE'])
def delete_Esti_ByID():
    try:
        app.logger.info('BI_Delete Method Starting')
        con = DataBase.getConnection()
        cur = con.cursor()
        data = request.get_json()
        BI_estimator_ID = data.get("BI_estimator_ID")
        cur.execute("SELECT * FROM bi_estimator WHERE BI_estimator_ID = %s", [BI_estimator_ID])
        row = cur.fetchone()
        if row is None:
            app.logger.info('Record Not Found to Delete for this Specific ID')
            return jsonify("Record not found"), 404        
        cur.execute("DELETE FROM bi_estimator WHERE BI_estimator_ID = %s", (BI_estimator_ID,))
        con.commit()
        con.close()
        app.logger.info('Estimator_Id Delete Process Successfully Executed')
        return jsonify({"message": f"BI_estimator_ID-{BI_estimator_ID} and associated task groups and tasks deleted successfully."})
    
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(e,"An ERROR occurred in table BI_DELETE Method")


if __name__ == '__main__':
   app.run()
