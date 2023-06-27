from flask import Flask,request,jsonify,send_file,make_response
import json
from data import DataBase
from logging.config import dictConfig
import xlsxwriter
import os
from io import BytesIO

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



@app.route('/Bi_Estimator_add',methods=['POST'])
def bi_add_Estimator():
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
            for tsklist in lst["bi_tasklist"]:
               effort_result_days = tsklist['simple']*tsklist['simpleWF'] + tsklist['medium']*tsklist['mediumWF'] + tsklist['complex']*tsklist['complexWF']
               effort_result_hrs = effort_result_days*8
               cur.execute('INSERT INTO  bi_tasklist( tasklist_id, simple, medium, complex,simpleWF,mediumWF,complexWF,effort_days,effort_hours,is_active, BI_taskGroup_id) VALUES (%s,%s,%s, %s,%s,%s,%s,%s,%s, %s,%s)',
                           ( tsklist['tasklist_id'], tsklist['simple'], tsklist['medium'], tsklist['complex'],tsklist['simpleWF'],tsklist['mediumWF'],tsklist['complexWF'],effort_result_days,effort_result_hrs,tsklist['is_active'],BI_taskGroup_id)) 
               app.logger.info('bi_tasklist Insert request received Successfully')
        con.commit()
        con.close()
        values = request.get_json()
        app.logger.info('Values Inserted Successfully')
        return jsonify(values,"Data Successfully Uploded")
    
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(e,"An ERROR occurred in table POST Method")
    
@app.route('/Bi_Est_Getall',methods=['GET'])
def bi_Get_allEst_tables():
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
                                              'BiTaskId', t.bi_tasklist_id, 
                                              'BiTtaskGroupId',t.BI_taskGroup_id,
                                              'taskName', t2.task_name, 
                                              'simple', t.simple, 
                                              'medium', t.medium, 
                                              'complex', t.complex,
                                              'simpleWF', t.simpleWF, 
                                              'mediumWF', t.mediumWF, 
                                              'complexWF', t.complexWF,
                                              'effortDays', t.effort_days, 
                                              'effortHours', t.effort_hours, 
                                              'createdDate',t.created_date,
                                              'updatedDate',t.updated_date,
                                              'isActive',t.is_active
                      )
                 ) FROM bi_tasklist t inner join tbltasklist t2 on t2.tasklist_id = t.tasklist_id  where t.BI_taskGroup_id =tg.BI_taskGroup_id and t.is_active=1 and t2.is_active=1)
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

@app.route('/Bi_EstGetByID/<int:BI_estimator_ID>', methods=['GET'])
def bi_Get_ByID_Estimator(BI_estimator_ID):
  try:
      app.logger.info('BI_Get_By_ID Process Starting')
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
                                              'BiTaskId', t.bi_tasklist_id, 
                                              'BiTtaskGroupId',t.BI_taskGroup_id,
                                              'taskName', t2.task_name, 
                                              'simple', t.simple, 
                                              'medium', t.medium, 
                                              'complex', t.complex,
                                              'simpleWF', t.simpleWF, 
                                              'mediumWF', t.mediumWF, 
                                              'complexWF', t.complexWF,
                                              'effortDays', t.effort_days, 
                                              'effortHours', t.effort_hours, 
                                              'createdDate',t.created_date,
                                              'updatedDate',t.updated_date,
                                              'isActive',t.is_active
                                          )
                                      ) FROM bi_tasklist t inner join tbltasklist t2 on t2.tasklist_id = t.tasklist_id WHERE t.BI_taskGroup_id = tg.BI_taskGroup_id and t.is_active=1 and t2.is_active=1)
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
      app.logger.info('Bi_Get_By_ID request received Successfully')
      return jsonify(f"Showing BI_estimator_ID : {BI_estimator_ID}", result_json)
  
  except Exception as e:
   app.logger.error('An error occurred: %s', str(e))
   return jsonify(e,"An ERROR occurred in table BI_GET_BY_ID Method")
  
@app.route('/GetAllCategories', methods=['GET'])
def getAllCategories():
    try:
        app.logger.info('getAllCategories Process Starting')
        con = DataBase.getConnection()
        cur = con.cursor()
        cur.execute("""SELECT JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'categoryId', c.category_id,
                                    'categoryName', c.category_name
                                )
                            )
                            FROM category as c;""")
        rows = cur.fetchall()
        con.close()
        result_json_str = rows[0][0]
        result_json = json.loads(result_json_str)
        app.logger.info('getAllCategories request received successfully')
        return jsonify(result_json)
    
    except Exception as e:
        app.logger.error(f'Error: {str(e)}')
        return jsonify(e,"An Error Occured in Getting getAllCategories")
    
@app.route('/GetAllTaskGroupName', methods=['GET'])
def GetAllTaskGroupName():
  try:
    app.logger.info('GetAllTaskGroupName Process Starting')
    con = DataBase.getConnection()
    cur = con.cursor()
    cur.execute("""SELECT JSON_ARRAYAGG(
                            JSON_OBJECT('categoryID',C.category_id,
                                        'taskGroupID',tbl.taskgroup_id,
                                        'taskGroupName',tbl.taskgroup_name 
                                        )
                                    )
                                    FROM tbltaskgroup AS tbl INNER JOIN category AS C On tbl.category_id = C.category_id""")
    rows = cur.fetchall()
    con.close()
    result_json_str = rows[0][0]
    result_json = json.loads(result_json_str)
    app.logger.info('GetAllTaskGroupName request received successfully')
    return jsonify(result_json)

  except Exception as e:
    app.logger.error('An error occurred: %s', str(e))
    return jsonify(e,"An Error Occured in Getting GetAllTaskGroupName")
  
@app.route('/GetAllTaskListName', methods=['GET'])
def GetAllTaskListName():
  try:
    app.logger.info('GetAllTaskListName Process Starting')
    con = DataBase.getConnection()
    cur = con.cursor()
    cur.execute("""SELECT JSON_ARRAYAGG(
                        JSON_OBJECT('categoryID', C.category_id,
                                    'taskGroupID', tbl.taskgroup_id,
                                    'taskGroupName', tbl.taskgroup_name,
                                    'TaskLists', (
                                        SELECT JSON_ARRAYAGG(
                                            JSON_OBJECT(
                                            'taskID',tlt.tasklist_id,
                                            'taskListName', tlt.task_name)
                                        )
                                        FROM tbltasklist AS tlt
                                        WHERE tlt.taskgroup_id = tbl.taskgroup_id
                                    )
                        )
                    )
                    FROM tbltaskgroup AS tbl
                    INNER JOIN category AS C ON tbl.category_id = C.category_id;""")
    rows = cur.fetchall()
    con.close()
    result_json_str = rows[0][0]
    result_json = json.loads(result_json_str)
    app.logger.info('GetAllTaskListName request received successfully')
    return jsonify(result_json)

  except Exception as e:
    app.logger.error('An error occurred: %s', str(e))
    return jsonify(e,"An Error Occured in Getting GetAllTaskListName")
  
@app.route('/Bi_GetFilterValues/<int:category_id>', methods=['GET'])
def bi_getFilterValues(category_id):
    try:
        app.logger.info('bi_getFilterValues Process Starting')
        con = DataBase.getConnection()
        cur = con.cursor()
        cur.execute(""" SELECT JSON_ARRAYAGG(
                           JSON_OBJECT(
                               'categoryID', c.category_id,
                               'categoryName', c.category_name,
                               'estimator', (
                                   SELECT JSON_ARRAYAGG(
                                       JSON_OBJECT(
                                           'estimatorID', e.bi_estimator_id,
                                            'estimatorName', e.estimatorName,
                                           'taskgroup', (
                                               SELECT JSON_ARRAYAGG(
                                                   JSON_OBJECT(
                                                       'taskgroupID', tg.bi_taskgroup_id,
                                                       'taskgroupName', t.taskgroup_name
                                                   )
                                               )
                                               FROM bi_taskgroup tg
                                               inner join tbltaskgroup t
                                               on tg.taskgroup_id = t.taskgroup_id
                                               WHERE tg.bi_estimator_id = e.bi_estimator_id
                                               and t.is_active = 1
                                               and tg.is_active = 1
                                           )
                                       )
                                   )
                                FROM bi_estimator e
                                WHERE c.category_id = e.category_id
                                and e.is_active = 1
                               )
                           )
                       ) AS json_data
                FROM category c
                where c.category_id = %s and c.is_active = 1""",(category_id,))
       
        rows = cur.fetchall()
        if len(rows) == 0:
            app.logger.info('Record Not Found for this Specific category_id')
            return jsonify("please enter a valid category_id")
        con.close()
        result_json_str = rows[0][0]
        result_json = json.loads(result_json_str)
        app.logger.info('bi_getFilterValues request received Successfully')
        return jsonify(f"Showing category_id : {category_id}", result_json)

    except Exception as e:
        app.logger.error(f'Error: {str(e)}')
        return jsonify(e,"An Error Occured in Getting bi_getFilterValues")
    
@app.route('/Get_Bi_Wf_Values/<int:category_id>', methods=['GET'])
def Get_Bi_Wf_Values(category_id):
  try:
    app.logger.info('Get_Bi_Wf_Values Process Starting')
    con = DataBase.getConnection()
    cur = con.cursor()
    cur.execute("""SELECT JSON_ARRAYAGG(
                            JSON_OBJECT('categoryID',C.category_id,
                                        'categoryName',C.category_name,
                                        'Workfactor',(
                                        SELECT JSON_ARRAYAGG(
												JSON_OBJECT('workfactor_id',twf.workfactor_id,
															'simpleWF',twf.simple_WF,
															'mediumWF',twf.medium_WF,
															'complexWF',twf.complex_WF
                                        )
                                    )FROM tblworkfactor AS twf Where twf.category_id = C.category_id
                                    )
							)
                            )as jsondata FROM category C where C.category_id = %s """,(category_id,))
    rows = cur.fetchall()
    if len(rows) == 0:
        app.logger.info('Record Not Found for this Specific category_id')
        return jsonify("please enter a valid category_id")
    con.close()
    result_json_str = rows[0][0]
    result_json = json.loads(result_json_str)
    app.logger.info('Get_Bi_Wf_Values request received Successfully')
    return jsonify(f"Showing category_id : {category_id}", result_json)

  except Exception as e:
    app.logger.error('An error occurred: %s', str(e))
    return jsonify(e,"An Error Occured in Getting Get_Bi_Wf_Values")

    
@app.route('/Bi_EstimatorUpdate',methods=['PUT'])
def bi_update_Estimator(): 
    try: 
        app.logger.info('Bi_EstimatorUpdate Method Starting')
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
                app.logger.info('Record Not Found to Update For This Specific BI_estimator_ID')
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
                cur.execute('UPDATE  bi_taskgroup SET BI_estimator_ID=%s,taskgroup_id=%s,updated_date=%s,is_active=%s WHERE BI_taskGroup_id=%s',
                        (lst['BI_estimator_ID'],lst['taskgroup_id'],lst['updated_date'],lst['is_active'],lst['BI_taskGroup_id']))
                app.logger.info('bi_taskgroup Update1 Request Received Successfully')
                for tsklist in lst["bi_tasklist"]: 
                    upt_effortRslt_days = tsklist['simple']*tsklist['simpleWF'] + tsklist['medium']*tsklist['mediumWF'] + tsklist['complex']*tsklist['complexWF']
                    upt_effortRslt_hrs = upt_effortRslt_days*8
                    cur.execute('UPDATE bi_tasklist SET tasklist_id=%s, simple=%s, medium=%s, complex=%s, simpleWF=%s, mediumWF=%s, complexWF=%s, effort_days=%s, effort_hours=%s,updated_date=%s,is_active=%s,BI_taskGroup_id=%s WHERE bi_tasklist_id=%s',
                                ( tsklist['tasklist_id'], tsklist['simple'], tsklist['medium'], tsklist['complex'], tsklist['simpleWF'], tsklist['mediumWF'], tsklist['complexWF'],upt_effortRslt_days, upt_effortRslt_hrs,tsklist['updated_date'],tsklist['is_active'],tsklist['BI_taskGroup_id'],tsklist['bi_tasklist_id']))   
                    app.logger.info('bi_tasklist Update1 Request Received Successfully')
            con.commit()
            con.close()
        values = request.get_json()
        app.logger.info('Bi_EstimatorUpdate Process Successfully Executed')
        return jsonify(values,"Data Successfully Updated")
    
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(e,"An ERROR occurred in table Bi_EstimatorUpdate Method")

@app.route('/Bi_Estimator_Updt_Delete', methods=['PUT'])
def bi_updateInsert_Estimator():
    try:
        app.logger.info('Bi_Estimator_Updt_Delete Method Starting')
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
                    cur.execute('UPDATE bi_taskgroup SET BI_estimator_ID=%s,taskgroup_id=%s,updated_date=%s,is_active=%s WHERE BI_taskGroup_id=%s',
                                (lst['BI_estimator_ID'],lst['taskgroup_id'],lst['updated_date'],lst['is_active'], BI_taskGroup_id))
                    app.logger.info("bi_taskgroup  Data Updated Successfully")
                else:
                    cur.execute('INSERT INTO bi_taskgroup(is_active,taskgroup_id, BI_estimator_ID) VALUES (%s, %s,%s)',
                                (lst['is_active'],lst["taskgroup_id"], BI_estimator_ID))
                    BI_taskGroup_id = cur.lastrowid
                    app.logger.info('bi_taskgroup Data Newly Inserted Successfully By PUT Method')
                for tsklist in lst["bi_tasklist"]:
                    bi_tasklist_id = tsklist.get("bi_tasklist_id")
                    if bi_tasklist_id is not None and bi_tasklist_id != "":
                        upt_effortRslt_days = tsklist['simple']*tsklist['simpleWF'] + tsklist['medium']*tsklist['mediumWF'] + tsklist['complex']*tsklist['complexWF']
                        upt_effortRslt_hrs = upt_effortRslt_days*8
                        cur.execute('UPDATE bi_tasklist SET tasklist_id=%s, simple=%s, medium=%s, complex=%s, simpleWF=%s, mediumWF=%s, complexWF=%s, effort_days=%s, effort_hours=%s,updated_date=%s,is_active=%s WHERE bi_tasklist_id=%s',
                                    (tsklist['tasklist_id'], tsklist['simple'], tsklist['medium'], tsklist['complex'], tsklist['simpleWF'], tsklist['mediumWF'], tsklist['complexWF'], upt_effortRslt_days, upt_effortRslt_hrs, tsklist['updated_date'],tsklist['is_active'], bi_tasklist_id))
                        app.logger.info("bi_tasklist Data Updated Successfully")
                    else:
                        effort_result_days = tsklist['simple']*tsklist['simpleWF'] + tsklist['medium']*tsklist['mediumWF'] + tsklist['complex']*tsklist['complexWF']
                        effort_result_hrs = effort_result_days*8
                        cur.execute('INSERT INTO bi_tasklist(tasklist_id,simple, medium, complex, simpleWF, mediumWF, complexWF, effort_days, effort_hours, is_active,BI_taskGroup_id) '
                                    'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                                    (tsklist['tasklist_id'], tsklist['simple'], tsklist['medium'], tsklist['complex'], tsklist['simpleWF'], tsklist['mediumWF'], tsklist['complexWF'],
                                      effort_result_days, effort_result_hrs,tsklist['is_active'], BI_taskGroup_id))
                        app.logger.info('bi_tasklist Data Newly Inserted Successfully By PUT Method')
            con.commit()
            con.close()
            values = request.get_json()
            app.logger.info('Bi_Estimator_Updt_Delete Process Successfully Executed')
            return jsonify(values,"Data Successfully Updated")
        
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(e,"An ERROR occurred in table Bi_Estimator_Updt_Delete Method")

@app.route('/Bi_Estimator_delete',methods=['DELETE'])
def bi_delete_Esti_ByID():
    try:
        app.logger.info('Bi_Estimator_delete Method Starting')
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
        app.logger.info('Bi_Estimator_delete  Process Successfully Executed')
        return jsonify({"message": f"BI_estimator_ID-{BI_estimator_ID} and associated task groups and tasks deleted successfully."})
    
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(e,"An ERROR occurred in table Bi_Estimator_delete Method")

@app.route('/Bi_download_excel_api/<int:category_id>/<estimator_ids>')
def bi_downloadExcelApi(category_id, estimator_ids):
    try:
        app.logger.info("Starting Bi_download_excel_api function")
        query = bi_generateQuery(category_id, estimator_ids)
        file_path = bi_writeExcelFile(query)
        app.logger.info("Excel File Returning process successfully executed")
        filename = 'data.xlsx'
        response = make_response(send_file(file_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'))
        response.headers['Content-Disposition'] = f"attachment; filename={filename}"
        return response

    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(error=str(e), message="An ERROR occurred in Bi_download_excel_api function")

def bi_generateQuery(category_id, estimator_ids):
    query = """
        SELECT  c.category_name,es.projectName,es.estimatorName,es.totalEfforts_inPersonHours,es.retestingEfforts,es.totalEfforts_inPersonDays,tg.taskgroup_name,
         tl2.task_name,tl.simple, tl.medium, tl.complex, tl.simpleWF, tl.mediumWF,tl.complexWF, tl.effort_days, tl.effort_hours
        FROM category c
        INNER JOIN tbltaskgroup tg ON c.category_id = tg.category_id
        INNER JOIN bi_estimator es ON es.category_id = c.category_id
        INNER JOIN bi_taskgroup tg1 ON tg1.BI_estimator_ID = es.BI_estimator_ID
        AND tg1.taskgroup_id = tg.taskgroup_id
        INNER JOIN bi_tasklist tl ON tl.BI_taskGroup_id = tg1.BI_taskGroup_id
        INNER JOIN tbltasklist tl2 ON tl2.tasklist_id=tl.tasklist_id
        WHERE c.category_id = {}
        AND c.is_active = 1
        AND tg.is_active = 1
        AND tg1.is_active = 1
        AND tl.is_active = 1
        AND tl2.is_active = 1
        AND es.is_active = 1
        AND es.BI_estimator_ID IN ({})
    """.format(category_id,estimator_ids)
    return query

def bi_writeExcelFile(query):
    try:
        app.logger.info("Updating data to Excel file")
        con = DataBase.getConnection()
        cur = con.cursor()

        # Create a temporary file path
        temp_dir = os.path.join(app.instance_path, 'temp')
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = os.path.join(temp_dir, 'data.xlsx')
        workbook = xlsxwriter.Workbook(temp_file)
        worksheet = workbook.add_worksheet()
        header_format = workbook.add_format(
            {'bold': True,
             'bg_color': '#F0FFFF',
             'border': 2
            })
        header_format2 = workbook.add_format({'bold': True, 'bg_color': '#EE4B2B', 'border': 2})
        border_format = workbook.add_format({'border': 2})
        merge_format = workbook.add_format(
                                      {
                                          "bold": 1,
                                          "border": 1,
                                          "align": "center",
                                          "valign": "vcenter",
                                      }
                                  )
        # Merge cells for the image
        worksheet.merge_range("A1:J3", "", merge_format)
        worksheet.insert_image("A1",r"C:\Users\Admin\Desktop\API_TOOL_2\emergere-logo.png",{"x_scale": 0.2, "y_scale": 0.2, "x_offset": 320, "y_offset": 10})
        worksheet.set_column(0,1,25)
        cur.execute(query)
        rows = cur.fetchall()
        if not rows:
            app.logger.warning("No rows returned from the query")
            workbook.close()
            return jsonify(message="No data available for the given parameters")
        # Write main headers
        worksheet.write(3, 0, 'CategoryName', header_format)
        worksheet.write(4, 0, 'ProjectName', header_format)
        worksheet.write(5, 0, 'EstimatorName', header_format)
        worksheet.write(6, 0,  'TotalEffortsInPersonHours', header_format)
        worksheet.write(7, 0,  'RetestingEfforts', header_format)
        worksheet.write(8, 0,  'TotalEffortsInPersonDays', header_format)
        # Write header values
        category_name = rows[0][0]
        project_name = rows[0][1]
        estimator_name = rows[0][2]
        TotalEffortsInPersonHours=rows[0][3]
        RetestingEfforts=rows[0][4]
        TotalEffortsInPersonDays=rows[0][5]
        worksheet.merge_range("B4:J4", category_name, merge_format,)
        worksheet.merge_range("B5:J5", project_name, merge_format)
        worksheet.merge_range("B6:J6", estimator_name, merge_format)
        worksheet.write("B7:J7", TotalEffortsInPersonHours, border_format)
        worksheet.write("B8:J8", RetestingEfforts, border_format)
        worksheet.write("B9:J9", TotalEffortsInPersonDays, border_format)
        #worksheet.merge_range(0,7, c)
        headers=['taskgroup_name','taskName', 'simple', 'medium', 'complex', 'simpleWF', 'mediumWF', 'complexWF', 'effortDays', 'effortHours',]
        for col, header_text in enumerate(headers):
           worksheet.write(9, col, header_text, header_format2)
        row_num = 10  # Starting row number for data

        for row_data in rows:
            worksheet.write_row(row_num, 0, row_data[6:], border_format)  # Write remaining data rows
            row_num += 1

        workbook.close()
        app.logger.info("Returning the Excel data")
        return temp_file

    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(error=str(e), message="An ERROR occurred in the bi_writeExcelFile method")


if __name__ == '__main__':
    app.run(debug=True)