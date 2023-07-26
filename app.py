from flask import Flask,request,jsonify,send_file,make_response
import json
from data import DataBase
from datetime import datetime
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
        category_id=request.json["categoryId"]
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
                            'biEstimatorId', e.BI_estimator_ID,
                            'projectName', e.projectName,
                            'estimatorName', e.estimatorName,
                            'biName', e.BIName,
                            'totalEffortsInPersonHours', e.totalEfforts_inPersonHours,
                            'retestingEfforts', e.retestingEfforts,
                            'totalEffortsInPersonDays', e.totalEfforts_inPersonDays,
                            'createdDate',e.created_date,
                            'updatedDate',e.updated_date,
                            'isActive',e.is_active,
                            'biTaskGroup', 
                        (SELECT JSON_ARRAYAGG(
                             JSON_OBJECT(
                                      'biTaskGroupId', tg.BI_taskGroup_id,
                                      'taskGroupId',tg.taskgroup_id,
                                      'taskGroupName', tg1.taskgroup_name,
                                      'biEstimatorId',tg.BI_estimator_ID,
                                      'createdDate',tg.created_date,
                                      'updatedDate',tg.updated_date,
                                      'isActive',tg.is_active,
                                      'biTasks', 
                                    (SELECT JSON_ARRAYAGG(
                                         JSON_OBJECT(
                                              'biTaskListId', t.bi_tasklist_id, 
                                              'taskListId',t.tasklist_id, 
                                              'biTaskGroupId',t.BI_taskGroup_id,
                                              'taskName', t2.task_name, 
                                              'simple', t.simple, 
                                              'medium', t.medium, 
                                              'complex', t.complex,
                                              'simpleWf', t.simpleWF, 
                                              'mediumWf', t.mediumWF, 
                                              'complexWf', t.complexWF,
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
                              'biEstimatorId', e.BI_estimator_ID,
                              'projectName', e.projectName,
                              'estimatorName', e.estimatorName,
                              'biName', e.BIName,
                              'totalEffortsInPersonHours', e.totalEfforts_inPersonHours,
                              'retestingEfforts', e.retestingEfforts,
                              'totalEffortsInPersonDays', e.totalEfforts_inPersonDays,
                              'createdDate',e.created_date,
                              'updatedDate',e.updated_date,
                              'isActive',e.is_active,
                              'biTaskGroup', 
                              (SELECT JSON_ARRAYAGG(
                                  JSON_OBJECT(
                                      'biTaskGroupId', tg.BI_taskGroup_id, 
                                      'taskGroupId', tg.taskgroup_id,
                                      'taskGroupName', tg1.taskgroup_name,
                                      'biEstimatorId',tg.BI_estimator_ID,
                                      'createdDate',tg.created_date,
                                      'updatedDate',tg.updated_date,
                                      'isActive',tg.is_active,
                                      'biTasks', 
                                      (SELECT JSON_ARRAYAGG(
                                          JSON_OBJECT(
                                              'biTaskListId', t.bi_tasklist_id, 
                                              'taskListId',t.tasklist_id,
                                              'biTaskGroupId',t.BI_taskGroup_id,
                                              'taskName', t2.task_name, 
                                              'simple', t.simple, 
                                              'medium', t.medium, 
                                              'complex', t.complex,
                                              'simpleWf', t.simpleWF, 
                                              'mediumWf', t.mediumWF, 
                                              'complexWf', t.complexWF,
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
  
@app.route('/GetAllTaskListName/<int:category_id>', methods=['GET'])
def getAllTaskListName(category_id):
  try:
    app.logger.info('GetAllTaskListName Process Starting')
    con = DataBase.getConnection()
    cur = con.cursor()
    cur.execute("""SELECT JSON_ARRAYAGG(
                        JSON_OBJECT(
                            'categoryID', C.category_id,
                            'categoryName', C.category_name,
                            'TaskGroup', (
                                SELECT JSON_ARRAYAGG(
                                    JSON_OBJECT(
                                        'taskGroupID', tbl.taskgroup_id,
                                        'taskGroupName', tbl.taskgroup_name,
                                        'TaskLists', (
                                            SELECT JSON_ARRAYAGG(
                                                JSON_OBJECT(
                                                    'taskID', tlt.tasklist_id,
                                                    'taskListName', tlt.task_name
                                                )
                                            )
                                            FROM tbltasklist AS tlt
                                            WHERE tlt.taskgroup_id = tbl.taskgroup_id
                                        )
                                    )
                                )
                                FROM tbltaskgroup AS tbl
                                WHERE tbl.category_id = C.category_id and tbl.is_active = 1
                            )
                        )
                    ) AS json_data
                    FROM category AS C
                    WHERE C.category_id = %s;""",(category_id,))
    rows = cur.fetchall()
    if len(rows) == 0:
        app.logger.info('Record Not Found for this Specific category_id')
        return jsonify("please enter a valid category_id")
    con.close()
    result_json_str = rows[0][0]
    result_json = json.loads(result_json_str)
    app.logger.info('GetAllTaskListName request received Successfully')
    return jsonify(f"Showing category_id : {category_id}", result_json)

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
            BI_estimator_ID=lst["biEstimatorId"]
            projectName=lst["projectName"]
            estimatorName=lst["estimatorName"]
            BIName=lst["bIName"]
            totalEfforts_inPersonHours=lst["totalEfforts_inPersonHours"]
            retestingEfforts=lst["retestingEfforts"]
            totalEfforts_inPersonDays=lst["totalEfforts_inPersonDays"]
            updated_date=datetime.now()
            is_active=lst["is_active"]
            bi_taskgroup=lst["biTaskgroup"]
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
                        (lst['BI_estimator_ID'],lst['taskgroup_id'],updated_date,lst['is_active'],lst['BI_taskGroup_id']))
                app.logger.info('bi_taskgroup Update1 Request Received Successfully')
                for tsklist in lst["bi_tasklist"]: 
                    upt_effortRslt_days = tsklist['simple']*tsklist['simpleWF'] + tsklist['medium']*tsklist['mediumWF'] + tsklist['complex']*tsklist['complexWF']
                    upt_effortRslt_hrs = upt_effortRslt_days*8
                    cur.execute('UPDATE bi_tasklist SET tasklist_id=%s, simple=%s, medium=%s, complex=%s, simpleWF=%s, mediumWF=%s, complexWF=%s, effort_days=%s, effort_hours=%s,updated_date=%s,is_active=%s,BI_taskGroup_id=%s WHERE bi_tasklist_id=%s',
                                ( tsklist['tasklist_id'], tsklist['simple'], tsklist['medium'], tsklist['complex'], tsklist['simpleWF'], tsklist['mediumWF'], tsklist['complexWF'],upt_effortRslt_days, upt_effortRslt_hrs,updated_date,tsklist['is_active'],tsklist['BI_taskGroup_id'],tsklist['bi_tasklist_id']))   
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
            biEstimatorId = lst.get("biEstimatorId")
            categoryId=lst["categoryId"]
            projectName = lst["projectName"]
            estimatorName = lst["estimatorName"]
            biName = lst["biName"]
            totalEffortsInPersonHours = lst["totalEffortsInPersonHours"]
            retestingEfforts = lst["retestingEfforts"]
            totalEffortsInPersonDays = lst["totalEffortsInPersonDays"]
            updatedDate = datetime.now()
            biTaskGroup = lst["biTaskGroup"]
            isActive=lst["isActive"]
            app.logger.info('Data update request received')
            con = DataBase.getConnection()
            cur = con.cursor()            
            if biEstimatorId is not None and biEstimatorId != "":
                sql = """UPDATE bi_estimator SET projectName=%s, estimatorName=%s, BIName=%s, 
                         totalEfforts_inPersonHours=%s, retestingEfforts=%s, totalEfforts_inPersonDays=%s,
                         updated_date=%s,is_active=%s WHERE  BI_estimator_ID=%s """
                cur.execute(sql, (projectName, estimatorName, biName, totalEffortsInPersonHours,
                                  retestingEfforts, totalEffortsInPersonDays, updatedDate,isActive, biEstimatorId))
                app.logger.info("bi_estimator Data Updated Successfully")
            else:
                sql = """INSERT INTO bi_estimator(category_id,projectName, estimatorName, BIName, 
                         totalEfforts_inPersonHours, retestingEfforts, totalEfforts_inPersonDays,is_active)
                         VALUES (%s, %s, %s, %s, %s, %s,%s,%s)"""
                cur.execute(sql, (categoryId,projectName, estimatorName, biName, totalEffortsInPersonHours,
                                  retestingEfforts, totalEffortsInPersonDays,isActive))
                biEstimatorId = cur.lastrowid
                app.logger.info('bi_estimator Data Newly Inserted Successfully By PUT Method')
            for lst in biTaskGroup:
                biTaskGroupId = lst.get("biTaskGroupId")
                if biTaskGroupId is not None and biTaskGroupId != "":
                    cur.execute('UPDATE bi_taskgroup SET taskgroup_id=%s,updated_date=%s,is_active=%s WHERE BI_taskGroup_id=%s',
                                (lst['taskGroupId'],updatedDate,lst['isActive'], biTaskGroupId))
                    app.logger.info("bi_taskgroup  Data Updated Successfully")
                else:
                    cur.execute('INSERT INTO bi_taskgroup(is_active,taskgroup_id, BI_estimator_ID) VALUES (%s, %s,%s)',
                                (lst['isActive'],lst["taskGroupId"], biEstimatorId))
                    biTaskGroupId = cur.lastrowid
                    app.logger.info('bi_taskgroup Data Newly Inserted Successfully By PUT Method')
                for tsklist in lst["biTasks"]:
                    biTaskListId = tsklist.get("biTaskListId")
                    if biTaskListId is not None and biTaskListId != "":
                        upt_effortRslt_days = tsklist['simple']*tsklist['simpleWf'] + tsklist['medium']*tsklist['mediumWf'] + tsklist['complex']*tsklist['complexWf']
                        upt_effortRslt_hrs = upt_effortRslt_days*8
                        cur.execute('UPDATE bi_tasklist SET tasklist_id=%s, simple=%s, medium=%s, complex=%s, simpleWF=%s, mediumWF=%s, complexWF=%s, effort_days=%s, effort_hours=%s,updated_date=%s,is_active=%s WHERE bi_tasklist_id=%s',
                                    (tsklist['taskListId'], tsklist['simple'], tsklist['medium'], tsklist['complex'], tsklist['simpleWf'], tsklist['mediumWf'], tsklist['complexWf'], upt_effortRslt_days, upt_effortRslt_hrs, updatedDate,tsklist['isActive'], biTaskListId))
                        app.logger.info("bi_tasklist Data Updated Successfully")
                    else:
                        effort_result_days = tsklist['simple']*tsklist['simpleWf'] + tsklist['medium']*tsklist['mediumWf'] + tsklist['complex']*tsklist['complexWf']
                        effort_result_hrs = effort_result_days*8
                        cur.execute('INSERT INTO bi_tasklist(tasklist_id,simple, medium, complex, simpleWF, mediumWF, complexWF, effort_days, effort_hours, is_active,BI_taskGroup_id) '
                                    'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                                    (tsklist['taskListId'], tsklist['simple'], tsklist['medium'], tsklist['complex'], tsklist['simpleWf'], tsklist['mediumWf'], tsklist['complexWf'],
                                      effort_result_days, effort_result_hrs,tsklist['isActive'], biTaskGroupId))
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
        filename = 'Bi_data.xlsx'
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
        temp_file = os.path.join(temp_dir, 'Bi_data.xlsx')
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
        worksheet.insert_image("A1",r"Image/emergere-logo.png",{"x_scale": 0.2, "y_scale": 0.2, "x_offset": 320, "y_offset": 10})
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
        app.logger.info("Returning the Excel BI_data")
        return temp_file

    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(error=str(e), message="An ERROR occurred in the bi_writeExcelFile method")

#---------------------------------ETL Estimator----------------------------------------------

@app.route('/Etl_Estimator_add',methods=['POST'])
def etl_add_Estimator():
    
    try:
        app.logger.info('Etl_Estimator Insert Process Starting')
        category_id=request.json["categoryId"]
        projectName=request.json["projectName"]
        estimatorName=request.json["estimatorName"]
        etlName=request.json["etlName"]
        totalEfforts_inPersonHours=request.json["totalEfforts_inPersonHours"]
        retestingEfforts=request.json["retestingEfforts"]
        totalEfforts_inPersonDays=request.json["totalEfforts_inPersonDays"]
        is_active=request.json["is_active"]
        etl_taskgroup=request.json["etl_taskgroup"]     
        app.logger.info('Data request received')
        con = DataBase.getConnection()
        cur = con.cursor()
        sql="""INSERT INTO etl_estimator(category_id,projectName,estimatorName,etlName,totalEfforts_inPersonHours,retestingEfforts,totalEfforts_inPersonDays,is_active)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
        cur.execute(sql,(category_id,projectName,estimatorName,etlName,totalEfforts_inPersonHours,retestingEfforts,totalEfforts_inPersonDays,is_active))
        etl_estimator_ID=cur.lastrowid
        app.logger.info('etl_estimator Insert request received Successfully')
        
        for lst in etl_taskgroup:   
            cur.execute('INSERT INTO etl_taskgroup(is_active,taskgroup_id, etl_estimator_ID) VALUES (%s,%s,%s)',
                       (lst["is_active"],lst["taskgroup_id"],etl_estimator_ID))
            etl_taskGroup_id=cur.lastrowid
            app.logger.info('etl_taskgroup Insert request received Successfully')
           
            for tsklist in lst["etl_tasklist"]:
                effort_result_days = tsklist['simple']*tsklist['simpleWF'] + tsklist['medium']*tsklist['mediumWF'] + tsklist['complex']*tsklist['complexWF']
                effort_result_hrs = effort_result_days*8
                cur.execute('INSERT INTO  etl_tasklist( tasklist_id, simple, medium, complex,simpleWF,mediumWF,complexWF,effort_days,effort_hours,is_active, etl_taskGroup_id) VALUES (%s,%s,%s, %s,%s,%s,%s,%s,%s, %s,%s)',
                           (tsklist['tasklist_id'], tsklist['simple'], tsklist['medium'], tsklist['complex'],tsklist['simpleWF'],tsklist['mediumWF'],tsklist['complexWF'],effort_result_days,effort_result_hrs,tsklist['is_active'],etl_taskGroup_id)) 
                app.logger.info('etl_tasklist Insert request received Successfully')
        
        con.commit()
        con.close()
        values = request.get_json()
        app.logger.info('Values Inserted Successfully in ETL Estimator')
        return jsonify(values,"Data Successfully Uploded in ETL")
    
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(e,"An ERROR occurred in table POST Method in etl"), 500


@app.route('/Etl_Est_Getall',methods=['GET'])
def etl_Get_allEst_tables():
  try:
      app.logger.info('Estimator ETL Get All Process Starting')
      con = DataBase.getConnection()
      cur = con.cursor()
      cur.execute  (""" SELECT JSON_ARRAYAGG(  
                            JSON_OBJECT(
                            'categoryId',e.category_id,
                            'categoryName',c.category_name,
                            'etlEstimatorId', e.etl_estimator_ID,
                            'projectName', e.projectName,
                            'estimatorName', e.estimatorName,
                            'etlName', e.etlName,
                            'totalEffortsInPersonHours', e.totalEfforts_inPersonHours,
                            'retestingEfforts', e.retestingEfforts,
                            'totalEffortsInPersonDays', e.totalEfforts_inPersonDays,
                            'createdDate',e.created_date,
                            'updatedDate',e.updated_date,
                            'isActive',e.is_active,
                            'etlTaskGroups', 
                        (SELECT JSON_ARRAYAGG(
                             JSON_OBJECT(
                                      'etlTaskGroupId', tg.etl_taskGroup_id,
                                      'taskGroupId',tg.taskgroup_id,
                                      'taskGroupName', tg1.taskgroup_name,
                                      'etlEstimatorId',tg.etl_estimator_ID,
                                      'createdDate',tg.created_date,
                                      'updatedDate',tg.updated_date,
                                      'isActive',tg.is_active,
                                      'etlTaskLists', 
                                    (SELECT JSON_ARRAYAGG(
                                         JSON_OBJECT(
                                              'etlTaskListId', t.etl_tasklist_id, 
                                              'taskListId',t.tasklist_id,
                                              'etlTaskGroupId',t.etl_taskGroup_id,
                                              'taskName', t2.task_name, 
                                              'simple', t.simple, 
                                              'medium', t.medium, 
                                              'complex', t.complex,
                                              'simpleWf', t.simpleWF, 
                                              'mediumWf', t.mediumWF, 
                                              'complexWf', t.complexWF,
                                              'effortDays', t.effort_days, 
                                              'effortHours', t.effort_hours, 
                                              'createdDate',t.created_date,
                                              'updatedDate',t.updated_date,
                                              'isActive',t.is_active
                      )
                 ) FROM etl_tasklist t inner join tbltasklist t2 on t2.tasklist_id = t.tasklist_id  where t.etl_taskGroup_id =tg.etl_taskGroup_id and t.is_active=1 and t2.is_active=1)
            )
       ) FROM etl_taskgroup tg inner join tbltaskgroup tg1 on tg1.taskgroup_id = tg.taskgroup_id WHERE tg.etl_estimator_ID = e.etl_estimator_ID and tg.is_active=1 and tg1.is_active= 1)
      )
      )FROM etl_estimator e  inner join category c on c.category_id = e.category_id WHERE e.is_active=1 and c.is_active=1""")
      
      rows = cur.fetchall()
      result_json_str=rows[0][0]
      result_json = json.loads(result_json_str)
      app.logger.info('ETL Get All Datas request received Successfully')
      return jsonify(result_json)
  
  except Exception as e:
   app.logger.error('An error occurred: %s', str(e))
   return jsonify(e,"An ERROR occurred in table ETL_GET Method")


@app.route('/Etl_EstGetByID/<int:etl_estimator_ID>', methods=['GET'])
def bi_Get_ByID_ETL(etl_estimator_ID):
  try:
      app.logger.info('ETL GET by ID Process Starting')
      con = DataBase.getConnection()
      cur = con.cursor()
      rows = cur.execute("""SELECT JSON_OBJECT(
                              'categoryId',e.category_id,
                              'categoryName',c.category_name,
                              'etlEstimatorId', e.etl_estimator_ID,
                              'projectName', e.projectName,
                              'estimatorName', e.estimatorName,
                              'etlName', e.etlName,
                              'totalEffortsInPersonHours', e.totalEfforts_inPersonHours,
                              'retestingEfforts', e.retestingEfforts,
                              'totalEffortsInPersonDays', e.totalEfforts_inPersonDays,
                              'createdDate',e.created_date,
                              'updatedDate',e.updated_date,
                              'isActive',e.is_active,
                              'etlTaskGroups', 
                              (SELECT JSON_ARRAYAGG(
                                  JSON_OBJECT(
                                      'etlTaskGroupId', tg.etl_taskGroup_id, 
                                      'taskGroupId', tg.taskgroup_id,
                                      'taskGroupName', tg1.taskgroup_name,
                                      'etlEstimatorId',tg.etl_estimator_ID,
                                      'createdDate',tg.created_date,
                                      'updatedDate',tg.updated_date,
                                      'isActive',tg.is_active,
                                      'etlTaskLists', 
                                      (SELECT JSON_ARRAYAGG(
                                          JSON_OBJECT(
                                              'etlTaskListId', t.etl_tasklist_id,
                                              'taskListId',t.tasklist_id, 
                                              'etlTaskGroupId',t.etl_taskGroup_id,
                                              'taskName', t2.task_name, 
                                              'simple', t.simple, 
                                              'medium', t.medium, 
                                              'complex', t.complex,
                                              'simpleWf', t.simpleWF, 
                                              'mediumWf', t.mediumWF, 
                                              'complexWf', t.complexWF,
                                              'effortDays', t.effort_days, 
                                              'effortHours', t.effort_hours, 
                                              'createdDate',t.created_date,
                                              'updatedDate',t.updated_date,
                                              'isActive',t.is_active
                                          )
                                      ) FROM etl_tasklist t inner join tbltasklist t2 on t2.tasklist_id = t.tasklist_id WHERE t.etl_taskGroup_id = tg.etl_taskGroup_id and t.is_active=1 and t2.is_active=1)
                                  )
                                ) FROM etl_taskgroup tg inner join tbltaskgroup tg1 on tg1.taskgroup_id = tg.taskgroup_id WHERE tg.etl_estimator_ID = e.etl_estimator_ID and tg.is_active=1 and tg1.is_active= 1)

                            ) FROM etl_estimator e  inner join category c on c.category_id = e.category_id WHERE e.etl_estimator_ID = %s and e.is_active=1 and c.is_active=1""", (etl_estimator_ID,))
                            
      rows = cur.fetchall()
      if len(rows) == 0:
          app.logger.info('Record Not Found for the Specific ETL Id')
          return jsonify("Enter a valid ETL Estimator ID")
      con.close()
      result_json_str = rows[0][0]
      result_json = json.loads(result_json_str)
      app.logger.info('Getting ID request received Successfully')
      return jsonify(f"Showing ETL ESTIMATOR ID : {etl_estimator_ID}", result_json)
  
  except Exception as e:
   app.logger.error('An error occurred: %s', str(e))
   return jsonify(e,"An ERROR occurred in table ETL_GET_BY_ID Method")

@app.route('/Etl_GetFilterValues/<int:category_id>', methods=['GET'])
def etl_getFilterValues(category_id):
    try:
        app.logger.info('Etl_GetFilterValues Process Starting')
        con = DataBase.getConnection()
        cur = con.cursor()
        cur.execute(""" SELECT JSON_ARRAYAGG(
                            JSON_OBJECT(
                                'categoryID', c.category_id,
                                'categoryName', c.category_name,
                                'estimator', (
                                    SELECT JSON_ARRAYAGG(
                                        JSON_OBJECT(
                                            'estimatorID', e.etl_estimator_id,
                                            'estimatorName', e.estimatorName,
                                            'taskgroup', (
                                                SELECT JSON_ARRAYAGG(
                                                    JSON_OBJECT(
                                                        'taskgroupID', tg.etl_taskgroup_id,
                                                        'taskgroupName', t.taskgroup_name
                                                    )
                                                )
                                                FROM etl_taskgroup tg
                                                INNER JOIN tbltaskgroup t ON tg.taskgroup_id = t.taskgroup_id
                                                WHERE tg.etl_estimator_id = e.etl_estimator_id
                                                AND t.is_active = 1
                                                AND tg.is_active = 1
                                            )
                                        )
                                    )
                                    FROM etl_estimator e
                                    WHERE c.category_id = e.category_id
                                    AND e.is_active = 1
                                )
                            )
                        ) AS json_data
                        FROM category c
                        WHERE c.category_id = %s
                        and c.is_active = 1""",(category_id,))
       
        rows = cur.fetchall()
        if len(rows) == 0:
            app.logger.info('Record Not Found for this Specific ETL category_id')
            return jsonify("please enter a valid category_id")
        con.close()
        result_json_str = rows[0][0]
        result_json = json.loads(result_json_str)
        app.logger.info('Etl_GetFilterValues request received Successfully')
        return jsonify(f"Showing category_id : {category_id}", result_json)
  

    except Exception as e:
        app.logger.error(f'Error: {str(e)}')
        return jsonify(e,"An Error Occured in Getting Etl_GetFilterValues")

@app.route('/Get_Etl_Wf_Values/<int:category_id>', methods=['GET'])
def get_Etl_Wf_Values(category_id):
  try:
    app.logger.info('Get_Etl_Wf_Values Process Starting')
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
                            )as jsondata FROM category C where C.category_id = %s""",(category_id,))
    rows = cur.fetchall()
    if len(rows) == 0:
        app.logger.info('Record Not Found for this Specific QA category_id')
        return jsonify("please enter a valid category_id")
    con.close()
    result_json_str = rows[0][0]
    result_json = json.loads(result_json_str)
    app.logger.info('Get_Etl_Wf_Values request received Successfully')
    return jsonify(f"Showing category_id : {category_id}", result_json)

  except Exception as e:
    app.logger.error('An error occurred: %s', str(e))
    return jsonify(e,"An Error Occured in Getting Get_Etl_Wf_Values")

@app.route('/Etl_EstimatorUpdate',methods=['PUT'])
def etl_update_Estimator(): 
    try: 
        app.logger.info('ETL Update Method Starting...')
        request1= request.get_json()    
        for lst in request1:
            etl_estimator_ID=lst["etlEstimatorId"]
            projectName=lst["projectName"]
            estimatorName=lst["estimatorName"]
            etlName=lst["etlName"]
            totalEfforts_inPersonHours=lst["totalEfforts_inPersonHours"]
            retestingEfforts=lst["retestingEfforts"]
            totalEfforts_inPersonDays=lst["totalEfforts_inPersonDays"]
            updated_date=datetime.now()
            is_active=lst["is_active"]
            etl_taskgroup=lst["etl_taskgroup"]
            app.logger.info('Data Update Request Received Successfully')
            con = DataBase.getConnection()
            cur = con.cursor()
            cur.execute("SELECT * FROM etl_estimator WHERE etl_estimator_ID = %s", [etl_estimator_ID])
            row = cur.fetchone()
            if row is None:
                app.logger.info('Record Not Found to Update for the Specific ETL ID')
                return jsonify("Record not found"), 404
            
            sql = """UPDATE etl_estimator SET
            projectName = %s,
            estimatorName = %s,
            etlName = %s,
            totalEfforts_inPersonHours = %s,
            retestingEfforts = %s,
            totalEfforts_inPersonDays = %s,
            updated_date=%s,
            is_active=%s
            WHERE etl_estimator_ID = %s"""

            cur.execute(sql,(projectName, estimatorName, etlName, totalEfforts_inPersonHours, retestingEfforts, totalEfforts_inPersonDays, updated_date ,is_active, etl_estimator_ID))
            app.logger.info('ETL Estimator Update Request Received Successfully')

            for lst in etl_taskgroup:
                   
                cur.execute('UPDATE  etl_taskgroup SET etl_estimator_ID=%s,taskgroup_id=%s,updated_date=%s,is_active=%s WHERE etl_taskGroup_id=%s',
                        (lst['etl_estimator_ID'],lst['taskgroup_id'],updated_date,lst['is_active'],lst['etl_taskGroup_id']))
                app.logger.info('ETL Taskgroup Update Request Received Successfully')
                
                for tsklist in lst["etl_tasklist"]: 
                    upt_effortRslt_days = tsklist['simple']*tsklist['simpleWF'] + tsklist['medium']*tsklist['mediumWF'] + tsklist['complex']*tsklist['complexWF']
                    upt_effortRslt_hrs = upt_effortRslt_days*8
                    cur.execute('UPDATE etl_tasklist SET tasklist_id=%s, simple=%s, medium=%s, complex=%s, simpleWF=%s, mediumWF=%s, complexWF=%s, effort_days=%s, effort_hours=%s,updated_date=%s,is_active=%s,etl_taskGroup_id=%s WHERE etl_tasklist_id=%s',
                                ( tsklist['tasklist_id'], tsklist['simple'], tsklist['medium'], tsklist['complex'], tsklist['simpleWF'], tsklist['mediumWF'], tsklist['complexWF'],upt_effortRslt_days, upt_effortRslt_hrs,updated_date,tsklist['is_active'],tsklist['etl_taskGroup_id'],tsklist['etl_tasklist_id']))   
                    app.logger.info('ETL_tasklist Update Request Received Successfully')
                    
            con.commit()
            con.close()
        values = request.get_json()
        app.logger.info('ETL Update Process Successfully Executed')
        return jsonify(values,"Data Successfully Updated")
    
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(e,"An ERROR occurred in table ETL PUT Method")
    

@app.route('/Etl_Estimator_Updt_Delete', methods=['PUT'])
def etl_updateInsert_Estimator():
    try:
        app.logger.info('ETL_Updt_Delete Method Starting')
        request1 = request.get_json()
        for lst in request1:
            etlEstimatorId = lst.get("etlEstimatorId")
            categoryId=lst["categoryId"]
            projectName = lst["projectName"]
            estimatorName = lst["estimatorName"]
            etlName = lst["etlName"]
            totalEffortsInPersonHours = lst["totalEffortsInPersonHours"]
            retestingEfforts = lst["retestingEfforts"]
            totalEffortsInPersonDays = lst["totalEffortsInPersonDays"]
            updatedDate = datetime.now()
            etlTaskGroups = lst["etlTaskGroups"]
            isActive=lst["isActive"]
            app.logger.info('Data update request received on ETL Table')
            con = DataBase.getConnection()
            cur = con.cursor()        
                
            if etlEstimatorId is not None and etlEstimatorId != "":
                sql = """UPDATE etl_estimator SET projectName=%s, estimatorName=%s, etlName=%s, 
                         totalEfforts_inPersonHours=%s, retestingEfforts=%s, totalEfforts_inPersonDays=%s,
                         updated_date=%s,is_active=%s WHERE  etl_estimator_ID=%s """
                cur.execute(sql, (projectName, estimatorName, etlName, totalEffortsInPersonHours,
                                  retestingEfforts, totalEffortsInPersonDays, updatedDate,isActive, etlEstimatorId))
                app.logger.info("ETL estimator_ID Data Updated Successfully")
           
            else:
                sql = """INSERT INTO etl_estimator(category_id,projectName, estimatorName, etlName, 
                         totalEfforts_inPersonHours, retestingEfforts, totalEfforts_inPersonDays,is_active)
                         VALUES (%s, %s, %s, %s, %s, %s,%s,%s)"""
                cur.execute(sql, (categoryId,projectName, estimatorName, etlName, totalEffortsInPersonHours,
                                  retestingEfforts, totalEffortsInPersonDays,isActive))
                etlEstimatorId = cur.lastrowid
                app.logger.info('ETL Estimator Data Newly Inserted Successfully By PUT Method')
                
            for lst in etlTaskGroups:
                etlTaskGroupId = lst.get("etlTaskGroupId")
                if etlTaskGroupId is not None and etlTaskGroupId != "":
                    cur.execute('UPDATE etl_taskgroup SET taskgroup_id=%s,updated_date=%s,is_active=%s WHERE etl_taskGroup_id=%s',
                                (lst['taskGroupId'],updatedDate,lst['isActive'], etlTaskGroupId))
                    app.logger.info("ETL_Taskgroup Data Updated Successfully")
                    
                else:
                    cur.execute('INSERT INTO etl_taskgroup (is_active,taskgroup_id, etl_estimator_ID) VALUES (%s, %s, %s)',
                                (lst['isActive'],lst["taskGroupId"], etlEstimatorId))
                    etlTaskGroupId = cur.lastrowid
                    app.logger.info('ETL TaskGroup Data Newly Inserted Successfully By PUT Method')
                    
                for tsklist in lst["etlTaskLists"]:
                    etlTaskListId = tsklist.get("etlTaskListId")
                    if etlTaskListId is not None and etlTaskListId != "":
                        upt_effortRslt_days = tsklist['simple']*tsklist['simpleWf'] + tsklist['medium']*tsklist['mediumWf'] + tsklist['complex']*tsklist['complexWf']
                        upt_effortRslt_hrs = upt_effortRslt_days*8
                        cur.execute('UPDATE etl_tasklist SET tasklist_id=%s, simple=%s, medium=%s, complex=%s, simpleWF=%s, mediumWF=%s, complexWF=%s, effort_days=%s, effort_hours=%s,updated_date=%s,is_active=%s WHERE etl_tasklist_id=%s',
                                    (tsklist['taskListId'], tsklist['simple'], tsklist['medium'], tsklist['complex'], tsklist['simpleWf'], tsklist['mediumWf'], tsklist['complexWf'], upt_effortRslt_days, upt_effortRslt_hrs, updatedDate,tsklist['isActive'], etlTaskListId))
                        app.logger.info("ETL_tasklist_id Data Updated Successfully")
                    
                    else:
                        effort_result_days = tsklist['simple']*tsklist['simpleWf'] + tsklist['medium']*tsklist['mediumWf'] + tsklist['complex']*tsklist['complexWf']
                        effort_result_hrs = effort_result_days*8
                        cur.execute('INSERT INTO etl_tasklist(tasklist_id,simple, medium, complex, simpleWF, mediumWF, complexWF, effort_days, effort_hours, is_active,etl_taskGroup_id) '
                                    'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                                    (tsklist['taskListId'], tsklist['simple'], tsklist['medium'], tsklist['complex'], tsklist['simpleWf'], tsklist['mediumWf'], tsklist['complexWf'],
                                      effort_result_days, effort_result_hrs,tsklist['isActive'], etlTaskGroupId))
                        app.logger.info('etl_tasklist Data Newly Inserted Successfully By PUT Method')
            con.commit()
            con.close()
            values = request.get_json()
            app.logger.info('ETL UPDATE AND INSERT Process Successfully Executed')
            return jsonify(values,"Data Successfully Updated")
        
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(e,"An ERROR occurred in table Etl_Estimator_Updt_Delete Method")
    
    
@app.route('/Etl_Estimator_delete',methods=['DELETE'])
def etl_delete_Esti_ByID():
    try:
        app.logger.info('ETL ID Delete Process Starting')
        con = DataBase.getConnection()
        cur = con.cursor()
        data = request.get_json()
        etl_estimator_ID = data.get("etlEstimatorId")
        cur.execute("SELECT * FROM etl_estimator WHERE etl_estimator_ID = %s", [etl_estimator_ID])
        row = cur.fetchone()
        
        if row is None:
            app.logger.info('Record Not Found to Delete for the Specific ETL ID')
            return jsonify("Record not found"), 404     
           
        cur.execute("DELETE FROM etl_estimator WHERE etl_estimator_ID = %s", (etl_estimator_ID,))
        con.commit()
        con.close()
        app.logger.info('Estimator ETL_Id Delete Process Successfully Executed')
        return jsonify({"message": f"etl_estimator_ID-{etl_estimator_ID} and associated task groups and tasks deleted successfully."})
    
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(e,"An ERROR occurred in table etl DELETE Method")

        
@app.route('/Etl_download_excel_api/<int:category_id>/<estimator_ids>')
def etl_downloadExcelApi(category_id, estimator_ids):
    try:
        app.logger.info("Starting Excel function For ETL Table")
        query = etl_generateQuery(category_id, estimator_ids)
        file_path = etl_writeExcelFile(query)
        app.logger.info("Excel File Returning process successfully executed")
        filename = 'ETLdata.xlsx'
        response = make_response(send_file(file_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'))
        response.headers['Content-Disposition'] = f"attachment; filename={filename}"
        return response

    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(error=str(e), message="An ERROR occurred in downloadExcel ETL function")

def etl_generateQuery(category_id, estimator_ids):
    query = """
        SELECT  c.category_name,es.projectName,es.estimatorName,es.totalEfforts_inPersonHours,es.retestingEfforts,es.totalEfforts_inPersonDays,tg.taskgroup_name,
        tl2.task_name,tl.simple, tl.medium, tl.complex, tl.simpleWF, tl.mediumWF,tl.complexWF, tl.effort_days, tl.effort_hours
        FROM category c
        INNER JOIN tbltaskgroup tg ON c.category_id = tg.category_id
        INNER JOIN etl_estimator es ON es.category_id = c.category_id
        INNER JOIN etl_taskgroup tg1 ON tg1.etl_estimator_ID = es.etl_estimator_ID
        AND tg1.taskgroup_id = tg.taskgroup_id
        INNER JOIN etl_tasklist tl ON tl.etl_taskGroup_id = tg1.etl_taskGroup_id
        INNER JOIN tbltasklist tl2 ON tl2.tasklist_id=tl.tasklist_id
        WHERE c.category_id = {}
        AND c.is_active = 1
        AND tg.is_active = 1
        AND tg1.is_active = 1
        AND tl.is_active = 1
        AND tl2.is_active = 1
        AND es.is_active = 1
        AND es.etl_estimator_ID IN ({})
    """.format(category_id,estimator_ids)
    return query

def etl_writeExcelFile(query):
    try:
        app.logger.info("Writing data to Excel file for ETL")
        con = DataBase.getConnection()
        cur = con.cursor()

        # Create a temporary file path
        temp_dir = os.path.join(app.instance_path, 'temp')
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = os.path.join(temp_dir, 'ETLdata.xlsx')
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
        worksheet.insert_image("A1",r"Image/emergere-logo.png",{"x_scale": 0.2, "y_scale": 0.2, "x_offset": 320, "y_offset": 10})
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
        return jsonify(error=str(e), message="An ERROR occurred in the writeExcelFile method ON ETL"), 404

#---------------------------------------------------QA Estimator---------------------------------------------------

@app.route('/Qa_Estimator_add',methods=['POST'])
def qa_Add_Estimator():
    try:
        app.logger.info('QA Insert Process Starting')
        category_id=request.json["categoryId"]
        projectName=request.json["projectName"]
        estimatorName=request.json["estimatorName"]
        QAName=request.json["qaName"]
        totalEfforts_inPersonHours=request.json["totalEfforts_inPersonHours"]
        retestingEfforts=request.json["retestingEfforts"]
        totalEfforts_inPersonDays=request.json["totalEfforts_inPersonDays"]
        is_active=request.json["is_active"]
        QA_taskgroup=request.json["qa_taskgroup"]     
        app.logger.info('Data request received')
        con = DataBase.getConnection()
        cur = con.cursor()
        sql="""INSERT INTO qa_estimator(category_id,projectName,estimatorName,qaName,totalEfforts_inPersonHours,retestingEfforts,totalEfforts_inPersonDays,is_active)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
        cur.execute(sql,(category_id,projectName,estimatorName,QAName,totalEfforts_inPersonHours,retestingEfforts,totalEfforts_inPersonDays,is_active))
        QA_estimator_ID=cur.lastrowid
        app.logger.info('QA_estimator Insert request received Successfully')
        for lst in QA_taskgroup:   
            cur.execute('INSERT INTO qa_taskgroup(is_active,taskgroup_id, qa_estimator_ID) VALUES (%s,%s,%s)',
                       (lst["is_active"],lst["taskgroup_id"],QA_estimator_ID))
            QA_taskGroup_id=cur.lastrowid
            app.logger.info('QA_taskgroup Insert request received Successfully')
            for tsklist in lst["qa_tasklist"]:
               effort_result_days = tsklist['simple']*tsklist['simpleWF'] + tsklist['medium']*tsklist['mediumWF'] + tsklist['complex']*tsklist['complexWF']
               effort_result_hrs = effort_result_days*8
               cur.execute('INSERT INTO  qa_tasklist( tasklist_id, simple, medium, complex,simpleWF,mediumWF,complexWF,effort_days,effort_hours,is_active, qa_taskGroup_id) VALUES (%s,%s,%s, %s,%s,%s,%s,%s,%s, %s,%s)',
                           ( tsklist['tasklist_id'], tsklist['simple'], tsklist['medium'], tsklist['complex'],tsklist['simpleWF'],tsklist['mediumWF'],tsklist['complexWF'],effort_result_days,effort_result_hrs,tsklist['is_active'],QA_taskGroup_id)) 
               app.logger.info('QA_tasklist Insert request received Successfully')
        con.commit()
        con.close()
        values = request.get_json()
        app.logger.info('Values Inserted Successfully in QA')
        return jsonify(values,"Data Successfully Uploded in QA")
    
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(e,"An ERROR occurred in table QA_POST Method")
    
@app.route('/Qa_Est_Getall',methods=['GET'])
def qa_Get_allEst_tables():
  try:
      app.logger.info('QA_Get All Process Starting')
      con = DataBase.getConnection()
      cur = con.cursor()
      cur.execute  (""" SELECT JSON_ARRAYAGG(  
                            JSON_OBJECT(
                            'categoryId',e.category_id,
                            'categoryName',c.category_name,
                            'qaEstimatorId', e.qa_estimator_ID,
                            'projectName', e.projectName,
                            'estimatorName', e.estimatorName,
                            'qaName', e.qaName,
                            'totalEffortsInPersonHours', e.totalEfforts_inPersonHours,
                            'retestingEfforts', e.retestingEfforts,
                            'totalEffortsInPersonDays', e.totalEfforts_inPersonDays,
                            'createdDate',e.created_date,
                            'updatedDate',e.updated_date,
                            'isActive',e.is_active,
                            'qaTaskGroups', 
                        (SELECT JSON_ARRAYAGG(
                             JSON_OBJECT(
                                      'qaTaskGroupId', tg.qa_taskGroup_id,
                                      'taskGroupId',tg.taskgroup_id,
                                      'taskGroupName', tg1.taskgroup_name,
                                      'qaEstimatorId',tg.qa_estimator_ID,
                                      'createdDate',tg.created_date,
                                      'updatedDate',tg.updated_date,
                                      'isActive',tg.is_active,
                                      'qaTasksLists', 
                                    (SELECT JSON_ARRAYAGG(
                                         JSON_OBJECT(
                                              'qaTaskListId', t.qa_tasklist_id, 
                                              'taskListId',t.tasklist_id,
                                              'qaTaskGroupId',t.qa_taskGroup_id,
                                              'taskName', t2.task_name, 
                                              'simple', t.simple, 
                                              'medium', t.medium, 
                                              'complex', t.complex,
                                              'simpleWf', t.simpleWF, 
                                              'mediumWf', t.mediumWF, 
                                              'complexWf', t.complexWF,
                                              'effortDays', t.effort_days, 
                                              'effortHours', t.effort_hours, 
                                              'createdDate',t.created_date,
                                              'updatedDate',t.updated_date,
                                              'isActive',t.is_active
                      )
                 ) FROM qa_tasklist t inner join tbltasklist t2 on t2.tasklist_id = t.tasklist_id  where t.qa_taskGroup_id =tg.qa_taskGroup_id and t.is_active=1 and t2.is_active=1)
            )
       ) FROM qa_taskgroup tg inner join tbltaskgroup tg1 on tg1.taskgroup_id = tg.taskgroup_id WHERE tg.qa_estimator_ID = e.qa_estimator_ID and tg.is_active=1 and tg1.is_active= 1)
      )
      )FROM qa_estimator e  inner join category c on c.category_id = e.category_id WHERE e.is_active=1 and c.is_active=1""")
      rows = cur.fetchall()
      result_json_str=rows[0][0]
      result_json = json.loads(result_json_str)
      app.logger.info('QA_Get All request received Successfully')
      return jsonify(result_json)
  
  except Exception as e:
   app.logger.error('An error occurred: %s', str(e))
   return jsonify(e,"An ERROR occurred in table QA_GET Method")

@app.route('/Qa_EstGetByID/<int:qa_estimator_ID>', methods=['GET'])
def qa_Get_ByID_Estimator(qa_estimator_ID):
  try:
      app.logger.info('QA_Get_By_ID Process Starting')
      con = DataBase.getConnection()
      cur = con.cursor()
      rows = cur.execute("""SELECT JSON_OBJECT(
                              'categoryId',e.category_id,
                              'categoryName',c.category_name,
                              'qaEstimatorId', e.qa_estimator_ID,
                              'projectName', e.projectName,
                              'estimatorName', e.estimatorName,
                              'qaName', e.qaName,
                              'totalEffortsInPersonHours', e.totalEfforts_inPersonHours,
                              'retestingEfforts', e.retestingEfforts,
                              'totalEffortsInPersonDays', e.totalEfforts_inPersonDays,
                              'createdDate',e.created_date,
                              'updatedDate',e.updated_date,
                              'isActive',e.is_active,
                              'qaTaskGroups', 
                              (SELECT JSON_ARRAYAGG(
                                  JSON_OBJECT(
                                      'qaTaskGroupId', tg.qa_taskGroup_id, 
                                      'taskGroupId', tg.taskgroup_id,
                                      'taskGroupName', tg1.taskgroup_name,
                                      'qaEstimatorId',tg.qa_estimator_ID,
                                      'createdDate',tg.created_date,
                                      'updatedDate',tg.updated_date,
                                      'isActive',tg.is_active,
                                      'qaTasksLists', 
                                      (SELECT JSON_ARRAYAGG(
                                          JSON_OBJECT(
                                              'qaTaskListId', t.qa_tasklist_id,
                                              'taskListId',t.tasklist_id, 
                                              'qaTaskGroupId',t.qa_taskGroup_id,
                                              'taskName', t2.task_name, 
                                              'simple', t.simple, 
                                              'medium', t.medium, 
                                              'complex', t.complex,
                                              'simpleWf', t.simpleWF, 
                                              'mediumWf', t.mediumWF, 
                                              'complexWf', t.complexWF,
                                              'effortDays', t.effort_days, 
                                              'effortHours', t.effort_hours, 
                                              'createdDate',t.created_date,
                                              'updatedDate',t.updated_date,
                                              'isActive',t.is_active
                                          )
                                      ) FROM qa_tasklist t inner join tbltasklist t2 on t2.tasklist_id = t.tasklist_id WHERE t.qa_taskGroup_id = tg.qa_taskGroup_id and t.is_active=1 and t2.is_active=1)
                                  )
                                ) FROM qa_taskgroup tg inner join tbltaskgroup tg1 on tg1.taskgroup_id = tg.taskgroup_id WHERE tg.qa_estimator_ID = e.qa_estimator_ID and tg.is_active=1 and tg1.is_active= 1)

                            ) FROM qa_estimator e  inner join category c on c.category_id = e.category_id WHERE e.qa_estimator_ID = %s and e.is_active=1 and c.is_active=1""", (qa_estimator_ID,))
                            
      rows = cur.fetchall()
      if len(rows) == 0:
          app.logger.info('Record Not Found for this Specific QA_Id')
          return jsonify("please enter a valid qa_estimator_ID")
      con.close()
      result_json_str = rows[0][0]
      result_json = json.loads(result_json_str)
      app.logger.info('QA Get_By_ID request received Successfully')
      return jsonify(f"Showing QA_estimator_ID : {qa_estimator_ID}", result_json)
  
  except Exception as e:
   app.logger.error('An error occurred: %s', str(e))
   return jsonify(e,"An ERROR occurred in table QA_GET_BY_ID Method")
  
@app.route('/Qa_GetFilterValues/<int:category_id>', methods=['GET'])
def qa_getFilterValues(category_id):
    try:
        app.logger.info('Qa_GetFilterValues Process Starting')
        con = DataBase.getConnection()
        cur = con.cursor()
        cur.execute(""" SELECT JSON_ARRAYAGG(
                           JSON_OBJECT(
                               'categoryID', c.category_id,
                               'categoryName', c.category_name,
                               'estimator', (
                                   SELECT JSON_ARRAYAGG(
                                       JSON_OBJECT(
                                           'estimatorID', e.qa_estimator_ID,
                                            'estimatorName', e.estimatorName,
                                           'taskgroup', (
                                               SELECT JSON_ARRAYAGG(
                                                   JSON_OBJECT(
                                                       'taskgroupID', tg.qa_taskGroup_id,
                                                       'taskgroupName', t.taskgroup_name
                                                   )
                                               )
                                               FROM qa_taskgroup tg
                                               inner join tbltaskgroup t
                                               on tg.taskgroup_id = t.taskgroup_id
                                               WHERE tg.qa_estimator_ID = e.qa_estimator_ID
                                               and t.is_active = 1
                                               and tg.is_active = 1
                                           )
                                       )
                                   )
                                FROM qa_estimator e
                                WHERE c.category_id = e.category_id
                                and e.is_active = 1
                               )
                           )
                       ) AS json_data
                FROM category c
                where c.category_id = %s 
                and c.is_active = 1""",(category_id,))
       
        rows = cur.fetchall()
        if len(rows) == 0:
            app.logger.info('Record Not Found for this Specific category_id')
            return jsonify("please enter a valid category_id")
        con.close()
        result_json_str = rows[0][0]
        result_json = json.loads(result_json_str)
        app.logger.info('Qa_GetFilterValues request received Successfully')
        return jsonify(f"Showing category_id : {category_id}", result_json)
  

    except Exception as e:
        app.logger.error(f'Error: {str(e)}')
        return jsonify(e,"An Error Occured in Getting Qa_GetFilterValues")

@app.route('/Get_Qa_Wf_Values/<int:category_id>', methods=['GET'])
def get_Qa_Wf_Values(category_id):
    try:
      app.logger.info('Get_Qa_Wf_Values Process Starting')
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
                              )as jsondata FROM category C where C.category_id = %s""",(category_id,))
      rows = cur.fetchall()
      if len(rows) == 0:
          app.logger.info('Record Not Found for this Specific Qa_Id')
          return jsonify("please enter a valid category_id")
      con.close()
      result_json_str = rows[0][0]
      result_json = json.loads(result_json_str)
      app.logger.info('Get_Qa_Wf_Values request received Successfully')
      return jsonify(f"Showing category_id : {category_id}", result_json)
    
    except Exception as e:
      app.logger.error('An error occurred: %s', str(e))
      return jsonify(e,"An Error Occured in Getting Get_Qa_Wf_Values")

@app.route('/Qa_EstimatorUpdate',methods=['PUT'])
def qa_update_Estimator(): 
    try: 
        app.logger.info('Qa_EstimatorUpdate Method Starting')
        request1= request.get_json()    
        for lst in request1:
            QA_estimator_ID=lst["qa_estimator_ID"]
            projectName=lst["projectName"]
            estimatorName=lst["estimatorName"]
            QAName=lst["qaName"]
            totalEfforts_inPersonHours=lst["totalEfforts_inPersonHours"]
            retestingEfforts=lst["retestingEfforts"]
            totalEfforts_inPersonDays=lst["totalEfforts_inPersonDays"]
            updated_date=datetime.now()
            is_active=lst["is_active"]
            QA_taskgroup=lst["qa_taskgroup"]
            app.logger.info('Data Update Request Received Successfully')
            con = DataBase.getConnection()
            cur = con.cursor()
            cur.execute("SELECT * FROM qa_estimator WHERE qa_estimator_ID = %s", [QA_estimator_ID])
            row = cur.fetchone()
            if row is None:
                app.logger.info('Record Not Found to Update For This Specific QA_estimator_ID')
                return jsonify("Record not found"), 404
            sql = """UPDATE qa_estimator SET
            projectName = %s,
            estimatorName = %s,
            qaName = %s,
            totalEfforts_inPersonHours = %s,
            retestingEfforts = %s,
            totalEfforts_inPersonDays = %s,
            updated_date=%s,
            is_active=%s
            WHERE qa_estimator_ID = %s"""

            cur.execute(sql,(projectName, estimatorName, QAName, totalEfforts_inPersonHours, retestingEfforts, totalEfforts_inPersonDays, updated_date ,is_active, QA_estimator_ID))
            app.logger.info('QA_estimator Update1 Request Received Successfully')

            for lst in QA_taskgroup:   
                cur.execute('UPDATE  qa_taskgroup SET qa_estimator_ID=%s,taskgroup_id=%s,updated_date=%s,is_active=%s WHERE qa_taskGroup_id=%s',
                        (lst['qa_estimator_ID'],lst['taskgroup_id'],updated_date,lst['is_active'],lst['qa_taskGroup_id']))
                app.logger.info('QA_taskgroup Update1 Request Received Successfully')
                for tsklist in lst["qa_tasklist"]: 
                    upt_effortRslt_days = tsklist['simple']*tsklist['simpleWF'] + tsklist['medium']*tsklist['mediumWF'] + tsklist['complex']*tsklist['complexWF']
                    upt_effortRslt_hrs = upt_effortRslt_days*8
                    cur.execute('UPDATE qa_tasklist SET tasklist_id=%s, simple=%s, medium=%s, complex=%s, simpleWF=%s, mediumWF=%s, complexWF=%s, effort_days=%s, effort_hours=%s,updated_date=%s,is_active=%s,qa_taskGroup_id=%s WHERE qa_tasklist_id=%s',
                                ( tsklist['tasklist_id'], tsklist['simple'], tsklist['medium'], tsklist['complex'], tsklist['simpleWF'], tsklist['mediumWF'], tsklist['complexWF'],upt_effortRslt_days, upt_effortRslt_hrs,updated_date,tsklist['is_active'],tsklist['qa_taskGroup_id'],tsklist['qa_tasklist_id']))   
                    app.logger.info('QA_tasklist Update1 Request Received Successfully')
            con.commit()
            con.close()
        values = request.get_json()
        app.logger.info('Qa_EstimatorUpdate Process Successfully Executed')
        return jsonify(values,"Data Successfully Updated")
    
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(e,"An ERROR occurred in table Qa_EstimatorUpdate Method")

@app.route('/Qa_Estimator_Updt_Delete', methods=['PUT'])
def qa_updateInsert_Estimator():
    try:
        app.logger.info('Qa_Estimator_Updt_Delete Method Starting')
        request1 = request.get_json()
        for lst in request1:
            qaEstimatorId = lst.get("qaEstimatorId")
            categoryId=lst["categoryId"]
            projectName = lst["projectName"]
            estimatorName = lst["estimatorName"]
            qaName = lst["qaName"]
            totalEffortsInPersonHours = lst["totalEffortsInPersonHours"]
            retestingEfforts = lst["retestingEfforts"]
            totalEffortsInPersonDays = lst["totalEffortsInPersonDays"]
            updatedDate = datetime.now()
            qaTaskGroups = lst["qaTaskGroups"]
            isActive=lst["isActive"]
            app.logger.info('Data update request received')
            con = DataBase.getConnection()
            cur = con.cursor()            
            if qaEstimatorId is not None and qaEstimatorId != "":
                sql = """UPDATE qa_estimator SET projectName=%s, estimatorName=%s, qaName=%s, category_id=%s,
                         totalEfforts_inPersonHours=%s, retestingEfforts=%s, totalEfforts_inPersonDays=%s,
                         updated_date=%s,is_active=%s WHERE  qa_estimator_ID=%s """
                cur.execute(sql, (projectName, estimatorName, qaName,categoryId, totalEffortsInPersonHours,
                                  retestingEfforts, totalEffortsInPersonDays, updatedDate,isActive, qaEstimatorId))
                app.logger.info("QA_estimator Data Updated Successfully")
            else:
                sql = """INSERT INTO qa_estimator(category_id,projectName, estimatorName, qaName, 
                         totalEfforts_inPersonHours, retestingEfforts, totalEfforts_inPersonDays,is_active)
                         VALUES (%s, %s, %s, %s, %s, %s,%s,%s)"""
                cur.execute(sql, (categoryId,projectName, estimatorName, qaName, totalEffortsInPersonHours,
                                  retestingEfforts, totalEffortsInPersonDays,isActive))
                qaEstimatorId = cur.lastrowid
                app.logger.info('QA_estimator Data Newly Inserted Successfully By PUT Method')
            for lst in qaTaskGroups:
                qaTaskGroupId = lst.get("qaTaskGroupId")
                if qaTaskGroupId is not None and qaTaskGroupId != "":
                    cur.execute('UPDATE qa_taskgroup SET taskgroup_id=%s,updated_date=%s,is_active=%s WHERE qa_taskGroup_id=%s',
                                (lst['taskGroupId'],updatedDate,lst['isActive'], qaTaskGroupId))
                    app.logger.info("QA_taskgroup  Data Updated Successfully")
                else:
                    cur.execute('INSERT INTO qa_taskgroup(is_active,taskgroup_id, qa_estimator_ID) VALUES (%s, %s,%s)',
                                (lst['isActive'],lst["taskGroupId"], qaEstimatorId))
                    qaTaskGroupId = cur.lastrowid
                    app.logger.info('QA_taskgroup Data Newly Inserted Successfully By PUT Method')
                for tsklist in lst["qaTasksLists"]:
                    qaTaskListId = tsklist.get("qaTaskListId")
                    if qaTaskListId is not None and qaTaskListId != "":
                        upt_effortRslt_days = tsklist['simple']*tsklist['simpleWf'] + tsklist['medium']*tsklist['mediumWf'] + tsklist['complex']*tsklist['complexWf']
                        upt_effortRslt_hrs = upt_effortRslt_days*8
                        cur.execute('UPDATE qa_tasklist SET tasklist_id=%s, simple=%s, medium=%s, complex=%s, simpleWF=%s, mediumWF=%s, complexWF=%s, effort_days=%s, effort_hours=%s,updated_date=%s,is_active=%s WHERE qa_tasklist_id=%s',
                                    (tsklist['taskListId'], tsklist['simple'], tsklist['medium'], tsklist['complex'], tsklist['simpleWf'], tsklist['mediumWf'], tsklist['complexWf'], upt_effortRslt_days, upt_effortRslt_hrs, updatedDate,tsklist['isActive'], qaTaskListId))
                        app.logger.info("QA_tasklist Data Updated Successfully")
                    else:
                        effort_result_days = tsklist['simple']*tsklist['simpleWf'] + tsklist['medium']*tsklist['mediumWf'] + tsklist['complex']*tsklist['complexWf']
                        effort_result_hrs = effort_result_days*8
                        cur.execute('INSERT INTO qa_tasklist(tasklist_id,simple, medium, complex, simpleWF, mediumWF, complexWF, effort_days, effort_hours, is_active,qa_taskGroup_id) '
                                    'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                                    (tsklist['taskListId'], tsklist['simple'], tsklist['medium'], tsklist['complex'], tsklist['simpleWf'], tsklist['mediumWf'], tsklist['complexWf'],
                                      effort_result_days, effort_result_hrs,tsklist['isActive'], qaTaskGroupId))
                        app.logger.info('QA_tasklist Data Newly Inserted Successfully By PUT Method')
            con.commit()
            con.close()
            values = request.get_json()
            app.logger.info('Qa_Estimator_Updt_Delete Process Successfully Executed')
            return jsonify(values,"Data Successfully Updated")
        
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(e,"An ERROR occurred in table Qa_Estimator_Updt_Delete Method")

@app.route('/Qa_Estimator_delete',methods=['DELETE'])
def qa_delete_Esti_ByID():
    try:
        app.logger.info('Qa_Estimator_delete Method Starting')
        con = DataBase.getConnection()
        cur = con.cursor()
        data = request.get_json()
        QA_estimator_ID = data.get("qa_estimator_ID")
        cur.execute("SELECT * FROM qa_estimator WHERE qa_estimator_ID = %s", [QA_estimator_ID])
        row = cur.fetchone()
        if row is None:
            app.logger.info('Record Not Found to Delete for this Specific Qa_Estimator_ID')
            return jsonify("Record not found"), 404        
        cur.execute("DELETE FROM qa_estimator WHERE qa_estimator_ID = %s", (QA_estimator_ID,))
        con.commit()
        con.close()
        app.logger.info('Qa_Estimator_Id Delete Process Successfully Executed')
        return jsonify({"message": f"QA_estimator_ID-{QA_estimator_ID} and associated task groups and tasks deleted successfully."})
    
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(e,"An ERROR occurred in table QA_DELETE Method")

@app.route('/Qa_download_excel_api/<int:category_id>/<estimator_ids>')
def qa_downloadExcelApi(category_id, estimator_ids):
    try:
        app.logger.info("Starting Qa_download_excel_api API function")
        query = qa_generateQuery(category_id, estimator_ids)
        file_path = qa_writeExcelFile(query)
        app.logger.info("Excel File Returning process successfully executed")
        filename = 'Qa_data.xlsx'
        response = make_response(send_file(file_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'))
        response.headers['Content-Disposition'] = f"attachment; filename={filename}"
        return response

    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return jsonify(error=str(e), message="An ERROR occurred in qa_downloadExcelApi function")

def qa_generateQuery(category_id, estimator_ids):
    query = """
        SELECT  c.category_name,es.projectName,es.estimatorName,es.totalEfforts_inPersonHours,es.retestingEfforts,es.totalEfforts_inPersonDays,tg.taskgroup_name,
         tl2.task_name,tl.simple, tl.medium, tl.complex, tl.simpleWF, tl.mediumWF,tl.complexWF, tl.effort_days, tl.effort_hours
        FROM category c
        INNER JOIN tbltaskgroup tg ON c.category_id = tg.category_id
        INNER JOIN qa_estimator es ON es.category_id = c.category_id
        INNER JOIN qa_taskgroup tg1 ON tg1.qa_estimator_ID = es.qa_estimator_ID
        AND tg1.taskgroup_id = tg.taskgroup_id
        INNER JOIN qa_tasklist tl ON tl.qa_taskGroup_id = tg1.qa_taskGroup_id
        INNER JOIN tbltasklist tl2 ON tl2.tasklist_id=tl.tasklist_id
        WHERE c.category_id = {}
        AND c.is_active = 1
        AND tg.is_active = 1
        AND tg1.is_active = 1
        AND tl.is_active = 1
        AND tl2.is_active = 1
        AND es.is_active = 1
        AND es.qa_estimator_ID IN ({})
    """.format(category_id,estimator_ids)
    return query

def qa_writeExcelFile(query):
    try:
        app.logger.info("Updating data to Excel file")
        con = DataBase.getConnection()
        cur = con.cursor()

        # Create a temporary file path
        temp_dir = os.path.join(app.instance_path, 'temp')
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = os.path.join(temp_dir, 'Qa_data.xlsx')
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
        worksheet.insert_image("A1",r"Image/emergere-logo.png",{"x_scale": 0.2, "y_scale": 0.2, "x_offset": 320, "y_offset": 10})
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
        return jsonify(error=str(e), message="An ERROR occurred in the qa_writeExcelFile method")


if __name__ == '__main__':
    app.run(debug=True)