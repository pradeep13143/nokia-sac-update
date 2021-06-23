import json
from datetime import datetime

import pyodbc
import requests
from requests.auth import HTTPBasicAuth

from compare import add_timestamp_to_date
from config import NDPD_DB_SERVER


def db_connection(db_name, username, password):
    try:
        server = NDPD_DB_SERVER
        cncx = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; \
                                      SERVER=' + server + ';DATABASE=' +
                              db_name + ';UID=' + username + ';PWD=' + password)
        return cncx
    except Exception as error:
        print(error)
        raise Exception(error)


def get_task_info_from_db(db_name, username, password, mapdata):
    cncx = ''
    cursor = ''
    try:
        cncx = db_connection(db_name, username, password)
        cursor = cncx.cursor()
        sql_query = "SELECT ActionName, ActionView FROM " \
                    "dbo.NDPdTaskActualization where " \
                    "ndpd_customerId='%s' AND ndpd_projectId='%s' AND " \
                    "ndpd_smpTemplateName='%s' " \
                    "AND ndpd_moduleName='%s' AND ndpd_TaskName='%s' AND " \
                    "STProjectTemplateId='%s' " \
                    "AND STMilestoneName='%s' AND Is_Active=1 AND " \
                    "IsDeleted=0;" \
                    % (mapdata['ndpd-customerId'],
                       mapdata["ndpd-projectId"],
                       mapdata['ndpd-smpName'],
                       mapdata['ndpd-moduleName'],
                       mapdata['ndpd-taskName'],
                       mapdata['st-project-template-id'],
                       mapdata['st-milestoneName'])
        print(str(sql_query))
        cursor.execute(sql_query)
        columns = [column[0] for column in cursor.description]
        data = cursor.fetchall()
        result = []
        for row in data:
            result.append(dict(zip(columns, row)))
        return result
    except Exception as error:
        if cncx:
            cncx.rollback()
            print("error in getting list")
            print(error)
        raise Exception(error)
    finally:
        if cursor:
            # cursor.close()
            del cursor
        if cncx:
            cncx.close()


def task_actualization(instance, db_name, username, password, ndpd_user,
                       ndpd_password, execute_task_endpoint, field, data):
    success_data, failure_data, warning_data = [], [], []
    action_name = ""
    action_value = ""
    # target_value = data['st-actualEndTime']
    db_result = get_task_info_from_db(db_name, username, password, data)
    for db_data in db_result:
        action_name = db_data.get("ActionName")
        action_value = db_data.get("ActionView")
    post_params = {
        "sfInstanceName": instance,
        "projectId": data['ndpd-projectId'],
        "smpId": data['ndpd-smpId'],
        "moduleId": data['ndpd-moduleId'],
        "taskName": data['ndpd-taskName'],
        "actionName": action_name,
        "value": action_value
    }
    '''
    response = session.post(
        url=execute_task_endpoint, data=json.dumps(post_params), verify=False)
    '''
    # Code for NDPd API revision
    header_content = {'Content-Type': "application/json"}
    response = requests.post(url=execute_task_endpoint,
                             data=json.dumps(post_params),
                             headers=header_content,
                             auth=HTTPBasicAuth(ndpd_user, ndpd_password))
    if response.status_code != 200:
        row_data = [data['ndpd-customerName'],
                    data['ndpd-projectId'],
                    data['ndpd-smpName'],
                    data['ndpd-smpId'],
                    data['ndpd-moduleId'],
                    data['ndpd-taskName'],
                    field,
                    data['st-projectId'],
                    data['p-number'],
                    data['st-project-template-id'],
                    data['st-project-template-name'],
                    data['st-milestoneName'],
                    action_name,
                    action_value,
                    "",
                    response.status_code,
                    response.reason]
        failure_data.append(row_data)
    elif "errorMessage" in json.loads(response.text):
        row_data = [data['ndpd-customerName'],
                    data['ndpd-projectId'],
                    data['ndpd-smpName'],
                    data['ndpd-smpId'],
                    data['ndpd-moduleId'],
                    data['ndpd-taskName'],
                    field,
                    data['st-projectId'],
                    data['p-number'],
                    data['st-project-template-id'],
                    data['st-project-template-name'],
                    data['st-milestoneName'],
                    action_name,
                    action_value,
                    "",
                    json.loads(response.text)['errorCode'],
                    json.loads(response.text)['errorMessage']]
        failure_data.append(row_data)
    elif "status" in json.loads(response.text) and json.loads(
            response.text)['status'] in ["success", "Success"]:
        row_data = [data['ndpd-customerName'],
                    data['ndpd-projectId'],
                    data['ndpd-smpName'],
                    data['ndpd-smpId'],
                    data['ndpd-moduleId'],
                    data['ndpd-taskName'],
                    field,
                    data['st-projectId'],
                    data['p-number'],
                    data['st-project-template-id'],
                    data['st-project-template-name'],
                    data['st-milestoneName'],
                    action_name,
                    action_value,
                    "",
                    "",
                    datetime.now().strftime("%m-%d-%Y-%H-%M-%S")]
        success_data.append(row_data)
    else:
        pass
    return success_data, failure_data, warning_data


def update_ndpd_side(db_name,
                     username,
                     password,
                     #session,
                     ndpd_user,
                     ndpd_password,
                     api_url,
                     instance,
                     forecast_endpoint,
                     actual_endpoint,
                     execute_task_endpoint,
                     ndpd_update_data):
    update_field = ''
    success_data, failure_data, warning_data = [], [], []
    url = ''
    for data in ndpd_update_data:
        print("checking order------------------")
        print(data['ndpd-smpId'], data['ndpd-taskName'])
        field = data['target-fields']
        if field == "Forecast Start Date":
            update_field = "plannedStartTime"
            url = api_url + forecast_endpoint
        if field == "Actual End Date":
            url = api_url + actual_endpoint
            update_field = "actualTime"
        execute_task_url = api_url + execute_task_endpoint
        target_value = add_timestamp_to_date(data['st-actualEndTime']) if \
            field == "Actual End Date" else add_timestamp_to_date(data['st-plannedStartTime'])
        old_value = data['ndpd-actualEndTime'] if \
            field == "Actual End Date" else data['ndpd-plannedStartTime']
        # calling api
        if data['ndpd-task-type'] == "Task" and field == "Actual End Date":
            success_list, failure_list, warning_list = task_actualization(
                instance, db_name, username, password, ndpd_user, ndpd_password,
                execute_task_url, field, data)
            success_data.extend(success_list)
            failure_data.extend(failure_list)
            warning_data.extend(warning_list)
        else:
            print(f"update field=============={update_field}")
            if update_field == "plannedStartTime":
                post_params = {
                    "sfInstanceName": instance,
                    "projectId": data['ndpd-projectId'],
                    "smpId": data['ndpd-smpId'],
                    "moduleId": data['ndpd-moduleId'],
                    "taskName": data['ndpd-taskName'],
                    update_field: target_value,
                    "plannedEndTime": target_value
                }
            else:
                post_params = {
                    "sfInstanceName": instance,
                    "projectId": data['ndpd-projectId'],
                    "smpId": data['ndpd-smpId'],
                    "moduleId": data['ndpd-moduleId'],
                    "taskName": data['ndpd-taskName'],
                    update_field: target_value
                }
            print(f"payload==================={post_params}")
            '''
            response = session.post(url=url,
                                    data=json.dumps(post_params),
                                    verify=False)
            '''
            # Code for NDPd API revision
            header_content = {'Content-Type': "application/json"}
            response = requests.post(url=url,
                                     data=json.dumps(post_params),
                                     headers=header_content,
                                     auth=HTTPBasicAuth(ndpd_user,
                                                        ndpd_password))
            if response.status_code != 200:
                row_data = [data['ndpd-customerName'],
                            data['ndpd-projectId'],
                            data['ndpd-smpName'],
                            data['ndpd-smpId'],
                            data['ndpd-moduleId'],
                            data['ndpd-taskName'],
                            field,
                            data['st-projectId'],
                            data['p-number'],
                            data['st-project-template-id'],
                            data['st-project-template-name'],
                            data['st-milestoneName'],
                            "",
                            "",
                            target_value,
                            response.status_code,
                            response.reason]
                failure_data.append(row_data)
            elif "status" in json.loads(response.text) and json.loads(
                    response.text)['status'] in ["success", "Success"]:
                row_data = [data['ndpd-customerName'],
                            data['ndpd-projectId'],
                            data['ndpd-smpName'],
                            data['ndpd-smpId'],
                            data['ndpd-moduleId'],
                            data['ndpd-taskName'],
                            field,
                            data['st-projectId'],
                            data['p-number'],
                            data['st-project-template-id'],
                            data['st-project-template-name'],
                            data['st-milestoneName'],
                            "",
                            "",
                            old_value,
                            target_value,
                            datetime.now().strftime("%m-%d-%Y-%H-%M-%S")]
                success_data.append(row_data)
            elif "errorMessage" in json.loads(response.text):
                row_data = [data['ndpd-customerName'],
                            data['ndpd-projectId'],
                            data['ndpd-smpName'],
                            data['ndpd-smpId'],
                            data['ndpd-moduleId'],
                            data['ndpd-taskName'],
                            field,
                            data['st-projectId'],
                            data['p-number'],
                            data['st-project-template-id'],
                            data['st-project-template-name'],
                            data['st-milestoneName'],
                            "",
                            "",
                            target_value,
                            json.loads(response.text)['errorCode'],
                            json.loads(response.text)['errorMessage']]
                failure_data.append(row_data)
            else:
                pass
    return success_data, failure_data, warning_data


def site_tracker_update_api_call(instance,
                                 instance_version,
                                 api_url,
                                 update_data,
                                 token):
    # /services/data/v48.0/sobjects/strk__Activity__c/a0222000002fzGIAAY
    url = instance + "/services/data/" + instance_version + api_url
    try:
        response = requests.patch(url, data=json.dumps(update_data),
                                  headers={"Authorization": "Bearer " + token,
                                           "Content-Type": "application/json"})
        return response
    except Exception as error:
        print(error)
        raise Exception(error)


def update_site_tracker_side(token,
                             st_instance,
                             st_instance_version,
                             site_tracker_data):
    """
    :param site_tracker_data: list of st data
    :param st_instance_version: st url
    :param st_instance: 48.0/
    :param token: token
    :return: success_data, failure_data in the form of list
    """
    success_data, failure_data = [], []
    old_value, target_value = '', ''
    for data in site_tracker_data:
        url = "sobjects/strk__Activity__c/"+str(data['st-milestoneId'])
        update_data = {}
        field = data['target-fields']
        if field == "Forecast Start Date":
            update_data = {"strk__Forecast_Date__c": data['ndpd-plannedStartTime']}
            target_value = data['ndpd-plannedStartTime']
            old_value = data['st-plannedStartTime']
        if field == "Actual End Date":
            update_data = {"strk__ActualDate__c": data['ndpd-actualEndTime']}
            target_value = data['ndpd-actualEndTime']
            old_value = data['st-actualEndTime']
        response = site_tracker_update_api_call(st_instance,
                                                st_instance_version,
                                                url, update_data, token)
        if response.status_code == 204 or response.status_code == "204":
            row_data = [data['ndpd-customerName'],
                        data['st-projectId'],
                        data['p-number'],
                        data['st-project-template-name'],
                        data['st-milestoneName'],
                        field,
                        data['ndpd-projectId'],
                        data['ndpd-smpName'],
                        data['ndpd-smpId'],
                        data['ndpd-moduleId'],
                        data['ndpd-taskName'],
                        old_value,
                        target_value,
                        datetime.now().strftime("%m-%d-%Y-%H-%M-%S")]
            success_data.append(row_data)
        else:
            row_data = [data['ndpd-customerName'],
                        data['st-projectId'],
                        data['p-number'],
                        data['st-project-template-name'],
                        data['st-milestoneName'],
                        field,
                        data['ndpd-projectId'],
                        data['ndpd-smpName'],
                        data['ndpd-smpId'],
                        data['ndpd-moduleId'],
                        data['ndpd-taskName'],
                        target_value,
                        json.loads(response.text)[0]['message']]
            failure_data.append(row_data)

    return success_data, failure_data
