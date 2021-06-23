import multiprocessing
import os
from datetime import datetime, timedelta
import json
from functools import partial

import requests
import pyodbc
import pandas as pd
from config import NDPD_DB_SERVER
from requests.auth import HTTPBasicAuth
import constants
from constants import (ndpd_success_header, ndpd_failure_header,
                       ndpd_warning_header, st_failure_header,
                       st_success_header, st_warning_header)
from xml.etree.ElementTree import Element, SubElement, Comment, tostring
from xml.etree import ElementTree



def get_time_execution(start_time, end_time):
    """
    :param start_time: start time
    :param end_time: end time
    :return: time taken to execute
    """
    time_delta = end_time - start_time
    s = time_delta.total_seconds()
    hours, remainder = divmod(s, 3600)
    minutes, seconds = divmod(remainder, 60)
    microseconds = time_delta.microseconds
    time_taken = "{:02}:{:02}:{:02}:{:02}".format(int(hours), int(minutes),
                                                  int(seconds), microseconds)
    time_taken = datetime.strptime(time_taken, "%H:%M:%S:%f")
    return time_taken


def get_session(**kwargs):
    """
    :return: session object
    """
    username = kwargs.get('username')
    password = kwargs.get('password')
    session = requests.Session()
    session.auth = HTTPBasicAuth(username, password)
    session.headers = {"Content-Type": "application/json"}
    return session


def get_formatted_mapped_data(mapping, data):
    """
    :param mapping: Mapping data to get project id, smpId, moduleId and taskId
    :param data: data from response
    :return: formatted data as per requirement
    """
    final_data = {
        "ndpd-projectId": mapping['ndpd-projectId'],
        "ndpd-smpId": mapping['ndpd-smpId'],
        "ndpd-moduleId": mapping['ndpd-moduleId'],
        "ndpd-taskName": data['name'],
        "ndpd-taskId": mapping['ndpd-taskId'],
        "actualStartTime": data['actualStartTime'],
        "actualEndTime": data['actualEndTime'],
        "plannedStartTime": data['plannedStartTime'],
        "plannedEndTime": data['plannedEndTime'],
        "currentOwnerName": data['currentOwnerName'],
        "assigneeUserName": data['assigneeUserName'],
        "lastModifiedTime": data['modifiedTime']
    }
    return final_data


def get_task_details_async(
        ndpd_instance, ndpd_url, get_task_details, user, password, mapping):
        #ndpd_instance, ndpd_url, get_task_details, session, mapping):
    '''
    This function triggers NDPd rest API in parallel using multi processing
    pool
    :param ndpd_instance: NDPd Instance
    :param ndpd_url: NDPd url
    :param session: NDPd session
    :param ibus_obj: ibus object
    :param logger: Logget object
    :param mapping: mapping json
    :return: list of object or empty list
    '''
    post_params = {
        "sfInstanceName": ndpd_instance,
        "smpId": mapping['ndpd-smpId'],
        "projectId": mapping['ndpd-projectId'],
        "moduleId": mapping['ndpd-moduleId'],
        "taskName": mapping['ndpd-taskName']
    }
    # calling api
    '''
    response = session.post(url=ndpd_url + get_task_details,
                            data=json.dumps(post_params),
                            verify=False)
    '''
    # Code for NDPd API revision
    header_content = {'Content-Type': "application/json"}
    response = requests.post(url=ndpd_url + get_task_details,
                             data=json.dumps(post_params),
                             headers=header_content,
                             auth=HTTPBasicAuth(user, password))

    if response.status_code == 200 or response.status_code == "200":
        if "errorMessage" in json.loads(response.text):
            pass
        else:
            return get_formatted_mapped_data(
                mapping, json.loads(response.text))


#def get_mapped_data_list(session, mappings, ibus_obj, logger, **kwargs):
def get_mapped_data_list(user, password, mappings, ibus_obj, logger, **kwargs):
    """
    :param ibus_obj: ibus obj
    :param logger: logger
    :param session: session object
    :param mappings: list of mappings received from input
    :return: formatted data as per requirement
    """
    # Using multiprocessing pool method, async calling rest api
    ndpd_instance = kwargs.get('SF_INSTANCE_NAME')
    ndpd_url = kwargs.get('url')
    get_task_details = kwargs.get('end_point')
    ibus_obj.logInfo("NumberOfAPICallsForGetTaskDetails: {}".format(len(mappings)))
    ibus_obj.logInfo("Start multiprocessing")
    results = []
    if mappings:
        pool = multiprocessing.Pool()
        func = partial(get_task_details_async, ndpd_instance, ndpd_url,
                       get_task_details, user, password)
                       #get_task_details, session)
        outputs_async = pool.map_async(func, mappings)
        results = outputs_async.get()
        # ibus_obj.logInfo("Multiprocessing result {}".format(results))
        ibus_obj.logInfo("Pool is closing")
        pool.close()
        ibus_obj.logInfo("Pool is joining")
        pool.join()

    return results


def get_smp_details(data, **kwargs):
    #session = get_session(username="CommonApiUser", password="Sf121Inn0@P!")
    params_data = {
            "projectId": data['projectid'],
            "smpId": data['smpId'],
            "sfInstanceName": data['sfInstanceName'],
            }
    # need to change this
    ndp_url = kwargs['url']
    endpoint = kwargs['get_smp_details_endpoint']
    url = ndp_url + endpoint
    '''
    response = session.post(url=url,
                            data=json.dumps(params_data),
                            verify=False)
    '''
    # Code for NDPd API revision
    header_content = {'Content-Type': "application/json"}
    response = requests.post(url=url,
                             data=json.dumps(params_data),
                             headers=header_content,
                             auth=HTTPBasicAuth("CommonApiUser", "Sf121Inn0@P!"))
    if response.status_code == 200:
        if "module" in response.json():
            return response.json()['module']
    else:
        return None


def get_module_details(data, **kwargs):
    # session = get_session(username="CommonApiUser", password="Sf121Inn0@P!")
    params_data = {
        "projectId": data['projectid'],
        "smpId": data['smpId'],
        "sfInstanceName": data['sfInstanceName'],
        "moduleId": data['moduleId']
    }
    ndp_url = kwargs['url']
    endpoint = kwargs['get_module_details_endpoint']
    url = ndp_url + endpoint
    # url = "https://nokiatraining.siteforge.com/rest
    # /SFCommonAPI/getModuleDetails"
    '''
    response = session.post(url=url,
                            data=json.dumps(params_data),
                            verify=False)
    '''
    # Code for NDPd API revision
    header_content = {'Content-Type': "application/json"}
    response = requests.post(url=url,
                             data=json.dumps(params_data),
                             headers=header_content,
                             auth=HTTPBasicAuth("CommonApiUser",
                                                "Sf121Inn0@P!"))
    if response.status_code == 200:
        if "tasks" in response.json():
            return response.json()['tasks']
    else:
        return None


def get_updated_mapping_data(
        mapping_list, db_name, username, password, ibus_obj, logger, **kwargs):
    updated_mapping_list = []
    ibus_obj.logInfo("entered into updating module id and adding st project id")
    project_id = '' # getting the project-id from mapping
    for mapping in mapping_list:
        if kwargs['instance-name'] == "Training":
            smpid = mapping['ndpd-playground-smpId']
            project_id = constants.PROJECT_ID
        else:
            smpid = mapping['ndpd-smpId']
            project_id = mapping['ndpd-projectId']
        # if mapping['ndpd-smpId'] in smp_mapping_dict:
        #     # ibus_obj.logInfo(f"existed SMP id in dict {mapping['ndpd-smpId']}")
        #     mapping['ndpd-smpId'] = smp_mapping_dict[mapping['ndpd-smpId']]
        module_ids = []
        # check if smp in mapping
        data = {
            'projectid': project_id,
            "smpId": smpid,
            "sfInstanceName": kwargs['instance-name']
        }
        mapping['ndpd-projectId'] = data['projectid']
        mapping['ndpd-smpId'] = data['smpId']
        response = get_smp_details(data, **kwargs)
        if response:
            module_ids = [i['workorderNumber'] for i in response if i['name'] == mapping['ndpd-moduleName']]
        for mod_id in module_ids:
            data['moduleId'] = mod_id
            response2 = get_module_details(data, **kwargs)
            if response2:
                for task in response2:
                    if mapping['ndpd-taskName'] == task['name']:
                        mapping['ndpd-moduleId'] = mod_id
                        mapping['ndpd-task-type'] = task['type']
                        break
        updated_mapping_list.append(mapping)
    ibus_obj.logInfo("finished updating of mapping list")
    # ibus_obj.logInfo("Started updating of project into mapping list")
    # st_project_list = get_smp_st_project_details_db(
    #     db_name, username, password, ibus_obj, logger)
    # for up_map in updated_mapping_list:
    #     for st in st_project_list:
    #         if up_map['ndpd-smpId'] == st['ndpd_smp_id']:
    #             up_map['st-projectId'] = st['st_project_id']
    #             break
    #         else:
    #             up_map['st-projectId'] = ''
    # ibus_obj.logInfo("finished updating of project into mapping list")
    return updated_mapping_list


def get_mapped_task_milestone_data_from_db(
        db_name, username, password, ibus_obj, logger, instance, smpids_string):
    """
    :param smpids_string: smpid string ex : "SMP-302200, SMP-36878, SMP-54765"
    :param db_name: DataBase Name for Ndpd DataBase server
    :param username: Username for authentication
    :param password: Password for authentication
    :param ibus_obj: To log messages in Ibus
    :return: final_dict = Is mapping data of Ndpd and site tracker
    """
    cncx = ''
    cursor = ''
    try:
        server = NDPD_DB_SERVER
        logger.info("Connecting to DataBase...")
        cncx = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; \
                              SERVER='+server+';DATABASE='+db_name+';UID='+username+';PWD='+password)
        logger.info("Connection to DataBase Successful, "
                    "creating cursor object...")
        cursor = cncx.cursor()
        logger.info("Calling Stored Procedure...")
        ibus_obj.logInfo(f"instance name is ====== {instance}")
        smpids = "'"+str(smpids_string)+"'"
        # ibus_obj.logInfo("'"+str(smpids_string)+"'")
        if instance == "Training":
            ibus_obj.logInfo(f"calling {'GetTaskAndFiledMappingDetailsMilestoneAlignment_v5'}")
            # cursor.execute(
            #     "{call GetTaskAndFiledMappingDetailsMilestoneAlignment_v2}")
            sql = f"exec GetTaskAndFiledMappingDetailsMilestoneAlignment_v5 " \
                  f"{smpids}"
        else:
            ibus_obj.logInfo(
                "calling GetTaskAndFiledMappingDetailsMilestoneAlignment_v4")
            # ibus_obj.logInfo("{call GetTaskAndFiledMappingDetailsMilestoneAlignment_v4 (?)}", smpids)
            sql = f"exec GetTaskAndFiledMappingDetailsMilestoneAlignment_v4 {smpids}"
        cursor.execute(sql)
            # cursor.execute("{call GetTaskAndFiledMappingDetailsMilestoneAlignment_v4 (?)}", smpids)
        logger.info("Stored Procedure called successful, "
                    "Creating Json Dictionary...")
        # Cursor.description gives tuple of tuples, every tuple have description
        # about one header & first element inside every tuple is header name
        columns = [column[0] for column in cursor.description]
        converted_columns = []
        for string in columns:
            new_string = string.replace("sf_", "ndpd-")
            replaced_string = new_string.replace("_", "-")
            converted_columns.append(replaced_string)
        # Fetches all the records without header & return type is list of tuples
        data = cursor.fetchall()
        final_dict = {"mappings": None}
        result = []
        if data:
            for row in data:
                result.append(dict(zip(converted_columns, row)))
        final_dict["mappings"] = result
        return final_dict
    except Exception as error:
        if cncx:
            cncx.rollback()
            logger.error(error)
        raise Exception(error)
    finally:
        if cursor:
            # cursor.close()
            del cursor
            logger.info(
                "Json Dictionary creation successful, Closing cursor...")
        if cncx:
            logger.info("Closing DataBase connection...")
            cncx.close()


def get_smp_from_cloned_db(
        db_name, username, password, ibus_obj, logger, instance, smpids_string):
    """
    :param smpids_string: smpid string ex : "SMP-302200, SMP-36878, SMP-54765"
    :param db_name: DataBase Name for Ndpd DataBase server
    :param username: Username for authentication
    :param password: Password for authentication
    :param ibus_obj: To log messages in Ibus
    :return: final_dict = Is mapping data of Ndpd and site tracker
    """
    cncx = ''
    cursor = ''
    try:
        server = NDPD_DB_SERVER
        logger.info("Connecting to DataBase...")
        cncx = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; \
                              SERVER='+server+';DATABASE='+db_name+';UID='+username+';PWD='+password)
        logger.info("Connection to DataBase Successful, "
                    "creating cursor object...")
        cursor = cncx.cursor()
        logger.info("Calling Stored Procedure...")
        smpids = "'"+str(smpids_string)+"'"
        # ibus_obj.logInfo("'"+str(smpids_string)+"'")
        ibus_obj.logInfo(
            "calling phase_1_CheckExistedSMP")
        # ibus_obj.logInfo("{call GetTaskAndFiledMappingDetailsMilestoneAlignment_v4 (?)}", smpids)
        sql = f"exec phase_1_CheckExistedSMP {smpids}"
        cursor.execute(sql)
        logger.info("Stored Procedure called successful, "
                    "Creating Json Dictionary...")
        # Cursor.description gives tuple of tuples, every tuple have description
        # about one header & first element inside every tuple is header name
        columns = [column[0] for column in cursor.description]
        converted_columns = []
        # for string in columns:
        #     new_string = string.replace("sf_", "ndpd-")
        #     replaced_string = new_string.replace("_", "-")
        #     converted_columns.append(replaced_string)
        # Fetches all the records without header & return type is list of tuples
        data = cursor.fetchall()
        final_dict = {"SMPS": None}
        result = []
        if data:
            for row in data:
                result.append(dict(zip(columns, row)))
        final_dict["SMPS"] = result
        return final_dict
    except Exception as error:
        if cncx:
            cncx.rollback()
            logger.error(error)
        raise Exception(error)
    finally:
        if cursor:
            # cursor.close()
            del cursor
            logger.info(
                "Json Dictionary creation successful, Closing cursor...")
        if cncx:
            logger.info("Closing DataBase connection...")
            cncx.close()


def get_smp_filtered_data(
        customer, username, password,
        db_name, logger, ibus_obj, instance, include_modified_time):
    cncx = ''
    cursor = ''
    today = datetime.now()
    ibus_obj.logInfo(f"Current date --------------{today}")
    try:
        server = NDPD_DB_SERVER
        logger.info("Connecting to DataBase...")
        cncx = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; \
                                  SERVER=' + server + ';DATABASE=' + db_name
                              + ';UID=' + username + ';PWD=' + password)
        logger.info("Connection to DataBase Successful, "
                    "creating cursor object...")
        cursor = cncx.cursor()
        cursor.execute(
            f"SELECT cutoff_date, days_modified, ndpd_project_ref_id, st_project, "
            f"hours_modified, mins_modified FROM phase_1_task_ms_update_config "
            f"WHERE ndpd_project='{customer}' AND ndpd_instance='{instance}'")
        ibus_obj.logInfo(f"query calling --------------")
        row = cursor.fetchone()
        if not row:
            error = f"No configuration found for project {customer}"
            raise Exception(error)
        created_date = str(row.cutoff_date) if row.cutoff_date else ''
        ibus_obj.logInfo(f"cutoff date --------{created_date}")
        days_delta = row.days_modified if row.days_modified else 0 # number of past days from today
        hours_delta = row.hours_modified if row.hours_modified else 0
        mins_delta = row.mins_modified if row.mins_modified else 0
        project_ref_id = row.ndpd_project_ref_id if \
            row.ndpd_project_ref_id else ''
        st_project_name = row.st_project if row.st_project else ""
        # get modified date to be used in SP
        modified_date = ''
        modified_formatted_date = ''# Initialize with empty string
        if days_delta or hours_delta or mins_delta:
            d = timedelta(days=days_delta, minutes=mins_delta, hours=hours_delta)
            modified_date = (today - d).strftime("%Y-%m-%d %H:%M:%S.%f")
            modified_formatted_date = modified_date[:-3]
        if include_modified_time == "false":
            ibus_obj.logInfo("Not filtering based on modified time")
            modified_date = ''
            modified_formatted_date = ''
        ibus_obj.logInfo(f"modified time-------------------{modified_formatted_date}")
        if instance == 'Training':
            cursor.execute("{call phase1_GetFilteredNDPDSMPS_UAT (?,?,?)}",
                           project_ref_id, created_date, modified_formatted_date)
            ibus_obj.logInfo(f"call phase1_GetFilteredNDPDSMPS_UAT "
                             f"{project_ref_id}, {created_date}, "
                             f"{modified_formatted_date}")
        else:
            ibus_obj.logInfo(f"call phase1_GetFilteredNDPDSMPS_NAM "
                             f"{project_ref_id}, {created_date}, {modified_formatted_date}")
            cursor.execute("{call phase1_GetFilteredNDPDSMPS_NAM (?,?,?)}",
                           project_ref_id, created_date, modified_formatted_date)
        smp_data = cursor.fetchall()
        return smp_data, project_ref_id, st_project_name, created_date, modified_date
    except Exception as error:
        if cncx:
            cncx.rollback()
            logger.error(error)
        raise Exception(error)
    finally:
        if cursor:
            # cursor.close()
            del cursor
            logger.info(
                "Json Dictionary creation successful, Closing cursor...")
        if cncx:
            logger.info("Closing DataBase connection...")
            cncx.close()


def get_proper_format(target_value, logger):
    target_date = ''
    try:
        target_date = datetime.strptime(target_value, '%Y-%m-%d')
    except Exception as error:
        logger.error(error)
    if target_date:
        return target_date.strftime("%Y-%m-%d")
    else:
        return None


def get_site_tracker_proper_date_format(target_value, logger):
    target_date = ''
    try:
        target_date = datetime.strptime(target_value, '%Y-%m-%d %H:%M:%S.%f')
    except Exception as error:
        logger.error(error)
    if target_date:
        return target_date.strftime("%Y-%m-%d")
    else:
        return None


# def get_ndpd_date(ndpd_date, logger):
#     target_date = ''
#     try:
#         target_date = datetime.strptime(ndpd_date, '%Y-%m-%d %H:%M:%S.%f')
#     except Exception as error:
#         logger.error(error)
#     if target_date:
#         return target_date.strftime("%Y-%m-%d")
#     else:
#         return None


def update_ndpd_fields(customer, ndpd, site_tracker, fields, ibus_obj, logger, task_type, **kwargs):
    success_data, failure_data, warning_data = [], [], []
    ibus_obj.logInfo("Fields received {}".format(fields))
    logger.info("Fields received {}".format(fields))
    for field in fields:
        ibus_obj.logInfo("processing the field {}".format(field))
        logger.info("processing the field {}".format(field))
        ndpd_date = get_site_tracker_proper_date_format(ndpd[field], logger)
        if ndpd_date == site_tracker[field]:
            ibus_obj.logInfo(f"ndpd data {ndpd_date} st data {site_tracker[field]}")
            ibus_obj.logInfo("No Changes detected in the field {}, "
                             "so skipping update".format(field))
            logger.info("No Changes detected in the field {}, "
                        "so skipping update".format(field))
            pass
        else:
            su_data, fail_data, war_data = \
                update_individual_ndpd_field(customer,
                                             ndpd,
                                             field,
                                             site_tracker[field],
                                             ibus_obj, logger, task_type, **kwargs)
            success_data.extend(su_data)
            failure_data.extend(fail_data)
            warning_data.extend(war_data)
    return success_data, failure_data, warning_data


def update_individual_ndpd_field(
        customer, ndpd, field, target_value, ibus_obj, logger, task_type, **kwargs):
    success_data, failure_data, warning_data = [], [], []
    actual_field = ''
    session = kwargs.get('session')
    API_URL = kwargs.get('url')
    SF_INSTANCE_NAME = kwargs.get('SF_INSTANCE_NAME')
    FORECAST_ENDPOINT = kwargs.get('forcast_endpoint')
    ACTUAL_ENDPOINT = kwargs.get('actual_endpoint')
    st_project_id = kwargs.get('st_project_id')
    st_project_template_id = kwargs.get('st_project_template_id')
    st_milestoneName = kwargs.get('st_milestoneName')
    url = ''
    # smpid = smp_mapping_dict[ndpd['ndpd-smpId']] if ndpd['ndpd-smpId']
    # in smp_mapping_dict else ndpd['ndpd-smpId']
    # projectid = "PR-0000081"
    moduleid = ndpd['ndpd-moduleId']
    # if ndpd['ndpd-moduleId'] in module_mapping_dict else
    # ndpd['ndpd-moduleId']
    logger.info("field received {}".format(field))
    if field == "plannedStartTime" or field == "plannedEndTime":
        if ndpd['actualEndTime'] == "null" or ndpd['actualEndTime'] == "":
            actual_field = field
            url = API_URL + FORECAST_ENDPOINT
        else:
            ibus_obj.logInfo("Actual date is present, so not "
                             "trying to update forecast date")
            ibus_obj.logInfo(f"actual date is {ndpd['actualEndTime']}")
            ibus_obj.logInfo(
                f"{ndpd['ndpd-smpId']}, {ndpd['ndpd-moduleId']}, "
                f"{ndpd['ndpd-taskName']}")
            return success_data, failure_data, warning_data
    if field == "actualStartTime" or field == "actualEndTime":
        url = API_URL + ACTUAL_ENDPOINT
        actual_field = field
        field = "actualTime"
    # calling api
    if task_type == "Task" and actual_field == "actualEndTime":
        return success_data, failure_data, warning_data
    if not target_value or target_value == "" or target_value == "null":
        ibus_obj.logInfo(f"target value is null or "
                         f"empty so skipping -- {actual_field}")
        # ignoring if field is empty or null
        # ibus_obj.logInfo("Empty date received".format(target_value))
        # row_data = [customer,
        #             projectid,
        #             SF_INSTANCE_NAME,
        #             smpid,
        #             moduleid,
        #             ndpd['ndpd-taskName'],
        #             field,
        #             target_value,
        #             '',
        #             str(field) + " should not be empty or null"]
        # ibus_obj.logInfo("Row data {}".format(row_data))
        # warning_data.append(row_data)
        # ibus_obj.logInfo("Warning data {}".format(warning_data))
        return success_data, failure_data, warning_data
    proper_target_value = get_proper_format(target_value, logger)
    if proper_target_value:
        ibus_obj.logInfo("calling API {}".format(url))
        post_params = {
            "sfInstanceName": SF_INSTANCE_NAME,
            "projectId": ndpd['ndpd-projectId'],
            "smpId": ndpd['ndpd-smpId'],
            "moduleId": ndpd['ndpd-moduleId'],
            "taskName": ndpd['ndpd-taskName'],
            field: proper_target_value
        }
        ibus_obj.logInfo("post parameters to API {}".format(post_params))
        ibus_obj.logInfo("Order ---{}".format(post_params))
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
                                 auth=HTTPBasicAuth("CommonApiUser",
                                                    "Sf121Inn0@P!"))
        ibus_obj.logInfo("response from API {}".format(response))
        logger.info("Response from API {}".format(
            json.loads(response.text)))
        # Removing ST_INSTANCE_NAME for report because we don't have api to get SITE Names
        if response.status_code == 401 or response.status_code == 500:
            row_data = [customer, ndpd['ndpd-projectId'],
                        ndpd['ndpd-smpId'],
                        moduleid,
                        ndpd['ndpd-taskName'],
                        actual_field, st_project_id,
                        st_project_template_id,
                        st_milestoneName,
                        "",
                        "",
                        target_value,
                        response.status_code,
                        response.reason]
            failure_data.append(row_data)
            return success_data, failure_data, warning_data
        if "status" in json.loads(response.text) and json.loads(
                response.text)['status'] in ["success", "Success"]:
            row_data = [customer, ndpd['ndpd-projectId'],
                        ndpd['ndpd-smpId'], moduleid, ndpd['ndpd-taskName'],
                        actual_field, st_project_id,
                        st_project_template_id,
                        st_milestoneName,
                        "",
                        "",
                        "" if ndpd[actual_field] == "null" else ndpd[actual_field],
                        target_value]
            success_data.append(row_data)
        if "errorMessage" in json.loads(response.text):
            row_data = [customer, ndpd['ndpd-projectId'],
                        ndpd['ndpd-smpId'], moduleid, ndpd['ndpd-taskName'],
                        actual_field, st_project_id,
                        st_project_template_id,
                        st_milestoneName,
                        "",
                        "",
                        target_value,
                        json.loads(response.text)['errorCode'],
                        json.loads(response.text)['errorMessage']]
            failure_data.append(row_data)
        return success_data, failure_data, warning_data
    else:
        row_data = [customer, ndpd['ndpd-projectId'],
                    ndpd['ndpd-smpId'], moduleid, ndpd['ndpd-taskName'],
                    actual_field,
                    st_project_id,
                    st_project_template_id,
                    st_milestoneName,
                    "",
                    "",
                    target_value,
                    '',
                    "Not a proper date format"]
        failure_data.append(row_data)
        return success_data, failure_data, warning_data


def site_tracker_token_generator(authentication_url, client_id,
                                 client_secret_key, security_token_key,
                                 username,
                                 password, ibus_obj, logger):
    """
    Input:
    :param authentication_url: Authentication url of Site-tracker.
    :param client_id: client-if of Site-tracker.
    :param client_secret_key: client security key of Site-tracker.
    :param security_token_key: security token of Site-tracker.
    :param username: username for Site-tracker.
    :param password: password for Site-tracker.
    :return:
        token: access token for Site-tracker.
    """
    request_token_url = authentication_url
    client_id = client_id
    client_secret = client_secret_key
    security_token = security_token_key
    email = username
    password = password

    payload = {
        'grant_type': 'password',
        'client_id': client_id,
        'client_secret': client_secret,
        'username': email,
        'password': password + security_token
    }
    try:
        ibus_obj.logInfo("Sending response...")
        r = requests.post(request_token_url, headers={
            "Content-Type": "application/x-www-form-urlencoded"}, data=payload)
        ibus_obj.logInfo("Response received...")
        logger.info("Response received...")
        body = r.json()
        ibus_obj.logInfo("response json -- {}".format(body))
        if "access_token" in body:
            token = body['access_token']
            ibus_obj.logInfo("Received Access Token...")
            logger.info("Received Access Token...")
            return token
        else:
            # token = None
            return None
    except Exception as error:
        logger.error(error)
        ibus_obj.logInfo("Error in utility {}".format(error))
        raise Exception(error)


def site_tracker_api_call_latest(instance,
                          instance_version, token_value, milestones, project_id_list):
    """
    Input
        :param logger:
        :param instance: Site_tracker instance.
        :param instance_version: Site_tracker instance version.
        :param api_url: Api to be called in Site-tracker.
        :param token_value: Site-tracker token value.
    :return:
        final_dict : updated json from site tracker.
    """
    try:
        api_url = "query?q=SELECT Id,LastModifiedDate,Name," \
                 "strk__ActualDate__c,strk__Forecast_Date__c," \
                 "strk__Project__c,strk__Project__r.Name FROM strk__Activity__c " \
                 f"WHERE strk__Project__c IN {project_id_list} AND Name" \
                 f"{milestones} AND strk__Activity_Type__c" \
                 " IN ('Milestone','Approval')"
        if len(project_id_list) == 1:
            api_url = "query?q=SELECT Id,LastModifiedDate,Name," \
                      "strk__ActualDate__c,strk__Forecast_Date__c," \
                      "strk__Project__c,strk__Project__r.Name FROM strk__Activity__c " \
                      f"WHERE strk__Project__c = '{project_id_list[0]}' AND Name" \
                      f"{milestones} AND strk__Activity_Type__c" \
                      " IN ('Milestone','Approval')"
        api_url = api_url.replace("&", "%26")
        url = instance + "/services/data/" + instance_version + api_url
        response_of_api = requests.get(url, headers={
            "Authorization": "Bearer " + token_value})
        return response_of_api.json()
    except Exception as error:
        raise Exception(error)


def site_tracker_api_call(instance,
                          instance_version,
                          api_url, token_value, ibus_obj, logger):
    """
    Input
        :param logger:
        :param instance: Site_tracker instance.
        :param instance_version: Site_tracker instance version.
        :param api_url: Api to be called in Site-tracker.
        :param token_value: Site-tracker token value.
    :return:
        final_dict : updated json from site tracker.
    """
    st_instance = instance
    st_instance_version = instance_version
    st_url = api_url
    st_token = token_value
    ibus_obj.logInfo("Preparing url...")
    url = st_instance + "/services/data/" + st_instance_version + st_url
    ibus_obj.logInfo("url --> {0}".format(url))
    ibus_obj.logInfo("Sending response to SiteTracker...")
    logger.info("Sending response to SiteTracker...")
    response_of_api = requests.get(url, headers={
        "Authorization": "Bearer " + st_token})
    ibus_obj.logInfo("Response received...")
    logger.info("Response received...")
    # ibus_obj.logInfo("Printing response --- {}".format(response_of_api.json()))
    final_dict = []
    try:
        logger.info("Preparing json from response...")
        if response_of_api.json()['totalSize'] != 0:
            for each in response_of_api.json()['records']:
                final_dict.append({
                    "st-projectId": each["strk__Project__c"],
                    "st-milestoneName": each["Name"],
                    "st-milestoneId": each["Id"],
                    "actualStartTime": "",
                    "actualEndTime": each["strk__ActualDate__c"],
                    "plannedStartTime": each["strk__Forecast_Date__c"],
                    "plannedEndTime": "",
                    "currentOwnerName": "",
                    "assigneeUserName": "",
                    "lastModifiedTime": each["LastModifiedDate"].replace("T", " ")[:23]
                })

            if 'nextRecordsUrl' in response_of_api.json().keys():
                api_url = response_of_api.json()['nextRecordsUrl']
                next_url = '/query/' + api_url.split('/')[-1]
                site_tracker_api_call(
                    instance=st_instance,
                    instance_version=st_instance_version, api_url=next_url,
                    token_value=st_token, ibus_obj=ibus_obj, logger=logger)
        ibus_obj.logInfo("Json prepared successful...")
        logger.info("Json prepared successful...")
        return final_dict
    except Exception as error:
        logger.error(error)
        raise Exception(error)


def get_st_filtered_projects_dict(token,
                                  instance,
                                  instance_version,
                                  api_url,
                                  logger,
                                  ibus_obj,
                                  final_dict):
    """
    Input
        :param token:
        :param logger:
        :param instance: Site_tracker instance.
        :param instance_version: Site_tracker instance version.
        :param api_url: Api to be called in Site-tracker.
        :param token_value: Site-tracker token value.
    :return:
        final_dict : updated json from site tracker.
    """
    st_instance = instance
    st_instance_version = instance_version
    st_url = api_url
    st_token = token
    ibus_obj.logInfo("Preparing url...")
    url = st_instance + "/services/data/" + st_instance_version + st_url
    ibus_obj.logInfo("url --> {0}".format(url))
    ibus_obj.logInfo("Sending response to SiteTracker...")
    logger.info("Sending response to SiteTracker...")
    response_of_api = requests.get(url, headers={
        "Authorization": "Bearer " + st_token})
    ibus_obj.logInfo("Response received...")
    logger.info("Response received...")
    try:
        logger.info("Preparing json from response...")
        ibus_obj.logInfo("Preparing json from response...")
        # ibus_obj.logInfo(response_of_api.json())
        if response_of_api.json()['totalSize'] != 0:
            for each in response_of_api.json()['records']:
                final_dict[each["NDPd_SMP_ID__c"]] = each["Id"]
            if 'nextRecordsUrl' in response_of_api.json().keys():
                api_url = response_of_api.json()['nextRecordsUrl']
                next_url = 'query/' + api_url.split('/')[-1]
                get_st_filtered_projects_dict(
                    instance=st_instance,
                    instance_version=st_instance_version, api_url=next_url,
                    token=token, ibus_obj=ibus_obj, logger=logger, final_dict = final_dict)
        ibus_obj.logInfo("Json prepared successful...")
        logger.info("Json prepared successful...")
        return final_dict
    except Exception as error:
        logger.error(error)
        raise Exception(error)


def site_tracker_update_api_call(instance,
                          instance_version,
                          api_url, update_data,
                                 token_value, ibus_obj, logger,
                                 request_type='get'):
    """
    Input
        :param instance: Site_tracker instance.
        :param instance_version: Site_tracker instance version.
        :param api_url: Api to be called in Site-tracker.
        :param token_value: Site-tracker token value.
    :return:
        final_dict : updated json from site tracker.
    """
    st_instance = instance
    updated = True
    st_instance_version = instance_version
    st_url = api_url
    st_token = token_value
    ibus_obj.logInfo("Preparing url...")
    # /services/data/v48.0/sobjects/strk__Activity__c/a0222000002fzGIAAY
    url = st_instance + "/services/data/" + st_instance_version + st_url
    ibus_obj.logInfo("url --> {0}".format(url))
    logger.info("url --> {0}".format(url))
    ibus_obj.logInfo("Sending response to SiteTracker...")
    ibus_obj.logInfo("Data we are trying to update {}".format(json.dumps(update_data)))
    logger.info("Sending response to SiteTracker...")
    try:
        # response_of_api = requests.post(url, headers={
        #     "Authorization": "Bearer " + st_token})
        response = requests.patch(url, data=json.dumps(update_data),
                                  headers={"Authorization": "Bearer " + st_token,
                                           "Content-Type": "application/json"})
        if list(update_data.keys())[0] == 'strk__Forecast_Date__c':
            recheck_response = requests.get(url,
                                            headers={"Authorization": "Bearer " + st_token,
                                                     "Content-Type": "application/json"})
            if recheck_response.status_code == "200" or recheck_response.status_code == 200:
                if not update_data[list(update_data.keys())[0]] == recheck_response.json()[list(update_data.keys())[0]]:
                    updated = False
        # ibus_obj.logInfo("Printing the response of update api ---- {}".format(response))
        return response, updated

    except Exception as error:
        logger.error(error)
        raise Exception(error)


def update_siteTracker_fields(token, customer, ndpd, site_tracker, fields,
                              ibus_obj, logger, st_instance, st_instance_version):
    """
    :param ndpd:
    :param site_tracker:
    :param fields:
    :param ibus_obj:
    :return:success, failure data info in the form of list
    """
    success_data, failure_data = [], []
    for field in fields:
        ibus_obj.logInfo("processing the field {}".format(field))
        ibus_obj.logInfo(f"NDPD Date ---{ndpd[field]}")
        ibus_obj.logInfo(f"ST Date ---{site_tracker[field]}")
        ndpd_date = get_site_tracker_proper_date_format(ndpd[field], logger)
        ibus_obj.logInfo(f"formatted NDPD Date ---{ndpd[field]}")
        if ndpd_date == site_tracker[field]:
            ibus_obj.logInfo(f"ndpd data {ndpd_date} st data {site_tracker[field]}")
            ibus_obj.logInfo("No Changes detected in the field {}, "
                             "so skipping update".format(field))
            pass
        else:
            success_data, failure_data = \
                update_individual_sitetracker_field(token,customer, site_tracker,
                                             field,
                                             ndpd[field],
                                             ibus_obj, logger, ndpd, st_instance, st_instance_version)
    return success_data, failure_data


def update_individual_sitetracker_field(token, customer, site_tracker, field,
                                        target_value,ibus_obj, logger, ndpd,
                                        st_instance, st_instance_version):
    """
    :param site_tracker: Site tracker list
    :param customer: name of the customer
    :param field: field that need to be updated
    :param target_value: value that need to be passed in to ST field
    :param ibus_obj:
    :return: success_data, failure_data in the form of list
    """
    success_data, failure_data = [], []
    smpid = ndpd['ndpd-smpId']
    projectid = ndpd['ndpd-projectId']
    moduleid = ndpd['ndpd-moduleId']
    # session = get_session()
    url = "sobjects/strk__Activity__c/"+str(site_tracker['st-milestoneId'])
    update_data = {}
    if not target_value or target_value == "" or target_value == "null":
        # ignoring if target field value is empty or null
        ibus_obj.logInfo("Empty value recieved so skipping : {}".format(target_value))
        return success_data, failure_data
    proper_value = get_site_tracker_proper_date_format(target_value, logger)
    if proper_value:
        if field == "plannedStartTime":
            if site_tracker['actualEndTime'] == "" or \
                    not site_tracker['actualEndTime'] or \
                    site_tracker['actualEndTime'] == "null":
                update_data = {"strk__Forecast_Date__c": proper_value}
            else:
                ibus_obj.logInfo("SKipping because actual date is present")
                ibus_obj.logInfo(
                    f"actual end time is  = {site_tracker['actualEndTime']}")
                ibus_obj.logInfo(
                    f" milestone name = {site_tracker['st-milestoneName']}, "
                    f"project id = {site_tracker['st-projectId']}")
                return success_data, failure_data
        if field == "actualEndTime":
            update_data = {"strk__ActualDate__c": proper_value}
        logger.info("Calling site_tracker_api_call function...")
        response, updated = site_tracker_update_api_call(st_instance,
                                    st_instance_version, url, update_data, token,
                                            ibus_obj, logger)
        ibus_obj.logInfo(
            "Site tracker api call function called successful...")
        logger.info("function called successful...")
        ibus_obj.logInfo("Order ---{}".format(site_tracker['st-milestoneName']))
        # ibus_obj.logInfo("Response from "
        #                  "API {}".format(json.loads(response.text)))
        if not updated:
            row_data = [customer, site_tracker['st-projectId'],
                        site_tracker['st-milestoneName'], field,
                        projectid,
                        smpid,
                        moduleid,
                        ndpd["ndpd-taskName"],
                        target_value,
                        "Not updated, because actual date is present"]
            failure_data.append(row_data)
        else:
            if response.status_code == 204 or response.status_code == "204":
                row_data = [customer, site_tracker['st-projectId'],
                            site_tracker['st-milestoneName'], field,
                            projectid,
                            smpid,
                            moduleid,
                            ndpd["ndpd-taskName"],
                            site_tracker[field], proper_value]
                success_data.append(row_data)
            else:
                row_data = [customer, site_tracker['st-projectId'],
                            site_tracker['st-milestoneName'],field,
                            projectid,
                            smpid,
                            moduleid,
                            ndpd["ndpd-taskName"],
                            target_value,
                            json.loads(response.text)[0]['message']]
                failure_data.append(row_data)
    else:
        row_data = [customer, site_tracker['st-projectId'],
                    site_tracker['st-milestoneName'], field,
                    projectid,
                    smpid,
                    moduleid,
                    ndpd["ndpd-taskName"],
                    target_value,
                    "Not a proper date format"]
        failure_data.append(row_data)
    return success_data, failure_data


def excel_forming(writer_object, sheet_name, dataframe_name, columns, ibus_obj):
    """
    This function will format the excel which is the report
    Input:
        :param ibus_obj:
        :param writer_object: writer object of excel
        :param sheet_name: name of the sheet
        :param dataframe_name: name of the dataframe
        :param columns: column range
    :return:
    """
    workbook = writer_object.book
    ibus_obj.logInfo("Workbook: {}".format(workbook))
    worksheet = writer_object.sheets[sheet_name]
    ibus_obj.logInfo("worksheet: {}".format(worksheet))
    worksheet.set_column(columns, 18)
    ibus_obj.logInfo("worksheet: {}".format(worksheet))
    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'valign': 'top',
        'fg_color': '#87CEEB',
        'border': 1})
    for col_num, col_val in enumerate(dataframe_name.columns.values):
        worksheet.write(0, col_num, col_val, header_format)


def write_to_excel(data_dict, reports, ibus_obj):
    # ibus_obj.logInfo("data dict received : {}".format(data_dict))
    with pd.ExcelWriter(reports) as writer:
        if "NDPD Success" in data_dict:
            ndpd_success_data = pd.DataFrame(
                data_dict['NDPD Success'], columns=ndpd_success_header)
            ndpd_success_data.to_excel(
                writer, sheet_name='NDPD Success', index=False)
            excel_forming(writer_object=writer,
                          sheet_name='NDPD Success',
                          dataframe_name=ndpd_success_data,
                          columns='A:K', ibus_obj = ibus_obj)
        if "NDPD Failure" in data_dict:
            ndpd_failure_data = pd.DataFrame(
                data_dict['NDPD Failure'], columns=ndpd_failure_header)
            ndpd_failure_data.to_excel(
                writer, sheet_name='NDPD Failure', startcol=-1)
            excel_forming(writer_object=writer,
                          sheet_name='NDPD Failure',
                          dataframe_name=ndpd_failure_data,
                          columns='A:K', ibus_obj = ibus_obj)
        if "NDPD Warning" in data_dict:
            ndpd_warning_data = pd.DataFrame(
                data_dict['NDPD Warning'], columns=ndpd_warning_header)
            ndpd_warning_data.to_excel(
                writer, sheet_name='NDPD Warning', startcol=-1)
            excel_forming(writer_object=writer,
                          sheet_name='NDPD Warning',
                          dataframe_name=ndpd_warning_data,
                          columns='A:K', ibus_obj = ibus_obj)
        if "SiteTracker Success" in data_dict:
            st_success_data = pd.DataFrame(
                data_dict['SiteTracker Success'],
                columns=st_success_header)
            st_success_data.to_excel(
                writer, sheet_name='SiteTracker Success', startcol=-1)
            excel_forming(writer_object=writer,
                          sheet_name='SiteTracker Success',
                          dataframe_name=st_success_data,
                          columns='A:K', ibus_obj = ibus_obj)
        if "SiteTracker Failure" in data_dict:
            st_failure_data = pd.DataFrame(
                data_dict['SiteTracker Failure'],
                columns=st_failure_header)
            st_failure_data.to_excel(
                writer, sheet_name='SiteTracker Failure', startcol=-1)
            excel_forming(writer_object=writer,
                          sheet_name='SiteTracker Failure',
                          dataframe_name=st_failure_data,
                          columns='A:I', ibus_obj = ibus_obj)
        if "SiteTracker Warning" in data_dict:
            st_warning_data = pd.DataFrame(
                data_dict['SiteTracker Warning'],
                columns=st_warning_header)
            st_warning_data.to_excel(
                writer, sheet_name='SiteTracker Warning', startcol=-1)
            excel_forming(writer_object=writer,
                          sheet_name='SiteTracker Warning',
                          dataframe_name=st_warning_data,
                          columns='A:J', ibus_obj = ibus_obj)
    return reports


# --------- Written by - sheshagiri --------------------
# Logic to get Missing site tracker project id
# takes the input from db query st and stores the info into db
def getMissingStProjectId(instance, db_name, username, password, st_username,
                          st_password, ibus_obj, logger, st_instance,
                          st_instance_version, st_authentication_url,
                          st_client_id, st_client_secrete_key,
                          st_security_token,
                          ndpd_project_ref_id):
    # Getting the template details from ST
    ibus_obj.logInfo("Calling get_st_template_details_from_db function...")
    logger.info("Calling get_st_template_details_from_db function...")
    st_template_names = get_st_template_details_from_db(db_name, username, password, ibus_obj, logger)
    ibus_obj.logInfo("function get_st_template_details_from_db called successful...")
    logger.info("function get_st_template_details_from_db called successful...")

    ibus_obj.logInfo("Calling get_smp_st_project_details_db function...")
    logger.info("Calling get_smp_st_project_details_db function...")
    smp_st_details = get_smp_st_project_details_db(db_name, username, password, ibus_obj, logger, instance, ndpd_project_ref_id)
    ibus_obj.logInfo("function get_smp_st_project_details_db called successful...")
    logger.info("function get_smp_st_project_details_db called successful...")

    if st_template_names[constants.Mappings]:
        ibus_obj.logInfo("Picking strk_project_Template_name and strk_customer_name .....")
        strk_project_template_name = []
        strk_customer_name = []
        for mappings in st_template_names[constants.Mappings]:
            if mappings[constants.StProjectTemplateName]:
                strk_project_template_name.append(
                    mappings[constants.StProjectTemplateName])
            if mappings[constants.STCustomerName]:
                strk_customer_name.append(mappings[constants.STCustomerName])
        ibus_obj.logInfo("Picked strk_project_Template_name and strk_customer_name .....")

        ibus_obj.logInfo("Removing the duplicates in strk_project_Template_name and strk_customer_name .....")
        strk_project_template_name = list(set(strk_project_template_name))
        strk_customer_name = list(set(strk_customer_name))
        ibus_obj.logInfo("Removed the duplicates in strk_project_Template_name and strk_customer_name .....")

        ibus_obj.logInfo("Preparing st_query....")
        if (len(strk_project_template_name) == 1) and len(strk_customer_name) == 1:
            strk_project_template_string = ' '.join([str(elem) for elem in strk_project_template_name])
            strk_customer_name_string = ' '.join([str(elem) for elem in strk_customer_name])
            query = "query?q=SELECT Id,NDPd_SMP_ID__c,strk__Customer__c,strk__ProjectTemplate__c,strk__Project_Template__c," \
                    "strk__Project__c, Secondary_Customer__c FROM strk__Project__c WHERE " \
                    "strk__Project_Template__c = '{}'  AND Secondary_Customer__c = '{}'".format(
                strk_project_template_string, strk_customer_name_string)
        elif len(strk_project_template_name)== 1:
            strk_project_template_string = ' '.join([str(elem) for elem in strk_project_template_name])
            query = "query?q=SELECT Id,NDPd_SMP_ID__c,strk__Customer__c,strk__ProjectTemplate__c,strk__Project_Template__c," \
                    "strk__Project__c, Secondary_Customer__c FROM strk__Project__c WHERE " \
                    "strk__Project_Template__c = '{}'  AND Secondary_Customer__c IN " + str(
                tuple(strk_customer_name)) + "".format(strk_project_template_string)
        elif len(strk_customer_name) == 1:
            strk_customer_name_string = ' '.join([str(elem) for elem in strk_customer_name])
            query = "query?q=SELECT Id,NDPd_SMP_ID__c,strk__Customer__c,strk__ProjectTemplate__c," \
                    "strk__Project_Template__c, strk__Project__c, Secondary_Customer__c " \
                    "FROM strk__Project__c WHERE strk__Project_Template__c IN " + str(
                tuple(strk_project_template_name)) + " AND Secondary_Customer__c = '{}'".format(
                strk_customer_name_string)
        else:
            query = "query?q=SELECT Id,NDPd_SMP_ID__c,strk__Customer__c,strk__ProjectTemplate__c," \
                    "strk__Project_Template__c, strk__Project__c, Secondary_Customer__c " \
                    "FROM strk__Project__c WHERE strk__Project_Template__c IN " + str(
                tuple(strk_project_template_name)) + " AND Secondary_Customer__c IN " + str(tuple(strk_customer_name)) + ""

        api_url = query
        api_url = api_url.replace("&", "%26")
        # token = site_tracker_token_generator(
        #     AUTHENTICATION_URL, CLIENT_ID, CLIENT_SECRET_KEY, SECURITY_TOKEN_KEY, st_username, st_password, ibus_obj, logger)
        # giving the input from json for the site-tracker
        token = site_tracker_token_generator(
            st_authentication_url, st_client_id, st_client_secrete_key,
            st_security_token, st_username, st_password, ibus_obj, logger)
        data = []
        if token:
            # changed the instance and instance-version to be picked from json
            data = get_site_tracker_project_api_call(st_instance, st_instance_version, api_url, token, ibus_obj)
        else:
            ibus_obj.logInfo("Token not created, invalid ST username and password")
            logger.info("Token not created, invalid ST username and password")
        # ibus_obj.logInfo("Printing data received from get_site_tracker_project_api_call ---- {}".format(data))

        missed_ndpd_smp_id = []
        for each in data:
            if smp_st_details:
                for smp_data in smp_st_details:
                    if each[constants.STNdpdSmpId]:
                        if each[constants.STNdpdSmpId] == smp_data[constants.NdpdSmpId]:
                            continue
                        else:
                            missed_ndpd_smp_id.append([
                                each[constants.STNdpdSmpId],
                                each[constants.STProjectId]
                            ])
                            break
            else:
                if each[constants.STNdpdSmpId]:
                    missed_ndpd_smp_id.append([
                        each[constants.STNdpdSmpId],
                        each[constants.STProjectId]
                    ])
        # ibus_obj.logInfo("Printing missed-ndpd-smp-id ---- {}".format(missed_ndpd_smp_id))
        if missed_ndpd_smp_id:
            xml_data = forming_xml(missed_ndpd_smp_id, delete_list=None, ibus_obj=ibus_obj)
            delete_data = None
            ibus_obj.logInfo("Inserting Data into table...")
            logger.info("Inserting Data into table...")
            data_insert_delete(xml_data, delete_data, db_name, username, password, ibus_obj, logger)
            ibus_obj.logInfo("Data inserted into table...")
            logger.info("Data inserted into table...")
        else:
            ibus_obj.logInfo("No data to insert...")
            logger.info("No data to insert...")
    else:
        ibus_obj.logInfo("No data to insert...")
        logger.info("No data to insert...")


def db_connection(db_name, username, password, ibus_obj, logger):
    try:
        server = NDPD_DB_SERVER
        logger.info("Connecting to DataBase...")
        ibus_obj.logInfo("Connecting to DataBase...")
        cncx = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; \
                                      SERVER=' + server + ';DATABASE=' + db_name + ';UID=' + username + ';PWD=' + password)
        logger.info("Connection to DataBase Successful, creating cursor object...")
        ibus_obj.logInfo("Connection to DataBase Successful, creating cursor object...")
        return cncx
    except Exception as error:
        logger.error(error)
        raise Exception(error)


def get_st_template_details_from_db(db_name, username, password, ibus_obj, logger):
    cncx = ''
    cursor = ''
    try:
        cncx = db_connection(db_name, username, password, ibus_obj, logger)
        cursor = cncx.cursor()
        logger.info("Calling db query in get-st-template-details-from-db function...")
        ibus_obj.logInfo("Calling db query in get-st-template-details-from-db function...")
        cursor.execute(constants.db_query_to_fetch_template_details)
        logger.info("DB Query called successful, Creating Json Dictionary...")
        ibus_obj.logInfo("DB Query called successful, Creating Json Dictionary...")
        columns = [column[0] for column in cursor.description]
        converted_columns = []
        for string in columns:
            new_string = string.replace("sf_", "ndpd-")
            replaced_string = new_string.replace("_", "-")
            converted_columns.append(replaced_string)
        data = cursor.fetchall()
        final_dict = {"mappings": None}
        result = []
        for row in data:
            result.append(dict(zip(converted_columns, row)))
        final_dict["mappings"] = result
        return final_dict
    except Exception as error:
        if cncx:
            cncx.rollback()
            logger.error(error)
        raise Exception(error)
    finally:
        if cursor:
            del cursor
            logger.info(
                "Json Dictionary creation successful, Closing cursor...")
        if cncx:
            logger.info("Closing DataBase connection...")
            cncx.close()


def get_smp_st_project_details_db(
        db_name, username, password, ibus_obj, logger, instance, ndpd_project_ref_id):
    cncx = ''
    cursor = ''
    try:
        cncx = db_connection(db_name, username, password, ibus_obj, logger)
        cursor = cncx.cursor()
        logger.info("Calling query in get_smp_st_project_details_db...")
        ibus_obj.logInfo("Calling query in get_smp_st_project_details_db...")
        if instance == "Training":
            cursor.execute(f"SELECT * FROM ndpd_smp_st_project_mapping_training WHERE ndpd_project_ref_id = '{ndpd_project_ref_id}';")
        else:
            cursor.execute(f"SELECT * FROM ndpd_smp_st_project_mapping_nam WHERE ndpd_project_ref_id = '{ndpd_project_ref_id}';")
        logger.info("DB Query called successful, Creating Json Dictionary...")
        ibus_obj.logInfo("DB Query called successful, Creating Json Dictionary...")
        columns = [column[0] for column in cursor.description]
        data = cursor.fetchall()
        result = []
        for row in data:
            result.append(dict(zip(columns, row)))
        return result
    except Exception as error:
        if cncx:
            cncx.rollback()
            logger.error(error)
        raise Exception(error)
    finally:
        if cursor:
            # cursor.close()
            del cursor
            logger.info(
                "Json Dictionary creation successful, Closing cursor...")
        if cncx:
            logger.info("Closing DataBase connection...")
            cncx.close()


def get_st_filtered_projects_dict_missing(token, instance, instance_version, customer, smps):
    """
    Input
        :param token:
        :param logger:
        :param instance: Site_tracker instance.
        :param instance_version: Site_tracker instance version.
        :param api_url: Api to be called in Site-tracker.
        :param token_value: Site-tracker token value.
    :return:
        final_dict : updated json from site tracker.
    """
    try:
        st_url = f"query?q=SELECT Id,NDPd_SMP_ID__c FROM strk__Project__c " \
                 f"" \
                 f"WHERE NDPd_SMP_ID__c != '' AND " \
                 f"Secondary_Customer__c = '{customer}'" \
                 f"AND NDPd_SMP_ID__c IN {smps}"
        if len(smps) == 1:
            st_url = f"query?q=SELECT Id,NDPd_SMP_ID__c FROM strk__Project__c " \
                     f"WHERE NDPd_SMP_ID__c != '' AND " \
                     f"Secondary_Customer__c = '{customer}'" \
                     f"AND NDPd_SMP_ID__c = '{smps[0]}'"
        print(st_url)
        url = instance + "/services/data/" + instance_version + st_url
        response_of_api = requests.get(url, headers={
            "Authorization": "Bearer " + token})
        print(response_of_api.json())
        return response_of_api.json()
    except Exception as error:
        raise Exception(error)


def forming_xml(xml_list, delete_list, ibus_obj):
    try:
        ibus_obj.logInfo("Executing forming_xml function")
        if delete_list:
            ibus_obj.logInfo("Forming the xml for the delete operation")
            top = Element('NdpSmpStProjectList')
            comment = Comment('SMP/ Project data to be deleted NDP-d')
            top.append(comment)
            # delete_list Ex: ['a0A22000000BGFCEA4','a0A22000000BGFHEA4']
            for each_row in xml_list:
                child1 = SubElement(top, 'NdpSmpStProject')
                child = SubElement(child1, 'ST-Project-Id')
                child.text = each_row
            xml_str = ElementTree.tostring(top).decode()
            input_xml = '{}{}{}'.format('"""', xml_str, '"""')
        else:
            ibus_obj.logInfo("Forming the xml for insert operation")
            top = Element('NdpSmpStProjectList')
            comment = Comment('SMP/ Project data for Site-Tracker and NDP-d')
            top.append(comment)
            # input_list ex: [('12345', 'a0A22000000BGFCEA4'),
            #               ('12345', 'a0A22000000BGFHEA4')]
            for each_row in xml_list:
                child1 = SubElement(top, 'NdpSmpStProject')
                child = SubElement(child1, 'SMP-Id')
                child.text = each_row[0]
                child2 = SubElement(child1, 'ST-Project_Id')
                child2.text = each_row[1]
            xml_str = ElementTree.tostring(top).decode()
            input_xml = '{}{}{}'.format('"""', xml_str, '"""')
        # ibus_obj.logInfo("Printing xml --- {}".format(input_xml))
        return input_xml
    except Exception as e:
        ibus_obj.logInfo("Exception occurred in forming_xml function")
        ibus_obj.logInfo(e)
        return None


def data_insert_delete(xml, delete_data, db_name, username, password, ibus_obj, logger):
    """
    This function will call SP to insert the XML into table
    :param xml: xml for the smp-id and st-project-id
    EX: <ArrayOfGetSiteTrackerProjectDBContext><!--SMP(Project) data
                from site-tracker.-->
            <GetSiteTrackerProjectDBContext>
                    <SMP-Id>12345</SMP-Id>
                    <ST-Project_Id>a0A22000000BGFCEA4</ST-Project_Id>
            </GetSiteTrackerProjectDBContext>
            <GetSiteTrackerProjectDBContext>
                    <SMP-Id>12345</SMP-Id>
                    <ST-Project_Id>a0A22000000BGFHEA4</ST-Project_Id>
            </GetSiteTrackerProjectDBContext>
        </ArrayOfGetSiteTrackerProjectDBContext>
    :return:
    """
    cur, con = None, None
    try:
        con = db_connection(db_name, username, password, ibus_obj, logger)
        if delete_data:
            cur = con.cursor()
            cur.execute(
                "{CALL DeleteSTProjectIdForProjectInitialization ('%s')}" %
                xml)
            cur.commit()
        else:
            cur = con.cursor()
            cur.execute("{CALL InsertSTProjectIdForProjectInitialization "
                        "('%s')}" % xml)
            cur.commit()
        return 1
    except Exception as e:
        ibus_obj.logInfo("error occured: %s" % e)
        logger.error("Error occured while inserting data in DB: %s" % e)
        return None
    finally:
        if cur:
            cur.close()
        if con:
            con.close()


def get_site_tracker_project_api_call(instance,
                          instance_version,
                          api_url, token_value, ibus_obj):
    st_instance = instance
    st_instance_version = instance_version
    st_url = api_url
    st_token = token_value
    url = st_instance + "/services/data/" + st_instance_version + st_url
    print("33333333333")
    print(url)
    ibus_obj.logInfo("Api url ---- {}".format(url))
    response_of_api = requests.get(url, headers={
        "Authorization": "Bearer " + st_token})
    ibus_obj.logInfo("Api response received successfully")
    final_dict = []
    try:
        # print(response_of_api.json())
        print(response_of_api)
        if response_of_api.json()['totalSize'] != 0:
            for each in response_of_api.json()['records']:
                final_dict.append({
                    "st-projectId": each["Id"],
                    "st-projectName": each["strk__Project__c"],
                    "st-ndpdSmpId": each["NDPd_SMP_ID__c"],
                    "st-customerName": each["Secondary_Customer__c"],
                    "st-project-template-id": each["strk__ProjectTemplate__c"],
                    "st-project-template-name": each["strk__Project_Template__c"]
                })

            if 'nextRecordsUrl' in response_of_api.json().keys():
                api_url = response_of_api.json()['nextRecordsUrl']
                next_url = '/query/' + api_url.split('/')[-1]
                get_site_tracker_project_api_call(
                    instance=st_instance,
                    instance_version=st_instance_version, api_url=next_url,
                    token_value=st_token, ibus_obj=ibus_obj)
        print(response_of_api)
        return final_dict
    except Exception as error:
        import traceback
        print(traceback.print_exc())
        print("22222222222222222222222222222222")
        print(error)
        return {'message': "No records found for the customer"}


# ---- Written by Sheshagiri -------
def task_actualization(customer, ndpd, field, target_value, ibus_obj, logger, **kwargs):
    """
    The main purpose of this function is get the action details from
    rest/SFCommonAPI/getTaskDetailsV2 and update task with the api
    rest/SFCommonAPI/executeTaskV2, after updating add the
    success or failure or warning message
    :param
    customer: contains customer name like verizon, etc
    ndpd: contains ndpd details like smp id, task name, project id etc.
    field: contains field values like actual end date
    target_value: contains old data like old date, old value, etc
    target_value: contains target value like
    kwargs: "sfInstanceName": "Training", session: session object
    :return: success of failure or warning message
    EX - "actions": [
        {
            "view": "VZ Accept Reject",
            "name": "NTP Received",
            "isActive": "false"
        }
    ]
    """
    ibus_obj.logInfo("Entered into task execution")
    success_data, failure_data, warning_data = [], [], []
    action_name = ""
    action_value = ""
    db_name = kwargs.get('db_name')
    username = kwargs.get('username')
    password = kwargs.get('password')
    st_project_id = kwargs.get('st_project_id')
    st_project_template_id = kwargs.get('st_project_template_id')
    st_milestoneName = kwargs.get('st_milestoneName')
    ndpd_customer = kwargs.get('ndpd_customer_id')
    ndpd_smp_name = kwargs.get('ndpd_smp_name')
    ndpd_module_name = kwargs.get('ndpd_module_name')

    ibus_obj.logInfo("Calling DataBase query to get Task Action Information...")
    logger.info("Calling DataBase query to get Task Action Information...")
    db_result = get_task_info_from_db(db_name, username, password, ibus_obj, logger, ndpd,
                                      st_project_id, st_project_template_id, st_milestoneName,
                                      ndpd_customer, ndpd_smp_name, ndpd_module_name)
    ibus_obj.logInfo("Called DataBase query to get Task Action Information...")
    logger.info("Called DataBase query to get Task Action Information...")

    ibus_obj.logInfo("Printing the db result -- {}...".format(db_result))
    logger.info("Printing the db result -- {}...".format(db_result))

    for data in db_result:
        action_name = data.get("ActionName")
        action_value = data.get("ActionView")
    ibus_obj.logInfo("Printing action name, value --- {},{}".format(action_name,action_value))

    ibus_obj.logInfo("Creating the session Obj...")
    session = kwargs.get('session')
    # GET_TAKS_DETAILS_API_URL = kwargs.get('get_task_details')
    EXECUTE_TASK_API_URL = kwargs.get('execute_task')
    SF_INSTANCE_NAME = kwargs.get('SF_INSTANCE_NAME')
    post_params = {
        "sfInstanceName": SF_INSTANCE_NAME,
        "projectId": ndpd['ndpd-projectId'],
        "smpId": ndpd['ndpd-smpId'],
        "moduleId": ndpd['ndpd-moduleId'],
        "taskName": ndpd['ndpd-taskName'],
        "actionName": action_name,
        "value": action_value
    }
    ibus_obj.logInfo("post parameters to API {}".format(post_params))
    ibus_obj.logInfo("url ---{}".format(EXECUTE_TASK_API_URL))
    response = session.post(
        url=EXECUTE_TASK_API_URL, data=json.dumps(post_params), verify=False)
    ibus_obj.logInfo("response from API {}".format(response))
    logger.info("Response from API {}".format(
        json.loads(response.text)))
    if response.status_code == 401 or response.status_code == 500:
        row_data = [customer, ndpd['ndpd-projectId'],
                    ndpd['ndpd-smpId'], ndpd['ndpd-moduleId'], ndpd['ndpd-taskName'],
                    field, st_project_id,
                    st_project_template_id,
                    st_milestoneName,
                    action_name,
                    action_value,
                    "",
                    response.status_code,
                    response.reason]
        failure_data.append(row_data)
        ibus_obj.logInfo("Got Failure list from Execute task API {}".format(row_data))
        return success_data, failure_data, warning_data
    if "errorMessage" in json.loads(response.text):
        row_data = [customer, ndpd['ndpd-projectId'],
                    ndpd['ndpd-smpId'], ndpd['ndpd-moduleId'], ndpd['ndpd-taskName'],
                    field, st_project_id,
                    st_project_template_id,
                    st_milestoneName,
                    action_name,
                    action_value,
                    "",
                    json.loads(response.text)['errorCode'],
                    json.loads(response.text)['errorMessage']]
        failure_data.append(row_data)
        ibus_obj.logInfo("Got Error list from Execute task Api {}".format(row_data))
        return success_data, failure_data, warning_data
    if "status" in json.loads(response.text) and json.loads(
            response.text)['status'] in ["success", "Success"]:
        row_data = [customer, ndpd['ndpd-projectId'],
                    ndpd['ndpd-smpId'], ndpd['ndpd-moduleId'], ndpd['ndpd-taskName'],
                    field, st_project_id,
                    st_project_template_id,
                    st_milestoneName,
                    action_name,
                    action_value,
                    "",
                    target_value]
        success_data.append(row_data)
        ibus_obj.logInfo("Got Success list from Execute task API {}".format(row_data))
        return success_data, failure_data, warning_data
    # ---- Commented the code based on changes made on 02/08/2020
    # post_params = {
    #     "sfInstanceName": SF_INSTANCE_NAME,
    #     "projectId": ndpd['ndpd-projectId'],
    #     "smpId": ndpd['ndpd-smpId'],
    #     "moduleId": ndpd['ndpd-moduleId'],
    #     "taskName": ndpd['ndpd-taskName'],
    # }
    # ibus_obj.logInfo("post parameters to API {}".format(post_params))
    # ibus_obj.logInfo("url --- {}".format(GET_TAKS_DETAILS_API_URL))
    # response = session.post(
    #     url=GET_TAKS_DETAILS_API_URL, data=json.dumps(post_params), verify=False)
    # ibus_obj.logInfo("response from API {}".format(response))
    # logger.info("Response from API {}".format(
    #     json.loads(response.text)))
    # if response.status_code == 401 or response.status_code == 500:
    #     row_data = [customer, ndpd['ndpd-projectId'],
    #                 ndpd['ndpd-smpId'], ndpd['ndpd-taskName'],
    #                 field, st_project_id,
    #                 st_project_template_id,
    #                 st_milestoneName,
    #                 "",
    #                 target_value,
    #                 response.status_code,
    #                 response.reason]
    #     failure_data.append(row_data)
    #     ibus_obj.logInfo("Got Failure list from GET_TAKS_DETAILS_API_URL {}".format(row_data))
    #     return success_data, failure_data, warning_data
    # if "errorMessage" in json.loads(response.text):
    #     row_data = [customer, ndpd['ndpd-projectId'],
    #                 ndpd['ndpd-smpId'], ndpd['ndpd-taskName'],
    #                 field, st_project_id,
    #                 st_project_template_id,
    #                 st_milestoneName,
    #                 "",
    #                 target_value,
    #                 json.loads(response.text)['errorCode'],
    #                 json.loads(response.text)['errorMessage']]
    #     failure_data.append(row_data)
    #     ibus_obj.logInfo("Got Error list from GET_TAKS_DETAILS_API_URL {}".format(row_data))
    #     return success_data, failure_data, warning_data
    # if ("status" not in json.loads(response.text)) and ("actions" in response.json()):
    #     ibus_obj.logInfo("Got success from GET TASK API")
    #     action = response.json()['actions']
    #     ibus_obj.logInfo("Printing action from GET TASK API -- {}".format(action))
    #     logger.info("Printing action data from GET TASK API --- {}".format(action))
    #     action_name = None
    #     value = None
    #     for action_data in action:
    #         action_name = action_data["name"]
    #         value = action_data["isActive"]


def get_task_info_from_db(db_name, username, password, ibus_obj, logger, ndpd,
                          st_project_id, st_project_template_id, st_milestoneName,
                          ndpd_customer, ndpd_smp_name, ndpd_module_name):
    cncx = ''
    cursor = ''
    try:
        cncx = db_connection(db_name, username, password, ibus_obj, logger)
        cursor = cncx.cursor()
        sql_query = "SELECT ActionName, ActionView FROM dbo.NDPdTaskActualization where ndpd_customerId='%s' AND ndpd_projectId='%s' AND ndpd_smpTemplateName='%s' AND ndpd_moduleName='%s' AND ndpd_TaskName='%s' AND STProjectTemplateId='%s' AND STMilestoneName='%s' AND Is_Active=1 AND IsDeleted=0;" % (ndpd_customer, ndpd["ndpd-projectId"], ndpd_smp_name, ndpd_module_name, ndpd['ndpd-taskName'], st_project_template_id, st_milestoneName)
        logger.info("Calling query in get_task_info_from_db...")
        ibus_obj.logInfo("Calling query in get_task_info_from_db...")
        ibus_obj.logInfo("Printing DB query ---{}".format(sql_query))
        cursor.execute(sql_query)
        logger.info("DB Query called successful, Creating Json Dictionary...")
        ibus_obj.logInfo("DB Query called successful, Creating Json Dictionary...")
        columns = [column[0] for column in cursor.description]
        data = cursor.fetchall()
        result = []
        for row in data:
            result.append(dict(zip(columns, row)))
        ibus_obj.logInfo("Printing the DB result -- {}".format(result))
        return result
    except Exception as error:
        if cncx:
            cncx.rollback()
            logger.error(error)
        raise Exception(error)
    finally:
        if cursor:
            # cursor.close()
            del cursor
            logger.info(
                "Json Dictionary creation successful, Closing cursor...")
        if cncx:
            logger.info("Closing DataBase connection...")
            cncx.close()


def cleanup(*files, **iBus_Obj):
    iBusObj = iBus_Obj['iBus_Obj']
    for file in files:
        if file:
            try:
                os.remove(file)
                iBusObj.logInfo("{} successfully deleted ".format(file))
            except Exception as e:
                iBusObj.logWarning("{} not found for cleanup".format(file))


def get_field_list(ndpd_field_list, st_field_list):
    """
    :param ndpd_field_list:list of ndpd fields from mapping info
    :param st_field_list: list of st fields from mapping info
    :return: final_list: final list of fields from ndpd & st
    """
    ndpd_list = []
    st_list = []
    if type(ndpd_field_list) == list():
        ndpd_list = ndpd_field_list
    else:
        ndpd_list.append(ndpd_field_list)
    if type(st_field_list) == list():
        st_list = st_field_list
    else:
        st_list.append(st_field_list)
    ndpd_list = [constants.field_dict[field] for field in ndpd_list
                 if field in constants.field_dict]
    st_list = [constants.field_dict[field] for field in st_list
               if field in constants.field_dict]
    final_list = list(set(ndpd_list + st_list))
    return final_list


def get_ndpd(map_data, ndpd_data):
    """
    :param map_data: mapping json
    :param ndpd_data: ndpd values
    :return: return empty dict if not matches, or return first dict in
    ndpd list
    """
    ndpd_list = []
    for ndpd in ndpd_data:
        if ndpd['ndpd-projectId'] == map_data['ndpd-projectId'] and \
                ndpd['ndpd-smpId'] == map_data['ndpd-smpId'] and \
                ndpd['ndpd-moduleId'] == map_data['ndpd-moduleId'] and \
                str(ndpd['ndpd-taskName']).lower() == str(map_data['ndpd-taskName']).lower():
            ndpd_list.append(ndpd)
    if ndpd_list:
        return ndpd_list[0]
    return {}


def get_site_tracker(map_data, st_data):
    """
    :param map_data: Mapping Json
    :param st_data: Site tracker details
    :return: return first elements of st if matches else empty dict
    """
    st_list = []
    for st in st_data:
        if st['st-projectId'] == map_data['st-projectId'] and \
                st['st-milestoneName'] == map_data['st-milestoneName']:
            st_list.append(st)
    if st_list:
        return st_list[0]
    return {}
