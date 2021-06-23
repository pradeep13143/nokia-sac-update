import json
import multiprocessing
import sys
import shutil
from functools import partial
from flask import request, Response
from async_execution import async_task

from compare import get_ndpd_st_data, get_ndpd_updated_data, get_st_updated_data
from update_nd_st import update_ndpd_side, update_site_tracker_side
from wsgi import application
from IBusPlatformInterface import IBusPlatformInterface
from config import VERSION1
import utility
from datetime import datetime
from utility import (get_mapped_task_milestone_data_from_db,
                     get_time_execution,
                     site_tracker_token_generator,
                     update_siteTracker_fields,
                     write_to_excel
                     )
from log_config import get_logger
from zipfile import ZipFile


log_file_name1 = "Task-Milestone-Alignment.log"
logger, log_file_name2 = get_logger(log_file_name1)


@application.route('/get-mapped-task-milestone', methods=['POST'])
@async_task
def get_mapped_task_milestone():
    """
    :param DB-Name: Name of the NDPD database
    :param Username: user name
    :param Password: user password
    :return: Mapping-Json: returns the mapping info of ndpd & st task/milestone
    1. Get SMP-IDs from DB, based on the following filters
        - create date => cut off date
        - modified date >= current date - num of days in the DB table
        - filter smpid based on instance from table
                ndpd_smp_st_project_mapping_training or
                ndpd_smp_st_project_mapping_nam
    2. Get project ids from ST, based on the following filters
        - create date => cut off date
        - modified date >= current date - num of days in the DB table
        - NDPD-SMP-ID != null
    3. Merge the above two lists
    4 . Get mapping for above merged list by calling SPs based on Instance
    """
    start_time = datetime.now()
    json_data = request.json
    ibus_obj = IBusPlatformInterface(VERSION1, True, json_data)
    ibus_obj.logInfo(
        f"Execution started HOUR : {datetime.now().hour} MINS: {datetime.now().minute}")
    mapping_data_file = ''
    try:
        ibus_obj.logInfo("Reading Json Data...")
        db_name = json_data.get("DB-Name")
        user_name = json_data.get("Username")
        password = json_data.get("Password")
        st_username = json_data.get("St-Username")
        st_password = json_data.get("St-Password")

        # getting the site-tracker details from input
        st_instance = json_data['St-Instance']
        st_instance_version = json_data['St-Instance-Version']
        st_authentication_url = json_data['St-Authentication-URL']
        st_client_id = json_data['St-Client-Id']
        st_client_secrete_key = json_data['St-Client-Secret-Key']
        st_security_token = json_data['St-Security-Token']
        Ndpd_URL = json_data["Ndpd-URL"]
        Ndpd_USERNAME = json_data["Ndpd-USERNAME"]
        Ndpd_PASSWORD = json_data["Ndpd-PASSWORD"]
        NDPD_SF_INSTANCE = json_data['Ndpd-INSTANCE']
        customer = json_data['Customer']
        get_smp_details_endpoint = json_data['GET_SMP_DETAILS_ENDPOINT']
        get_module_details_endpoint = json_data['GET_MODULE_DETAILS_ENDPOINT']
        include_all = json_data['Include-all-mapping']
        include_modified_time = json_data['Include-modified-time']
        ibus_obj.logInfo(f"include all-----------{include_all}")
        ibus_obj.logInfo(
            f"include modified time-----------{include_modified_time}")

        # ibus_obj.logInfo("Calling getMissingStProjectId function...")
        # logger.info("Calling getMissingStProjectId function...")
        # passing all the inputs for the ST from json
        # commented based on request from SC
        # getMissingStProjectId(NDPD_SF_INSTANCE,db_name,
        #                       user_name, password, st_username,
        #                       st_password, ibus_obj, logger, st_instance,
        #                       st_instance_version, st_authentication_url,
        #                       st_client_id, st_client_secrete_key,
        #                       st_security_token,
        #                       ndpd_project_ref_id)
        # ibus_obj.logInfo(
        #     "function getMissingStProjectId called successful...")
        # logger.info("function getMissingStProjectId called successful...")

        # creating token to call site tracker rest api
        token = site_tracker_token_generator(
            st_authentication_url, st_client_id, st_client_secrete_key,
            st_security_token, st_username,
            st_password, ibus_obj, logger)

        if token:
            '''
            Step 1. Get SMP-IDs from DB, based on the following filters
                - create date => cut off date
                - modified date >= current date - num of days in the DB table
                - filter smpid based on instance from table 
                ndpd_smp_st_project_mapping_training or 
                ndpd_smp_st_project_mapping_nam
            '''
            smp_data, ndpd_project_ref_id, st_project, created_date, \
            modified_date = \
                utility.get_smp_filtered_data(customer,
                                              user_name,
                                              password,
                                              db_name,
                                              logger,
                                              ibus_obj,
                                              NDPD_SF_INSTANCE,
                                              include_modified_time)
            ibus_obj.logInfo(f"SMP data --------------- {len(smp_data)}")

            # modifying date format according to Sitetracker
            if created_date:
                st_created_date = datetime.strptime(
                    created_date, '%Y-%m-%d').strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            else:
                st_created_date = ''
            if modified_date:
                st_modified_date = datetime.strptime(
                    modified_date, '%Y-%m-%d %H:%M:%S.%f').strftime(
                    '%Y-%m-%dT%H:%M:%S.%fZ')
            else:
                st_modified_date = ''

            # making dict looks like {'SMPID':'ST_Projectid'}
            # ndpd_smp_dict = {smp[0]: smp[1] for smp in smp_data}
            # making dict looks like {'SMPID':'ST_Projectid'}
            ndpd_smp_dict = {smp[0]: smp[1] for smp in smp_data if smp[1]}
            ibus_obj.logInfo(f"NDPD SMPID which are having "
                             f"project id -------------- {ndpd_smp_dict}")
            missing_st_project_smps = [str(smp[0]) for smp in smp_data if
                                       not smp[1]]
            ibus_obj.logInfo(f"NDPD SMPID which are not having"
                             f" project id -------------- "
                             f"{missing_st_project_smps}")
            # new changes
            # ['smpdi1', 'smpid2', 'smpid3']
            missing_smp_project_dict = {}
            if missing_st_project_smps:
                if len(missing_st_project_smps) >= 100:
                    n = 100
                    smps = [tuple(missing_st_project_smps[i:i + n]) for i in
                            range(0, len(missing_st_project_smps), n)]
                else:
                    smps = [tuple(missing_st_project_smps)]
                pool = multiprocessing.Pool()
                func = partial(utility.get_st_filtered_projects_dict_missing,
                               token, st_instance,
                               st_instance_version, st_project)
                map_object = pool.map_async(func, smps)
                result = map_object.get()
                for data in result:
                    if data['totalSize'] != 0:
                        for each in data['records']:
                            missing_smp_project_dict[each["NDPd_SMP_ID__c"]] = \
                            each["Id"]
                ibus_obj.logInfo("Pool is closing")
                pool.close()
                ibus_obj.logInfo("Pool is joining")
                pool.join()

            ibus_obj.logInfo("from site tracker smpids for manual created smps")
            # ibus_obj.logInfo(f"{missing_smp_project_dict}")
            # end
            # ST Query to get ST Project id and NDPD SMP ids based on
            # filtered data(st_created_date, st_modified_date,st_project
            '''
            Step 2. Get project ids from ST, based on the following filters
                - create date => cut off date
                - modified date >= current date - num of days in the DB table
                - NDPD-SMP-ID != null
            '''
            if st_created_date and st_modified_date:
                st_filter_query = f"query?q=SELECT NDPd_SMP_ID__c,Id,Secondary_Customer__c " \
                                  f"FROM strk__Project__c WHERE NDPd_SMP_ID__c != " \
                                  f"'' AND CreatedDate >= {st_created_date} AND LastModifiedDate " \
                                  f">= {st_modified_date}  AND Secondary_Customer__c = '{st_project}' "
            elif st_created_date and not st_modified_date:
                st_filter_query = f"query?q=SELECT NDPd_SMP_ID__c,Id," \
                                  f"Secondary_Customer__c " \
                                  f"FROM strk__Project__c WHERE " \
                                  f"NDPd_SMP_ID__c != " \
                                  f"'' AND CreatedDate >= {st_created_date} " \
                                  f"AND Secondary_Customer__c = '{st_project}' "
            elif not st_created_date and st_modified_date:
                st_filter_query = f"query?q=SELECT NDPd_SMP_ID__c,Id," \
                                  f"Secondary_Customer__c " \
                                  f"FROM strk__Project__c WHERE " \
                                  f"NDPd_SMP_ID__c != " \
                                  f"'' AND LastModifiedDate >= {st_modified_date} " \
                                  f"AND Secondary_Customer__c = '{st_project}' "
            else:
                st_filter_query = f"query?q=SELECT NDPd_SMP_ID__c,Id," \
                                  f"Secondary_Customer__c " \
                                  f"FROM strk__Project__c WHERE " \
                                  f"NDPd_SMP_ID__c != ''" \
                                  f"AND Secondary_Customer__c = '{st_project}' "
            # calling above query and getting results
            final_st_smp_dict = {}
            st_smp_dict = utility.get_st_filtered_projects_dict(
                token, st_instance, st_instance_version, st_filter_query,
                logger, ibus_obj, final_st_smp_dict)
            # ibus_obj.logInfo(f"ST-SMP-dict ------------{st_smp_dict}")

            '''
            step 3 : Merge the above two lists
            '''
            # Merging both ndpd smp dict and sitetracker dict
            filtered_dict = {**ndpd_smp_dict, **st_smp_dict}
            if missing_smp_project_dict:
                filtered_dict = {**filtered_dict, **missing_smp_project_dict}
            # ibus_obj.logInfo(f"filtered-dict ------------{filtered_dict}")
            ibus_obj.logInfo("Filtered from ndpd an sitetracker smpids")

            if include_all == "true":
                # preparing input for SP ex : 'SMPID1,SMPID2'
                smp_ids_string = ",".join(filtered_dict.keys())
            else:
                '''
                Step 4 : Only keep the SMP-IDs, project IDs 
                that is there in the 
                'ndpd_smp_st_project_mapping' mapping         
                '''
                ibus_obj.logInfo("filtering smps only which "
                                 "are exists in the UC2 table")
                existing_smp_list_from_db = []
                smps_in_nam_training_table = \
                    utility.get_smp_st_project_details_db(
                    db_name, user_name, password, ibus_obj, logger,
                    NDPD_SF_INSTANCE, ndpd_project_ref_id)
                for smp in smps_in_nam_training_table:
                    for filtered_smp in filtered_dict.keys():
                        if smp['ndpd_smp_id'] == filtered_smp:
                            existing_smp_list_from_db.append(filtered_smp)
                smp_ids_string = ",".join(existing_smp_list_from_db)
            ibus_obj.logInfo(
                "Calling get_mapped_task_milestone_data function...")
            logger.info("Calling get_mapped_task_milestone_data function...")

            # getting mapping information for filtered smpid
            final_dict = get_mapped_task_milestone_data_from_db(
                db_name, user_name, password, ibus_obj, logger,
                NDPD_SF_INSTANCE, str(smp_ids_string))
            # ibus_obj.logInfo(f"smp ids string ================={str(smp_ids_string)}")
            kwargs = {"url": Ndpd_URL, "ndpd-username": Ndpd_USERNAME,
                      "ndpd-password": Ndpd_PASSWORD,
                      "instance-name": NDPD_SF_INSTANCE,
                      "get_smp_details_endpoint": get_smp_details_endpoint,
                      "get_module_details_endpoint": get_module_details_endpoint}
            ibus_obj.logInfo(
                f"len of data {len(final_dict['mappings'])}")

            # adding st project ids to mapping
            for data in final_dict['mappings']:
                data['st-projectId'] = filtered_dict.get(data['ndpd-smpId'], '')

            # Updating module ids of playground instance
            if NDPD_SF_INSTANCE == "Training":
                mapping_list = utility.get_updated_mapping_data(
                    final_dict['mappings'],
                    db_name,
                    user_name,
                    password,
                    ibus_obj,
                    logger, **kwargs)
                ibus_obj.logInfo(f"len of updated data {len(mapping_list)}")
            else:
                mapping_list = final_dict['mappings']

            ibus_obj.logInfo("function called successful")
            logger.info("function called successful")
            final_list = []
            ibus_obj.logInfo("Adding to st_projectId to mappings")
            smp_result_list = []
            for data in mapping_list:
                smp_result_list.append(data['ndpd-smpId'])
                if not data['st-projectId'] or data['st-projectId'] == "null":
                    pass
                else:
                    final_list.append(data)
            smp_result_list = list(set(smp_result_list))
            input_smps = smp_ids_string.split(',')
            not_found_smp_in_mapping = [item for item in input_smps if item not
                                        in smp_result_list]
            ibus_obj.logInfo("*********************************")
            warning_data = []
            if not_found_smp_in_mapping and not not_found_smp_in_mapping[0] == '':
                not_found_smp_in_mapping_string = ",".join(not_found_smp_in_mapping)
                cloned_db_final_smps_dict = utility.get_smp_from_cloned_db(
                    db_name, user_name, password, ibus_obj, logger,
                    NDPD_SF_INSTANCE, str(not_found_smp_in_mapping_string))
                cloned_smps = []
                ibus_obj.logInfo(f"not_found_smp_in_mapping_string =========== {not_found_smp_in_mapping_string}")
                for smp in cloned_db_final_smps_dict['SMPS']:
                    cloned_smps.append(smp['smpId'])
                ibus_obj.logInfo(f"cloned db smps =========== {cloned_smps}")
                warning_data = [[customer,
                                 ndpd_project_ref_id,
                                 '',
                                 item,
                                 '',
                                 '',
                                 '',
                                 filtered_dict.get(item, ''),
                                 '',
                                 '',
                                 '',
                                 '',
                                 '',
                                 '',
                                 '',
                                 '',
                                 f"{str(item)} not found in cloned database"]
                                for item in not_found_smp_in_mapping
                                if item not in cloned_smps]
                ibus_obj.logInfo(
                    f"not_found_smp_in_mappin"
                    f"g =========== {not_found_smp_in_mapping}")
            ibus_obj.logInfo(f"len of final updated data {len(final_list)}")
            final_data_dict = {'mappings': final_list,
                               'invalidsmps': warning_data}
            mapping_data_file = "Mapping-Json_{}.json".format(
                datetime.now().strftime("%d%m%Y%H%M%S%f"))
            with open(mapping_data_file, 'w') as f:
                f.write(str(json.dumps(final_data_dict)))
            ibus_obj.uploadOutputParameterFile('Mapping-Json',
                                               mapping_data_file)
            end_time = datetime.now()
            ibus_obj.logInfo("Calling get_time_execution function...")
            logger.info("Calling get_time_execution function...")
            time_taken = get_time_execution(start_time, end_time)
            ibus_obj.logInfo("function called successful")
            logger.info("function called successful")
            return Response(response=json.dumps(
                {"Mapping-Json": mapping_data_file,
                 "Start-Time": start_time.strftime("%H:%M:%S:%f")[:-3],
                 "End-Time": end_time.strftime("%H:%M:%S:%f")[:-3],
                 "Time-Taken": time_taken.strftime("%H:%M:%S:%f")[:-3],
                 "Operation_Status": "Completed"}),
                status=200, mimetype='application/json')
        else:
            return Response(response=json.dumps(
                {"message": "Invalid token, "
                            "please check username and password"}),
                status=400, mimetype='application/json')
    except Exception as error:
        import traceback
        print(traceback.print_exc())
        logger.error("Exception : {}".format(error))
        ibus_obj.logError("Exception : {}".format(error))
        # ibus_obj.logError("Exception eval : {}".format(eval(error)))
        # ibus_obj.logError("Exception eval type : {}".format(type(eval(error))))
        ibus_obj.logError("Exception dict : {}".format(error))
        if str(error) == "'access_token'":
            error = "Invalid ST Username / Password"
        if "Login failed for user" in str(error):
            error = "Invalid username / password"
        exc_type, exc_obj, exc_tb = sys.exc_info()
        ibus_obj.logError("Exception Type : {} at line {}".format(
            exc_type, exc_tb.tb_lineno))
        return Response(response=json.dumps(
            {"message": "Error in get-mapped-task-milestone --> {0}".format(
                error)}),
            status=400, mimetype='application/json')
    finally:
        ibus_obj.uploadLogFile()
        utility.cleanup(mapping_data_file, iBus_Obj=ibus_obj)


@application.route('/get-ndpd-data-for-mapping-fields', methods=['POST'])
@async_task
def get_ndpd_data_for_mapping_fields():
    """
    This function reads the mapping json and returns the ndpd data json file
    based on project_id, smpid and task names
    :return: getting ndpd data for mapping fields from API
    """
    json_data = request.json
    # creating ibus object
    ibus_obj = IBusPlatformInterface(VERSION1, True, json_data)
    ibus_obj.logInfo("Getting ndpd data for mapping started")
    ndpd_data_file = ''
    try:
        mapping_json_file = "Mapping-Json_{}.json".format(
            datetime.now().strftime("%d%m%Y%H%M%S%f"))
        start_time = datetime.now()
        if not (ibus_obj.getInputParameterFile(
                "Mapping-Json", mapping_json_file)):
            return Response(response="Mapping-Json.json File not found",
                            status=404)
        with open(mapping_json_file, 'r') as fp:
            mapping_json_data = json.load(fp)
        mappings = mapping_json_data['mappings']
        ibus_obj.logInfo("Execution started...")
        logger.info("mapping data received: {}".format(mappings))

        # ------- Ndpd Details ---------
        Ndpd_URL = json_data["Ndpd-URL"]
        Ndpd_USERNAME = json_data["Ndpd-USERNAME"]
        Ndpd_PASSWORD = json_data["Ndpd-PASSWORD"]
        NDPD_SF_INSTANCE = json_data['Ndpd-INSTANCE']

        # ------- NDPd APIs ------------
        # NDPd API to get task details
        # api_query_task_details = "/rest/SFCommonAPI/getTaskDetails"
        api_query_task_details = json_data["Ndpd-GET-TASK-DETAILS-ENDPOINT"]

        # getting session object
        #session = utility.get_session(username=Ndpd_USERNAME,
        #                              password=Ndpd_PASSWORD)
        #if session:
        if Ndpd_USERNAME and Ndpd_PASSWORD:
            # if mappings:
            # calling api using session object and mappings data
            # and getting formatted data
            ndpd_data_list = utility.get_mapped_data_list(Ndpd_USERNAME,
                                                          Ndpd_PASSWORD,
                                                          mappings,
                                                          ibus_obj, logger,
                                                          SF_INSTANCE_NAME=NDPD_SF_INSTANCE,
                                                          url=Ndpd_URL,
                                                          end_point=api_query_task_details)
            ndpd_data_list = [dict(y) for y in set(tuple(x.items())
                                                   for x in ndpd_data_list if x)]
            ndpd_data = json.dumps({'ndpdData': ndpd_data_list})
            ndpd_data_file = "Ndpd-Data_{}.json".format(
                datetime.now().strftime("%d%m%Y%H%M%S%f"))
            with open(ndpd_data_file, 'w') as f:
                f.write(ndpd_data)
            ibus_obj.uploadOutputParameterFile('Ndpd-Data', ndpd_data_file)
            logger.info("NDPd data list received ...")
            end_time = datetime.now()
            time_taken = utility.get_time_execution(start_time, end_time)
            # preparing final output
            result = {
                'Ndpd-Data': ndpd_data_file,
                "Start-Time": start_time.strftime("%H:%M:%S:%f")[:-3],
                "End-Time": end_time.strftime("%H:%M:%S:%f")[:-3],
                "Time-Taken": time_taken.strftime("%H:%M:%S:%f")[:-3],
                "Operation_Status": "Completed"
            }
            # logger.info("Final NDPd data result : {}".format(result))
            return Response(response=json.dumps(result), status=200,
                            mimetype='application/json')
            # else:
            #     ibus_obj.logError("No Mappings data found")
            #     return Response(response=json.dumps(
            #         {"message": "No Mappings data found"}),
            #         status=400, mimetype='application/json')
        else:
            ibus_obj.logError("Error in Creating session, session not created")
            logger.error("Error in Creating session, session not created")
            return Response(response=json.dumps(
                {"message": "Error in Creating session"}),
                status=400, mimetype='application/json')
    except Exception as e:
        ibus_obj.uploadLogFile()
        ibus_obj.logError("Exception : {}".format(e))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        ibus_obj.logError("Exception Type : {} at line "
                          "{}".format(exc_type, exc_tb.tb_lineno))
        logger.error("Exception Type : {} at line {}".format(
            exc_type, exc_tb.tb_lineno))
        print("Exception : {}".format(e))
        print("Exception Type : {} at line {}".format(exc_type,
                                                      exc_tb.tb_lineno))
        if str(e) == "'access_token'":
            e = "Invalid ST Username / Password"
        if "Login failed for user" in str(e):
            e = "Invalid username / password"
        return Response(response=json.dumps(
            {
                "message": "Error in getting NDPd data for mapping fields --> {}".format(
                    e)}),
            status=400, mimetype='application/json')
    finally:
        ibus_obj.uploadLogFile()
        utility.cleanup(ndpd_data_file, iBus_Obj=ibus_obj)


@application.route('/get-site-tracker-data-for-mapping-fields',
                   methods=['POST'])
def get_site_tracker_data_for_mapping_fields():
    """
    :param: Mapping-Json: json having mapping info of ndpd and st
    :return: ST-data: returns ST-data based on mapping information
    """
    start_time = datetime.now()
    json_data = request.json
    site_tracker_user_name = json_data['ST-Username']
    site_tracker_password = json_data['ST-Password']

    # getting the site-tracker details from input
    st_instance = json_data['St-Instance']
    st_instance_version = json_data['St-Instance-Version']
    st_authentication_url = json_data['St-Authentication-URL']
    st_client_id = json_data['St-Client-Id']
    st_client_secrete_key = json_data['St-Client-Secret-Key']
    st_security_token = json_data['St-Security-Token']

    ibus_obj = IBusPlatformInterface(VERSION1, True, json_data)
    ibus_obj.logInfo("Getting sitetracker data execution started")
    st_data_file = ''
    final_mapped_dict = {"stData": []}
    try:
        ibus_obj.logInfo("Reading Json Data...")
        mapping_json_file = "Mapping-Json_{}.json".format(
            datetime.now().strftime("%d%m%Y%H%M%S%f"))
        if not (ibus_obj.getInputParameterFile(
                "Mapping-Json", mapping_json_file)):
            return Response(response="Mapping-Json.json File not found",
                            status=404)
        with open(mapping_json_file, 'r') as fp:
            mapping_json_data = json.load(fp)
        if not mapping_json_data:
            ibus_obj.logError(
                json_data["digimop-operation-id"] +
                " Error in get-site-tracker-data - " +
                "No data available in Mapping Json")
            logger.error(json_data["digimop-operation-id"] +
                         " Error in get-site-tracker-data - " +
                         "No data available in Mapping Json")
            return Response(response=json.dumps({"message": "No data available "
                                                            "in Mapping Json"}),
                            status=400, mimetype='application/json')
        # mappings is a List of mapping(each mapping is a dict)
        mappings = mapping_json_data.get("mappings", None)
        if not mappings:
            ibus_obj.logError(
                json_data["digimop-operation-id"] +
                " Error in /get-site-tracker-data - " +
                "No records found in mappings")
            logger.error(json_data["digimop-operation-id"] +
                         " Error in /get-site-tracker-data - " +
                         "No records found in mappings")
            final_mapped_dict["stData"].append({})
            st_data_file = "ST-Data_{}.json".format(
                datetime.now().strftime("%d%m%Y%H%M%S%f"))
            with open(st_data_file, 'w') as f:
                f.write(json.dumps(final_mapped_dict))
            ibus_obj.uploadOutputParameterFile('ST-Data', st_data_file)
            end_time = datetime.now()
            ibus_obj.logInfo("Calling get_time_execution function...")
            logger.info("Calling get_time_execution function...")
            time_taken = get_time_execution(start_time, end_time)
            ibus_obj.logInfo("function called successful...")
            logger.info("function called successful...")
            return Response(response=json.dumps(
                {"ST-Data": st_data_file,
                 "Start-Time": start_time.strftime("%H:%M:%S:%f")[:-3],
                 "End-Time": end_time.strftime("%H:%M:%S:%f")[:-3],
                 "Time-Taken": time_taken.strftime("%H:%M:%S:%f")[:-3]}),
                status=200, mimetype='application/json')
        required_fields = ['st-projectId', 'st-milestoneName',
                           'st-project-template-name']
        # Generating Token
        token = site_tracker_token_generator(
            st_authentication_url, st_client_id, st_client_secrete_key,
            st_security_token, site_tracker_user_name,
            site_tracker_password, ibus_obj, logger)
        if token:
            ibus_obj.logInfo("Token - {}".format(token))
            logger.info("Token - {}".format(token))
            # Calling Site Tracker end point
            project_id = []
            milestone_names = []
            project_template_names = []
            for mapped_data in mappings:
                if not mapped_data['st-projectId'] == "null":
                    project_id.append(mapped_data[required_fields[0]])
                    milestone_names.append(mapped_data[required_fields[1]])
                    # project_template_names.append(mapped_data[
                    # required_fields[2]])
            # New changes, for parallel process
            project_id_list = list(set(project_id))
            milestone_names = set(milestone_names)
            if len(milestone_names) == 1:
                milestone_names = f" = '{tuple(milestone_names)[0]}'"
            else:
                milestone_names = " IN " + str(tuple(milestone_names))
            final_dict = []
            ibus_obj.logInfo(f"project id count ----{len(project_id_list)}")
            ibus_obj.logInfo(f"Milestone names -- {milestone_names}")
            if project_id_list:
                if len(project_id_list) >= 100:
                    n = 100
                    project_ids = [tuple(project_id_list[i:i + n]) for i
                                   in
                                   range(0, len(project_id_list), n)]
                else:
                    project_ids = [tuple(project_id_list)]
                pool = multiprocessing.Pool()
                func = partial(utility.site_tracker_api_call_latest,
                               st_instance,
                               st_instance_version,
                               token,
                               milestone_names)
                map_object = pool.map_async(func, project_ids)
                result = map_object.get()
                ibus_obj.logInfo("Pool is closing")
                pool.close()
                ibus_obj.logInfo("Pool is joining")
                pool.join()
                for data in result:
                    if isinstance(data, dict) and data['totalSize'] != 0:
                        for each in data['records']:
                            final_dict.append({
                                "st-projectId": each[
                                    "strk__Project__c"],
                                "st-milestoneName": each["Name"],
                                "st-milestoneId": each["Id"],
                                "actualStartTime": "",
                                "actualEndTime": each[
                                    "strk__ActualDate__c"],
                                "plannedStartTime": each[
                                    "strk__Forecast_Date__c"],
                                "plannedEndTime": "",
                                "currentOwnerName": "",
                                "assigneeUserName": "",
                                "lastModifiedTime": each["LastModifiedDate"].replace("T", " ")[:23],
                                "p-number": each["strk__Project__r"]["Name"]
                            })
            final_mapped_dict["stData"] = final_dict
            # project_id = set(project_id)
            # milestone_names = set(milestone_names)
            # if len(project_id) == 1:
            #     project_id = f" = '{tuple(project_id)[0]}'"
            # else:
            #     project_id = " IN "+str(tuple(project_id))
            # if len(milestone_names) == 1:
            #     milestone_names = f" = '{tuple(milestone_names)[0]}'"
            # else:
            #     milestone_names = " IN "+str(tuple(milestone_names))
            # ST_URL = "query?q=SELECT Id,LastModifiedDate,Name," \
            #          "strk__ActualDate__c,strk__Forecast_Date__c," \
            #          "strk__Project__c FROM strk__Activity__c " \
            #          f"WHERE strk__Project__c {project_id} AND Name " \
            #          f"{milestone_names} AND strk__Activity_Type__c" \
            #          " IN ('Milestone','Approval')"
            # ST_URL = ST_URL.replace("&", "%26")
            # # getting response
            # ibus_obj.logInfo("Calling site_tracker_api_call function...")
            # logger.info("Calling site_tracker_api_call function...")
            # data = site_tracker_api_call(
            #     st_instance, st_instance_version, ST_URL, token, ibus_obj,
            #     logger)
            # ibus_obj.logInfo("function called successful...")
            # logger.info("function called successful...")
            # for each in data:
            #     # for mapped_data in mappings:
            #     #     if not mapped_data['st-projectId'] == "null":
            #     #         if each[required_fields[0]] == mapped_data[required_fields[0]]:
            #     #             each.update({'st-milestoneId': mapped_data['st-milestoneName']})
            #     #         else:
            #     #             continue
            #     # ibus_obj.logInfo("print each...{}".format(each))
            #     final_mapped_dict["stData"].append(each)
            #     # ibus_obj.logInfo("print final dict. inside..{}".format(final_mapped_dict))
            # # ibus_obj.logInfo("print final dict...{}".format(final_mapped_dict))
            st_data_file = "ST-Data_{}.json".format(
                datetime.now().strftime("%d%m%Y%H%M%S%f"))
            with open(st_data_file, 'w') as f:
                f.write(json.dumps(final_mapped_dict))
            ibus_obj.uploadOutputParameterFile('ST-Data', st_data_file)
            end_time = datetime.now()
            ibus_obj.logInfo("Calling get_time_execution function...")
            logger.info("Calling get_time_execution function...")
            time_taken = get_time_execution(start_time, end_time)
            ibus_obj.logInfo("function called successful...")
            logger.info("function called successful...")
            return Response(response=json.dumps(
                {"ST-Data": st_data_file,
                 "Start-Time": start_time.strftime("%H:%M:%S:%f")[:-3],
                 "End-Time": end_time.strftime("%H:%M:%S:%f")[:-3],
                 "Time-Taken": time_taken.strftime("%H:%M:%S:%f")[:-3]}),
                status=200, mimetype='application/json')
        else:
            return Response(response=json.dumps(
                {"message": "Invalid token, "
                            "please check username and password"}),
                status=400, mimetype='application/json')

    except Exception as error:
        ibus_obj.logError("Exception : {}".format(error))
        logger.error("Exception : {}".format(error))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        ibus_obj.logError("Exception Type : {} at line {}".format(
            exc_type, exc_tb.tb_lineno))
        logger.error("Exception Type : {} at line {}".format(
            exc_type, exc_tb.tb_lineno))
        if str(error) == "'access_token'":
            error = "Invalid ST Username / Password"
        if "Login failed for user" in str(error):
            error = "Invalid username / password"
        return Response(response=json.dumps(
            {"message": "Error in get-site-tracker-data --> "
                        "{0}".format(error)}),
            status=400, mimetype='application/json')
    finally:
        ibus_obj.uploadLogFile()
        utility.cleanup(st_data_file, iBus_Obj=ibus_obj)


@application.route('/compare-ndpd-st-data', methods=['POST'])
@async_task
def compare_ndpd_st_data():
    updated_ndpd_json, updated_st_json = '', ''
    start_time = datetime.now()
    json_data = request.json
    ibus_obj = IBusPlatformInterface(VERSION1, True, json_data)
    ndpd_json_file = "Ndpd-Data_{}.json".format(
        datetime.now().strftime("%d%m%Y%H%M%S%f"))
    mapping_json_file = "Mapping-Json_{}.json".format(
        datetime.now().strftime("%d%m%Y%H%M%S%f"))
    st_json_file = "ST-Data_{}.json".format(
        datetime.now().strftime("%d%m%Y%H%M%S%f"))
    if not (ibus_obj.getInputParameterFile("Mapping-Json", mapping_json_file)):
        return Response(response="Mapping-Json.json File not found",
                        status=404)
    if not (ibus_obj.getInputParameterFile("NDPd-Data", ndpd_json_file)):
        return Response(response="Ndpd-Data.json File not found",
                        status=404)
    if not (ibus_obj.getInputParameterFile("ST-Data", st_json_file)):
        return Response(response="ST-Data.json File not found",
                        status=404)
    ndpd_dict = {"ndpdData": []}
    st_dict = {"stData": []}

    try:
        with open(mapping_json_file, 'r') as fp:
            mapping_json_data = json.load(fp)
        with open(ndpd_json_file, 'r') as fp:
            ndpd_json_data = json.load(fp)
        with open(st_json_file, 'r') as fp:
            st_json_data = json.load(fp)
        mappings = mapping_json_data['mappings']
        ndpd_data = ndpd_json_data['ndpdData']
        st_data = st_json_data['stData']
        invalidsmps = mapping_json_data['invalidsmps']
        ibus_obj.logInfo("Comparing of NDPD and ST data")
        logger.info("Comparing of NDPD and ST data")
        ndpd_update_data, st_update_data = get_ndpd_st_data(mappings,
                                                            ndpd_data,
                                                            st_data,
                                                            ibus_obj,
                                                            logger)
        ibus_obj.logInfo("Comparision completed")
        logger.info("Comparision completed")
        ibus_obj.logInfo("Preparing NDPD Data")
        logger.info("Preparing NDPD Data")
        final_ndpd_data = get_ndpd_updated_data(ndpd_update_data,
                                                logger,
                                                ibus_obj)
        ibus_obj.logInfo("Preparing ST data")
        logger.info("Preparing ST data")
        final_st_data = get_st_updated_data(st_update_data, logger, ibus_obj)
        st_project_list = [data['st-projectId'] for data in
                           final_st_data]
        st_project_list = list(set(st_project_list))
        ndpd_smp_list = [data['ndpd-smpId'] for data in final_ndpd_data]
        ndpd_smp_list = list(set(ndpd_smp_list))

        st_main_list = []
        ndpd_main_list = []
        ibus_obj.logInfo("Preparing order of ST")
        logger.info("Preparing order of ST")
        for st_project in st_project_list:
            update_list = []
            for data in final_st_data:
                if st_project == data['st-projectId']:
                    update_list.append(data)
            st_main_list.append(update_list)
        ibus_obj.logInfo("Preparing order of NDPD")
        logger.info("Preparing order of NDPD")
        for ndpd_smp in ndpd_smp_list:
            update_list = []
            for data in final_ndpd_data:
                if ndpd_smp == data['ndpd-smpId']:
                    update_list.append(data)
            ndpd_main_list.append(update_list)
        ndpd_dict['ndpdData'] = ndpd_main_list
        st_dict['stData'] = st_main_list
        ndpd_dict['invalidsmps'] = invalidsmps
        updated_ndpd_json = "Updated_Ndpd_{}.json".format(
            datetime.now().strftime("%d%m%Y%H%M%S%f"))
        with open(updated_ndpd_json, 'w') as f:
            f.write(json.dumps(ndpd_dict))
        ibus_obj.uploadOutputParameterFile('NDPd-Data',
                                           updated_ndpd_json)
        updated_st_json = "Updated_ST_{}.json".format(
            datetime.now().strftime("%d%m%Y%H%M%S%f"))
        with open(updated_st_json, 'w') as f:
            f.write(json.dumps(st_dict))
        ibus_obj.uploadOutputParameterFile('ST-Data',
                                           updated_st_json)
        end_time = datetime.now()
        time_taken = get_time_execution(start_time, end_time)
        return Response(response=json.dumps(
            {
                "NDPd-Data": updated_ndpd_json,
                "ST-Data": updated_st_json,
                "Start-Time": start_time.strftime("%H:%M:%S:%f")[:-3],
                "End-Time": end_time.strftime("%H:%M:%S:%f")[:-3],
                "Time-Taken": time_taken.strftime("%H:%M:%S:%f")[:-3],
                "Operation_Status": "Completed"
            }),
            status=200, mimetype='application/json')
    except Exception as e:
        ibus_obj.uploadLogFile()
        ibus_obj.logError("Exception : {}".format(e))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        ibus_obj.logError("Exception Type : {} at line "
                          "{}".format(exc_type, exc_tb.tb_lineno))
        logger.error("Exception Type : {} at line ""{}".
                     format(exc_type, exc_tb.tb_lineno))
        print("Exception : {}".format(e))
        print("Exception Type : {} at line {}".format(exc_type,
                                                      exc_tb.tb_lineno))
        return Response(response=json.dumps(
            {"message": "Error in getting NDPd data for mapping fields"}),
            status=400, mimetype='application/json')
    finally:
        ibus_obj.uploadLogFile()
        utility.cleanup(updated_ndpd_json, updated_st_json, iBus_Obj=ibus_obj)


@application.route('/update-ndpd-fields', methods=['POST'])
@async_task
def update_ndpd_fields():
    """
    this function accepts mapping json file, ndpd json file and st_json file
    checks for source is "SiteTracker" and target is "NDPD" and updates
    the fields provided in ndpd_fields
    :return: return excel file of reports
    containing Update ndpd success and ndpd failure data
    """
    updated_ndpd_data_file = ''
    start_time = datetime.now()
    # zip_file_name = ''
    data_dict = {"SiteTracker Success": [],
                 "SiteTracker Failure": [],
                 "SiteTracker Warning": [],
                 "NDPD Success": [],
                 "NDPD Failure": [],
                 "NDPD Warning": []
                 }
    json_data = request.json
    ibus_obj = IBusPlatformInterface(VERSION1, True, json_data)
    ndpd_json_file = "Ndpd-Data_{}.json".format(
        datetime.now().strftime("%d%m%Y%H%M%S%f"))
    if not (ibus_obj.getInputParameterFile("NDPd-Data", ndpd_json_file)):
        return Response(response="Ndpd-Data.json File not found",
                        status=404)
    ibus_obj.logInfo("Updating ndpd side started")
    try:
        with open(ndpd_json_file, 'r') as fp:
            ndpd_json_data = json.load(fp)
        ndpd_data = ndpd_json_data['ndpdData']
        invalidsmps = ndpd_json_data['invalidsmps']
        # for next CR below code is required
        # customer = json_data['Customer']

        # ------------------------------
        # Changes made based on CR-1: Update Ndpd fields based on order
        # Before we were having two list ndpd and ndpd_both, now this
        # is made one and sorting is done based on order
        # ------------------------------
        # ------- Ndpd Details ---------
        db_name = json_data["Ndpd-DB-NAME"]
        db_username = json_data["Ndpd-DB-Username"]
        db_password = json_data["Ndpd-DB-Password"]
        ndpd_url = json_data["Ndpd-URL"]
        ndpd_username = json_data["Ndpd-USERNAME"]
        ndpd_password = json_data["Ndpd-PASSWORD"]
        instance = json_data['Ndpd-INSTANCE']

        # ------- NDPd APIs ------------
        forecast_endpoint = json_data["Ndpd-FORECAST_ENDPOINT"]
        actual_endpoint = json_data["Ndpd-ACTUAL_ENDPOINT"]
        execute_endpoint = json_data["Ndpd-EXECUTE-TASK"]
        # ibus_obj.logInfo(ndpd_data)
        # getting session object
        # session = utility.get_session(username=ndpd_username,
        #                               password=ndpd_password)
        pool = multiprocessing.Pool()
        func = partial(update_ndpd_side,
                       #db_name, db_username, db_password, session,
                       db_name, db_username, db_password, ndpd_username, ndpd_password,
                       ndpd_url, instance, forecast_endpoint,
                       actual_endpoint, execute_endpoint)
        map_object = pool.map_async(func, ndpd_data)
        result = map_object.get()
        ndpd_success_data = []
        ndpd_failure_data = []
        ndpd_warning_data = []
        for success, failure, warning in result:
            ndpd_success_data.extend(success)
            ndpd_failure_data.extend(failure)
            ndpd_warning_data.extend(warning)
        pool.close()
        pool.join()
        ndpd_warning_data.extend(invalidsmps)
        data_dict['NDPD Success'].extend(ndpd_success_data)
        data_dict['NDPD Failure'].extend(ndpd_failure_data)
        data_dict['NDPD Warning'].extend(ndpd_warning_data)
        updated_ndpd_data_file = "Updated_Ndpd_{}.json".format(
            datetime.now().strftime("%d%m%Y%H%M%S%f"))
        with open(updated_ndpd_data_file, 'w') as f:
            f.write(json.dumps(data_dict))
        ibus_obj.uploadOutputParameterFile('NDPd-Report',
                                           updated_ndpd_data_file)
        end_time = datetime.now()
        time_taken = get_time_execution(start_time, end_time)
        return Response(response=json.dumps(
            {
                "NDPd-Report": updated_ndpd_data_file,
                "Start-Time": start_time.strftime("%H:%M:%S:%f")[:-3],
                "End-Time": end_time.strftime("%H:%M:%S:%f")[:-3],
                "Time-Taken": time_taken.strftime("%H:%M:%S:%f")[:-3],
                "Operation_Status": "Completed"
            }),
            status=200, mimetype='application/json')
    except Exception as e:
        ibus_obj.uploadLogFile()
        ibus_obj.logError("Exception : {}".format(e))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        ibus_obj.logError("Exception Type : {} at line "
                          "{}".format(exc_type, exc_tb.tb_lineno))
        logger.error("Exception Type : {} at line ""{}".
                     format(exc_type, exc_tb.tb_lineno))
        print("Exception : {}".format(e))
        print("Exception Type : {} at line {}".format(exc_type,
                                                      exc_tb.tb_lineno))
        return Response(response=json.dumps(
            {"message": "Error in getting NDPd data for mapping fields"}),
            status=400, mimetype='application/json')
    finally:
        ibus_obj.uploadLogFile()
        utility.cleanup(updated_ndpd_data_file, iBus_Obj=ibus_obj)


@application.route('/update-site-tracker-fields', methods=['POST'])
@async_task
def update_site_tracker_fields():
    """
    :param Mapping-Info : Mapping json data
    :param NDPd-Data : NDPD json data
    :param ST-Data : NDPD Site tracker data
    :return:Site-Tracker-Update-Report: An excel having success & failure info
    """
    global log_file_name
    json_data = request.json
    wf_id = json_data.get("workflow-instance-id", '')
    reports = "SACUsecases-Phase1-Task-Milestone-Alignment-Report_{}.{}.xlsx".format(datetime.now().strftime("%m-%d-%Y-%H-%M-%S-%f"), wf_id)
    start_time = datetime.now()
    zip_file_name = ""
    path_of_excel_file = ""
    report_data = ''
    ibus_obj = IBusPlatformInterface(VERSION1, True, json_data)
    site_tracker_user_name = json_data['ST-Username']
    site_tracker_password = json_data['ST-Password']

    # getting the site-tracker details from input
    st_instance = json_data['St-Instance']
    st_instance_version = json_data['St-Instance-Version']
    st_authentication_url = json_data['St-Authentication-URL']
    st_client_id = json_data['St-Client-Id']
    st_client_secrete_key = json_data['St-Client-Secret-Key']
    st_security_token = json_data['St-Security-Token']
    st_json_file = "ST-Data_{}.json".format(
        datetime.now().strftime("%d%m%Y%H%M%S%f"))
    ndpd_report_file = "Updated_Ndpd_{}.json".format(
        datetime.now().strftime("%d%m%Y%H%M%S%f"))
    if not (ibus_obj.getInputParameterFile("ST-Data", st_json_file)):
        return Response(response="ST-Data.json File not found",
                        status=404)
    if not (ibus_obj.getInputParameterFile("NDPd-Report", ndpd_report_file)):
        return Response(response="NDPd-Report.json File not found",
                        status=404)
    token = site_tracker_token_generator(st_authentication_url, st_client_id,
                                         st_client_secrete_key,
                                         st_security_token,
                                         site_tracker_user_name,
                                         site_tracker_password, ibus_obj,
                                         logger)
    if not token:
        return Response(response=json.dumps(
            {"message": "Invalid token, please check username or password"}),
            status=400, mimetype='application/json')
    try:
        ibus_obj.logInfo("Reading json files ....")
        with open(st_json_file, 'r') as fp:
            st_json_data = json.load(fp)
        with open(ndpd_report_file, 'r') as fp:
            report_data = json.load(fp)
        ibus_obj.logInfo("Reading json files done ...")
        st_data = st_json_data['stData']
        st_success_data = []
        st_failure_data = []
        pool1 = multiprocessing.Pool()
        func1 = partial(update_site_tracker_side,
                        token, st_instance, st_instance_version)
        map_object = pool1.map_async(func1, st_data)
        result1 = map_object.get()
        for success, failure in result1:
            st_success_data.extend(success)
            st_failure_data.extend(failure)
        pool1.close()
        pool1.join()
        report_data['SiteTracker Success'].extend(st_success_data)
        report_data['SiteTracker Failure'].extend(st_failure_data)
        report_name = reports.split(".")
        final_report_name = report_name[0] + '.' + report_name[-1]
        print(final_report_name)
        path_of_excel_file = write_to_excel(report_data, final_report_name,
                                            ibus_obj)
        log_file_name = "Task-Milestone-Alignment-{}.log".format(
            datetime.now().strftime("%m-%d-%Y-%H-%M-%S-%f"))
        shutil.copy(log_file_name2, log_file_name)
        end_time = datetime.now()
        time_taken = get_time_execution(start_time, end_time)
        zip_file_name = "Task-Milestone-Alignment-{}.zip" \
            .format(datetime.now().strftime("%m-%d-%Y-%H-%M-%S-%f"))
        with ZipFile(zip_file_name, 'w') as zip_object:
            zip_object.write(path_of_excel_file)
            zip_object.write(log_file_name)

        if log_file_name2:
            with open(log_file_name2, 'w'):
                ibus_obj.logInfo("Emptying the file")
                pass
        ibus_obj.uploadOutputParameterFile('Reports', path_of_excel_file)
        ibus_obj.uploadOutputParameterFile(
            'Reports-And-Logs', zip_file_name)
        return Response(response=json.dumps(
            {
                "Reports-And-Logs": zip_file_name,
                "Reports": path_of_excel_file,
                "Start-Time": start_time.strftime("%H:%M:%S:%f")[:-3],
                "End-Time": end_time.strftime("%H:%M:%S:%f")[:-3],
                "Time-Taken": time_taken.
                                  strftime("%H:%M:%S:%f")[:-3],
                "Operation_Status": "Completed"
            }),
            status=200, mimetype='application/json')
    except Exception as error:
        logger.error("Exception : {}".format(error))
        ibus_obj.logError("Exception : {}".format(error))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        ibus_obj.logError("Exception Type : {} at line {}".format(
            exc_type, exc_tb.tb_lineno))
        return Response(response=json.dumps(
            {"message": "Error in updating site tracker --> {0}".format(
                error)}),
            status=400, mimetype='application/json')
    finally:
        ibus_obj.uploadLogFile()
        utility.cleanup(zip_file_name, path_of_excel_file, log_file_name,
                        iBus_Obj=ibus_obj)
