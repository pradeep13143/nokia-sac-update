import datetime

from utility import get_ndpd, get_site_tracker, \
    get_site_tracker_proper_date_format, get_proper_format


def add_timestamp_to_date(date_string):
    target_date = ""
    if date_string:
        try:
            target_date = datetime.datetime.strptime(date_string, '%Y-%m-%d')
            target_date = datetime.datetime.combine(target_date.date(), datetime.time(12))
        except Exception as error:
            print(error)
        if target_date:
            return target_date.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return ""
    else:
        return None


def form_update_data(mapping, ndpd, site_tracker):
    mapping['ndpd-plannedStartTime'] = ndpd['plannedStartTime'] if ndpd[
        'plannedStartTime'] else ""
    mapping['ndpd-actualEndTime'] = ndpd['actualEndTime'] if \
        ndpd['actualEndTime'] else ""
    mapping['st-plannedStartTime'] = site_tracker['plannedStartTime'] if \
        site_tracker['plannedStartTime'] else ""
    mapping['st-actualEndTime'] = site_tracker['actualEndTime'] if \
        site_tracker['actualEndTime'] else ""
    mapping['st-milestoneId'] = site_tracker['st-milestoneId']
    mapping['p-number'] = site_tracker['p-number']
    return mapping


def get_ndpd_st_data(mappings, ndpd_data, st_data, ibus_obj, logger):
    ndpd_update_data = []
    st_update_data = []
    ibus_obj.logInfo(f"length of total mappings received {len(mappings)}")
    logger.info(f"length of total mappings received {len(mappings)}")
    for mapping in mappings:
        ndpd = get_ndpd(mapping, ndpd_data)
        site_tracker = get_site_tracker(mapping, st_data)
        if ndpd and site_tracker:
            if (mapping['source'] == mapping['target']) and (
                    mapping[
                        'target-fields'] == "Actual End Date") and (
                    mapping['ndpd-task-type'] == "Task") and (
                    ndpd["actualEndTime"] == "null") and (
                    mapping[
                        'source-fields'] == "Actual End Date") and (
                    site_tracker["actualEndTime"] is not None):
                updated_data = form_update_data(mapping,
                                                ndpd, site_tracker)
                ndpd_update_data.append(updated_data)
            elif (mapping['source'] == mapping['target']) and \
                    (site_tracker["actualStartTime"] is None and ndpd[
                        "actualStartTime"]) and \
                    (mapping['source-fields'] == "Actual End Date"):
                updated_data = form_update_data(mapping,
                                                ndpd, site_tracker)
                st_update_data.append(updated_data)
            elif mapping['source'] == mapping['target']:
                if datetime.datetime.strptime(ndpd['lastModifiedTime'],
                                     "%Y-%m-%d %H:%M:%S.%f") < \
                        datetime.datetime.strptime(
                            site_tracker['lastModifiedTime'],
                            "%Y-%m-%d %H:%M:%S.%f"):
                    if (mapping['ndpd-task-type'] == "Task") and \
                            (mapping['target-fields'] == "Actual End Date"):
                        pass
                    else:
                        updated_data = form_update_data(mapping,
                                                        ndpd, site_tracker)
                        ndpd_update_data.append(updated_data)
                if datetime.datetime.strptime(ndpd['lastModifiedTime'],
                                     "%Y-%m-%d %H:%M:%S.%f") > \
                        datetime.datetime.strptime(site_tracker['lastModifiedTime'],
                                          "%Y-%m-%d %H:%M:%S.%f"):
                    updated_data = form_update_data(mapping,
                                                    ndpd, site_tracker)
                    st_update_data.append(updated_data)
            else:
                if (mapping['target'] == "NDPD") and (
                        mapping['source'] == "SiteTracker") and (
                        mapping['target-fields'] == "Actual End Date") and (
                        mapping['ndpd-task-type'] == "Task") and (
                        mapping[
                            'source-fields'] == "Actual End Date") and (
                        ndpd["actualEndTime"] == "null") and (
                        site_tracker["actualEndTime"] is not None):
                    updated_data = form_update_data(mapping,
                                                    ndpd, site_tracker)
                    ndpd_update_data.append(updated_data)
                else:
                    if (mapping['target'] == "NDPD") and (
                            mapping['source'] == "SiteTracker"):
                        if (mapping['ndpd-task-type'] == "Task") and \
                                (mapping['target-fields'] == "Actual End Date"):
                            pass
                        else:
                            updated_data = form_update_data(mapping,
                                                            ndpd, site_tracker)
                            ndpd_update_data.append(updated_data)
                    elif (mapping['target'] == "SiteTracker") and (
                            mapping['source'] == "NDPD"):
                        updated_data = form_update_data(mapping,
                                                        ndpd, site_tracker)
                        st_update_data.append(updated_data)
    ibus_obj.logInfo(f"length of ndpd data to update {len(ndpd_update_data)}")
    ibus_obj.logInfo(f"length of st data to update {len(st_update_data)}")
    return ndpd_update_data, st_update_data


def get_ndpd_updated_data(ndpd_update_data, logger, ibus_obj):
    final_ndpd_update_data = []
    for data in ndpd_update_data:
        if data['target-fields'] == "Actual End Date":
            if not data['st-actualEndTime'] or \
                    data['st-actualEndTime'] == "null" or \
                    data['st-actualEndTime'] is None or \
                    data['st-actualEndTime'] == "":
                ibus_obj.logInfo(
                    f"sitetracker actual end is null or none so skipping"
                    f"{data['ndpd-taskName']} - "
                    f"{data['ndpd-smpId']}")
                pass
            else:
                ndpd_date = get_site_tracker_proper_date_format(
                    data['ndpd-actualEndTime'], logger)
                st_date = get_proper_format(data['st-actualEndTime'], logger)
                if ndpd_date == st_date and ndpd_date and st_date:
                    pass
                else:
                    data['ndpd-actualEndTime'] = ndpd_date
                    data['st-actualEndTime'] = st_date
                    final_ndpd_update_data.append(data)
        elif data['target-fields'] == "Forecast Start Date":
            if not data['st-plannedStartTime'] or \
                    data['st-plannedStartTime'] == "null" or \
                    data['st-plannedStartTime'] is None or \
                    data['st-plannedStartTime'] == "":
                pass
            elif data['ndpd-actualEndTime'] == "null" or \
                    data['ndpd-actualEndTime'] == "":
                ndpd_date = get_site_tracker_proper_date_format(
                    data['ndpd-plannedStartTime'], logger)
                st_date = get_proper_format(data['st-plannedStartTime'], logger)
                if ndpd_date == st_date:
                    pass
                else:
                    data['ndpd-plannedStartTime'] = ndpd_date
                    data['st-plannedStartTime'] = st_date
                    final_ndpd_update_data.append(data)
            else:
                pass
    ibus_obj.logInfo(f"final ndpd data to be updated after all filters "
                     f"{len(final_ndpd_update_data)}")
    return final_ndpd_update_data


def get_st_updated_data(st_update_data, logger, ibus_obj):
    final_st_update_data = []
    for data in st_update_data:
        if data['target-fields'] == "Actual End Date":
            if not data['ndpd-actualEndTime'] or \
                    data['ndpd-actualEndTime'] == "null" or \
                    data['ndpd-actualEndTime'] is None:
                pass
            else:
                ndpd_date = get_site_tracker_proper_date_format(
                    data['ndpd-actualEndTime'], logger)
                if ndpd_date == data['st-actualEndTime'] and ndpd_date:
                    pass
                else:
                    data['ndpd-actualEndTime'] = ndpd_date
                    final_st_update_data.append(data)
        elif data['target-fields'] == "Forecast Start Date":
            if not data['ndpd-plannedStartTime'] or \
                    data['ndpd-plannedStartTime'] == "null" or \
                    data['ndpd-plannedStartTime'] is None:
                pass
            elif data['st-actualEndTime'] == "" or \
                    not data['st-actualEndTime'] or \
                    data['st-actualEndTime'] == "null":
                ndpd_date = get_site_tracker_proper_date_format(
                    data['ndpd-plannedStartTime'], logger)
                if ndpd_date == data['st-plannedStartTime'] and ndpd_date:
                    pass
                else:
                    data['ndpd-plannedStartTime'] = ndpd_date
                    final_st_update_data.append(data)
            else:
                pass
    ibus_obj.logInfo(f"length of final st data to be updated {len(final_st_update_data)}")
    return final_st_update_data
