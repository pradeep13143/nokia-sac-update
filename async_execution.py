"""
This file is designed to handle long running digiMOPs.
"""
import json
import threading
import uuid
from functools import wraps

from flask import current_app, request, url_for, Response
from werkzeug.exceptions import HTTPException, InternalServerError

from config import DIGIMOP_DELAY, DIGIMOP_TIMEOUT
from wsgi import application
from setting import TASKS


def async_task(wrapped_function):
    """This is a decorator function."""

    @wraps(wrapped_function)
    def new_function(*args, **kwargs):
        def task_call(flask_app, environ):
            # Create a request context similar to that of the original request
            # so that the task can have access to flask.g, flask.request, etc.
            with flask_app.request_context(environ):
                try:
                    # TASKS[task_id]['return_value'] = wrapped_function(*args, **kwargs)
                    cached_data = TASKS.hgetall(task_id)
                    print("Cached data", cached_data)
                    response = wrapped_function(*args, **kwargs)
                    cached_data['return_value'] = response.data
                    cached_data['status'] = response.status_code
                    TASKS.hmset(task_id, cached_data)
                    print("Updated redis")
                except HTTPException as http_exception:
                    # TASKS[task_id]['return_value'] = current_app.handle_http_exception(
                    #     http_exception)
                    cached_data = TASKS.hgetall(task_id)
                    cached_data['return_value'] = str(current_app.handle_http_exception(
                        http_exception))
                    cached_data['status'] = 400
                    TASKS.hmset(task_id, cached_data)
                except Exception:
                    # The function raised an exception, so we set a 500 error
                    # TASKS[task_id]['return_value'] = InternalServerError()
                    cached_data = TASKS.hgetall(task_id)
                    cached_data['return_value'] = str(InternalServerError())
                    cached_data['status'] = 500
                    TASKS.hmset(task_id, cached_data)
                    if current_app.debug:
                        # We want to find out if something happened so reraise
                        raise

        # If the delay is more than timeout, raise an error and stop execution
        if DIGIMOP_DELAY > DIGIMOP_TIMEOUT:
            return Response(response=json.dumps({
                "message": "Delay should not be more than timeout"
            }), status=400, mimetype='application/json')

        # Assign an id to the asynchronous task
        task_id = uuid.uuid4().hex
        # Create a thread
        _thered = threading.Thread(target=task_call, args=(
            current_app._get_current_object(), request.environ))

        # Store task_id in cache
        TASKS.hmset(task_id, {'task_thread': str(_thered)})
        print("Set task_id in redis cache", task_id)
        # Starting a thread
        _thered.start()

        # Return a 200 response, with a link that the client can use to
        # obtain task status
        return Response(response=json.dumps({
            "Operation_Status": "Accepted",
            "Delay": str(DIGIMOP_DELAY),
            "Timeout": str(DIGIMOP_TIMEOUT),
            "Operation_Id": str(task_id),
            "status_url": url_for('request_status', task_id=task_id),
        }), status=200, mimetype='application/json')

    return new_function


# Below rest API would be used to get the status for long running digiMOP
@application.route('/OperationStatus', methods=['GET', 'POST'])
def request_status():
    """
    Return status about an asynchronous task. If this request returns Operation_Status as In-Progress,
    it means that task hasn't finished yet. Else, the response
    from the task is returned.
    """
    print("Calling operation status API")
    json_data = request.json
    task_id = json_data["Operation_Id"]
    print(f"Task id", task_id)
    task = TASKS.hgetall(task_id)
    print("task in operation status", task)
    if not task:
        return Response(response=json.dumps({
            "Operation_Status": "Failed",
            "Failed_Message": "Digimop failed to execute. Operation id is null"
        }), status=400, mimetype='application/json')

    task = {
        key.decode() if isinstance(key, bytes) else key:
            val.decode() if isinstance(val, bytes) else val
        for key, val in task.items()
    }

    if 'return_value' not in task:
        print("Status- Inprogress")
        return Response(response=json.dumps({
            "Operation_Status": "In-Progress",
            "status_url": url_for('request_status', task_id=task_id),
        }), status=200, mimetype='application/json')

    else:
        print("Removing task-id from cache")
        TASKS.delete(task_id)
        print("Sending response", json.dumps(json.loads(task['return_value'])))
        return Response(response=json.dumps(json.loads(task['return_value'])),
                        status=task['status'], mimetype='application/json')
