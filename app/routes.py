import os
import json

from app import webserver
from flask import request, jsonify

# Example endpoint definition
@webserver.route('/api/post_endpoint', methods=['POST'])
def post_endpoint():
    """
    Example POST endpoint that receives JSON data and processes it.
    """
    if request.method == 'POST':
        # Assuming the request contains JSON data
        data = request.json
        print(f"got data in post {data}")

        # Process the received data
        # For demonstration purposes, just echoing back the received data
        response = {"message": "Received data successfully", "data": data}
        webserver.logger.info("Accessed /api/post_endpoint and got %s", data)

        # Sending back a JSON response
        return jsonify(response)


    return jsonify({"error": "Method not allowed"}), 405

@webserver.route('/api/jobs', methods=['GET'])
def get_jobs():
    """
    Endpoint to get the status of all jobs.
    """
    # Format the job statuses with the desired format "job_id_X"
    job_data = [{"job_id_" + str(job_id): status}
                for job_id, status in webserver.tasks_runner.job_status.items()]
    webserver.logger.info("Job statuses requested: %s", job_data)
    # Return the job statuses in the correct format
    return jsonify({
        "status": "done",
        "data": job_data
    })

@webserver.route('/api/get_results/<int:job_id>', methods=['GET'])
def get_results(job_id):
    """"
    Endpoint to get the results of a specific job."
    """

    # Check if job_id exists in the job status dictionary
    if job_id not in webserver.tasks_runner.job_status:
        webserver.logger.info("Invalid job_id requested: %s", job_id)
        return jsonify({
            "status": "error",
            "reason": "Invalid job_id"
        })

    # Check if the job is still running
    if webserver.tasks_runner.job_status[job_id] == "running":
        webserver.logger.info("Job %s is still running", job_id)
        return jsonify({
            "status": "running"
        })

    # If the job is done, return the result
    if webserver.tasks_runner.job_status[job_id] == "done":
        webserver.logger.info("Job %s completed successfully", job_id)
        # Assuming results are stored in a file with job_id as the filename
        result_file = f'results/{job_id}.json'

        if os.path.exists(result_file):
            with open(result_file, 'r') as file:
                result_data = json.load(file)
                webserver.logger.info("Result file %s found and loaded", result_file)
            return jsonify({
                "status": "done",
                "data": result_data
            })

        webserver.logger.info("Result file %s not found", result_file)
        # If for some reason the result file is missing (unexpected), return an error
        return jsonify({
            "status": "error",
            "reason": "Result file not found"
        })

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    """
    Endpoint to get the mean of a specific question for all states.
    """
    # Check if the server is shutting down
    if webserver.tasks_runner.shutting_down:
        webserver.logger.info("Rejected states_mean - shutting down")
        return jsonify({"status": "error",
                        "reason": "shutting down"})

    # Get request data
    req = request.json
    print(f"Got request {req}")

    # Get question
    question = req.get("question")

    webserver.logger.info("Received /states_mean - question: %s", question)

    # Register job
    job_id = webserver.job_counter
    webserver.logger.info("Assigned job_id %s for states_mean", job_id)
    webserver.job_counter += 1

    # Get job function
    def job_func():
        return webserver.data_ingestor.get_states_mean(question)

    # Submit job
    webserver.tasks_runner.submit_job(job_id, job_func)

    return jsonify({"status" : "submitted",
                    "job_id": job_id})

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    """"
    Endpoint to get the mean of a specific question for a specific state."
    """
    # Check if the server is shutting down
    if webserver.tasks_runner.shutting_down:
        webserver.logger.info("Rejected state_mean - shutting down")
        return jsonify({"status": "error",
                        "reason": "shutting down"})

    req = request.json
    print(f"Got request {req}")

    # Get question
    question = req.get("question")

    # Get state
    state = req.get("state")
    webserver.logger.info("Received /state_mean - question: %s, state: %s", question, state)

    # Register job
    job_id = webserver.job_counter
    webserver.logger.info("Assigned job_id %s for state_mean", job_id)
    webserver.job_counter += 1

    # Get job function
    def job_func():
        return webserver.data_ingestor.get_state_mean(question, state)

    # Submit job
    webserver.tasks_runner.submit_job(job_id, job_func)

    return jsonify({"status" : "submitted",
                    "job_id": job_id})

@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    """"
    Endpoint to get the best 5 states for a specific question.
    """
    # Check if the server is shutting down
    if webserver.tasks_runner.shutting_down:
        webserver.logger.info("Rejected best5 - shutting down")
        return jsonify({"status": "error",
                        "reason": "shutting down"})

    req = request.json
    print(f"Got request {req}")

    # Get question
    question = req.get("question")
    webserver.logger.info("Received /best5 - question: %s", question)

    # Register job
    job_id = webserver.job_counter
    webserver.logger.info("Assigned job_id %s for best5", job_id)
    webserver.job_counter += 1

    # Get job function
    def job_func():
        return webserver.data_ingestor.get_best5(question)

    # Submit job
    webserver.tasks_runner.submit_job(job_id, job_func)

    return jsonify({"status" : "submitted",
                    "job_id": job_id})

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    """"
    Endpoint to get the worst 5 states for a specific question.
    """

    # Check if the server is shutting down
    if webserver.tasks_runner.shutting_down:
        webserver.logger.info("Rejected worst5 - shutting down")
        return jsonify({"status": "error",
                        "reason": "shutting down"})

    req = request.json
    print(f"Got request {req}")

    # Get question
    question = req.get("question")
    webserver.logger.info("Received /worst5 - question: %s", question)

    # Register job
    job_id = webserver.job_counter
    webserver.logger.info("Assigned job_id %s for worst5", job_id)
    webserver.job_counter += 1

    # Get job function
    def job_func():
        return webserver.data_ingestor.get_worst5(question)

    # Submit job
    webserver.tasks_runner.submit_job(job_id, job_func)

    return jsonify({"status" : "submitted",
                    "job_id": job_id})

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    """"
    Endpoint to get the global mean for a specific question."
    """

    # Check if the server is shutting down
    if webserver.tasks_runner.shutting_down:
        webserver.logger.info("Rejected global_mean - shutting down")
        return jsonify({"status": "error",
                        "reason": "shutting down"})

    req = request.json
    print(f"Got request {req}")

    # Get question
    question = req.get("question")

    webserver.logger.info("Received /global_mean - question: %s", question)

    # Register job
    job_id = webserver.job_counter
    webserver.logger.info("Assigned job_id %s for global_mean", job_id)
    webserver.job_counter += 1

    # Get job function
    def job_func():
        return webserver.data_ingestor.global_mean(question)

    # Submit job
    webserver.tasks_runner.submit_job(job_id, job_func)

    return jsonify({"status" : "submitted",
                    "job_id": job_id})

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    """
    Endpoint to get the difference from the global mean for a specific question.
    """

    # Check if the server is shutting down
    if webserver.tasks_runner.shutting_down:
        webserver.logger.info("Rejected diff_from_mean - shutting down")
        return jsonify({"status": "error",
                        "reason": "shutting down"})

    req = request.json
    print(f"Got request {req}")

    # Get question
    question = req.get("question")

    webserver.logger.info("Received /diff_from_mean - question: %s", question)

    # Register job
    job_id = webserver.job_counter
    webserver.logger.info("Assigned job_id %s for diff_from_mean", job_id)
    webserver.job_counter += 1

    # Get job function
    def job_func():
        return webserver.data_ingestor.diff_from_mean(question)

    # Submit job
    webserver.tasks_runner.submit_job(job_id, job_func)

    return jsonify({"status" : "submitted",
                    "job_id": job_id})

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    """"
    Endpoint to get the difference from the global mean for a specific question for a specific state."
    """

    # Check if the server is shutting down
    if webserver.tasks_runner.shutting_down:
        webserver.logger.info("Rejected state_diff_from_mean - shutting down")
        return jsonify({"status": "error",
                        "reason": "shutting down"})

    req = request.json
    print(f"Got request {req}")

    # Get question
    question = req.get("question")

    # Get state
    state = req.get("state")

    webserver.logger.info("Received /state_diff_from_mean - question: %s, state: %s", question, state)

    # Register job
    job_id = webserver.job_counter
    webserver.logger.info("Assigned job_id %s for state_diff_from_mean", job_id)
    webserver.job_counter += 1

    # Get job function
    def job_func():
        return webserver.data_ingestor.state_diff_from_mean(question, state)

    # Submit job
    webserver.tasks_runner.submit_job(job_id, job_func)

    return jsonify({"status" : "submitted",
                    "job_id": job_id})

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    """
    "Endpoint to get the mean by category for a specific question.""
    """

    # Check if the server is shutting down
    if webserver.tasks_runner.shutting_down:
        webserver.logger.info("Rejected mean_by_category - shutting down")
        return jsonify({"status": "error",
                        "reason": "shutting down"})

    req = request.json
    print(f"Got request {req}")

    # Get question
    question = req.get("question")

    webserver.logger.info("Received /mean_by_category - question: %s", question)

    # Register job
    job_id = webserver.job_counter
    webserver.logger.info("Assigned job_id %s for mean_by_category", job_id)
    webserver.job_counter += 1

    # Get job function
    def job_func():
        return webserver.data_ingestor.mean_by_category(question)

    # Submit job
    webserver.tasks_runner.submit_job(job_id, job_func)

    return jsonify({"status" : "submitted",
                    "job_id": job_id})

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    """
    Endpoint to get the mean by category for a specific question for a specific state.
    """
    # Check if the server is shutting down
    if webserver.tasks_runner.shutting_down:
        webserver.logger.info("Rejected state_mean_by_category - shutting down")
        return jsonify({"status": "error",
                        "reason": "shutting down"})

    req = request.json
    print(f"Got request {req}")

    # Get question
    question = req.get("question")

    # Get state
    state = req.get("state")

    webserver.logger.info("Received /state_mean_by_category - question: %s, state: %s", question, state)

    # Register job
    job_id = webserver.job_counter
    webserver.logger.info("Assigned job_id %s for state_mean_by_category", job_id)
    webserver.job_counter += 1

    # Get job function
    def job_func():
        return webserver.data_ingestor.state_mean_by_category(question, state)

    # Submit job
    webserver.tasks_runner.submit_job(job_id, job_func)

    return jsonify({"status" : "submitted",
                    "job_id": job_id})

@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown():
    """"
    Endpoint to initiate a graceful shutdown of the server."
    """
    # Check if there are any pending jobs
    if webserver.tasks_runner.has_pending_jobs():

        webserver.logger.info("Shutdown request received, but jobs are still running.")

        return jsonify({"status": "running"})

    # If no jobs are pending, shutdown can happen
    webserver.logger.info("Shutdown request received, no jobs are running.")
    webserver.tasks_runner.shutdown()

    return jsonify({"status": "done"})  # Shutdown complete, no more jobs in the queue

# You can check localhost in your browser to see what this displays
@webserver.route('/')
@webserver.route('/index')
def index():
    """"
    Welcome to the webserver! Here are the available routes.
    """

    routes = get_defined_routes()
    msg = "Hello, World! Interact with the webserver using one of the defined routes:"

    # Display each route as a separate HTML <p> tag
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    return msg

def get_defined_routes():
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes
