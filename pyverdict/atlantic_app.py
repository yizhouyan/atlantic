from flask import Flask, request, render_template, flash, redirect, url_for, Response, send_file
import pyverdict
from loguru import logger
import time
from shutil import copyfile
from dataclasses import dataclass
from pandas.core.common import flatten
import pandas as pd
import decimal

app = Flask(__name__)
app.config['DEBUG'] = True  # start debugging
app.secret_key = "super secret key"
current_db = "mysql"
current_host = "localhost"
current_port = 3306
current_user = "root"
current_pwd = "yizhouyan"
current_create_table_query = ""
current_scramble_query = "SELECT product, count(price) as cnt_products FROM myschema.sales_scrambled s group by product"
current_original_query = "SELECT product, count(price) as cnt_products FROM myschema.sales s group by product"
current_epsilon = 0.01
current_delta = 0.000001

LOGGING_PATH = "static/job.log"
DOWNLOAD_FOLDER = 'results/'
# configure logger
logger.add(LOGGING_PATH, format="{time} - {message}")


@dataclass
class FinalQueryResults:
    scramble_query_time = None
    original_query_time = None
    accuracy_to_gt = None
    # accuracy_to_sampling = None
    scramble_query_results = None
    original_query_results = None
    processing_logs = None


def connect_to_db(enable_privacy=True):
    logger.info(
        f"Connecting to {current_db} with host={current_host}, port={current_port}, user={current_user}")
    verdict_conn = pyverdict.mysql(
        host=current_host,
        user=current_user,
        password=current_pwd,
        port=current_port,
        enable_dp="true" if enable_privacy else "false",
        delta=str(current_delta),
        epsilon=str(current_epsilon),
    )
    logger.info(f"Connected to DB...")
    return verdict_conn


def close_db(verdict_conn):
    verdict_conn.close()
    logger.info(f"DB Connection Closed...")


def overwrite_db_parameters(request):
    global current_user, current_host, current_pwd, current_db, current_port
    current_host = request.form['host']
    current_port = request.form['port']
    current_user = request.form['user']
    current_pwd = request.form['password'].replace("/", "")
    current_db = request.form['database']


def flask_logger():
    """creates logging information"""
    with open(LOGGING_PATH) as log_info:
        while True:
            data = log_info.read()
            yield data.encode()
            time.sleep(1)


@app.route("/atlantic/running_logs", methods=["GET"])
def running_logs():
    """returns logging information"""
    return Response(flask_logger(), mimetype="text/plain", content_type="text/event-stream")


@app.route('/', methods=['GET', 'POST'])
def home():
    return redirect('/atlantic/home')


@app.route('/atlantic/home', methods=['GET'])
def atlantic_home():
    return render_template('home.html')


@app.route('/atlantic/load_data', methods=['GET'])
def atlantic_load_data_get():
    return render_template('load_data.html', current_db=current_db, current_host=current_host,
                           current_port=current_port, current_user=current_user, current_pwd=current_pwd,
                           create_table_query=current_create_table_query)


@app.route('/atlantic/load_data', methods=['POST'])
def atlantic_load_data_post():
    global current_create_table_query
    try:
        overwrite_db_parameters(request)
        current_create_table_query = request.form['create_scramble_query']
        if current_create_table_query:
            db_conn = connect_to_db()
            logger.info(f"Executing Query: {current_create_table_query}")
            db_conn.sql(current_create_table_query)
            close_db(db_conn)
            flash("Data Loaded", "info")
        else:
            flash("Please provide a valid create table query. ", "error")
        return redirect(request.url)
    except Exception as e:
        flash(str(e), "error")
        return redirect(request.url)
@dataclass
class MetaData:
    schemaName: str
    tableName: str
    originalTableName: str
    privacyMetaColMax: str
    privacyMetaColMin: str
    privacyMetaMaxFreq: str

@app.route('/atlantic/show_data', methods=['GET'])
def atlantic_show_data():
    db_conn = connect_to_db(enable_privacy=False)
    meta_data = db_conn.sql("select data from `verdictdbmeta`.`verdictdbmeta`;")
    close_db(db_conn)
    structured_metadata = []
    for data in meta_data.values.tolist():
        import json
        data_dict = json.loads(data[0])
        structured_metadata.append(MetaData(
            schemaName=data_dict['schemaName'],
            tableName=data_dict['tableName'],
            originalTableName = data_dict['originalTableName'],
            privacyMetaColMax=data_dict['privacyMetaColMax'],
            privacyMetaColMin=data_dict['privacyMetaColMin'],
            privacyMetaMaxFreq=data_dict['privacyMetaMaxFreq'],
        ))
    return render_template('show_data.html', meta_data=structured_metadata)


@app.route('/atlantic/query_data', methods=['GET'])
def atlantic_query_data_get(final_results=None):
    return render_template('query_data.html', current_db=current_db, current_host=current_host,
                           current_port=current_port, current_user=current_user, current_pwd=current_pwd,
                           scramble_query=current_scramble_query,
                           original_query=current_original_query, final_results=final_results)

def extract_numeric_values(results):
    numeric_results = []
    for x in results:
        if isinstance(x, (float, int)):
            numeric_results.append(x)
        if isinstance(x, decimal.Decimal):
            numeric_results.append(pd.to_numeric(x))
        if isinstance(x, str) and x.isnumeric():
            numeric_results.append(float(x))
    return numeric_results

def persist_final_results(scramble_query_results, original_query_results, final_results):
    if scramble_query_results is not None and original_query_results is not None:
        scramble_result_list = extract_numeric_values(scramble_query_results.values.flatten())
        original_result_list = extract_numeric_values(original_query_results.values.flatten())
        accuracy_list = [abs(s_r - o_r) / abs(o_r) for s_r, o_r in zip(scramble_result_list, original_result_list)]
        final_results.accuracy_to_gt = 1 - sum(accuracy_list) * 1.0 / len(accuracy_list) if len(accuracy_list) > 0 else None

    # if scramble_query_results is not None and scramble_query_results_no_privacy is not None:
    #     scramble_result_list = extract_numeric_values(scramble_query_results.values.flatten())
    #     scramble_result_no_privacy_list = extract_numeric_values(scramble_query_results_no_privacy.values.flatten())
    #     accuracy_list = [abs(s_r - o_r) / abs(o_r) for s_r, o_r in zip(scramble_result_list, scramble_result_no_privacy_list)]
    #     final_results.accuracy_to_sampling = 1 - sum(accuracy_list) * 1.0 / len(accuracy_list) if len(
    #         accuracy_list) > 0 else None

    if scramble_query_results is not None:
        logger.info(f"Scramble Results: {scramble_query_results}")
        scramble_result_filename = f"scramble_results_{int(time.time())}"
        scramble_query_results.to_csv(DOWNLOAD_FOLDER + scramble_result_filename)
        logger.info(f"Successfully saved scramble query results to {scramble_result_filename}")
        final_results.scramble_query_results = scramble_result_filename

    if original_query_results is not None:
        logger.info(f"Original Results: {original_query_results}")
        original_result_filename = f"original_results_{int(time.time())}"
        original_query_results.to_csv(DOWNLOAD_FOLDER + original_result_filename)
        logger.info(f"Successfully saved original query results to {original_result_filename}")
        final_results.original_query_results = original_result_filename


@app.route('/return-files/<filename>')
def return_files_tut(filename):
    file_path = DOWNLOAD_FOLDER + filename
    return send_file(file_path, as_attachment=False, attachment_filename='')


@app.route('/atlantic/query_data', methods=['POST'])
def atlantic_query_data_post():
    global current_scramble_query, current_original_query
    try:
        overwrite_db_parameters(request)
        current_scramble_query = request.form['scramble_query']
        current_original_query = request.form['original_query']

        final_results = FinalQueryResults()

        original_query_results = None
        if current_original_query:
            original_start_time = time.process_time()
            db_conn = connect_to_db(enable_privacy=False)
            logger.info(f"Executing Original Query: {current_original_query}")
            original_query_results = db_conn.sql(current_original_query)
            original_elapsed_time = time.process_time() - original_start_time
            logger.info(f"Original Query executed, time cost = {original_elapsed_time}")
            final_results.original_query_time = original_elapsed_time
            close_db(db_conn)
        #
        # scramble_query_results_no_privacy = None
        # if current_scramble_query:
        #     db_conn = connect_to_db(enable_privacy=False)
        #     logger.info(f"Executing Scramble Query: {current_scramble_query} without privacy")
        #     scramble_query_results_no_privacy = db_conn.sql(current_scramble_query)
        #     close_db(db_conn)

        scramble_query_results = None
        if current_scramble_query:
            db_conn = connect_to_db()
            scramble_start_time = time.process_time()
            logger.info(f"Executing Scramble Query: {current_scramble_query} with privacy")
            scramble_query_results = db_conn.sql(current_scramble_query)
            scramble_elapsed_time = time.process_time() - scramble_start_time
            logger.info(f"Scramble Query executed, time cost = {scramble_elapsed_time}")
            final_results.scramble_query_time = scramble_elapsed_time
            close_db(db_conn)

        # persist final results
        if original_query_results is not None or scramble_query_results is not None:
            persist_final_results(scramble_query_results, original_query_results, final_results)

        # persist processing logs
        final_log_filename = f"log_{int(time.time())}"
        copyfile(LOGGING_PATH, DOWNLOAD_FOLDER + final_log_filename)
        open(LOGGING_PATH, 'w').close()
        final_results.processing_logs = final_log_filename

        return atlantic_query_data_get(final_results=final_results)
    except Exception as e:
        print(e)
        flash(str(e), "error")
        return redirect(request.url)


@app.route('/atlantic/contact', methods=['GET'])
def atlantic_contact():
    return render_template('contact.html')


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080)
