import sys
import os
import logging
import requests
from datetime import datetime
import psycopg2
import re
import json
import gspread
from psycopg2.extras import RealDictCursor
from oauth2client.service_account import ServiceAccountCredentials
from statistics import mean


# custom import from my private config file to get DB host, port etc
from f_db_config import db_host, db_port, db_name, db_user, db_password


log_dir = "ETL-project/logs"
today_date = datetime.today().date()

# set logger and level of logging
logger = logging.getLogger(f"ppf-{today_date}")
logger.setLevel(logging.INFO)
# set up the handler and formatter for my logger
handler = logging.FileHandler(f"{log_dir}/ppf-{today_date}.log")  # , mode="w"
formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")
# add the formatter to the handler
handler.setFormatter(formatter)
# add the handler to the logger
logger.addHandler(handler)

logger.info("")
logger.info(f" --- STARTING THE CUSTOM LOGGER FOR THE MODULE 'fin_python_v2.py' --- ")

logger.info("Checking log files...")
# delete the oldest log-files if there are more than 4 log-files in the logs dir
log_files = os.listdir(log_dir)
cnt_logs = len(log_files)
deleted_logs = []
if cnt_logs > 4:
    for log in log_files[:-4]:
        file_path = os.path.join(log_dir, log)
        os.remove(file_path)
        deleted_logs.append(log)
if deleted_logs:
    logger.info(f"Deleted old log files: {deleted_logs}")
else:
    logger.info("No log files were deleted.")


def get_data(start_day: str, finish_day: str):
    logger.info(
        f"Extracting data from the API for the interval {start_day} - {finish_day}..."
    )
    try:
        date_pattern = re.compile(
            r"^\d{4}-\d{2}-\d{2}$"
        )  # regular expression pattern for requirement YYYY-MM-DD format

        if not date_pattern.match(start_day) or not date_pattern.match(finish_day):
            raise ValueError(
                "Invalid date format for 'start_day' or 'finish_day', use 'YYYY-MM-DD'"
            )

        # Check if start_day is less than or equal to finish_day
        # Check if the difference between start_date and finish_date is more than 5 days
        start_date = datetime.strptime(start_day, "%Y-%m-%d")
        finish_date = datetime.strptime(finish_day, "%Y-%m-%d")
        if start_date > finish_date:
            raise ValueError("'start_day' cannot be greater than 'finish_day'.")
        if (finish_date - start_date).days > 6:
            raise ValueError(
                "The difference between 'start_day' and 'finish_day' cannot be more than 6 days."
            )

        prms = {
            "client": "Skillfactory",
            "client_key": "M2MGWS",
            "start": f"{start_day} 00:00:00.0",
            "end": f"{finish_day} 23:59:59.999999",
        }
        resp = requests.get("https://b2b.itresume.ru/api/statistics", params=prms)
        data = resp.json()

        logger.info("Data extracted successfully from API.")
        return data

    except Exception as er:
        logger.error(f"Failed to retrieve data from API: {er}")
        sys.exit(1)


def validate_data(data):
    logger.info("Starting data validation...") if data else sys.exit(1)
    validated_data = []

    for idx, row in enumerate(data, start=1):
        try:
            # check for missing required fields
            required_fields = [
                "lti_user_id",
                "passback_params",
                "is_correct",
                "attempt_type",
                "created_at",
            ]
            for field in required_fields:
                if field not in row:
                    raise ValueError(f"Missing '{field}' field")

            # validate 'lti_user_id' field
            lti_user_id = row.get("lti_user_id")
            if lti_user_id is not None and not isinstance(lti_user_id, str):
                raise ValueError("'lti_user_id' must be a string or None")

            # validate 'is_correct' field
            is_correct = row.get("is_correct")
            if is_correct not in (0, 1, None):
                raise ValueError("'is_correct' must be 0, 1, or None")

            # validate 'passback_params' field
            passback_params = row.get("passback_params")
            if passback_params is not None:
                if not isinstance(passback_params, str):
                    raise ValueError("'passback_params' must be a string or None")
                # Replace single quotes with double quotes to make the JSON valid
                passback_params = passback_params.replace("'", '"')
                passback_params_json = json.loads(passback_params)
                for value in passback_params_json.values():
                    if value is not None and not isinstance(value, str):
                        raise ValueError(
                            "All values in 'passback_params' must be strings"
                        )
                oauth_consumer_key = passback_params_json.get("oauth_consumer_key")
                lis_result_sourcedid = passback_params_json.get("lis_result_sourcedid")
                lis_outcome_service_url = passback_params_json.get(
                    "lis_outcome_service_url"
                )
            else:
                # Handle the case where passback_params is None
                oauth_consumer_key = None
                lis_result_sourcedid = None
                lis_outcome_service_url = None

            # validate 'attempt_type' and 'created_at'
            attempt_type = row.get("attempt_type")
            created_at = row.get("created_at")
            if not isinstance(attempt_type, str) or not isinstance(created_at, str):
                raise ValueError("'attempt_type' and 'created_at' must be a string")

            # if no errors, add THE RECORD row to the validated data
            validated_data.append(
                {
                    "user_id": lti_user_id,
                    "oauth_consumer_key": oauth_consumer_key,
                    "lis_result_sourcedid": lis_result_sourcedid,
                    "lis_outcome_service_url": lis_outcome_service_url,
                    "is_correct": is_correct,
                    "attempt_type": attempt_type,
                    "created_at": created_at,
                }
            )

        except ValueError as er:
            logger.error(f"Error in record {idx}: {er}. Record: {row}")

    if len(validated_data) == len(data):
        logger.info("Data validation completed successfully.")
        return validated_data
    else:
        logger.error("Data validation failed. Please check the logs for details.")
        sys.exit(1)  # exit if error (non-zero status code to indicate failure)


class Database:
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "instance"):
            cls.instance = super().__new__(cls)
        return cls.instance

    def __init__(self, host, port, db, user, pss):
        try:
            self.con = psycopg2.connect(
                host=host, port=port, database=db, user=user, password=pss
            )
            self.cur = self.con.cursor()
            logger.info("Connected to database.")
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            sys.exit(1)

    def __del__(self):
        try:
            self.cur.close()
            self.con.close()
            logger.info("Database connection closed.")
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")

    def get_all_tables(self):
        try:
            self.cur.execute(
                """SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_name LIKE 'pp%'"""
            )
            tables = self.cur.fetchall()
            logger.info(
                f"The database contains {len(tables)} tables named python_project: {tables}"
            )
            return tables
        except psycopg2.Error as e:
            logger.error(f"Error selecting tables from the database: {e}")

    def create_table(self, table_name):
        logger.info("Creating table stage...")
        try:
            self.cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = %s
                )
                """,
                (table_name,),
            )
            table_exists = self.cur.fetchone()[0]

            if not table_exists:
                create_table_query = f"""
                    CREATE TABLE {table_name} (
                        id SERIAL PRIMARY KEY,
                        user_id VARCHAR(255),
                        oauth_consumer_key VARCHAR(255),
                        lis_result_sourcedid VARCHAR(255),
                        lis_outcome_service_url VARCHAR(255),
                        is_correct INTEGER,
                        attempt_type VARCHAR(255),
                        created_at VARCHAR(255)
                    )
                """

                self.cur.execute(create_table_query)
                self.con.commit()
                logger.info(f"Table '{table_name}' created successfully!")
            else:
                logger.info(f"Table '{table_name}' already exists.")
        except psycopg2.Error as er:
            logger.error(f"Error creating table '{table_name}': {er}")

    def load_data_to_db(self, table_name, vdata):
        logger.info("Loading data to the db table...")
        try:
            skipped_count = 0
            for idx, row in enumerate(vdata, start=1):
                # check if the data has already been loaded
                self.cur.execute(
                    f"""SELECT COUNT(*) 
                        FROM {table_name} 
                        WHERE (user_id = %s or user_id is Null) AND created_at = %s""",
                    (row["user_id"], row["created_at"]),
                )
                count = self.cur.fetchone()[0]
                skipped_count += count  # Update the skipped count

                if count == 0:
                    # data has not been loaded previously, insert it into the database
                    self.cur.execute(
                        f"""
                        INSERT INTO {table_name} 
                        (user_id, oauth_consumer_key, lis_result_sourcedid, lis_outcome_service_url, is_correct, attempt_type, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                        (
                            row["user_id"],
                            row["oauth_consumer_key"],
                            row["lis_result_sourcedid"],
                            row["lis_outcome_service_url"],
                            row["is_correct"],
                            row["attempt_type"],
                            row["created_at"],
                        ),
                    )

            logger.info(
                f"Checking previously loaded data found and skipped {skipped_count} lines."
            )

            self.con.commit()
            logger.info("Data loaded to the database successfully!")
        except psycopg2.Error as er:
            logger.error(f"Error loading data to the database: {er}")

    def select_query(self, table_name, sql_query):
        try:
            self.cur = self.con.cursor(cursor_factory=RealDictCursor)
            self.cur.execute(f"""{sql_query}""")
            result = self.cur.fetchall()
            logger.info(
                f"Select query for '{table_name}' table completed successfully."
            )
            return result
        except psycopg2.Error as e:
            logger.error(f"Error select query: {e}")


def calc_for_gsheets(data):
    try:
        gs_result = {
            "all attempts": 0,
            "run_attempts": 0,
            "submits": 0,
            "successfull submits": 0,
        }

        users_acts = {}
        time_d = {}

        for row in data:
            user = row["user_id"]
            if user is not None:
                gs_result["all attempts"] += 1
                gs_result["run_attempts"] += 1 if row["is_correct"] is None else 0
                gs_result["submits"] += 1 if row["is_correct"] is not None else 0
                gs_result["successfull submits"] += 1 if row["is_correct"] == 1 else 0

                users_acts[user] = users_acts.get(user, []) + [
                    [row["created_at"], row["is_correct"]]
                ]

                time = row["created_at"]
                hour = time.split(" ")[1].split(":")[0]
                if hour not in time_d:
                    time_d[hour] = 1
                else:
                    time_d[hour] += 1

        gs_result["percent of successfull submits"] = round(
            gs_result["successfull submits"] / gs_result["submits"] * 100.0, 2
        )
        gs_result["unique users"] = len(users_acts)

        max_hour = max(time_d, key=time_d.get)
        gs_result["max activity hour"] = max_hour

        users_summary = {}
        users_summary = {
            user: [
                len(acts),
                len([act[1] for act in acts if act[1] is None]),
                len([act[1] for act in acts if act[1] is not None]),
                len([act[1] for act in acts if act[1] == 1]),
            ]
            for user, acts in users_acts.items()
        }

        avg_atts_per_user = mean([value[0] for value in users_summary.values()])
        avg_runs_per_user = mean([value[1] for value in users_summary.values()])
        avg_submits_per_user = mean([value[2] for value in users_summary.values()])
        avg_corr_subts_per_user = mean([value[3] for value in users_summary.values()])

        gs_result["avg attempts per user"] = round(avg_atts_per_user, 1)
        gs_result["avg run_attempts per user"] = round(avg_runs_per_user, 1)
        gs_result["avg submits per user"] = round(avg_submits_per_user, 1)
        gs_result["avg successfull submits per user"] = round(
            avg_corr_subts_per_user, 1
        )
        logger.info("Data successfull aggregated for Google Sheets file.")
        return gs_result

    except Exception as er:
        logger.error(f"Error calc/aggregate data for Google Sheets: {er}")


def auth_goo_shee():
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive",
        ]
        # Reading Credentails from ServiceAccount Keys file
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            "ETL-project/cool-reality-416812-1f47e0aa1de9.json", scope
        )
        # intitialize authorization object
        gc_auo = gspread.authorize(credentials)

        logger.info("Successfull authenticate with Google Sheets.")
        return gc_auo

    except Exception as er:
        print(repr(er))


class GoogleSheets:
    def __init__(self, gspread_auth_client, sheet_file: str):
        self.auth_obj = gspread_auth_client
        self.sheet_file = sheet_file

        self.opened_file = gspread_auth_client.open(sheet_file)

    def get_data(self, sheet_num: int):
        try:
            sheet_data = self.opened_file.get_worksheet(sheet_num).get_all_records()
            return sheet_data
        except Exception as er:
            logger.error(repr(er))

    def load_aggregated_data_to_sheets(self, sheet_name, aggregated_data):
        try:
            sheet_fu = self.opened_file.worksheet(sheet_name)
            sheet_fu.update(
                range_name="A1",
                values=[[f"Aggregated Data for dates: {start_date} - {end_date}"]],
            )
            row = 2
            for key, value in aggregated_data.items():
                sheet_fu.update(range_name=f"A{row}", values=[[key]])
                sheet_fu.update(range_name=f"B{row}", values=[[value]])
                row += 1
            logger.info(
                f"Aggregated data for interval {start_date} - {end_date} successfully loaded to Google Sheets file '{self.sheet_file}'."
            )
        except Exception as er:
            logger.error(repr(er))


# Call the functions
if __name__ == "__main__":
    try:
        # extract the dataset ---------------------------------------
        start_date = "2024-05-01"
        end_date = "2024-05-01"
        api_data1 = get_data(start_date, end_date)  # !!!

        # validate the dataset --------------------------------------
        valid_data1 = validate_data(api_data1)  # !!!

        # work with Google Sheets -----------------------------------
        gs_data = calc_for_gsheets(valid_data1)
        gs_auth = auth_goo_shee()
        gs = GoogleSheets(gs_auth, "simulative_basic_py_project")
        gs.load_aggregated_data_to_sheets("Sheet1", gs_data)

        # Connect to the DB ------------------------------------------
        db = Database(db_host, db_port, db_name, db_user, db_password)
        s = db.get_all_tables()
        cr = db.create_table("ppf_2024_may")
        l = db.load_data_to_db("ppf_2024_may", valid_data1)

    except Exception as er:
        logger.error(f"An error occurred: {er}")
        sys.exit(1)
