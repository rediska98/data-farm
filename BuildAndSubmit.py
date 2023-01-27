import os
import subprocess
import re
from glob import glob
from CONFIG import CONFIG
import requests
import json
import psycopg2


class TaskManagerClient:

    def __init__(self, BASE_URL):
        self.GET_JOBS = BASE_URL + "jobs/"

    def get_job(self, j_id):
        GET_JOB = self.GET_JOBS + j_id

        resp = requests.get(GET_JOB)
        if resp.status_code != 200:
            # This means something went wrong.
            raise Exception('GET {} {}'.format(GET_JOB, resp.status_code))

        return resp.json()

    def get_jobs(self):
        resp = requests.get(self.GET_JOBS)
        if resp.status_code != 200:
            # This means something went wrong.
            raise Exception('GET {} {}'.format(self.GET_JOBS, resp.status_code))

        return resp.json()


    @staticmethod
    def save_job_details(job_details, dest_file):
        with open(dest_file, "w") as fp:
            json.dump(job_details, fp)

    def get_last_job_details(self, persist_job_details=True, job_details_dest=None):
        jobs = self.get_jobs()
        print("Jobs history:", jobs)

        js_dets = [self.get_job(j["id"]) for j in jobs["jobs"]]

        last_job_details = max(js_dets, key = lambda j_d: j_d["end-time"])

        #job_details = self.get_job(jobs["jobs"][0]["id"])

        if persist_job_details and job_details_dest is not None:
            self.save_job_details(last_job_details, job_details_dest)

        return last_job_details



def update_sbt(jp_path, target_conf="compile"):
    # Read in the file
    with open(os.path.join(jp_path, CONFIG.BUILD_SBT), 'r') as file:
        filedata = file.read()

    # Replace the target string
    if target_conf == "compile":
        filedata = filedata.replace('val flinkConf = "provided"', f'val flinkConf = "{target_conf}"', 1)
    elif target_conf == "provided":
        filedata = filedata.replace('val flinkConf = "compile"', f'val flinkConf = "{target_conf}"', 1)
    else:
        print(f"WARNING - Invalid target conf '{target_conf}'. ")

    # Write the file out again
    with open(os.path.join(jp_path, CONFIG.BUILD_SBT), 'w') as file:
        file.write(filedata)


def get_job_projects():
    return [jf for jf in os.listdir(CONFIG.GENERATED_JOB_FOLDER) if re.match(r"Job[0-9]+", jf)]

def get_sql_job_projects():
    job_list = [jf for jf in os.listdir(CONFIG.GENERATED_JOB_FOLDER) if re.match(r"Job[0-9]+.*\.txt$", jf)]
    return [s.replace(".txt", "") for s in job_list]


def get_jar_path(job_path):
    return glob(f'{job_path}/target/*/*.jar')


def get_jars_path():
    return glob(f'{CONFIG.GENERATED_JOB_FOLDER}/*/target/*/*.jar')


def get_exec_plans_path():
    return glob(f'{CONFIG.GENERATED_JOB_OUTPUT_PLAN_PATH}/*.json')


#this preparation step needed since our generated sql jobs have SQL Server / MS Access syntax. We make them runnable on Postgres.
def prep_sql(raw_sql):
    query = re.subn(r"TOP [0-9]+ ", "", raw_sql)
    if query[1] != 0:
        match = re.findall(r"TOP [0-9]+", raw_sql)
        return query[0] + " LIMIT " + match[0].split()[1]
    else:
        return raw_sql

def run_sqls(job_projects):
    # connect to db
    conn = psycopg2.connect(
        host=CONFIG.PG_HOST,
        database=CONFIG.PG_DATABASE,
        user=CONFIG.PG_USER,
        password=CONFIG.PG_PASSWORD)
    cur = conn.cursor()
    # execute sqls
    for jp in job_projects:
        jp_path = os.path.join(CONFIG.GENERATED_JOB_FOLDER, jp + '.txt')
        print(jp_path)
        if os.path.isfile(jp_path):
            raw_sql = open(jp_path, "r")
            sql = prep_sql(raw_sql.read())
            try:
                cur.execute("EXPLAIN (ANALYZE TRUE)" + sql)
            except:
                print("Failed to execute SQL query")
                conn.rollback()
                continue
            else:
                conn.commit()
            plan = cur.fetchall()
            cardinality = re.search(r"actual .+ rows=(\d+)", plan[0][0]).group(1)
            exec_time = plan.pop()
            exec_time = exec_time[0].split(" ")[2]
            print(jp, " Execution Time:", exec_time)
            print("Writing to ", CONFIG.GENERATED_JOB_OUTPUT_PLAN_PATH)
            exec_time = float("%.4f" % (float(exec_time)))
            #escaping exec_times smaller than 1.0 because of future log()
            if exec_time <= 1.0:
                exec_time = 1.0001
            net_run_time = {"netRunTime": exec_time, "cardinality": cardinality}
            with open(CONFIG.GENERATED_JOB_OUTPUT_PLAN_PATH + jp + '$.json', 'w', encoding='utf-8') as nf:
                json.dump(net_run_time, nf, ensure_ascii=False, indent=2)
    # close connection
    cur.close()
    conn.close()

    return

def assembly(job_projects, run=False, flink_provided=True):
    for jp in job_projects:
        jp_path = os.path.join(CONFIG.GENERATED_JOB_FOLDER, jp)
        # subprocess.run(["sbt", f'"assembly {jf_path}"'])

        if not flink_provided:
            update_sbt(jp_path, target_conf="compile")

        if run:
            os.system(f'cd {jp_path}; sbt "run {CONFIG.GENERATED_JOB_INPUT_DATA_PATH} {CONFIG.GENERATED_JOB_OUTPUT_PLAN_PATH} exec {CONFIG.LOCAL} {CONFIG.LOCAL_HEAP} {CONFIG.PARALLELISM}"')
            os.system(f'cd {jp_path}; sbt clean clean-files')
            # os.system(f'find {jp_path} -name target -type d -exec rm -r {"{}"} \;')
            if os.path.isfile(os.path.join(jp_path, "build.sbt")):
                print("Cleaning target directories...")
                os.system(f'rm -r {os.path.join(jp_path, "project/project/target/")}*')
                os.system(f'rm -r {os.path.join(jp_path, "project/target/")}*')
                os.system(f'rm -r {os.path.join(jp_path, "target/")}*')
        else:
            print("WARNING - Assembly many jobs if Flink is not provided can be very memory intensive!")
            os.system(f"cd {jp_path}; sbt assembly")

        if not flink_provided:
            update_sbt(jp_path, target_conf="provided")

    paths = get_jars_path()
    return paths


def submit(job_projects):
    jars_path = []
    for jp in job_projects:
        jp_path = os.path.join(CONFIG.GENERATED_JOB_FOLDER, jp)

        print("Jar project path:", jp_path)

        jar_path = get_jar_path(jp_path)
        if jar_path.__len__() == 0:
            #print("WARNING - Assembly many jobs if Flink is not provided can be very memory intensive!")
            os.system(f"cd {jp_path}; sbt assembly")
            jar_path = get_jar_path(jp_path)

            print("jar_path:", jar_path)
            jar_path = jar_path[0]

        #Submit job to flink

        command_seq = [
            os.path.join(CONFIG.FLINK_HOME, "bin/flink"), 'run',
            '-c', str(jp), str(jar_path),
            str(CONFIG.GENERATED_JOB_INPUT_DATA_PATH),
            str(CONFIG.GENERATED_JOB_OUTPUT_PLAN_PATH),
            'exec', 'nolocal', '-1', '-1'
        ]

        print(subprocess.list2cmdline(command_seq))

        try:
            subprocess.run(command_seq, shell=False, timeout=10800)
        except subprocess.TimeoutExpired:
            print(f"Timeout for {jp} expired.")
            continue

        os.system(f'cd {jp_path}; sbt clean cleanFiles')

        # Get Task Manager last job details
        task_manager_client = TaskManagerClient(BASE_URL=CONFIG.FLINK_TASK_MANAGER_URL)
        task_manager_client.get_last_job_details(persist_job_details=True, job_details_dest=CONFIG.GENERATED_JOB_TASK_MANAGER_DETAILS_OUTPUT_PATH + f"{jp}-job_details.json")

        jars_path.append(jar_path)

    print("jars_path:", jars_path)
    return jars_path

def submit_spark(job_projects):
    jars_path = []
    for jp in job_projects:
        jp_path = os.path.join(CONFIG.GENERATED_JOB_FOLDER, jp)

        print("Jar project path:", jp_path)

        jar_path = get_jar_path(jp_path)
        if jar_path.__len__() == 0:
            #print("WARNING - Assembly many jobs if Flink is not provided can be very memory intensive!")
            try:
                subprocess.check_output(f"cd {jp_path}; sbt package", shell = True)
            except subprocess.CalledProcessError:
                print(f"Failed to compile {jp}.")
                continue
            jar_path = get_jar_path(jp_path)

            print("jar_path:", jar_path)

        #Submit job to spark

        command_seq = [
            os.path.join(CONFIG.SPARK_HOME, "bin/spark-submit"),
            '--master', str(CONFIG.SPARK_JOB_MASTER),
            '--driver-memory', str(CONFIG.SPARK_DRIVER_MEMORY),
            '--class', str(jp), str(jar_path[0]),
            str(CONFIG.GENERATED_JOB_INPUT_DATA_PATH),
            str(CONFIG.GENERATED_JOB_OUTPUT_PLAN_PATH)
        ]

        print(subprocess.list2cmdline(command_seq))

        try:
            subprocess.check_output(command_seq, shell=False)
        except subprocess.CalledProcessError:
            print(f"Timeout for {jp} expired. Or other exception.")
            continue

        os.system(f'cd {jp_path}; sbt clean cleanFiles')

        jars_path.append(jar_path)

    print("jars_path:", jars_path)
    return jars_path

def get_executed_plans():
    executed_plans_path = glob(f'{CONFIG.GENERATED_JOB_OUTPUT_PLAN_PATH}/*.json')
    data_id = os.path.dirname(CONFIG.GENERATED_JOB_OUTPUT_PLAN_PATH).split("/")[-1]
    executed_plans = {}
    for ep in executed_plans_path:
        ep_id = os.path.basename(ep).replace("$.json", "")
        with open(ep, "r") as f:
            ep_j = json.load(f)
        #trick to fix future log function
        if ep_j["netRunTime"] <= 1.0:
            ep_j["netRunTime"] = 1.0001
        executed_plans[(ep_id, data_id)] = ep_j

    return executed_plans


def job_id_v(s):
    s = s.replace("Job", "")
    ss = s.split("v")
    return int(ss[0]), int(ss[1])


def run_jobs(job_projects):
    if job_projects.__len__() == 0:
        print("All jobs already executed, remove filter to re-execute everything and override results.")
    else:
        if CONFIG.LOCAL == "local":
            print("WARNING - Running locally!!!")
            print(f"Running #{job_projects.__len__()} jobs:", job_projects)
            compiled_jars = assembly(job_projects, run=CONFIG.RUN, flink_provided=False)
            print("Compiled jars:", compiled_jars)
        else:
            print(f"Running #{job_projects.__len__()} jobs:", job_projects)
            submit(job_projects)

    return

def run_spark_jobs(job_projects):
    if job_projects.__len__() == 0:
        print("All jobs already executed, remove filter to re-execute everything and override results.")
    else:
        if CONFIG.LOCAL == "local":
            print(f"Running #{job_projects.__len__()} jobs:", job_projects)
            submit_spark(job_projects)

        else:
            print(f"TODO run Spark jobs non locally")

    return

def run_sql_jobs(job_projects):
    if job_projects.__len__() == 0:
        print("All jobs already executed, remove filter to re-execute everything and override results.")
    else:
        # I am going to keep decision-making between local and nonlocal for sql queries too
        if CONFIG.LOCAL == "local":
            print(f"Running #{job_projects.__len__()} jobs:", job_projects)
            run_sqls(job_projects)
        else:
            print(f"Running #{job_projects.__len__()} jobs:", job_projects)
            print("TODO Implement nonlocal sqls run")

    return


if __name__ == '__main__':
    exec_plans_path_already_computed = get_exec_plans_path()
    exec_plans_already_computed = {os.path.basename(ep).replace("$.json", "") for ep in
                                   exec_plans_path_already_computed}

    job_projects = get_job_projects()
    job_projects = sorted(job_projects, key=job_id_v)
    # print(f"Found #{job_projects.__len__()} jobs:", job_projects)

    # Filter jobs already executed
    job_projects = [jp for jp in job_projects if jp not in exec_plans_already_computed]
    print(f"Submitting #{job_projects.__len__()} jobs:", job_projects)
    # job_projects = [jp for jp in job_projects if jp in "Job2v1"]

    run_jobs(job_projects)
