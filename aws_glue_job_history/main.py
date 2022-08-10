import datetime
import sys

import boto3

def main():
    profile, region, start, help_flag = parse_args()
    if help_flag:
        print_help()
        return

    session = boto3_session(profile, region)
    glue_client = session.client("glue")
    history = fetch_job_history(glue_client, start)
    print_job_history(history)

def print_help():
    help_str = """
aws-glue-job-history [OPTIONS]

OPTION:
    --help
    --profile <AWS_PROFILE_NAME>
    --region <AWS_REGION_NAME>
    --start "YYYY-MM-DD HH:MM:SS"
        specify in UTC
""".strip()
    print(help_str)

def parse_args():
    help_flag = False
    profile = None
    region = None
    start = None
    argCount = len(sys.argv)
    i = 1
    while i < argCount:
        a = sys.argv[i]
        if a == "--profile":
            if i + 1 >= argCount:
                raise Exception(f"{a} needs parameter")
            i += 1
            profile = sys.argv[i]
        elif a == "--region":
            if i + 1 >= argCount:
                raise Exception(f"{a} needs parameter")
            i += 1
            region = sys.argv[i]
        elif a == "--start":
            if i + 1 >= argCount:
                raise Exception(f"{a} needs parameter")
            i += 1
            start_str = sys.argv[i]
            start = datetime.datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S").astimezone(datetime.timezone.utc)
        elif a == "--help":
            help_flag = True
        else:
            raise Exception(f"Unknown parameter: {a}")
        i += 1
    return [profile, region, start, help_flag]

def boto3_session(profile, region):
    session = boto3.session.Session(profile_name = profile, region_name = region)
    return session

def fetch_job_history(glue_client, start):
    jobs = []
    res = glue_client.get_jobs()
    while True:
        for elem in res["Jobs"]:
            jobs.append(elem["Name"])
        if "NextToken" not in res:
            break
        res = glue_client.get_jobs(NextToken=res["NextToken"])

    result = []

    for job_name in jobs:
        res = glue_client.get_job_runs(JobName=job_name)
        while True:
            end_flag = False
            for run in res["JobRuns"]:
                started = run["StartedOn"]
                if start is not None and started < start:
                    end_flag = True
                    break
                started_str = started.strftime("%Y-%m-%d %H:%M:%S")
                if "CompletedOn" in run:
                    completed_str = run["CompletedOn"].strftime("%Y-%m-%d %H:%M:%S")
                else:
                    completed_str = ""
                executionTime = str(run["ExecutionTime"])
                if executionTime == "0":
                    executionTime = ""
                status = run["JobRunState"]
                if "ErrorMessage" in run:
                    errorMessage = run["ErrorMessage"]
                else:
                    errorMessage = ""
                allocatedCapacity = str(run["AllocatedCapacity"])
                maxCapacity = str(run["MaxCapacity"])
                glueVersion = str(run["GlueVersion"])
                result.append([
                    started_str,
                    completed_str,
                    executionTime,
                    status,
                    job_name,
                    allocatedCapacity,
                    maxCapacity,
                    glueVersion,
                    errorMessage,
                ])
            if end_flag or "NextToken" not in res:
                break
            res = glue_client.get_job_runs(JobName=job_name, NextToken=res["NextToken"])

    result.sort(key = lambda r: r[0])

    return result

def print_job_history(history):
    header = [
        "started",
        "completed",
        "executionTime",
        "status",
        "name",
        "allocatedCapacity",
        "maxCapacity",
        "glueVersion",
        "errorMessage",
       ]
    print("\t".join(header))

    for h in history:
        print("\t".join(h))

if __name__ == "__main__":
    main()

