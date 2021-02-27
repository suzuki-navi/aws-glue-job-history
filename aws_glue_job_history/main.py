import sys
import boto3

def main():
    profile, region = parse_args()
    session = boto3_session(profile, region)
    glue_client = session.client("glue")
    print_job_history(glue_client)

def parse_args():
    profile = None
    region = None
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
        i += 1
    return [profile, region]

def boto3_session(profile, region):
    session = boto3.session.Session(profile_name = profile, region_name = region)
    return session

def print_job_history(glue_client):
    jobs = glue_client.get_jobs()

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

    result = []

    for job in jobs["Jobs"]:
        name = job["Name"]
        history = glue_client.get_job_runs(JobName = name)
        for run in history["JobRuns"]:
            started = run["StartedOn"].strftime("%Y-%m-%d %H:%M:%S")
            if "CompletedOn" in run:
                completed = run["CompletedOn"].strftime("%Y-%m-%d %H:%M:%S")
            else:
                completed = ""
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
                started,
                completed,
                executionTime,
                status,
                name,
                allocatedCapacity,
                maxCapacity,
                glueVersion,
                errorMessage,
            ])

    result.sort(key = lambda r: r[0])

    print("\t".join(header))
    for r in result:
        print("\t".join(r))

