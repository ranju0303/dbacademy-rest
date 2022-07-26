from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.rest.common import ApiContainer


class JobsClient(ApiContainer):
    def __init__(self, client: DBAcademyRestClient):
        self.client = client      # Client API exposing other operations to this class

    def create(self, params):
        if "notebook_task" in params:
            print("*"*80)
            print("* DEPRECATION WARNING")
            print("* You are using the Jobs 2.0 version of create as noted by the existence of the notebook_task parameter.")
            print("* Please upgrade to the 2.1 version.")
            print("*"*80)
            return self.create_2_0(params)
        else:
            return self.create_2_1(params)

    def create_2_0(self, params):
        return self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/jobs/create", params)

    def create_2_1(self, params):
        return self.client.execute_post_json(f"{self.client.endpoint}/api/2.1/jobs/create", params)

    def get(self, job_id):
        return self.client.execute_get_json(f"{self.client.endpoint}/api/2.0/jobs/get?job_id={job_id}")

    def run_now(self, job_id: str, notebook_params: dict = None):
        payload = {
            "job_id": job_id
        }
        if notebook_params is not None:
            payload["notebook_params"] = notebook_params

        return self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/jobs/run-now", payload)

    def delete_by_job_id(self, job_id):
        return self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/jobs/delete", {"job_id": job_id})

    def list(self):
        response = self.client.execute_get_json(f"{self.client.endpoint}/api/2.0/jobs/list")
        return response.get("jobs", list())

    def delete_by_name(self, jobs, success_only):
        if type(jobs) == dict:
            job_list = list(jobs.keys())
        elif type(jobs) == list:
            job_list = jobs
        elif type(jobs) == str:
            job_list = [jobs]
        else:
            raise Exception(f"Unsupported type: {type(jobs)}")

        jobs = self.list()

        deleted = 0
        # s = "s" if len(jobs) != 1 else ""
        # print(f"Found {len(jobs)} job{s} total")

        for job_name in job_list:
            for job in jobs:
                if job_name == job["settings"]["name"]:
                    job_id = job["job_id"]

                    runs = self.client.runs().list_by_job_id(job_id)
                    # s = "s" if len(runs) != 1 else ""
                    # print(f"Found {len(runs)} run{s} for job {job_id}")
                    delete_job = True

                    for run in runs:
                        state = run.get("state")
                        result_state = state.get("result_state", None)
                        life_cycle_state = state.get("life_cycle_state", None)

                        if success_only and life_cycle_state != "TERMINATED":
                            delete_job = False
                            print(f""" - The job "{job_name}" was not "TERMINATED" but "{life_cycle_state}", this job must be deleted manually""")
                        if success_only and result_state != "SUCCESS":
                            delete_job = False
                            print(f""" - The job "{job_name}" was not "SUCCESS" but "{result_state}", this job must be deleted manually""")

                    if delete_job:
                        print(f"Deleting job #{job_id}, \"{job_name}\"")
                        for run in runs:
                            run_id = run.get("run_id")
                            print(f""" - Deleting run #{run_id}""")
                            self.client.runs().delete(run_id)

                        self.delete_by_job_id(job_id)
                        deleted += 1

        print(f"Deleted {deleted} jobs")
