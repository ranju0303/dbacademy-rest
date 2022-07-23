from dbacademy.dougrest.common import *
from typing import Optional, Union, Literal, List, Dict, Any


class DatabricksApi(dict):
    class Jobs(object):
        def __init__(self, databricks):
            self.databricks = databricks
            self.runs = DatabricksApi.Jobs.Runs(databricks)

        def _id(self, job, *, if_not_exists="error"):
            if isinstance(job, dict):
                if "job_id" in job:
                    job = job["job_id"]
                elif "name" in job:
                    job = job["name"]
                else:
                    raise ValueError(f"Job dict must have job_id or name: {job!r}")
            if isinstance(job, str):
                existing_jobs = self.get_all(job)
                if len(existing_jobs) == 0:
                    if if_not_exists == "error":
                        raise DatabricksApiException(f"No jobs with name={job!r} found.", 404)
                    elif if_not_exists == "ignore":
                        return None
                    else:
                        raise ValueError(
                            f"if_not_exists argument must be either 'ignore' or 'error'.  Found if_not_exists={if_not_exists!r}.")
                elif len(existing_jobs) == 1:
                    job = existing_jobs[0]["job_id"]
                else:
                    raise DatabricksApiException(f"Ambiguous command.  Multiple jobs with name={job!r} found.", 400)
            if isinstance(job, int):
                return job
            raise ValueError(
                f"DatabricksApi.jobs.delete(job): job must be id:int, name:str, job:dict.  Found: {job!r}.")

        def get(self, job, *, return_list=False, if_not_exists="error"):
            if isinstance(job, dict):
                if "job_id" in job:
                    job = job["job_id"]
                elif "name" in job:
                    job = job["name"]
                else:
                    raise ValueError(f"Job dict must have job_id or name: {job!r}")
            if isinstance(job, str):
                existing_jobs = self.get_all(job)
                if len(existing_jobs) == 0:
                    if if_not_exists == "error":
                        raise DatabricksApiException(f"No jobs with name={job!r} found.", 404)
                    elif if_not_exists == "ignore":
                        return existing_jobs if return_list else None
                    else:
                        raise ValueError(
                            f"if_not_exists argument must be either 'ignore' or 'error'.  Found if_not_exists={if_not_exists!r}.")
                elif return_list:
                    return existing_jobs
                elif len(existing_jobs) == 1:
                    return existing_jobs[0]
                else:
                    raise DatabricksApiException(f"Ambiguous command.  Multiple jobs with name={job!r} found.", 400)
            if isinstance(job, int):
                job = self.databricks.api("GET", "2.1/jobs/get", data={"job_id": job})
                return [job] if return_list else job
            raise ValueError(
                f"DatabricksApi.jobs.delete(job): job must be id:int, name:str, job:dict.  Found: {job!r}.")

        def list(self):
            return self.databricks.api("GET", "2.1/jobs/list").get('jobs', [])

        def list_by_name(self):
            results = {}
            for job in self.list():
                results.setdefault(job["settings"]["name"], []).append(job)
            return results

        def list_names(self):
            return self.list_by_name().keys()

        def get_all(self, name):
            return self.list_by_name().get(name, [])

        def exists(self, job):
            job = self.get(job, return_list=True, if_not_exists="ignore")
            return bool(job)

        def update(self, job):
            return self.databricks.api("POST", "2.1/jobs/update", job)

        def delete(self, job, *, if_not_exists="error"):
            if isinstance(job, str):
                existing_jobs = self.get_all(job)
                if not existing_jobs and if_not_exists == "error":
                    raise DatabricksApiException(f"No jobs with name={job!r} found.", 404)
                for j in existing_jobs:
                    self.delete(j)
                return len(existing_jobs)
            if isinstance(job, dict):
                job = job["job_id"]
            if not isinstance(job, int):
                raise ValueError(
                    f"DatabricksApi.jobs.delete(job): job must be id:int, name:str, job:dict.  Found: {job!s}.")
            if if_not_exists == "error":
                self.databricks.api("POST", "2.1/jobs/delete", data={"job_id": job})
                return 1
            elif if_not_exists == "ignore":
                try:
                    self.databricks.api("POST", "2.1/jobs/delete", data={"job_id": job})
                    return 1
                except DatabricksApiException as e:
                    if e.http_code == 404 and e.error_code == 'RESOURCE_DOES_NOT_EXIST':
                        return 0
                    elif e.http_code == 400 and e.error_code == 'INVALID_PARAMETER_VALUE' and "does not exist" in e.message:
                        return 0
                    else:
                        raise e
            else:
                raise ValueError(
                    f"if_not_exists argument must be either 'ignore' or 'error'.  Found if_not_exists={if_not_exists!r}.")

        def create_single_task_job(self, name, *, notebook_path=None, timeout_seconds=None, max_concurrent_runs=1,
                                   new_cluster=None, existing_cluster_id=None, if_exists="proceed", **args):
            task = {"task_key": name}
            if notebook_path:
                task["notebook_task"] = {"notebook_path": notebook_path}
            if new_cluster:
                task["new_cluster"] = new_cluster
            elif existing_cluster_id:
                task["existing_cluster_id"] = existing_cluster_id
            else:
                task["new_cluster"] = {
                    "num_workers": 0,
                    "spark_version": self.databricks.default_spark_version,
                    "spark_conf": {"spark.master": "local[*]"},
                    "node_type_id": self.databricks.default_machine_type,
                }
            return self.create_multi_task_job(name, [task],
                                              max_concurrent_runs=max_concurrent_runs,
                                              new_cluster=new_cluster,
                                              if_exists=if_exists, **args)

        def create_multi_task_job(self, name, tasks, *, max_concurrent_runs=1, new_cluster=None, if_exists="proceed",
                                  **args):
            spec = {
                "name": name,
                "max_concurrent_runs": 1,
                "format": "MULTI_TASK",
                "tasks": tasks,
            }
            spec.update(**args)
            if if_exists == "overwrite":
                self.delete(name, if_not_exists="ignore")
                return self.databricks.api("POST", "2.1/jobs/create", data=spec)["job_id"]
            elif not self.exists(name) or if_exists == "proceed":
                return self.databricks.api("POST", "2.1/jobs/create", data=spec)["job_id"]
            elif if_exists == "ignore":
                return None
            elif if_exists == "error":
                raise DatabricksApiException(f"Job with name={name!r} already exists", 409)
            else:
                raise ValueError(
                    f"if_exists argument must be one of 'ignore', 'proceed', or 'error'.  Found if_exists={if_exists!r}.")

        def run(self,
                job: Union[int, str, dict],
                idempotency_token: str = None,
                notebook_params: Dict[str, str] = None,
                jar_params: List[str] = None,
                python_params: List[str] = None,
                spark_submit_params: List[str] = None,
                python_named_params: Dict[str, Any] = None,
                if_not_exists: str = "error"
                ) -> Optional[dict]:
            job = self._id(job)
            spec = {"job_id": job}
            vars = locals()
            for param in ("idempotency_token", "notebook_params", "jar_params", "python_params", "spark_submit_params",
                          "python_named_params"):
                if vars[param]:
                    spec[param] = vars[param]
            if if_not_exists == "error":
                return self.databricks.api("POST", "2.1/jobs/run-now", data=spec)
            elif if_not_exists == "ignore":
                try:
                    return self.databricks.api("POST", "2.1/jobs/run-now", data=spec)
                except DatabricksApiException as e:
                    if e.http_code == 404 and e.error_code == 'RESOURCE_DOES_NOT_EXIST':
                        return None
                    elif e.http_code == 400 and e.error_code == 'INVALID_PARAMETER_VALUE' and "does not exist" in e.message:
                        return None
                    else:
                        raise e
            else:
                raise ValueError(
                    f"if_not_exists argument must be one of 'ignore', 'error', or 'create'.  Found if_not_exists={if_not_exists!r}.")

        class Runs(object):
            def __init__(self, databricks):
                self.databricks = databricks

            @staticmethod
            def _id(run: Union[int, dict]) -> int:
                if isinstance(run, dict):
                    if "run_id" in run:
                        run = run["run_id"]
                    else:
                        raise ValueError(f"Run definition missing run_id: {run!r}")
                if not isinstance(run, int):
                    raise ValueError(
                        f"DatabricksApi.jobs.runs._id(run): run must be run_id:int or run:dict.  Found: {run!r}.")
                return run

            def get(self, run: Union[int, dict], *, if_not_exists: str = "error") -> dict:
                run = self._id(run)
                return self.databricks.api("GET", "2.1/jobs/runs/get", data={"run_id": run})

            def get_output(self, run: Union[int, dict], *, if_not_exists: str = "error") -> dict:
                # Detect if it's a multi-task job with only 1 task.
                if isinstance(run, dict) and len(run.get("tasks", ())) == 1:
                    run = run["tasks"][0]["run_id"]
                else:
                    run = self._id(run)
                try:
                    return self.databricks.api("GET", "2.1/jobs/runs/get-output", data={"run_id": run})
                except DatabricksApiException as e:
                    if e.http_code == 400 and "Retrieving the output of runs with multiple tasks is not supported" in e.message:
                        tasks = self.databricks.jobs.runs.get(run).get("tasks", ())
                        if len(tasks) == 1:
                            return self.get_output(tasks[0])
                    raise e

            def list(self,
                     active_only: bool = False,
                     completed_only: bool = False,
                     job_id: int = None,
                     offset: int = 0,
                     limit: int = 25,
                     run_type: Optional[
                         Union[Literal["JOB_RUN"], Literal["WORKFLOW_RUN"], Literal["SUBMIT_RUN"]]] = None,
                     expand_tasks: bool = False,
                     start_time_from: int = None,
                     start_time_to: int = None,
                     ) -> list:
                params = ("active_only", "completed_only", "job_id", "offset", "limit", "run_type", "expand_tasks",
                          "start_time_from", "start_time_to")
                values = locals()
                spec = {k: values[k] for k in params if values[k] is not None}
                return self.databricks.api("GET", "2.1/jobs/runs/list", data=spec).get("runs", [])

            def delete(self, run: Union[int, dict], *, if_not_exists: str = "error") -> dict:
                run = self._id(run)
                if if_not_exists == "ignore":
                    return self.databricks.api("POST", "2.1/jobs/runs/delete", data={"run_id": run})
                else:
                    try:
                        return self.databricks.api("POST", "2.1/jobs/runs/delete", data={"run_id": run})
                    except DatabricksApiException as e:
                        raise (e)

            def cancel(self, run: Union[int, dict], *, if_not_exists: str = "error") -> dict:
                run = self._id(run)
                if if_not_exists == "ignore":
                    return self.databricks.api("POST", "2.1/jobs/runs/cancel", data={"run_id": run})
                else:
                    try:
                        return self.databricks.api("POST", "2.1/jobs/runs/cancel", data={"run_id": run})
                    except DatabricksApiException as e:
                        raise (e)

            def delete_all(self, runs: List[Union[int, dict]] = None) -> list:
                if runs is None:
                    if_not_exists = "ignore"
                    runs = self.list()
                if not runs:
                    return []
                from multiprocessing.pool import ThreadPool
                with ThreadPool(min(len(runs), 500)) as pool:
                    pool.map(lambda run: self.delete(run, if_not_exists="ignore"), runs)

            def cancel_all(self, runs: List[Union[int, dict]] = None) -> list:
                if runs is None:
                    if_not_exists = "ignore"
                    runs = self.list()
                if not runs:
                    return []
                from multiprocessing.pool import ThreadPool
                with ThreadPool(min(len(runs), 500)) as pool:
                    pool.map(lambda run: self.cancel(run, if_not_exists="ignore"), runs)

    class Users(object):
        def __init__(self, databricks):
            self.databricks = databricks

        def list(self):
            return self.databricks.api("GET", "2.0/preview/scim/v2/Users").get("Resources", [])

        def list_usernames(self):
            return sorted([u["userName"] for u in self.list()])

        def list_by_username(self):
            return {u["userName"]: u for u in self.list()}

        def get_by_id(self, id):
            return self.databricks.api("GET", f"2.0/preview/scim/v2/Users/{id}")

        def get_by_username(self, username):
            for u in self.list():
                if u["username"] == username:
                    return u

        def update(self, user):
            id = user["id"]
            return self.databricks.api("PATCH", f"2.0/preview/scim/v2/Users/{id}", data=user)

        def create(self, username, allow_cluster_create=True):
            entitlements = []
            if allow_cluster_create:
                entitlements.append({"value": "allow-cluster-create"})
                entitlements.append({"value": "allow-instance-pool-create"})
            data = {
                "schemas": [
                    "urn:ietf:params:scim:schemas:core:2.0:User"
                ],
                "userName": username,
                "entitlements": entitlements
            }
            return self.databricks.api("POST", "2.0/preview/scim/v2/Users", data=data)

        def delete_by_id(self, id):
            return self.databricks.api("DELETE", f"2.0/preview/scim/v2/Users/{id}")

        def delete_by_username(self, *usernames):
            user_id_map = {u['userName']: u['id'] for u in self.list()["Resources"]}
            for u in usernames:
                if u in user_id_map:
                    self.delete_by_id(user_id_map[u])

    class SCIM(object):
        def __init__(self, databricks):
            self.databricks = databricks
            self.groups = DatabricksApi.SCIM.Groups(databricks)

        class Groups(object):
            def __init__(self, databricks):
                self.databricks = databricks

            def list(self):
                return self.databricks.api("GET", "2.0/preview/scim/v2/Groups")["Resources"]

            def list_by_name(self):
                return {g["displayName"]: g for g in self.list()}

            def get(self, group_id=None, group_name=None):
                groups = self.list_by_name()
                if group_name:
                    return groups.get(group_name)
                elif id:
                    next((g for g in groups if g["id"] == group_id), None)
                else:
                    raise Exception("Must specify group_id or group_name")

            def add_entitlement(self, entitlement, group=None, group_id=None, group_name=None):
                if group_id:
                    pass
                elif group:
                    group_id = group["id"]
                elif group_name:
                    group_id = self.get(group_name=group_name)["id"]
                else:
                    raise Exception("Must provide group, group_id, or group_name")
                return self.databricks.api("PATCH", f"2.0/preview/scim/v2/Groups/{group_id}", data={
                    "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                    "Operations": [{"op": "add", "path": "entitlements", "value": [{"value": entitlement}]}]
                })

            def remove_entitlement(self, entitlement, group=None, group_id=None, group_name=None):
                if group_id:
                    pass
                elif group:
                    group_id = group["id"]
                elif group_name:
                    group_id = self.get(group_name=group_name)["id"]
                else:
                    raise Exception("Must provide group, group_id, or group_name")
                return self.databricks.api("PATCH", f"2.0/preview/scim/v2/Groups/{group_id}", data={
                    "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                    "Operations": [{"op": "remove", "path": f'entitlements[value eq "{entitlement}"]'}]
                })

            def allow_cluster_create(self, value=True, group=None, group_id=None, group_name=None):
                if value:
                    self.add_entitlement("allow-cluster-create", group=group, group_id=group_id, group_name=group_name)
                else:
                    self.remove_entitlement("allow-cluster-create", group=group, group_id=group_id,
                                            group_name=group_name)

    class Groups(object):
        def __init__(self, databricks):
            self.databricks = databricks

        def add_member(self, parent_name, *, user_name=None, group_name=None):
            """Add a user or group to the parent group."""
            data = {"parent_name": parent_name}
            if user_name:
                data["user_name"] = user_name
            elif group_name:
                data["group_name"] = group_name
            else:
                raise DatabricksApiException("Must provide user_name or group_name.")
            return self.databricks.api("POST", "2.0/groups/add-member", data=data)

        def create(self, group_name, *, if_exists="error"):
            data = {"group_name": group_name}
            if if_exists == "ignore":
                try:
                    return self.databricks.api("POST", "2.0/groups/create", data=data)
                except DatabricksApiException as e:
                    if e.http_code == 400 and e.error_code == 'RESOURCE_ALREADY_EXISTS':
                        return None
                    elif e.http_code == 500 and "already exists" in e.message:
                        return None
                    else:
                        raise e
            elif if_exists == "overwrite":
                try:
                    return self.databricks.api("POST", "2.0/groups/create", data=data)
                except DatabricksApiException as e:
                    if e.http_code == 400 and e.error_code == 'RESOURCE_ALREADY_EXISTS':
                        self.delete(group_name)
                        return self.databricks.api("POST", "2.0/groups/create", data=data)
                    elif e.http_code == 500 and "already exists" in e.message:
                        self.delete(group_name)
                        return self.databricks.api("POST", "2.0/groups/create", data=data)
                    else:
                        raise e
            else:
                return self.databricks.api("POST", "2.0/groups/create", data=data)

        def list(self):
            """List all groups."""
            return self.databricks.api("GET", "2.0/groups/list")["group_names"]

        def list_members(self, group_name=None):
            return self.databricks.api("GET", "2.0/groups/list-members", data={"group_name": group_name})["members"]

        def list_parents(self, *, user_name=None, group_name=None):
            if user_name:
                data = {"user_name": user_name}
            elif group_name:
                data = {"group_name": group_name}
            else:
                raise DatabricksApiException("Must provide user_name or group_name.")
            return self.databricks.api("GET", "2.0/groups/list-parents", data=data)["group_names"]

        def remove_member(self, parent_name, *, user_name=None, group_name=None):
            """Add a user or group to the parent group."""
            data = {"parent_name": parent_name}
            if user_name:
                data["user_name"] = user_name
            elif group_name:
                data["group_name"] = group_name
            else:
                raise DatabricksApiException("Must provide user_name or group_name.")
            return self.databricks.api("POST", "2.0/groups/remove-member", data=data)

        def delete(self, group_name, *, if_not_exists="error"):
            if if_not_exists == "ignore":
                try:
                    return self.databricks.api("POST", "2.0/groups/delete", data={"group_name": group_name})
                except DatabricksApiException as e:
                    if e.http_code == 404 and e.error_code == 'RESOURCE_DOES_NOT_EXIST':
                        return None
                    else:
                        raise e
            else:
                return self.databricks.api("POST", "2.0/groups/delete", data={"group_name": group_name})

    class Pools(object):
        def __init__(self, databricks):
            self.databricks = databricks

        def list(self):
            response = self.databricks.api("GET", "2.0/instance-pools/list")
            return response.get("instance_pools", [])

        def get_by_id(self, id):
            response = self.databricks.api("GET", f"2.0/instance-pools/get?instance_pool_id={id}")
            return response

        def get_by_name(self, name):
            return next((p for p in self.list() if p["instance_pool_name"] == name), None)

        def create(self, name, machine_type=None, min_idle=3):
            if machine_type == None:
                machine_type = self.databricks.default_machine_type
            data = {
                'instance_pool_name': name,
                'min_idle_instances': min_idle,
                'node_type_id': machine_type,
                'idle_instance_autotermination_minutes': 5,
                'enable_elastic_disk': True,
                'preloaded_spark_versions': [self.databricks.default_preloaded_versions],
            }
            response = self.databricks.api("POST", "2.0/instance-pools/create", data)
            return response["instance_pool_id"]

        def edit(self, pool, min_idle):
            if isinstance(pool, str):
                pool = self.get_by_id(pool)
            valid_keys = ['instance_pool_id', 'instance_pool_name', 'min_idle_instances',
                          'node_type_id', 'idle_instance_autotermination_minutes']
            data = {key: pool[key] for key in valid_keys}
            data["min_idle_instances"] = min_idle
            response = self.databricks.api("POST", "2.0/instance-pools/edit", data)
            return pool["instance_pool_id"]

        def edit_by_name(self, name, min_idle):
            pool = self.get_by_name(name)
            return self.edit(pool, min_idle)

        def edit_or_create(self, name, machine_type=None, min_idle=3):
            if machine_type == None:
                machine_type = self.databricks.default_machine_type
            pool = self.get_by_name(name)
            if pool:
                return self.edit(pool, min_idle)
            else:
                return self.create(name, machine_type, min_idle)

        def set_acl(self, instance_pool_id,
                    user_permissions: Dict[str,str] = {},
                    group_permissions: Dict[str,str] = {"users": "CAN_ATTACH_TO"}):
            data = {
                "access_control_list": [
                                           {
                                               "user_name": user_name,
                                               "permission_level": permission,
                                           } for user_name, permission in user_permissions.items()
                                       ] + [
                                           {
                                               "group_name": group_name,
                                               "permission_level": permission,
                                           } for group_name, permission in group_permissions.items()
                                       ]
            }
            return self.databricks.api(
                "PUT", f"2.0/preview/permissions/instance-pools/{instance_pool_id}", data)

        def add_to_acl(self, instance_pool_id,
                       user_permissions : Dict[str,str] = {},
                       group_permissions : Dict[str,str] = {"users": "CAN_ATTACH_TO"}):
            data = {
                "access_control_list": [
                                           {
                                               "user_name": name,
                                               "permission_level": permission,
                                           } for name, permission in user_permissions.items()
                                       ] + [
                                           {
                                               "group_name": name,
                                               "permission_level": permission,
                                           } for name, permission in group_permissions.items()
                                       ]
            }
            return self.databricks.api(
                "PATCH", f"2.0/preview/permissions/instance-pools/{instance_pool_id}", data)

    class Clusters(object):
        def __init__(self, databricks):
            self.databricks = databricks

        def get(self, id):
            return self.databricks.api("GET", "2.0/clusters/get", data={"cluster_id": id})

        def list(self):
            response = self.databricks.api("GET", "2.0/clusters/list")
            return response.get("clusters", [])

        def list_by_name(self):
            response = self.databricks.api("GET", "2.0/clusters/list")
            return {c["cluster_name"]: c for c in response.get("clusters", ())}

        def create(self, cluster_name, node_type_id=None, driver_node_type_id=None,
                   timeout_minutes=120, num_workers=0, num_cores="*", instance_pool_id=None, spark_version=None,
                   start=True, **cluster_spec):
            data = {
                "cluster_name": cluster_name,
                "spark_version": spark_version or self.databricks.default_spark_version,
                "autotermination_minutes": timeout_minutes,
                "num_workers": num_workers,
                "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
            }
            if self.databricks.cloud == "AWS":
                data["aws_attributes"] = {
                    "first_on_demand": 1,
                    "availability": "SPOT_WITH_FALLBACK",
                }
            elif self.databricks.cloud == "Azure":
                data["azure_attributes"] = {
                    "first_on_demand": 1,
                    "availability": "ON_DEMAND_AZURE",
                    "spot_bid_max_price": -1,
                }
            if instance_pool_id:
                data["instance_pool_id"] = instance_pool_id
            else:
                node_type_id = node_type_id or self.databricks.default_machine_type
                driver_node_type_id = driver_node_type_id or node_type_id
                data["node_type_id"] = node_type_id
                data["driver_node_type_id"] = driver_node_type_id
                data["enable_elastic_disk"] = "true"
            if num_workers == 0:
                data["spark_conf"] = {
                    "spark.databricks.cluster.profile": "singleNode",
                    "spark.master": f"local[{num_cores}]",
                }
                data["custom_tags"] = {"ResourceClass": "SingleNode"}
            data.update(cluster_spec)
            response = self.databricks.api("POST", "2.0/clusters/create", data)
            if not start:
                self.terminate(response["cluster_id"])
            data["cluster_id"] = response["cluster_id"]
            return data

        def update(self, cluster):
            response = self.databricks.api("POST", "2.0/clusters/edit", cluster)
            return response

        def edit(self, cluster_id, cluster_name=None, *, machine_type=None, driver_machine_type=None,
                 timeout_minutes=120, num_workers=0, num_cores="*", instance_pool_id=None,
                 spark_version=None, **cluster_spec):
            data = {
                "cluster_id": cluster_id,
                "spark_version": spark_version or self.databricks.default_spark_version,
                "autotermination_minutes": timeout_minutes,
                "num_workers": num_workers,
                "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
            }
            if cluster_name:
                data["cluster_name"] = cluster_name
            if self.databricks.cloud == "AWS":
                data["aws_attributes"] = {
                    "first_on_demand": 1,
                    "availability": "SPOT_WITH_FALLBACK",
                }
            elif self.databricks.cloud == "Azure":
                data["azure_attributes"] = {
                    "first_on_demand": 1,
                    "availability": "ON_DEMAND_AZURE",
                    "spot_bid_max_price": -1,
                }
            if instance_pool_id:
                data["instance_pool_id"] = instance_pool_id
            else:
                machine_type = machine_type or self.databricks.default_machine_type
                driver_machine_type = driver_machine_type or machine_type
                data["node_type_id"] = machine_type
                data["driver_node_type_id"] = driver_machine_type
                data["enable_elastic_disk"] = "true"
            if num_workers == 0:
                data["spark_conf"] = {
                    "spark.databricks.cluster.profile": "singleNode",
                    "spark.master": f"local[{num_cores}]",
                }
                data["custom_tags"] = {"ResourceClass": "SingleNode"}
            data.update(cluster_spec)
            response = self.databricks.api("POST", "2.0/clusters/edit", data)
            return cluster_id

        def start(self, id):
            data = {"cluster_id": id}
            response = self.databricks.api("POST", "2.0/clusters/start", data)
            return response

        def restart(self, id):
            data = {"cluster_id": id}
            response = self.databricks.api("POST", "2.0/clusters/restart", data)
            return response

        def terminate(self, id):
            data = {"cluster_id": id}
            response = self.databricks.api("POST", "2.0/clusters/delete", data)
            return response

        def delete(self, id):
            data = {"cluster_id": id}
            response = self.databricks.api("POST", "2.0/clusters/permanent-delete", data)
            return response

        def create_or_start(self, name, machine_type=None, driver_machine_type=None,
                            timeout_minutes=120, num_workers=2, num_cores="*", instance_pool_id=None,
                            existing_clusters=None, cluster_spec=None):
            if existing_clusters == None:
                existing_clusters = self.databricks.clusters.list()
            cluster = next((c for c in existing_clusters if c["cluster_name"] == name), None)
            if not cluster:
                return self.create(name, machine_type, driver_machine_type, timeout_minutes,
                                   num_workers, num_cores, instance_pool_id, cluster_spec)
            elif cluster["state"] == "TERMINATED":
                id = cluster["cluster_id"]
                self.edit(cluster_id=id,
                          cluster_name=name,
                          machine_type=machine_type,
                          driver_machine_type=driver_machine_type,
                          timeout_minutes=timeout_minutes,
                          num_workers=num_workers,
                          num_cores=num_cores,
                          instance_pool_id=instance_pool_id)
                self.start(id)
                return id
            else:
                return cluster["cluster_id"]

        def set_acl(self, cluster_id, user_permissions: Dict[str,str] = {}, group_permissions: Dict[str,str] ={}):
            data = {
                "access_control_list": [
                                           {
                                               "user_name": name,
                                               "permission_level": permission,
                                           } for name, permission in user_permissions.items()
                                       ] + [
                                           {
                                               "group_name": name,
                                               "permission_level": permission,
                                           } for name, permission in group_permissions.items()
                                       ]
            }
            return self.databricks.api("PUT", f"2.0/preview/permissions/clusters/{cluster_id}", data)

        def add_to_acl(self, cluster_id, user_permissions: Dict[str,str] = {}, group_permissions: Dict[str,str] = {}):
            data = {
                "access_control_list": [
                                           {
                                               "user_name": user_name,
                                               "permission_level": permission,
                                           } for user_name, permission in user_permissions.items()
                                       ] + [
                                           {
                                               "group_name": group_name,
                                               "permission_level": permission,
                                           } for group_name, permission in group_permissions.items()
                                       ]
            }
            return self.databricks.api("PATCH", f"2.0/preview/permissions/clusters/{cluster_id}", data)

    class Workspace(object):
        def __init__(self, databricks):
            self.databricks = databricks

        def list(self, workspace_path, sort_key=lambda f: f['path']):
            files = self.databricks.api("GET", "2.0/workspace/list", {"path": workspace_path}).get("objects", [])
            return sorted(files, key=sort_key)

        def list_names(self, workspace_path, sort_key=lambda f: f['path']):
            files = self.list(workspace_path, sort_key=sort_key)
            filenames = [f['path'] + ('/' if f['object_type'] == 'DIRECTORY' else '') for f in files]
            return filenames

        def walk(self, workspace_path, sort_key=lambda f: f['path']):
            """Recursively list files into an iterator.  Sorting within a directory is done by the provided sort_key."""
            for f in self.list(workspace_path, sort_key=sort_key):
                yield f
                if f['object_type'] == 'DIRECTORY':
                    yield from self.walk(f['path'])

        def mkdirs(self, workspace_path):
            self.databricks.api("POST", "2.0/workspace/mkdirs", {"path": workspace_path})

        def copy(self, source_path, target_path, *, target_connection=None, if_exists="overwrite", exclude=set(),
                 dry_run=False):
            source_connection = self.databricks
            if target_connection == None:
                target_connection = source_connection
            # Strip trailing '/', except for root '/'
            source_path = source_path.rstrip("/")
            target_path = target_path.rstrip("/")
            if not source_path:
                source_path = "/"
            if not target_path:
                target_path = "/"
            if source_path in exclude:
                print("skip", source_path, target_path)
                return
            print("copy", source_path, target_path)
            # Try to copy the entire directory at once
            try:
                bytes = source_connection.workspace.export(workspace_path=source_path, format="DBC").get("content",
                                                                                                         None)
                if bytes and not dry_run:
                    target_connection.workspace.import_from_data(workspace_path=target_path, content=bytes,
                                                                 format="DBC", if_exists=if_exists)
                return
            except DatabricksApiException as e:
                cause = e
                if "exceeded the limit" in e.message:
                    pass
                elif "Subtree size exceeds" in e.message:
                    pass
                elif source_path.endswith("/Trash") and "RESOURCE_DOES_NOT_EXIST":
                    return  # Skip trash folders
                elif "BAD_REQUEST: Cannot serialize item" in e.message:
                    return  # Can't copy MLFow experiments this way.  Skip it.
                elif "BAD_REQUEST: Cannot serialize library" in e.message:
                    return  # Can't copy libraries this way.  Skip it.
                else:
                    raise e
            # If the size limit was exceeded for a file (not a directory) raise the error
            source_files = source_connection.workspace.list_names(workspace_path=source_path)
            if source_files == [source_path]:
                raise cause
            # If the size limit was exceeded for a directory, copy the directory item by item to break it into smaller chunks
            prefix_len = len(source_path)
            if source_path == "/":
                prefix_len = 0
            target_connection.workspace.mkdirs(target_path)
            for s in sorted(source_files):
                t = target_path + s[prefix_len:]
                self.copy(source_path=s,
                          target_path=t,
                          target_connection=target_connection,
                          if_exists=if_exists,
                          exclude=exclude)

        def compare(self, source_path, target_path, target_connection=None, compare_contents=False):
            """
            Recursively compare the filenames and types, but not contents of the files.
            Yields files that don't have a match on the other side.

            compare_contents is a no-op currently and doesn't change anything.

            This methods signature and behavior is subject to change.  Maintainers are invited to improve it.
            """
            source_connection = self.databricks
            if target_connection == None:
                target_connection = self
            # Strip training '/', except for root '/'
            source_path = source_path.rstrip("/")
            target_path = target_path.rstrip("/")
            if not source_path:
                source_path = "/"
            if not target_path:
                target_path = "/"
            # Compare the tree contents
            source_iter = source_connection.workspace.walk(source_path)
            target_iter = target_connection.workspace.walk(target_path)
            source_prefix_len = len(source_path)
            target_prefix_len = len(target_path)
            try:
                s = next(source_iter)
                t = next(target_iter)
                while True:
                    s_name = s['path'][source_prefix_len:]
                    t_name = t['path'][target_prefix_len:]
                    if s_name < t_name:
                        yield (s, None)
                        s = next(source_iter)
                    elif s_name > t_name:
                        yield (None, t)
                        t = next(target_iter)
                    else:
                        if s['object_type'] != t['object_type'] or s.get('language', None) != t.get('language', None):
                            yield (s, t)
                        s = next(source_iter)
                        t = next(target_iter)
            except StopIteration as e:
                from itertools import zip_longest
                yield from zip_longest(source_iter, [], fillvalue=None)
                yield from zip_longest([], target_iter, fillvalue=None)

        def exists(self, workspace_path):
            try:
                self.list(workspace_path)
                return True
            except DatabricksApiException as e:
                if e.error_code == "RESOURCE_DOES_NOT_EXIST":
                    return False
                else:
                    raise e

        def is_empty(self, workspace_path):
            files = self.list(workspace_path)
            if len(files) == 1:
                return files[0]['path'].endswith("/Trash")
            else:
                return not files

        def delete(self, workspace_path, recursive=True):
            self.databricks.api("POST", "2.0/workspace/delete", {
                "path": workspace_path,
                "recursive": "true" if recursive else "false"
            })

        def read_data_from_url(self, source_url, format="DBC"):
            import requests, base64
            r = requests.get(source_url)
            if format == "DBC":
                content = base64.b64encode(r.content).decode("utf8")
            elif format == "SOURCE":
                content = r.text
            else:
                raise DatabricksApiException("Unknown format.")
            return content

        def import_from_url(self, source_url, workspace_path, format="DBC", *, if_exists="error"):
            content = self.read_data_from_url(source_url)
            self.import_from_data(content, workspace_path, format, if_exists=if_exists)

        def import_from_data(self, content, workspace_path, format="DBC", *, language=None, if_exists="error"):
            data = {
                "content": content,
                "path": workspace_path,
                "format": format,
                "language": language,
            }
            try:
                return self.databricks.api("POST", "2.0/workspace/import", data)
            except DatabricksApiException as e:
                if e.error_code != "RESOURCE_ALREADY_EXISTS":
                    raise e
                else:
                    if if_exists == "overwrite":
                        self.delete(workspace_path)
                        return self.databricks.api("POST", "2.0/workspace/import", data)
                    elif if_exists == "ignore":
                        pass
                    elif if_exists == "error":
                        raise e
                    else:
                        print("Invalid if_exists: " + if_exists)
                        raise e

        def export(self, workspace_path, format="DBC"):
            data = {
                "path": workspace_path,
                "format": format,
            }
            return self.databricks.api("GET", "2.0/workspace/export", data)

    class Repos(object):
        def __init__(self, databricks):
            self.databricks = databricks

        def list(self):
            repos = self.databricks.api("GET", "2.0/repos").get("repos", [])
            return repos

        def list_by_path(self):
            repos = self.list()
            results = {r['path']: r for r in repos}
            return results

        def exists(self, workspace_path):
            return workspace_path in self.list_by_path()

        def create(self, url, path=None, provider="gitHub", *, if_exists="error"):
            data = {
                "url": url,
                "path": path,
                "provider": provider
            }
            #        for k,v in data.items():
            #          if v is None:
            #            del data[k]
            self.databricks.api("POST", "2.0/repos", data)

        def delete(self, repo=None, id=None, workspace_path=None):
            if repo:
                id = repo["id"]
            elif id:
                pass
            elif workspace_path:
                repo = self.list_by_path().get(workspace_path)
                if not repo:
                    raise DatabricksApiException("Repo not found at: " + workspace_path)
                id = repo["id"]
            self.databricks.api("DELETE", f"2.0/repos/{id}")
            return id

    class MLFlow(object):
        def __init__(self, databricks):
            self.databricks = databricks
            self.registered_models = self.RegisteredModels(databricks)
            self.model_versions = self.ModelVersions(databricks)

        class RegisteredModels(object):
            def __init__(self, databricks):
                self.databricks = databricks

            def list(self, *, models_per_page=None):
                page_token = None
                while True:
                    response = self.databricks.api("GET", "2.0/mlflow/registered-models/list", {
                        "max_results": models_per_page,
                        "page_token": page_token,
                    })
                    page_token = response.get("next_page_token")
                    yield from response["registered_models"]
                    if not page_token:
                        return

            def create(self, name, description=None, tags={}):
                return self.databricks.api("POST", "2.0/mlflow/registered-models/create", {
                    "name": name,
                    "description": description,
                    "tags": tags
                })

            def rename(self, name, new_name):
                return self.databricks.api("POST", "2.0/mlflow/registered-models/create", {
                    "name": name,
                    "new_name": new_name,
                })

            def update(self, model):
                return self.databricks.api("PATCH", "2.0/mlflow/registered-models/update", model)

            def delete(self, model, *, force=False):
                while force:
                    model = self.get(model["name"])
                    force = False
                    for v in model.get("latest_versions", ()):
                        if v.get("current_stage") in ("Production", "Staging"):
                            self.databricks.mlflow.model_versions.transition_stage(model["name"], v["version"],
                                                                                   "Archived")
                            force = True
                return self.databricks.api("DELETE", "2.0/mlflow/registered-models/delete", {
                    "name": model["name"]
                })

            def get(self, name):
                return self.databricks.api("GET", "2.0/mlflow/registered-models/get", {
                    "name": name,
                }).get("registered_model")

            def search(self, filter, order_by, *, models_per_page=None):
                page_token = None
                while True:
                    response = self.databricks.api("GET", "2.0/mlflow/registered-models/list", {
                        "filter": filter,
                        "order_by": order_by,
                        "max_results": models_per_page,
                        "page_token": page_token,
                    })
                    page_token = response.get("next_page_token")
                    yield from response["registered_models"]
                    if not page_token:
                        return

        class ModelVersions(object):
            def __init__(self, databricks):
                self.databricks = databricks

            def transition_stage(self, name, version, new_stage, *, archive_existing_versions=None):
                if archive_existing_versions == None:
                    archive_existing_versions = new_stage in ("Production", "Staging")
                return self.databricks.api("POST", "2.0/mlflow/model-versions/transition-stage", {
                    "name": name,
                    "version": version,
                    "stage": new_stage,
                    "archive_existing_versions": archive_existing_versions,
                })

    class Secrets(object):
        def __init__(self, databricks):
            self.databricks = databricks

        def create_scope(self, name, initial_manage_principal):
            """
            Create a Databricks-backed secret scope in which secrets are stored in Databricks-managed storage and encrypted with a cloud-based specific encryption key.

            >>> self.create_scope("my-simple-databricks-scope", "users")
            """
            pass

    class Sql(object):
        def __init__(self, databricks):
            self.databricks = databricks

        def get_by_id(self, id):
            return self.databricks.api("GET", f"2.0/sql/endpoints/{id}")

        def get_by_name(self, name):
            return next((ep for ep in self.list() if ep["name"] == name), None)

        def list(self):
            response = self.databricks.api("GET", "2.0/sql/endpoints/")
            return response.get("endpoints", [])

        def list_by_name(self):
            endpoints = self.list()
            return {e["name"]: e for e in endpoints}

        def create(self, name, size="XSMALL", min_num_clusters=1, max_num_clusters=1, timeout_minutes=120,
                   photon=False, spot=False, preview_channel=False):
            data = {
                "name": name,
                "size": size,
                "min_num_clusters": min_num_clusters,
                "max_num_clusters": max_num_clusters,
                "spot_instance_policy": "COST_OPTIMIZED" if spot else "RELIABILITY_OPTIMIZED",
                "enable_photon": str(bool(photon)).lower(),
            }
            if preview_channel:
                data["channel"] = {"name": "CHANNEL_NAME_PREVIEW"}
            if timeout_minutes and timeout_minutes > 0:
                data["auto_stop_mins"] = timeout_minutes
            response = self.databricks.api("POST", "2.0/sql/endpoints/", data)
            return response["id"]

        def edit(self, endpoint):
            id = endpoint["id"]
            response = self.databricks.api("POST", f"2.0/sql/endpoints/{id}/edit", endpoint)
            return response

        def edit_by_name(self, name, size="XSMALL", min_num_clusters=1, max_num_clusters=1, timeout_minutes=120,
                         photon=False, spot=False, preview_channel=False):
            raise Exception("Untested, function.  Test me first.")
            data = {
                "name": name,
                "size": size,
                "min_num_clusters": min_num_clusters,
                "max_num_clusters": max_num_clusters,
                "spot_instance_policy": "COST_OPTIMIZED" if spot else "RELIABILITY_OPTIMIZED",
                "enable_photon": str(bool(photon)).lower()
            }
            if timeout_minutes and timeout_minutes > 0:
                data["auto_stop_mins"] = timeout_minutes
            if preview_channel:
                data["channel"] = {"name": "CHANNEL_NAME_PREVIEW"}
            response = self.databricks.api("POST", f"2.0/sql/endpoints/{id}/edit", data)
            return response["id"]

        def start(self, id):
            response = self.databricks.api("POST", f"2.0/sql/endpoints/{id}/start")
            return response

        def stop(self, id):
            response = self.databricks.api("POST", f"2.0/sql/endpoints/{id}/stop")
            return response

        def delete(self, id):
            response = self.databricks.api("DELETE", f"2.0/sql/endpoints/{id}")
            return response

    default_machine_types = {
        "AWS": "i3.xlarge",
        "Azure": "Standard_DS3_v2",
        "GCP": "n1-standard-4",
    }

    def __init__(self, hostname=None, *, token=None, user=None, password=None, authorization_header=None, cloud="AWS",
                 deployment_name=None):
        super().__init__(self)
        """If a password is given then the token is ignored."""
        import requests
        if hostname:
            self.url = f'https://{hostname}/api/'
        else:
            from dbacademy import dbgems
            self.url = dbgems.get_notebooks_api_endpoint() + "/api/"
            print("************", self.url)
        if authorization_header:
            pass
        elif token is not None:
            authorization_header = 'Bearer ' + token
        elif user is not None and password is not None:
            import base64
            encoded_auth = (user + ":" + password).encode()
            authorization_header = "Basic " + base64.standard_b64encode(encoded_auth).decode()
        else:
            from dbacademy import dbgems
            token = dbgems.get_notebooks_api_token()
            authorization_header = 'Bearer ' + token
        self.session = requests.Session()
        self.session.headers = {'Authorization': authorization_header, 'Content-Type': 'text/json'}
        self.cloud = cloud
        self.user = user
        self["deployment_name"] = deployment_name
        self.default_machine_type = DatabricksApi.default_machine_types[self.cloud]
        self.default_preloaded_versions = ["10.4.x-cpu-ml-scala2.12", "10.4.x-cpu-scala2.12"]
        self.default_spark_version = self.default_preloaded_versions[0]
        self.scim = DatabricksApi.SCIM(self)
        self.jobs = DatabricksApi.Jobs(self)
        self.users = DatabricksApi.Users(self)
        self.groups = DatabricksApi.Groups(self)
        self.clusters = DatabricksApi.Clusters(self)
        self.pools = DatabricksApi.Pools(self)
        self.workspace = DatabricksApi.Workspace(self)
        self.repos = DatabricksApi.Repos(self)
        self.mlflow = DatabricksApi.MLFlow(self)
        self.sql = DatabricksApi.Sql(self)

    def api(self, method, path, data={}):
        from json import JSONDecodeError
        from simplejson.errors import JSONDecodeError as SimpleJSONDecodeError
        response = self.api_raw(method, path, data)
        try:
            return response.json()
        except JSONDecodeError as e:
            e2 = DatabricksApiException(e.msg, 500)
            e2.__cause__ = e
            e2.cause = e
            e2.response = response
            raise e2
        except SimpleJSONDecodeError as e:
            e2 = DatabricksApiException(e.msg, 500)
            e2.__cause__ = e
            e2.cause = e
            e2.response = response
            raise e2

    def api_raw(self, method, path, data={}):
        import requests, pprint, json
        if method == 'GET':
            translated_data = {k: DatabricksApi._translate_boolean_to_query_param(data[k]) for k in data}
            resp = self.session.request(method, self.url + path, params=translated_data)
        else:
            resp = self.session.request(method, self.url + path, data=json.dumps(data))
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            message = e.args[0]
            try:
                reason = pprint.pformat(json.loads(resp.text), indent=2)
                message += '\n Response from server: \n {}'.format(reason)
            except ValueError:
                pass
            if 400 <= e.response.status_code < 500:
                raise DatabricksApiException(http_exception=e)
            else:
                raise requests.exceptions.HTTPError(message, response=e.response)
        return resp

    @staticmethod
    def _translate_boolean_to_query_param(value):
        assert not isinstance(value, list), 'GET parameters cannot pass list of objects'
        if isinstance(value, bool):
            if value:
                return 'true'
            else:
                return 'false'
        return value

    # @property
    # def workspace_config(self):
    #     driver = getattr(getattr(sc._jvm, "com.databricks.backend.common.util.Project$Driver$"), "MODULE$")
    #     empty = sc._jvm.com.databricks.conf.Configs.empty()
    #     dbHome = sc._jvm.com.databricks.conf.StaticConf.DB_HOME()
    #     configFile = sc._jvm.com.databricks.conf.trusted.ProjectConf.loadLocalConfig(driver, empty, False, dbHome)
    #     driverConf = sc._jvm.com.databricks.backend.daemon.driver.DriverConf(configFile)
    #     return driverConf
    #
    # @property
    # def execution_context(self):
    #     return dbutils.entry_point.getDbutils().notebook().getContext()
    #
    # @property
    # def cloud_provider(self):
    #     return self.workspace_config.cloudProvider().get()
    #
    # @property
    # def cluster_id(self):
    #     return self.execution_context.clusterId().get()
