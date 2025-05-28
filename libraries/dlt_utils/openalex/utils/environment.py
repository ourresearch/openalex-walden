import os

def get_dbutils():
        try:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]
        except ImportError:
            from databricks.sdk import WorkspaceClient
            wc = WorkspaceClient()
            dbutils = wc.dbutils
        return dbutils

def get_env() -> str:
    """
    Utility method to get environment variable - This is expected to be set at cluster level.
    :return: value of environment - dev/test/prod.
    """
    try:
        env = os.environ.get("environment")

        if env is None or env == "":  # fail over to using workspace ids
            dbutils = get_dbutils()
            workspace_id = dbutils.entry_point.getDbutils().notebook().getContext().workspaceId().get()
            # WARNING: workspace ids are not stable and can change over time, specific to OurResearch environment
            if workspace_id == "3025117139199542":
                env = "dev"
            elif workspace_id == "3315557480496264":
                env = "prod"

        return lower(env)
    except Exception as e:
        print(f"Error retrieving environment: {e}")
        return None

def get_env_suffix() -> str:
    """
    Utility method to get suffix for environment which can be used in complete codebase. For prod no suffix applicable. Default value _dev if environment variable is not set.
    :return: value of environment suffix - _dev/_test etc. 
    """
    env = get_env()
    if(env is None or env =="" or env.lower() == "dev"):
        return "_dev"    
    elif (env.lower() == "prod"):
        return ""
    else:
        return "_" + env.lower()
            

"""
Utility method call to set environment specific variables for easy access in other scripts without redundant method calls.
"""
ENV = get_env()
ENV_SUFFIX = get_env_suffix()