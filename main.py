import sys
from typing import List

from engine.static import Project, Task
from projects.AGAM import AGAM
from projects.Las_Rozas import LasRozas



def run_task(project_name: Project, task_name:Task, extra_args: List[str]) -> None:
    project = getattr(sys.modules[__name__], project_name.value)(extra_args)
    getattr(project, task_name.value)()


if __name__ == '__main__':
    try:
        project_name: Project = Project[sys.argv[1]]
        task_name: Task = Task[sys.argv[2]]

    except IndexError as index_error:
        raise (Exception("Arguments required ([] is optional): project_name task_name [shapefile] [year month day]")
               .with_traceback(index_error.__traceback__))
    except KeyError as key_error:
        raise (Exception(f"The only possible projects are {[p.value for p in Project]}. "
                         f"The only possible tasks are {[t.value for t in Task]}.")
               .with_traceback(key_error.__traceback__))

    extra_args: List[str] = sys.argv[3:]

    run_task(project_name, task_name, extra_args)





