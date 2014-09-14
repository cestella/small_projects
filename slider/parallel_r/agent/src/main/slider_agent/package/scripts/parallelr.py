import sys
from resource_management import *

class ParallelR(Script):
  def install(self, env):
    self.install_packages(env)

  def configure(self, env):
    import params
    env.set_params(params)

  def start(self, env):
    import params
    env.set_params(params)
    self.configure(env)
    process_cmd = format("{java64_home}/bin/java -Xmx{xmx_val} -Xms{xms_val} -classpath {app_root}/*:{additional_cp} com.thimbleware.jmemcached.Main --memory={memory_val} --port={port}")

    Execute(process_cmd,
        user=params.app_user,
        logoutput=False,
        wait_for_finish=False,
        pid_file=params.pid_file
    )

  def stop(self, env):
    import params
    env.set_params(params)

  def status(self, env):
    import params
    env.set_params(params)
    check_process_status(params.pid_file)
