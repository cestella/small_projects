from resource_management import *

config = Script.get_config()

app_root = config['configurations']['global']['app_root']
java64_home = config['hostLevelParams']['java_home']
app_user = config['configurations']['global']['app_user']
pid_file = config['configurations']['global']['pid_file']

additional_cp = config['configurations']['global']['additional_cp']
xmx_val = config['configurations']['global']['xmx_val']
xms_val = config['configurations']['global']['xms_val']
memory_val = config['configurations']['global']['memory_val']
