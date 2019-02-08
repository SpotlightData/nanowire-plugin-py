from nanowire_plugin.single_file_tools import task_handler

@task_handler
def my_analysis(metadata, jsonld, url):
    print("testing this python function")
    return jsonld
