from nanowire_plugin.single_file_tools import task_handler

@task_handler
def my_analysis(metadata, jsonld, url):
    raise Exception("ayylmaooooooo")
    return jsonld
