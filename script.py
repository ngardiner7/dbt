'''
Takes a dbt project.yaml file from a dbt repository and generates a list of all
models with the following metadata:
- model_name
- schema
- path
- materialization
- dist key(s)
- sort key(s)
- number of times that specified model is incorrectly referenced in other models
  (e.g. not using {{ref('model_name')}})
'''

import yaml
import os
import pandas as pd

# define the path containing your dbt models
MODELS_PATH = ''


def get_yaml_dict():

    with open('dbt_project.yml', 'r') as f:
        dbt_dict = yaml.load(f, Loader=yaml.FullLoader)

        return dbt_dict.get("models")


def get_model_metadata(dbt_models, models_metadata, model_name="", path=""):

    contains_sql_file = True
    for model in dbt_models:
        # assumes any dictionary that isn't a "vars" attribute is a model
        # this will break if there are any other attributes that are a dictionary
        if model != "vars" and isinstance(dbt_models.get(model), dict):
            contains_sql_file = False
            # recursively iterate through all of the models until we get to a model that
            # doesn't contain a dictionary
            get_model_metadata(dbt_models.get(model), models_metadata, model, "{0}.{1}".format(path, model))

    if contains_sql_file:

        reformatted_path = get_reformatted_path(path)
        schema = get_schema(reformatted_path)
        models_metadata.append({'model_name': model_name
                      , 'schema': schema
                      , 'path': reformatted_path
                      , 'materialized': dbt_models.get('materialized')
                      , 'dist_key': dbt_models.get('dist')
                      , 'sort_key': dbt_models.get('sort')
                      , 'unique_key': dbt_models.get('unique_key')})


def get_schema(path):
    '''
    get schema from the path
    '''
    if path.count(".") == 0:
        return path
    else:
        return path[:path.index('.')]


def get_reformatted_path(path):
    '''
    remove the schema and any trailing/leading periods from the path
    '''
    reformatted_path = path[:path.rindex('.')+1]
    reformatted_path = reformatted_path.strip('.')
    return reformatted_path


def find_missing_refs(models_metadata):

    for model_metadata in models_metadata:

        table_name = model_metadata.get('model_name')
        schema = model_metadata.get('schema')
        # define the ways a model can be incorrectly reference
        # this will not perfectly catch everything e.g. if there are multiple spaces
        table_name_variants = ["from {0} ".format(table_name)
                             , "from {0}.{1} ".format(schema, table_name)
                             , "join {0} ".format(table_name)
                             , "join {0}.{1} ".format(schema, table_name)]

        model_metadata['num_incorrect_refs'] = get_count_missing_refs(table_name_variants)


def get_count_missing_refs(table_name_variants):
    '''
    iterate through every sql file in your models directory and search for incorrect
    reference of each model in other models
    '''
    count = 0

    for variant in table_name_variants:
        for root, dirs, files in os.walk(MODELS_PATH):
            for name in files:
                if name.endswith(".sql"):
                    with open(os.path.join(root, name),'r') as f:
                        data = f.read()
                        count += data.lower().count(variant)

    return count


def run():
    models_metadata = []
    dbt_dict = get_yaml_dict()

    get_model_metadata(dbt_dict, models_metadata)
    find_missing_refs(models_metadata)

    df = pd.DataFrame(models_metadata)
    df.to_csv('audit_results.csv', index=False)

run()
