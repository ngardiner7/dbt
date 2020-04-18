Takes a dbt project.yaml file from a dbt repository and generates a list of all models with the following metadata:
- model_name
- schema
- path
- materialization
- dist key(s)
- sort key(s)
- number of times that specified model is incorrectly referenced in other models (e.g. not using {{ref('model_name')}})

# H2 Installation
```
(in your DBT virtualenv)
$ pip install pandas
$ pip install pyaml

(copy script into the same directory as your dbt_project.yml file)
```

# H2 Configuration

define the PATH global variable

# H2 Use
```
$ python3 script.py
```

A file named audit_results.csv containing a list of all models & specified metadata will be saved in your directory
