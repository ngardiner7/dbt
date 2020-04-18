Given a set of U.S. cities (i.e. weather stations) and climate measures, scrape monthly 1981-2010 climate normals from the NOAA's National Centers for Environmental Information (NCEI).

# H1 Installation
> (in your DBT virtualenv)
> $ pip install pandas
> $ pip install pyaml
>
> (copy script into the same directory as your dbt_project.yml file)

# H1 Configuration

define the PATH global variable

# H1 Use
> $ python3 script.py

A file named audit_results.csv containing a list of all models & specified metadata will be saved in your directory
