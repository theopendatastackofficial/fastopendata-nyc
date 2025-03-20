"""

This is a script that inspect the structure of some tables in the nyc open data namespace
(available by default in the Bauplan public sanbox) and show how to launch queries programmatically.

This code has been written with Bauplan client: 0.0.3a342

If the API changed, or you need help, please get in touch and we'll be super happy to help: https://www.bauplanlabs.com/

The Bauplan Team, March 2025


---

This folder is self-contained:

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python query_playground.py

---

"""

import bauplan
# global Bauplan client
# we assume you have bauplan installed and a working API key configured
bauplan_client = bauplan.Client()
# namespace hosting the tables
BAUPLAN_TABLE_NAMESPACE = 'nyc_open_data'
TABLE = 'mta_subway_hourly_ridership'
FQN = f'{BAUPLAN_TABLE_NAMESPACE}.{TABLE}'
BRANCH = 'main'
# print the schema of the table
print(bauplan_client.get_table(FQN, BRANCH))
# run a query and print the results
query = f"SELECT * FROM {FQN} LIMIT 10"
# this will return an Arrow table by default
_table = bauplan_client.query(query)
print(_table)