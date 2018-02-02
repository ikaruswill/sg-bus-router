import pandas as pd
from sqlalchemy import create_engine

print('Loading tables')
db_conn = create_engine('sqlite:///sg-bus-router.db')
rt = pd.read_sql_table(table_name='bus_routes', con=db_conn)
bs = pd.read_sql_table(table_name='bus_stops', con=db_conn)

def main():
    # Fill NULL distances with 0
    print('Filling NA distances with 0.0')
    rt.fillna(value=0.0, inplace=True)

    # Find rows that are not mutually present
    print('Searching for invalid rows')
    rt.reset_index(inplace=True)
    merged = rt.reset_index().merge(bs, on='BusStopCode', how='left')
    invalid_rows = rt[merged.isnull().any(axis=1)]
    if not invalid_rows.empty:
        print('Found invalid rows')
        print(invalid_rows)
        new_rt = rt[~merged.isnull().any(axis=1)]
        print('Saving cleaned bus_routes table')
        new_rt.to_sql(name='bus_routes', con=db_conn, if_exists='replace',
                      index=False)
    else:
        print('No invalid rows found')

if __name__ == '__main__':
    main()
