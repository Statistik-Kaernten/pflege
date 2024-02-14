#IMPORT LIBRARIES
import psycopg2
import csv
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from scipy.stats import chi2_contingency

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

def connection(db, user, password):
  conn = psycopg2.connect(
  host="172.21.25.109",
  port="5432",
  database=db,
  user=user,
  password=password)
  cur = conn.cursor()
  print(f"CONNECTION TO {db} ESTABLISHED w USER {user}")
  return cur, conn

def pflege(db, user, password):
  cur, conn = connection(db, user, password)

  def fetch(query):
    cur.execute(query)
    tuples_list = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    df = pd.DataFrame(tuples_list, columns=colnames)

    print(df)
    print('NaN: ', df.isna().sum())
    
    df.dropna(inplace=True)

    df['no'] = df['pop'].astype(int) - df['pgb'].astype(int)
    df['yes'] = df['pgb'].astype(int)

    numpy_array = df[['yes', 'no']].to_numpy()

    res = chi2_contingency(numpy_array)

    print("Chi-square statistic:", res.statistic)
    print("p-value:", res.pvalue)



  #<>
  fetch(f'''SELECT 
                jahr,
                sum(pgb) AS pgb,
                sum(bev) AS pop,
                sum(pgb) / sum(bev) AS rate,
                bkz_id
            FROM 
                projekte.v_pflege_plz
            LEFT JOIN
                (SELECT 
                    id, 
                    plz_unique,
                    bkz_id 
                FROM
                    public.l_plz 
                WHERE 
                    plz_unique = '1') plz 
            ON
                projekte.v_pflege_plz.plz = plz.id
            WHERE
               jahr = 2022
            --AND
            --  bkz_id = '203'
            GROUP BY
                jahr,
                bkz_id
            ORDER BY 
                jahr
             --   bkz_id ASC;''')
                
  cur.close()
  conn.close()


def pflegestufe(db, user, password):
  cur, conn = connection(db, user, password)

  def fetch(query):
    cur.execute(query)
    tuples_list = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    df = pd.DataFrame(tuples_list, columns=colnames)
    df = df.groupby(['bkz_id', 'pflegestufe']).agg({'pgb': 'sum'})
    print(df)
    print(df['pgb'].sum())

  fetch(f'''SELECT 
                jahr,
                pflegestufe,
                plz,
                bkz_id,
                sum(anzahl) AS pgb
            FROM 
                projekte.t_pflege
            LEFT JOIN
                (SELECT 
                    id, 
                    plz_unique,
                    bkz_id 
                FROM
                    public.l_plz 
                WHERE 
                    plz_unique = '1') plz 
            ON
                projekte.t_pflege.plz = plz.id
            WHERE
               jahr = 2021
            GROUP BY
                jahr,
                bkz_id,
                plz,
                pflegestufe
            ORDER BY 
                jahr,
                pflegestufe,
                plz''')
                
  cur.close() 
  conn.close()

def main():
  pflege("statistik", "mwritz", "dbadmin")
  #pflegestufe("statistik", "mwritz", "dbadmin")


if __name__ == '__main__':
    main()
