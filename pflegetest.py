#IMPORT LIBRARIES
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from scipy.stats import chi2_contingency

#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)

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

def pflegeprognose(db, user, password):
  cur, conn = connection(db, user, password)

  query1 = '''SELECT 
                jahr,
                gruppe_20,
                geschlecht,
                bkz_id,
                SUM(pgb) AS pgb,
                SUM(bev) AS bev,
                SUM(pgb)/SUM(bev)*100 AS rate
            FROM 
                projekte.v_pflege_plz
            LEFT JOIN
                public.v_plz ON
                    projekte.v_pflege_plz.plz = public.v_plz.id
            WHERE 
                jahr = 2022
            GROUP BY
                jahr,
                gruppe_20,
                geschlecht,
                bkz_id
                ;'''
    
  cur.execute(query1)
  tuples_list = cur.fetchall()
  colnames = [desc[0] for desc in cur.description]
  df = pd.DataFrame(tuples_list, columns=colnames)
  #df['id'] = df['geschlecht'] + df['bkz_id'] + df['gruppe_20']
  df.drop(columns=['pgb', 'bev', 'jahr'], axis=1, inplace=True)
  df['geschlecht'] = df['geschlecht'].map(lambda x: '1' if (x == 'männlich') else '2')
  df['rate'] = df['rate'].astype(float)
  query2  = '''
            SELECT
                jahr,
                CAST(LEFT(gkz, 3) AS INT) AS bkz_id,
                geschlecht,
                gruppe_20,
                SUM(pop)
            FROM
                public.t_einzeljahre_prog
            LEFT JOIN
                projekte.l_pflege_alter ON
                    CAST (projekte.l_pflege_alter.id AS INT) = public.t_einzeljahre_prog.alter
            GROUP BY
                bkz_id,
                geschlecht,
                gruppe_20,
                jahr
            '''
  cur.execute(query2)
  tuples_list = cur.fetchall()
  colnames = [desc[0] for desc in cur.description]
  df2 = pd.DataFrame(tuples_list, columns=colnames)
 
  df2['bkz_id'] = df2['bkz_id'].astype(object)
  #df2['id'] = df2['geschlecht'].astype(str) + df2['bkz_id'].astype(str) + df2['gruppe_20'].astype(str)
  full_df = df.merge(df2, on=['bkz_id', 'geschlecht', 'gruppe_20'],  how='right')
  #print(df)
  #df.to_csv("C:/Users/mwritz/Documents/VisualStudioCode/pflege/df.csv", sep=";", index=False)
  #print(df2)
  #df2.to_csv("C:/Users/mwritz/Documents/VisualStudioCode/pflege/df2.csv", sep=";", index=False)
  #print(df)
  print(full_df)

  #print(df['pgb'].sum())

  cur.close()

  conn.close()



def main():
  #pflege("statistik", "mwritz", "dbadmin")
  #pflegestufe("statistik", "mwritz", "dbadmin")
  pflegeprognose("statistik", "mwritz", "dbadmin")
  pass


if __name__ == '__main__':
    main()
