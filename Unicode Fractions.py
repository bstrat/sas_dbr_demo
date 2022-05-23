# Databricks notebook source
import saspy
# Start a SAS Session using config file
sas = saspy.SASsession(
  cfgfile = '/databricks/sasconfig/sascfg_personal.py',
  cfgname = 'ssh_sas',
  display = 'databricks')
# Redefine `sas.DISPLAY`
def dbDISPLAY(x):
  displayHTML(x)
sas.DISPLAY = dbDISPLAY

# COMMAND ----------

# MAGIC %md
# MAGIC Parse the javascript [math.js](https://github.com/lights0123/fractions/blob/master/math.js) from the [Unicode Fraction generator](https://lights0123.com/fractions/) to CSV format.

# COMMAND ----------

# MAGIC %sh
# MAGIC curl https://raw.githubusercontent.com/lights0123/fractions/master/math.js | \
# MAGIC awk -F'\x27' -v OFS=';' '
# MAGIC  BEGIN { print "ASCII;UTF8;TYPE" }
# MAGIC  /superscript/ {T="P"}
# MAGIC  /subscript/   {T="B"}
# MAGIC  /fractions/   {T="F"}
# MAGIC  NF>4 {print $2, $4, T}
# MAGIC ' > /dbfs/FileStore/tables/sas_dbr_demo/ms.csv

# COMMAND ----------

import pandas as pd
ms = pd.read_csv("/dbfs/FileStore/tables/sas_dbr_demo/ms.csv", sep=';', encoding='utf-8')
display(ms)

# COMMAND ----------

import math
D = 12
rs = [] # rational numbers 
fs = [] # fraction strings
ns = [] # numerators
ds = [] # denominators

def su(ascii, type):
  return ''.join( [ms.query(f"ASCII=='{c}' & TYPE=='{type}'").UTF8.iloc[0] for c in str(ascii)] )

for d in list(range(2,D+1)):
  for n in range(1,d):
    if math.gcd(d,n)==1:
      rs += [n/d]
      ds += [d]
      ns += [n]
      u = ms.query(f"ASCII=='{n}/{d}'")
      if not u.empty:
        fs += [u.UTF8.iloc[0]]
      else:
        fs += [f"{su(n,'P')}⁄{su(d,'B')}"] # Note the special "slash"

uf = spark.createDataFrame(pd.DataFrame({ 'r': rs, 'f': fs, 'n': ns, 'd': ds }))
display(uf)


# COMMAND ----------

uf_sas = sas.df2sd( uf.toPandas(), 'uf' )
sas.submitLST('''
 proc transpose;
 proc sql; select * from uf; quit;
''')

# COMMAND ----------


