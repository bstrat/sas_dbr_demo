# Databricks notebook source
# MAGIC %md
# MAGIC #### Unicode Fractions
# MAGIC 
# MAGIC In this notebook we test the processing of [Unicode](https://home.unicode.org) characters by playing around with [glyphs used for displaying fractions](https://en.wikipedia.org/wiki/Number_Forms).
# MAGIC 
# MAGIC First we create a *SAS* session and redefine `dbDISPLAY` - for details see the `Classy Cars` notebook. 

# COMMAND ----------

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
# MAGIC Then we extract the required fraction glyphs by parsing - here we utilize `curl` and `awk` on the command line with the `%sh` directive - the javascript library [math.js](https://github.com/lights0123/fractions/blob/master/math.js) from the [Unicode Fraction Creator](https://lights0123.com/fractions/) site, store the information in CSV format...

# COMMAND ----------

# MAGIC %sh
# MAGIC curl https://raw.githubusercontent.com/lights0123/fractions/master/math.js | \
# MAGIC awk -F'\x27' -v OFS=';' ' # \x27 is single quote
# MAGIC  BEGIN { print "ASCII;UTF8;TYPE" }
# MAGIC  /superscript/ {T="P"}
# MAGIC  /subscript/   {T="B"}
# MAGIC  /fractions/   {T="F"}
# MAGIC  NF>4 {print $2, $4, T}
# MAGIC ' > /dbfs/FileStore/tables/sas_dbr_demo/ms.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ... and load it to the `ms` Pandas data frame. Note, that `TYPE='F'` contains the few fractions available as as a symbol. All other fractions have to be constructed from superscripts `TYPE='P'` and subscripts `TYPE='B'` together with the [FRACTION SLASH](https://util.unicode.org/UnicodeJsps/character.jsp?a=2044) character `⁄`.

# COMMAND ----------

import pandas as pd
ms = pd.read_csv("/dbfs/FileStore/tables/sas_dbr_demo/ms.csv", sep=';', encoding='utf-8')
display(ms)

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, create the Pandas data frame `uf` with all reduced fractions between 0 and 1 with denomiator ≤12 ...

# COMMAND ----------

import math
D = 12
rs = [] # rational numbers 
fs = [] # fraction strings
ns = [] # numerators
ds = [] # denominators

def u8(ascii, type): # Receive UTF8 from `ms` given ASCII and TYPE  
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
        fs += [f"{u8(n,'P')}⁄{u8(d,'B')}"] # Note the FRACTION SLASH character

uf = pd.DataFrame({ 'r': rs, 'f': fs, 'n': ns, 'd': ds })
display(uf)


# COMMAND ----------

# MAGIC %md
# MAGIC ... and create a *SQL* query in *SAS*. Note, that we have to use [National Language Support (NLS) functions](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/nlsref/p18bboh5zrwqw5n1kkhonig4jpwq.htm) like `unicodewidth` and `kindex` (column `xs` is the index of the FRACTION SLASH); `length` will mislead us here as it shows the byte length `bl` which for example is different for `½` and `⅓` depending on their *UTF-8* representation.

# COMMAND ----------

uf_sas = sas.df2sd( uf, 'uf' )
sas.submitLST("""
 proc sql; select *, length(f) AS bl, unicodewidth(f) AS uw, kindex(f,'⁄') AS xs from uf; quit;
""")


# COMMAND ----------

# MAGIC %md
# MAGIC Showing all our reduced fractions in a `PROC SGPLOT` bubble plot.
# MAGIC For some reason only *SVG* output format renders all fractions correctly, if using any image format like *PNG* or *JPG* the 3 fractions `⅐`, `⅑` and `⅒` cannot be displayed.

# COMMAND ----------

sas.submitLST('''
ods html5 (id=saspy_internal) file=stdout options(svg_mode='inline') device=svg style=Dove;
ods graphics on / outputfmt=jpg;
title "Reduced fractions";
proc sgplot data=uf noborder;
  bubble x=d y=n size=r /
    fillattrs=graphdata1 bradiusmin=2px bradiusmax=10px nooutline
    datalabel=f datalabelpos=right datalabelattrs=(family="Arial Unicode MS" size=12);
  xaxis values=(2 to 12 by 1) grid display=(noline noticks nolabel);
  yaxis values=(1 to 11 by 1) grid display=(noline noticks nolabel);
run;
''')
# Arial Unicode MS
# Times New Roman Uni

# COMMAND ----------


