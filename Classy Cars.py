# Databricks notebook source
# MAGIC %md
# MAGIC ## 1. Start a *SAS* session & work with it 
# MAGIC #### 1.1 Show connection details
# MAGIC The connection we choose here is communicating via ssh, see [Configuration](https://sassoftware.github.io/saspy/configuration.html) for details.

# COMMAND ----------

# MAGIC %sh awk '/^ssh_sas/,/}/' /databricks/sasconfig/sascfg_personal.py

# COMMAND ----------

# MAGIC %md
# MAGIC ####1.2 Start the SAS session
# MAGIC *SAS* code is submitted from Python through the [*SASPy*](https://sassoftware.github.io/saspy/) module using the `SASsession` object `sas` which is instantiated below. 

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
# MAGIC Note, we redefined the *SASPy* function `dbDISPLAY` as we experienced issues with the scope of the original definition - see below where it's located.

# COMMAND ----------

# MAGIC %sh awk '/dbDISPLAY|displayHTML/ {print NR, $0}' /databricks/python3/lib/python3.8/site-packages/saspy/sasbase.py

# COMMAND ----------

# MAGIC %md
# MAGIC ####1.3 Submit SAS code
# MAGIC The `submitLST` method sends a string of *SAS* code to the *SAS* session and returns the result delivered from the [SAS Output Delivery System (ODS)](https://documentation.sas.com/doc/en/dispatch/9.4/ods.htm). Here we show the content of the [Class Data](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/statug/statug_sashelp_sect009.htm)  `sashelp.class`.

# COMMAND ----------

sas.submitLST("PROC PRINT DATA=sashelp.class; RUN;")

# COMMAND ----------

# MAGIC %md
# MAGIC *SASPy* contains convenience functions, which already generate the necessary *SAS* code under the hood - e.g. `describe` and `hist` below are called on the [SAS data object](https://sassoftware.github.io/saspy/api.html#sas-data-object).

# COMMAND ----------

cs = sas.sasdata('class', 'sashelp')
print(cs.describe())
cs.hist('Height')

# COMMAND ----------

# MAGIC %md
# MAGIC The generated *SAS* code can be inspected with the `teach_me_SAS` method of the *SAS* session object.

# COMMAND ----------

sas.teach_me_SAS(1)
print(cs.describe())
cs.hist('Height')
sas.teach_me_SAS(0)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1.4 Write data to the Databricks spark cluster.
# MAGIC 
# MAGIC For this we create the database `sas_dbr_demo` - unless it exists - and potentially drop the `class` table from it...

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS sas_dbr_demo;
# MAGIC DROP TABLE IF EXISTS sas_dbr_demo.class;

# COMMAND ----------

# MAGIC %md
# MAGIC ...then we connect *SAS* to this database - here via *JDBC*, also *ODBC* is possible - by creating the `sdd` library. Note, that `submitLST` will return the *SAS* log if no output for *ODS* is created.

# COMMAND ----------

sas.submitLST("""
option set=SAS_ACCESS_CLASSPATH="/opt/sas/drivers";
libname sdd jdbc driverclass="com.simba.spark.jdbc.Driver"
url="jdbc:spark://adb-6447497411580861.1.azuredatabricks.net:443/default;transportMode=http;
ssl=1;httpPath=sql/protocolv1/o/6447497411580861/1019-182657-tjwinew;AuthMech=3;
UID=token;PWD=dapi4371523961bf74ea5e9eaf3a6f303abc-3"  readbuff=10000 schema="sas_dbr_demo";
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Eventually, we create the table `class` in the database `sas_dbr_demo` using library `sdd`, show it's contents ...

# COMMAND ----------

sas.submitLST("""
DATA sdd.class; SET sashelp.class; RUN;
PROC CONTENTS DATA=sdd.class; RUN;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ... and show it via Databricks' *SQL*. The result can be shown as a plot, e.g. resembling the histogram generated with *SAS* above

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sas_dbr_demo.class;

# COMMAND ----------

# MAGIC %md
# MAGIC Note that `class` is automatically stored as a Delta table of type `MANAGED`:

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED sas_dbr_demo.class;

# COMMAND ----------

# MAGIC %md
# MAGIC ####1.5 Directly transfer `class` to a Pandas data frame `cp`
# MAGIC 
# MAGIC by calling the `to_df()` method on `cs` - the Python variable representing the `sashelp.class` *SAS* data set ...

# COMMAND ----------

import pandas as pd
cp = cs.to_df()
display(cp)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can use the whole stack of python libraries for analyzing the `cp` data frame - e.g. creating a [linear model plot with *Seaborn*](https://seaborn.pydata.org/generated/seaborn.lmplot.html)

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

sns.lmplot(data=cp,
           x='Height',
           y='Weight',
           hue='Sex',
           markers=['^', '+'],
           palette='inferno',
           height=7,
           aspect=1.3)
plt.xlabel('Height', fontsize=14, labelpad=20)
plt.ylabel('Weight', fontsize=14)
plt.title('The relationship between Height and Weight', fontsize=20)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's store the table once more - this time as `class_p` - taking it from the Pandas data frame `cp`.

# COMMAND ----------

# https://docs.microsoft.com/en-us/azure/databricks/spark/latest/spark-sql/spark-pandas
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.get("spark.sql.execution.arrow.fallback.enabled") # Should be "true"

st = spark.sql(f"show tables from sas_dbr_demo")
if st[st.tableName=='class_p'].count() < 1:
  save_path = '/FileStore/tables/sas_dbr_demo/class_p'
  spark.createDataFrame(cp).write.format('delta').save(save_path)
  spark.sql("CREATE TABLE sas_dbr_demo.class_p USING DELTA LOCATION '" + save_path + "'")

# COMMAND ----------

# MAGIC %md
# MAGIC This time we explicitely gave a save location, resulting in type `EXTERNAL`.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED sas_dbr_demo.class_p;

# COMMAND ----------

# MAGIC %md
# MAGIC ##2. Work in *R*
# MAGIC ####2.1 Use the Class Data
# MAGIC First we create a Spark data frame `cs` from Delta table `sas_dbr_demo.class` aka `sashelp.class`...

# COMMAND ----------

# MAGIC %r
# MAGIC library(SparkR)
# MAGIC sparkR.session()
# MAGIC cs = tableToDF("sas_dbr_demo.class")
# MAGIC display(cs)

# COMMAND ----------

# MAGIC %md
# MAGIC ... then we fit the linear model `cm` for `Weight` based on `Height` and `Sex`.
# MAGIC Note, `lm` expects an *R* data frame; therefore we use `collect`
# MAGIC on the *SparkDataFrame* `cs` to convert it to an *R* data frame.

# COMMAND ----------

# MAGIC %r
# MAGIC print(cs)
# MAGIC cm <- lm(Weight~Height + Sex, data=collect(cs))
# MAGIC summary(cm)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2.2 Use *R*'s `mtcars` data set
# MAGIC 
# MAGIC Create and display *Spark* data frame `mtc` from `mtcars`, and additionally show the help entry for `mtcars`. 

# COMMAND ----------

# MAGIC %r
# MAGIC mtc = createDataFrame(mtcars)
# MAGIC display(mtc)
# MAGIC help(mtcars)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2.3 Fit linear model for `mtcars`
# MAGIC 
# MAGIC This time we first split the data frame into training sample `mtc_trn` and test sample `mtc_tst`, fit the linear model `mtc_mod` on the training data and show the model summary.

# COMMAND ----------

# MAGIC %r
# MAGIC data(mtcars)
# MAGIC set.seed(42)                                              # setting seed to reproduce results of random sampling
# MAGIC mtc_rix <- base::sample(1:nrow(mtcars), 0.7*nrow(mtcars)) # row indices for 70% training sample
# MAGIC mtc_trn <- mtcars[mtc_rix, ]                              # model training data
# MAGIC mtc_tst <- mtcars[-mtc_rix, ]                             # test data
# MAGIC mtc_mod <- lm(mpg ~ wt+hp+am, data=mtc_trn)               # build the model
# MAGIC summary(mtc_mod)

# COMMAND ----------

# MAGIC %md
# MAGIC With `mtc_pred`, the predictions on the test data, we then calculate R squared of the test data and the residuals of actual agains predicted values.

# COMMAND ----------

# MAGIC %r
# MAGIC mtc_prd <- predict(mtc_mod, mtc_tst)                       # predict miles/gallon
# MAGIC act_prd <- data.frame(cbind(act=mtc_tst$mpg, prd=mtc_prd)) # data frame actuals vs predictions
# MAGIC print( cor(act_prd)^2 )                                    # r.squared of test data
# MAGIC act_prd$rsd <- act_prd$act - act_prd$prd                   # add residuals
# MAGIC display(act_prd)

# COMMAND ----------

# MAGIC %md
# MAGIC Eventually we store the `mtc_(all|trn|tst)` data frames in database `sas_dbr_demo` using the same name, respectively.

# COMMAND ----------

# MAGIC %r
# MAGIC st = sql(paste("show tables from sas_dbr_demo"))
# MAGIC if ( count(filter(st, st$tableName=='mtc_all')) < 1 ) {
# MAGIC   save_path = '/FileStore/tables/sas_dbr_demo/mtc/all'
# MAGIC   write.df(mtc, source = 'delta', path = save_path)
# MAGIC   sql(paste("CREATE TABLE sas_dbr_demo.mtc_all USING DELTA LOCATION '", save_path, "'", sep = ""))
# MAGIC }
# MAGIC if ( count(filter(st, st$tableName=='mtc_trn')) < 1 ) {
# MAGIC   save_path = "/FileStore/tables/sas_dbr_demo/mtc/trn"
# MAGIC   write.df(createDataFrame(mtc_trn), source="delta", path=save_path)
# MAGIC   sql(paste("CREATE TABLE sas_dbr_demo.mtc_trn USING DELTA LOCATION '", save_path, "'", sep = ""))
# MAGIC }
# MAGIC if ( count(filter(st, st$tableName=='mtc_tst')) < 1 ) {
# MAGIC   save_path = "/FileStore/tables/sas_dbr_demo/mtc/tst"
# MAGIC   write.df(createDataFrame(mtc_tst), source="delta", path=save_path)
# MAGIC   sql(paste("CREATE TABLE sas_dbr_demo.mtc_tst USING DELTA LOCATION '", save_path, "'", sep = ""))
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Analysis of `mtcars` in SAS
# MAGIC ####3.1 Visualize `mpg` against `cyl`
# MAGIC using the `heatmap` method

# COMMAND ----------

sas.submitLST('''
proc sql; 
  create table mtc as select * from sdd.mtc_all;
  select count(*) from mtc;
quit;
''')
mtc = sas.sasdata('mtc', 'work')
mtc.heatmap('mpg', 'cyl')

# COMMAND ----------

# MAGIC %md
# MAGIC ####3.2 Create a linear model
# MAGIC resembling the *R* regression model from **2.3** with `proc reg` and storing the score code in `mtc_prd.sas` in the *SAS* work folder.

# COMMAND ----------

sas.submitLST('''
proc reg data=sdd.mtc_trn outest=mtc_est tableout;
  model mpg = wt hp am;
  code file="%sysfunc(GETOPTION(WORK))/mtc_prd.sas";
run;
''')

# COMMAND ----------

# MAGIC %md
# MAGIC ####3.3 Score the test sample `mtc_tst`
# MAGIC using the score code `mtc_prd.sas` generated from `proc reg` above, store the predictions as `sas_dbr_demo.mtc_tst_p`...

# COMMAND ----------

sas.submitLOG('''
data sdd.mtc_tst_p (keep=mpg P_mpg);
  set sdd.mtc_tst;
  %include "%sysfunc(GETOPTION(WORK))/mtc_prd.sas";
run;
''')

# COMMAND ----------

# MAGIC %md
# MAGIC ...and calculate the R squared, this time using *R*. 

# COMMAND ----------

# MAGIC %r
# MAGIC print( cor( collect(tableToDF("sas_dbr_demo.mtc_tst_p")) )^2 )

# COMMAND ----------

# MAGIC %md
# MAGIC ####3.4 Take a look at the score code `mtc_prd.sas`
# MAGIC First store the code in *SAS* data set `mtc_prd`, load it to a Pandas data frame with the same name and finally print it.

# COMMAND ----------

sas.submitLST('''
data mtc_prd;
  infile "%sysfunc(GETOPTION(WORK))/mtc_prd.sas";
  input;
  code = _infile_;
run;
proc sql; select count(*) from mtc_prd; quit;
''')
mtc_prd = sas.sasdata('mtc_prd', 'work')
print( mtc_prd.to_df()['code'].str.cat(sep='\n') )


# COMMAND ----------

# MAGIC %md
# MAGIC ##4. Annex
# MAGIC Statements for dropping all stored tables, if we want to start from scratch...

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sas_dbr_demo.mtc_all;
# MAGIC DROP TABLE IF EXISTS sas_dbr_demo.mtc_trn;
# MAGIC DROP TABLE IF EXISTS sas_dbr_demo.mtc_tst;
# MAGIC DROP TABLE IF EXISTS sas_dbr_demo.mtc_tst_p;
# MAGIC DROP TABLE IF EXISTS sas_dbr_demo.class;
# MAGIC DROP TABLE IF EXISTS sas_dbr_demo.class_p;
# MAGIC DROP DATABASE sas_dbr_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ...and remove the storage for table type `EXTERNAL` in the file system.

# COMMAND ----------

# MAGIC %fs rm -r /FileStore/tables/sas_dbr_demo
