# sas_dbr_demo

A demonstration on how to work with *SAS* in *Databricks* using the *SASPy* module. It contains two Databricks notebooks:

* [Classy Cars](https://github.com/bstrat/sas_dbr_demo/blob/main/ipynb/Classy%20Cars.ipynb) - shows the application of analytical tools in *SAS*, *Python* and *R* and the transfer between the respective environments using two well known datasets: *SAS*' [`sashelp.class`](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.4/statug/statug_sashelp_sect009.htm) data set and *R*'s [`mtcars`](https://www.rdocumentation.org/packages/datasets/versions/3.6.2/topics/mtcars) data frame.
* [Unicode Fractions](https://github.com/bstrat/sas_dbr_demo/blob/main/ipynb/Unicode%20Fractions.ipynb) - demonstrates the correct processing of *Unicode* characters in *SAS* when being transferred from *Python* via Pandas. Actually, there is an issue in `PROC SGPLOT` when using the default *PNG* output format - [*SVG* format works](https://github.com/bstrat/sas_dbr_demo/blob/main/ipynb/Unicode%20Fractions%20SVG.ipynb).
