%let pgm =utl-converting-sas-proc-rank-to-wps-python-r-sql;

Converting sas proc rank to python wps r sql and native python
Problem: Rank the hourly wages for mechanics by state

SOAPBOX ON
Is sql and sas/wps more readable than this native python solution?
This native python does not have the bigest python issue.
The elephant in the room is automatic changes in datatypes and data structures.
Luckily this solution outputs a dataframe.
AOAPBOX OFF

NATIVE PYTHON
=============

%utl_submit_py64_310("
import xport;
import xport.v56;
import pandas as pd;
mylist= list(range(1,322));
test1 = pd.DataFrame({'wage': mylist});
test1['rank'] = test1['wage'].rank();
test1['state'] = round(test1['rank']*(1000-1)/(len(test1['wage'])+1));
test1.info();
print(test1);
");

SQL
===
select;
   wage;
  ,rank () over (
        order by wage desc
  ) as wgernk;
from;
  have;

      SOLUTIONS

      1 SAS proc rank
      2 WPS proc r
      3 WPS proc sql
      4 Python sql
      5 WPS proc python

github
https://tinyurl.com/8n87tm42
https://github.com/rogerjdeangelis/utl-converting-sas-proc-rank-to-wps-python-r-sql

StackOverflow
https://tinyurl.com/e7437tpr
https://stackoverflow.com/questions/64889028/proc-rank-alternative-python

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

libname sd1 "d:/sd1";
data sd1.have;
  do states="AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MS";
    wage=int(50*(uniform(4321)))+ 1;
    output;
  end;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* Hourly wage for mechanics by state                                                                                     */
/*                                                                                                                        */
/*  SD1.HAVE total obs=20 01MAY2023:16:27:52                                                                              */
/*                                                                                                                        */
/*  Obs    STATES     WAGE                                                                                                */
/*                                                                                                                        */
/*    1      AL        12                                                                                                 */
/*    2      AK        38                                                                                                 */
/*    3      AZ        33                                                                                                 */
/*    4      AR        44                                                                                                 */
/*    5      CA        13                                                                                                 */
/*    6      CO        13                                                                                                 */
/*    7      CT        11                                                                                                 */
/*    8      DE        22                                                                                                 */
/*    9      FL         8                                                                                                 */
/*   10      GA        32                                                                                                 */
/*   11      HI        14                                                                                                 */
/*   12      ID        33                                                                                                 */
/*   13      IL        26                                                                                                 */
/*   14      IN        16                                                                                                 */
/*   15      IA        16                                                                                                 */
/*   16      KS         9                                                                                                 */
/*   17      KY        48                                                                                                 */
/*   18      LA        10                                                                                                 */
/*   19      ME        31                                                                                                 */
/*   20      MS        37                                                                                                 */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*                                                      _
 ___  __ _ ___   _ __  _ __ ___   ___   _ __ __ _ _ __ | | __
/ __|/ _` / __| | `_ \| `__/ _ \ / __| | `__/ _` | `_ \| |/ /
\__ \ (_| \__ \ | |_) | | | (_) | (__  | | | (_| | | | |   <
|___/\__,_|___/ | .__/|_|  \___/ \___| |_|  \__,_|_| |_|_|\_\
                |_|
*/

proc rank descending  ties=low
    data   = sd1.have
    out    = havNonSrt
    ;
var wage;
ranks rnkWge;
run;quit;

proc sort data=havNonSrt out=havSrt noequals;
  by rnkWge;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* Up to 40 obs from HAVSRT total obs=20 01MAY2023:16:50:11                                                               */
/* Obs    STATES    WAGE    RNKWGE                                                                                        */
/*                                                                                                                        */
/*   1      KY       48        1                                                                                          */
/*   2      AR       44        2                                                                                          */
/*   3      AK       38        3                                                                                          */
/*   4      MS       37        4                                                                                          */
/*   5      ID       33        5                                                                                          */
/*   6      AZ       33        5                                                                                          */
/*   7      GA       32        7                                                                                          */
/*   8      ME       31        8                                                                                          */
/*   9      IL       26        9                                                                                          */
/*  10      DE       22       10                                                                                          */
/*  11      IA       16       11                                                                                          */
/*  12      IN       16       11                                                                                          */
/*  13      HI       14       13                                                                                          */
/*  14      CO       13       14                                                                                          */
/*  15      CA       13       14                                                                                          */
/*  16      AL       12       16                                                                                          */
/*  17      CT       11       17                                                                                          */
/*  18      LA       10       18                                                                                          */
/*  19      KS        9       19                                                                                          */
/*  20      FL        8       20                                                                                          */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*
__      ___ __  ___   _ __  _ __ ___   ___   _ __
\ \ /\ / / `_ \/ __| | `_ \| `__/ _ \ / __| | `__|
 \ V  V /| |_) \__ \ | |_) | | | (_) | (__  | |
  \_/\_/ | .__/|___/ | .__/|_|  \___/ \___| |_|
         |_|         |_|
*/

%utl_submit_wps64("
libname sd1 'd:/sd1';
proc r;
export data=sd1.have r=have;
submit;
library(sqldf);
want<-sqldf('
    select;
       wage;
      ,rank () over (
            order by wage desc
      ) as wgernk;
    from;
      have;
');
endsubmit;
import data=want_wps_proc_r r=want;
proc print data=want_wps_proc_r;
run;quit;
");

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  SAME AS SAS PROC R                                                                                                    */
/*                                                                                                                        */
/*  The WPS System                                                                                                        */
/*                                                                                                                        */
/*  Obs    WAGE    WGERNK                                                                                                 */
/*                                                                                                                        */
/*    1     48        1                                                                                                   */
/*    2     44        2                                                                                                   */
/*    3     38        3                                                                                                   */
/*    4     37        4                                                                                                   */
/*    5     33        5                                                                                                   */
/*    6     33        5                                                                                                   */
/*    7     32        7                                                                                                   */
/*    8     31        8                                                                                                   */
/*    9     26        9                                                                                                   */
/*   10     22       10                                                                                                   */
/*   11     16       11                                                                                                   */
/*   12     16       11                                                                                                   */
/*   13     14       13                                                                                                   */
/*   14     13       14                                                                                                   */
/*   15     13       14                                                                                                   */
/*   16     12       16                                                                                                   */
/*   17     11       17                                                                                                   */
/*   18     10       18                                                                                                   */
/*   19      9       19                                                                                                   */
/*   20      8       20                                                                                                   */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*                                                           _
__      ___ __  ___   _ __  _ __ ___   ___   _ __ __ _ _ __ | | __
\ \ /\ / / `_ \/ __| | `_ \| `__/ _ \ / __| | `__/ _` | `_ \| |/ /
 \ V  V /| |_) \__ \ | |_) | | | (_) | (__  | | | (_| | | | |   <
  \_/\_/ | .__/|___/ | .__/|_|  \___/ \___| |_|  \__,_|_| |_|_|\_\
         |_|         |_|
*/

%utl_submit_wps64("

libname sd1 'd:/sd1';

proc rank descending  ties=low
    data   = sd1.have
    out    = havNonSrt
    ;
var wage;
ranks rnkWge;
run;quit;

proc sort data=havNonSrt out=havSrt noequals;
  by rnkWge;
run;quit;

proc print;
run;quit;
");

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                          rnk                                                                                           */
/* Obs    STATES    WAGE    Wge                                                                                           */
/*                                                                                                                        */
/*   1      KY       48       1                                                                                           */
/*   2      AR       44       2                                                                                           */
/*   3      AK       38       3                                                                                           */
/*   4      MS       37       4                                                                                           */
/*   5      AZ       33       5                                                                                           */
/*   6      ID       33       5                                                                                           */
/*   7      GA       32       7                                                                                           */
/*   8      ME       31       8                                                                                           */
/*   9      IL       26       9                                                                                           */
/*  10      DE       22      10                                                                                           */
/*  11      IN       16      11                                                                                           */
/*  12      IA       16      11                                                                                           */
/*  13      HI       14      13                                                                                           */
/*  14      CA       13      14                                                                                           */
/*  15      CO       13      14                                                                                           */
/*  16      AL       12      16                                                                                           */
/*  17      CT       11      17                                                                                           */
/*  18      LA       10      18                                                                                           */
/*  19      KS        9      19                                                                                           */
/*  20      FL        8      20                                                                                           */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*           _   _                             _
 _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
| `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
| |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
| .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
|_|    |___/                                |_|
*/

%utlfkil(d:/xpt/want.xpt);

%utl_pybegin;
parmcards4;
from os import path
import pandas as pd
import xport
import xport.v56
import pyreadstat
import numpy as np
import pandas as pd
from pandasql import sqldf
mysql = lambda q: sqldf(q, globals())
from pandasql import PandaSQL
pdsql = PandaSQL(persist=True)
sqlite3conn = next(pdsql.conn.gen).connection.connection
sqlite3conn.enable_load_extension(True)
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
mysql = lambda q: sqldf(q, globals())
have, meta = pyreadstat.read_sas7bdat("d:/sd1/have.sas7bdat")
print(have)
want   = pdsql("""
    select
       WAGE
      ,rank () over (
            order by WAGE desc
      ) as wgernk
   from
      have
""")
print(want)
ds = xport.Dataset(want, name='want')
with open('d:/xpt/want.xpt', 'wb') as f:
    xport.v56.dump(ds, f)
;;;;
%utl_pyend;


libname pyxpt xport "d:/xpt/want.xpt";

proc contents data=pyxpt._all_;
run;quit;

proc print data=pyxpt.want;
run;quit;

data want;
   set pyxpt.want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*      Obs    GROUP_V1    GROUP_PY                                                                                       */
/*                                                                                                                        */
/*        1        -1         -1                                                                                          */
/*        2         0          0                                                                                          */
/*        3         1          1                                                                                          */
/*        4        11          2                                                                                          */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*                                                       _   _
__      ___ __  ___   _ __  _ __ ___   ___   _ __  _   _| |_| |__   ___  _ __
\ \ /\ / / `_ \/ __| | `_ \| `__/ _ \ / __| | `_ \| | | | __| `_ \ / _ \| `_ \
 \ V  V /| |_) \__ \ | |_) | | | (_) | (__  | |_) | |_| | |_| | | | (_) | | | |
  \_/\_/ | .__/|___/ | .__/|_|  \___/ \___| | .__/ \__, |\__|_| |_|\___/|_| |_|
         |_|         |_|                    |_|    |___/
*/

libname sd1 "d:/sd1";

proc datasets lib=sd1 nodetails nolist;
 delete want_py;
run;quit;

%utl_wpsbegin;
parmcards4;
libname sd1 "d:/sd1";
proc python;
export data=sd1.have python=have;
submit;
from os import path
import xport
import xport.v56
import numpy as np
import pandas as pd
from pandasql import sqldf
mysql = lambda q: sqldf(q, globals())
from pandasql import PandaSQL
pdsql = PandaSQL(persist=True)
sqlite3conn = next(pdsql.conn.gen).connection.connection
sqlite3conn.enable_load_extension(True)
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
mysql = lambda q: sqldf(q, globals())
want   = pdsql("""
    select
       wage
      ,rank () over (
            order by wage desc
      ) as wgernk
   from
      have
""")
print(want);
endsubmit;
import data=sd1.want_py python=want;
;;;;
%utl_wpsend;

proc print data=sd1.want_py ;
run;quit;

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
