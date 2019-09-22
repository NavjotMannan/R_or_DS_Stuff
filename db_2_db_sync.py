from subprocess import Popen, PIPE
import os, csv

"""Utility to sync data from One EDW DB to Another. Really useful to sync data from Prod to TEST for faster testing/UAT
   Benefits: 1. No Landing of data on Disk 2. No need to define columns or manual control file generator, only DB details reqd.
   3. Single Utility for both Export & Load from one db to another
   4. Driven using TPT, so better than legacy utlities
"""

def gen_col_list():
    """Generates ColumnList based on Source/Target ColumnList"""
    list1=[]
    list2=[]
    list3=[]
    list4=[]
    with open("out_file", "r") as file:
        reader = csv.reader(file, delimiter="|")
        for i in reader:
            list1.append(i[0])
            list2.append(i[1])
            list3.append(i[2])
            list4.append(i[3])
    file.close()
    x=", ".join(list1)
    y=", ".join(list2)
    z=", ".join(list3)
    w=", ".join(list4)
    return x, y, z, w


def retrieve_query(dml_type):
    """Retrieve Query based on Operation:Export/Schema/Select"""
    x, y, z, w = gen_col_list()
    if dml_type == "insert":
        return """
           insert into """+db_schema+"."+db_tablename+""" 
           (\n"""+x.replace(",","\n,")+"""\n)
           VALUES
           (\n"""+y.replace(",","\n,")+"""\n);""".strip("\n")
    elif dml_type == "schema":
        return z.replace(",","\n,")
    else:
        return """
            lock row for access SELECT\n\t"""+w.replace(",","\n,")+"""\n FROM \n"""+db_schema+"."+db_tablename+"""
            ;'
            """.strip("\n")


def oper_bl_gen(oper_type="", schema="", table=""):
    """Creates Block based on EXPORT or LOAD operation"""
    init_str="""
    DEFINE OPERATOR {0}_TABLE
    TYPE {0}
    SCHEMA {1}_TABLE
    ATTRIBUTES
    (
    VARCHAR PrivateLogName = '{0}oper_privatelog',
    INTEGER MaxSessions    =  5,
    INTEGER MinSessions    =  2,
    VARCHAR TdpId          = '{2}',
    VARCHAR UserName       = '{3}',
    VARCHAR UserPassword   = '{4},
    """
    select_var="VARCHAR SelectStmt     = '"
    if oper_type == "EXPORT":
        return init_str.format(oper_type, "SCHEMA", db_env, db_username, db_password)+select_var \
               +retrieve_query("extract").strip("\n")+""",
        VARCHAR Dateform  = 'ANSIDATE'
        );""".strip("\n")
    else:
        return init_str.format(oper_type, "SCHEMA", tgt_db_env, tgt_db_username, tgt_db_password)+"""
    VARCHAR TargetTable    = '{0}.{1}',
    VARCHAR ErrorTable1    = '{0}.{1}_f1',
    VARCHAR ErrorTable2    = '{0}.{1}_f2',
    VARCHAR LogTable       = '{0}.{1}_LT'        
        );""".format(schema, table).strip("\n")


def block_gen(block_type=""):
    """Returns Block based on Block Type: SCHEMA/EXPORT/LOAD/APPLY"""
    if block_type == "SCHEMA":
        outer_block_str = """
        DEFINE {0} {0}_TABLE
        (
        """.format(block_type)+retrieve_query("schema")+"""
        );
        """
    elif block_type == "EXPORT":
        outer_block_str = oper_bl_gen(block_type, db_schema, db_tablename)
    elif block_type == "LOAD":
        outer_block_str = oper_bl_gen(block_type, tgt_schema, tgt_tablename)
    else:
        outer_block_str="""
        APPLY
        ('
        """+retrieve_query("insert")+"""
        ')
        """
    return outer_block_str


def ctl_file_gen(job_name):
    """Generates the final TPT CONTROL FILE"""
    charset_str="USING CHARACTER SET UTF8"
    job_def="""
DEFINE JOB {0}
(    
 """.format(job_name)+block_gen("SCHEMA")+"""
 """+block_gen("EXPORT")+"""
 """+block_gen("LOAD")+"""
 """+block_gen()+"""
TO OPERATOR (
              Load_Table[1]
            )
 SELECT * FROM OPERATOR  (export_Table[1]);
 );                           
 """
    return charset_str+job_def


class Connection(object):
    """
    Connection for executing queries and generating exports for column list retrieval
    """
    def __init__(self, in_db_name, in_db_user, in_db_password, in_exp_flag=False, in_export_file_path=""
                 ,in_export_file_name=""):
        # initialise the DB variables for validation
        self.db_name = in_db_name
        self.db_user = in_db_user
        self.db_password = in_db_password
        # self.db_queries = in_db_queries
        self.export_file_path = in_export_file_path
        self.export_file_name = in_export_file_name
        self.connection_string = ""
        self.connection_string_header = ""
        self.connection_string_footer = ""
        self.conn_str_header_exp = ""
        self.exp_flag = in_exp_flag
        self.export_abs_path = self.export_file_path + "/" + self.export_file_name

    def export_file_exists(self, file_nm=None):

        if file_nm is not None:
            # print("File Name is {}".format(str(file_nm)))
            file_nm1 = self.export_file_path + "/" + file_nm
            if os.path.exists(file_nm1):
                #                 print("rm {}".format(file_nm))
                os.system("rm " + file_nm1)
        #                 print("hi howr ()")
        else:
            if os.path.exists(self.export_abs_path):
                #                 print("rm {}".format(self.export_abs_path))
                os.system("rm " + self.export_abs_path)

    def prep_conn_ingr(self, out_file=None):

        """
        initialise the DB connection ingredients for DB
        """
        if out_file is None:
            export_file_name = self.export_file_name
        else:
            export_file_name = out_file
        #             print(export_file_name)

        self.connection_string_header = """
            .set separator "|"
            .set titledashes off
            .set format off
            .set width 9999
            .set recordmode off
            .set underline off
            .set echoreq off
            .set retlimit * *
            .set NULL ''
            """

        if self.exp_flag:
            self.connection_string_header = ".export report file="+self.export_file_path+"/"+export_file_name \
                                            +self.connection_string_header

        self.connection_string_footer = """
        .IF errorcode != 0 THEN .EXIT 2;
        .export reset
        .IF errorcode != 0 THEN .EXIT 3;
        .logoff
        .exit
            """

    def execute_queries(self, query=""):
        """
        initialise the DB connection string for DB
        """
        # try:
        db_queries = query
        db_session = Popen(["bteq", ".logon", self.db_name+"/"+self.db_user+","+self.db_password], stdin=PIPE, stdout=PIPE,stderr=PIPE)
        db_session.stdin.write(self.connection_string_header)
        db_session.stdin.write(db_queries)
        db_session.stdin.write(self.connection_string_footer)
        query_output, query_error = db_session.communicate()

        return query_output


#########SRC DB DETAILS##############***Below can be moved to a config/env file if required
db_env = "******"
db_username = "******"
db_password = "******"
db_schema = "******"
db_tablename = "******"
#########TGT DB DETAILS##############
tgt_db_env = "******"
tgt_db_username = "******"
tgt_db_password = "******"
tgt_schema='******'
tgt_tablename="******"
#########OTHER VARIABLES##############
bteq_output_path = "******"
out_file = "out_file"
db_query="""lock row for access select 
Trim(columnname)||'|'||
':'||TRIM(columnname)||'|'||
TRIM(columnname)||' '||TRIM(SCHEMA_DATATYPE)||'|'||
'CAST('||TRIM(columnname)||' AS '||TRIM(DATATYPE)||')'||' AS '||Trim(columnname)
(title '') 
from (select columnname, CASE ColumnType
    WHEN 'BF' THEN 'VARCHAR('         || TRIM(ColumnLength (FORMAT '-(9)9')) || ')'
    WHEN 'BV' THEN 'VARCHAR('         || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
    WHEN 'CF' THEN 'VARCHAR('         || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
    WHEN 'CV' THEN 'VARCHAR('         || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
    WHEN 'D ' THEN 'VARCHAR('         || TRIM(DecimalTotalDigits (FORMAT '-(9)9'))|| ')'
    WHEN 'DA' THEN 'VARCHAR('         || TRIM(ColumnLength (FORMAT '-(9)9')) || ')'
    WHEN 'F ' THEN 'VARCHAR'
    WHEN 'I1' THEN 'VARCHAR'
    WHEN 'I2' THEN 'VARCHAR'
    WHEN 'I8' THEN 'VARCHAR'
    WHEN 'I ' THEN 'VARCHAR('         || TRIM(10 (FORMAT '-(9)9')) || ')'
    WHEN 'TS' THEN 'VARCHAR('         || TRIM(30 (FORMAT '-(9)9')) || ')'
    WHEN 'TZ' THEN 'VARCHAR('         || TRIM(30 (FORMAT '-(9)9')) || ')' || ' WITH TIME ZONE'    
    ELSE 'UNKNOWN' END AS DATATYPE,
    CASE ColumnType
    WHEN 'BF' THEN 'VARCHAR('         || TRIM(ColumnLength*3 (FORMAT '-(9)9')) || ')'
    WHEN 'BV' THEN 'VARCHAR('         || TRIM(ColumnLength*3 (FORMAT 'Z(9)9')) || ')'
    WHEN 'CF' THEN 'VARCHAR('         || TRIM(ColumnLength*3 (FORMAT 'Z(9)9')) || ')'
    WHEN 'CV' THEN 'VARCHAR('         || TRIM(ColumnLength*3 (FORMAT 'Z(9)9')) || ')'
    WHEN 'D ' THEN 'VARCHAR('         || TRIM(DecimalTotalDigits*3 (FORMAT '-(9)9'))|| ')'
    WHEN 'DA' THEN 'VARCHAR('         || TRIM(ColumnLength*3 (FORMAT '-(9)9')) || ')'
    WHEN 'F ' THEN 'FLOAT'
    WHEN 'I1' THEN 'BYTEINT'
    WHEN 'I2' THEN 'SMALLINT'
    WHEN 'I8' THEN 'BIGINT'
    WHEN 'I ' THEN 'VARCHAR('         || TRIM(10*3 (FORMAT '-(9)9')) || ')'
    WHEN 'TS' THEN 'VARCHAR('         || TRIM(30*3 (FORMAT '-(9)9')) || ')'
    WHEN 'TZ' THEN 'TIME('            || TRIM(30*3 (FORMAT '-(9)9')) || ')' || ' WITH TIME ZONE'        
    ELSE 'UNKNOWN' END AS SCHEMA_DATATYPE
 from DBC.ColumnsV where tablename='******' and databasename='******')w;"""


conn = Connection(db_env, db_username, db_password, True, bteq_output_path, out_file)
conn.export_file_exists()
conn.prep_conn_ingr()
conn.execute_queries(query=db_query)

with open("tpt_ctl_file.ctl","w") as file:
    print(ctl_file_gen("Export_Load_Table"), file=file)