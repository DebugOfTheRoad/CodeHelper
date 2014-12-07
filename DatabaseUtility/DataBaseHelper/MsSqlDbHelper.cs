using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;

namespace DataBaseHelper
{
    public abstract  class MsSqlDbHelper
    {
        public string SqlConnectionString = string.Empty;
        private SqlConnection Connection;
        private SqlDataAdapter SqlAdapter;
        private SqlDataReader SqlReader;
        private SqlCommand SqlCmd;
        private SqlParameter SqlParam;
        private DataSet Ds;
        private DataView Dv;

        public MsSqlDbHelper()
        {

        }
        /// <summary>
        /// 初始化数据库连接
        /// </summary>
        /// <param name="connection"></param>
        public MsSqlDbHelper(string connection)
        {
            this.SqlConnectionString = connection;
        }

        /// <summary>
        /// 手动设置数据库连接串
        /// </summary>
        /// <param name="server"></param>
        /// <param name="database"></param>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <param name="trusted"></param>
        public void SetConnection(string server, string database, string username, string password, bool trusted)
        {
            /*
             <add name="DefaultConnection" connectionString="Data Source=10.10.11.202;Initial Catalog=TestDb;User ID=sa;Password=mpdgI38G5n;Connection Timeout=6000" providerName="System.Data.SqlClient" />
             */
            ///使用原始的数据库连接字符串。连接串可在config文件中进行配置
            if (trusted)
            {
                SqlConnectionString = @"Server=" + server + ";Database=" + database + ";Trusted_Connection=True;";
            }
            else
            {
                SqlConnectionString = @"Server=" + server + ";Database=" + database + ";User ID=" + username + ";Password=" + password + ";Trusted_Connection=True;";
            }
        }

        /// <summary>
        /// 打开数据连接
        /// </summary>
        /// <returns></returns>
        public bool Open()
        {
             try
             {
                 Connection = new SqlConnection(SqlConnectionString);
                 Connection.Open();
                 return true;
             }
             catch
             {
               
                 return false;
             }
        }
        /// <summary>
        ///  关闭数据库连接
        /// </summary>
        public void Close()
        {
           if (Connection != null)
           {
               Connection.Close();
               Connection.Dispose();
           }
        }
        /// <summary>
        /// 获取数据集
        /// </summary>
        /// <param name="strSql"></param>
        /// <returns></returns>
        public DataSet GetDataSet(string strSql)
        {

            bool result = Open();
            if (result)
            {
                SqlAdapter = new SqlDataAdapter(strSql, Connection);
                Ds = new DataSet();
                SqlAdapter.Fill(Ds);
                Close();
                return Ds;
            }
            else
            {
                Close();
                return null;
            }
        }

        public DataSet GetDataSet(string strSql, string strTableName)
        {
          
                bool result = Open();
                if (result)
                {
                    DataSet ds = new DataSet();
                    SqlAdapter = new SqlDataAdapter(strSql, Connection);
                    SqlAdapter.Fill(ds, strTableName);
                    Close();
                    return ds;
                }
                else
                {
                    Close();
                    return null;
                }
        }

        public DataView GetDataView(string strSql)
        {
            Dv = GetDataSet(strSql).Tables[0].DefaultView;
            if (Dv != null)
            {
                return Dv;
            }
            else
            {
                return null;
            }
        }

        public DataTable GetTable(string strSql)
        {
            return GetDataSet(strSql).Tables[0];
        }

        public SqlDataReader GetDataReader(string strSql)
        {
            bool result = Open();
            if (result)
            {
                SqlCmd = new SqlCommand(strSql, Connection);
                SqlReader = SqlCmd.ExecuteReader(CommandBehavior.CloseConnection);
                return SqlReader;
            }
            else
            {
                return null;
            }
        }

        public bool ExecuteSql(string strSql)
        {
            bool result = Open();
            if (result)
            {
                SqlCmd = new SqlCommand(strSql, Connection);
                int iresult= SqlCmd.ExecuteNonQuery();
                if (iresult>0)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }


        public string ExecuteSqlReturn(string strSql)
        {
            string strReturn = "";
            bool result = Open();
            SqlCmd = new SqlCommand(strSql, Connection);
            strReturn = SqlCmd.ExecuteScalar().ToString();
            Close();
            return strReturn;
        }

        private SqlCommand CreateCommand(string procName, SqlParameter[] prams)
        {
            Open();
            SqlCmd = new SqlCommand(procName, Connection);
            SqlCmd.CommandType = CommandType.StoredProcedure;
            if (prams != null)
            {
                foreach (SqlParameter parameter in prams)
                {
                    SqlCmd.Parameters.Add(parameter);
                }
            }
            SqlCmd.Parameters.Add(
                new SqlParameter("ReturnValue", SqlDbType.Int, 4,
                ParameterDirection.ReturnValue, false, 0, 0,
                string.Empty, DataRowVersion.Default, null
                ));
            return SqlCmd;
        }
        public int ExecuteProc(string procName)
        {
            SqlCmd = CreateCommand(procName, null);
            SqlCmd.ExecuteNonQuery();
            return (int)SqlCmd.Parameters["ReturnValue"].Value;
        }

        public int ExecuteProc(string procName, SqlParameter[] prams)
        {
            SqlCmd = CreateCommand(procName, prams);
            SqlCmd.ExecuteNonQuery();
            return (int)SqlCmd.Parameters["ReturnValue"].Value;
        }

        public void ExecuteProc(string procName, SqlDataReader sdr)
        {
            SqlCmd = CreateCommand(procName, null);
            sdr = SqlCmd.ExecuteReader(CommandBehavior.CloseConnection);
        }

        public void ExecuteProc(string procName, SqlParameter[] prams, SqlDataReader sdr)
        {
            SqlCmd = CreateCommand(procName, prams);
            sdr = SqlCmd.ExecuteReader(CommandBehavior.CloseConnection);
        }

        private static SqlCommand BuildQueryCommand(SqlConnection connection, string procName, IDataParameter[] prams)
        {
            SqlCommand cmd = new SqlCommand(procName, connection);
            cmd.CommandType = CommandType.StoredProcedure;
            foreach (SqlParameter paramter in prams)
            {
                if (paramter != null)
                {
                    if ((paramter.Direction == ParameterDirection.InputOutput ||
                        paramter.Direction == ParameterDirection.Input) &&(paramter.Value == null))
                    {
                        paramter.Value = DBNull.Value;
                    }
                    cmd.Parameters.Add(paramter);
                }
            }
            return cmd;
        }


        public DataSet ExecuteProc(string procName, IDataParameter[] prams, string tableName)
        {
            using (SqlConnection connection = new SqlConnection(this.SqlConnectionString))
            {
                DataSet dataset = new DataSet();
                try
                {
                    connection.Open();
                    SqlDataAdapter sda = new SqlDataAdapter();
                    sda.SelectCommand = BuildQueryCommand(connection, procName, prams);
                    sda.Fill(dataset, tableName);
                    connection.Close();
                    return dataset;
                }
                catch (System.Data.SqlClient.SqlException ex)
                {
                    throw new Exception(ex.Message);
                }
            }
        }

        public SqlParameter MakeParam(string ParamName, SqlDbType DbType, Int32 Size, ParameterDirection Direction, object Value)
        {
            if (Size>0)
            {
                SqlParam = new SqlParameter(ParamName, DbType, Size);
            }
            else
            {
                SqlParam = new SqlParameter(ParamName, DbType);
            }

            SqlParam.Direction = Direction;
            if (!(Direction == ParameterDirection.Output && Value == null))
            {
                SqlParam.Value = Value;
            }
            return SqlParam;
        }

        public SqlParameter MakeInParam(string ParamName, SqlDbType DbType, int Size, object Value)
        {
            return MakeParam(ParamName, DbType, Size, ParameterDirection.Input, Value);
        }
        public SqlParameter makeOutParam(string ParamName, SqlDbType DbType, int Size)
        {
            return MakeParam(ParamName, DbType, Size, ParameterDirection.Output, null);
        }


        public DataTable GetTables(string strSql)
        {
            bool result = Open();
            if (result)
            {
                DataTable schemaTable;
                SqlCmd = new SqlCommand(strSql, Connection);
                SqlReader = SqlCmd.ExecuteReader(CommandBehavior.SchemaOnly);
                schemaTable = SqlReader.GetSchemaTable();
                Close();
                return schemaTable;
            }
            else
            {
                Close();
                return null;
            }
        }


        public DataTable GetTableSchema(string tableName)
        {
            bool result = Open();
            if (result)
            {
                DataTable schemaTable;
                SqlCmd = new SqlCommand("select * from [" + tableName + "]", Connection);
                SqlReader = SqlCmd.ExecuteReader(CommandBehavior.SchemaOnly);
                schemaTable = SqlReader.GetSchemaTable();
                Close();
                return schemaTable;
            }
            else
            {
                return null;
            }
        }

        public DataTable GetTables()
        {
            bool result = Open();
            if (result)
            {
                DataTable dt;
                dt = GetTable("select name from sysobjects where type ='U' order by name ");
                Close();
                return dt;
            }
            else
            {
                Close();
                return null;
            }
        }

        public DataTable GetViews()
        {
            bool result = Open();
            if (result)
            {
                DataTable dt;
                dt = GetTable("select name from sysobjects where type ='V'  order by name ");
                Close();
                return dt;
            }
            else
            {
                return null;
            }
        }

        public DataTable GetDatabases()
        {
            bool result = Open();
            if (result)
            {
                DataTable dt;
                dt = GetTable("select name  from master..sysdatabases");
                Close();
                return dt;
            }
            else
            {
                return null;
            }
        }

        public DataTable GetPks(string tableName)
        {
            SqlParameter[] parameters = {
	                new SqlParameter("@table_name", SqlDbType.VarChar, 50) };
            parameters[0].Value = tableName;


            DataSet ds = ExecuteProc("sp_pkeys", parameters, "ds");
            return ds.Tables[0];
        }

        public string GetCurrentDbName()
        {
            return this.Connection.Database;
        }

    }
}
