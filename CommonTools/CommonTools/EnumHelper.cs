using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CommonTools
{
//     class EnumHelper
//     {
//     }

    /// <summary>
    /// 数据库类型
    /// </summary>
    [Description("数据库类型")]
    public enum DataBaseType
    {
        /// <summary>
        /// Ms Sql 数据库
        /// </summary>
        [Description("MSSQL")]
        MSSQL = 0,
        /// <summary>
        /// MySql数据库
        /// </summary>
        [Description("MYSQL")]
        MYSQL = 1,

        /// <summary>
        /// Oracle数据库
        /// </summary>
        [Description("ORACLE")]
        ORACLE = 2,

        /// <summary>
        /// Sqlite数据库
        /// </summary>
        [Description("SQLITE")]
        SQLITE = 3,

        /// <summary>
        /// Access数据库
        /// </summary>
        [Description("ACCESS")]
        ACCESS = 4
    }
}
