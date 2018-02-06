using NBsoft.Logs.Interfaces;
using System;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace NBsoft.Logs.Sql
{
    public class SqlServerLogger : ILogger
    {
        private readonly string connString;
        private readonly string logTable;
        private DbConnection conn = null;

        public SqlServerLogger(string connString, string logTable)
        {
            this.connString = connString;
            this.logTable = logTable;

            CheckDatabase();
        }

        private void CheckDatabase()
        {
            DbConnection testConn = new SqlConnection(connString);
            testConn.Open();
            try
            {
                bool LogTableExists = false;
                try
                {
                    // Check if log table exists
                    DbCommand cmd = testConn.CreateCommand();
                    cmd.CommandText = string.Format("select case when exists((select * from information_schema.tables where table_name = '{0}')) then 1 else 0 end", logTable);
                    LogTableExists = (int)cmd.ExecuteScalar() == 1;
                }
                catch { LogTableExists = false; }
                if (!LogTableExists)
                {
                    string sql = string.Format(
                        "CREATE TABLE [{0}] (" +
                        " [Id]                  [bigint] IDENTITY(1, 1)     NOT NULL, " +
                        " [DateTime]            [datetime]                  NOT NULL, " +
                        " [Level]               [nvarchar](16)	            NOT NULL, " +
                        " [Component]           [text]                      NULL, " +
                        " [Process]             [text]                      NULL, " +
                        " [Context]             [text]                      NULL, " +
                        " [Type]                [text]                      NULL, " +
                        " [Stack]               [text]                      NULL, " +
                        " [Msg]                 [text]                      NULL, " +
                        " CONSTRAINT[PK_{0}] PRIMARY KEY CLUSTERED([Id] ASC)" +
                        ");", logTable);

                    DbCommand createCommand = testConn.CreateCommand();
                    createCommand.CommandText = sql;
                    createCommand.ExecuteNonQuery();

                }
            }
            finally
            {
                testConn.Dispose();
            }

        }

        private void OpenDb()
        {
            if (conn == null)
            {
                conn = new SqlConnection(connString);
                conn.Open();
            }

        }
        private async Task<int> Insert(ILogItem logItem)
        {
            OpenDb();

            using (DbCommand cmd = conn.CreateCommand())
            {
                cmd.CommandText = string.Format("INSERT INTO [{0}]  ([DateTime],[Level],[Component],[Process],[Context],[Type],[Stack],[Msg])" +
                    " VALUES " +
                    "(@DateTime,@Level,@Component,@Process,@Context,@Type,@Stack,@Msg)"
                    , logTable);

                cmd.Parameters.Add(new SqlParameter("@DateTime", logItem.DateTime));
                cmd.Parameters.Add(new SqlParameter("@Level", logItem.Level));
                cmd.Parameters.Add(new SqlParameter("@Component", (object)logItem.Component ?? DBNull.Value));
                cmd.Parameters.Add(new SqlParameter("@Process", (object)logItem.Process ?? DBNull.Value));
                cmd.Parameters.Add(new SqlParameter("@Context", (object)logItem.Context ?? DBNull.Value));
                cmd.Parameters.Add(new SqlParameter("@Type", (object)logItem.Type ?? DBNull.Value));
                cmd.Parameters.Add(new SqlParameter("@Stack", (object)logItem.Stack ?? DBNull.Value));
                cmd.Parameters.Add(new SqlParameter("@Msg", (object)logItem.Message ?? DBNull.Value));
                try { return await cmd.ExecuteNonQueryAsync(); }
                catch (Exception ex01)
                {
                    Console.WriteLine(ex01.Message);
                    throw;
                }
            }

        }

        public async Task WriteLogAsync(ILogItem item)
        {
            int res = await Insert(item);
            if (res != 1)
                throw new InvalidOperationException("Insert failed");
        }
        private Task WriteLogAsync(LogType level, string component, string process, string context, string message, string stack, string type, DateTime? dateTime = default(DateTime?))
        {
            return WriteLogAsync(new Models.LogItem()
            {
                Level = level,
                Component = component,
                Process = process,
                Context = context,
                Message = message,
                Stack = stack,
                Type = type,
                DateTime = dateTime ?? DateTime.UtcNow
            });
        }
        public Task WriteInfoAsync(string component, string process, string context, string message, DateTime? dateTime = default(DateTime?))
        {
            return WriteLogAsync(LogType.Info, component, process, context, message, null, null, dateTime);
        }
        public Task WriteWarningAsync(string component, string process, string context, string message, DateTime? dateTime = default(DateTime?))
        {
            return WriteLogAsync(LogType.Warning, component, process, context, message, null, null, dateTime);
        }
        public Task WriteErrorAsync(string component, string process, string context, Exception exception, DateTime? dateTime = default(DateTime?))
        {
            return WriteLogAsync(LogType.Error, component, process, context, exception.Message, exception.GetBaseException().StackTrace, exception.GetBaseException().GetType().ToString(), dateTime);
        }
        public Task WriteFatalErrorAsync(string component, string process, string context, Exception exception, DateTime? dateTime = default(DateTime?))
        {
            return WriteLogAsync(LogType.FatalError, component, process, context, exception.Message, exception.GetBaseException().StackTrace, exception.GetBaseException().GetType().ToString(), dateTime);
        }

        public void Dispose()
        {
            if (conn != null)
                conn.Dispose();
            conn = null;
        }
    }
}
