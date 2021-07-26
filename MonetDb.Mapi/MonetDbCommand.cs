namespace MonetDb.Mapi
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Text;
    using System.Text.RegularExpressions;
    using System.Threading;

    using MonetDb.Mapi.Helpers.Mapi;

    /// <summary>
    /// Represents an SQL command to send to a <c>MonetDbConnection</c>
    /// </summary>
    public class MonetDbCommand : DbCommand
    {
        private MonetDbConnection _connection;
        private DbParameterCollection dbParameterCollection;
        private CancellationTokenSource cancellationTokenSource;

        /// <summary>
        /// Initializes a new command
        /// </summary>
        public MonetDbCommand() : base()
        {
            this.dbParameterCollection = new MonetDbParameterCollection();
        }

        /// <summary>
        /// Initializes a new command
        /// </summary>
        /// <param name="cmdText"></param>
        public MonetDbCommand(string cmdText)
            : this()
        {
            CommandText = cmdText;
        }

        /// <summary>
        /// Initializes a new command.
        /// </summary>
        /// <param name="cmdText"></param>
        /// <param name="connection"></param>
        public MonetDbCommand(string cmdText, MonetDbConnection connection)
            : this(cmdText)
        {
            Connection = connection;
        }

        /// <summary>
        /// Initializes a new command.
        /// </summary>
        /// <param name="cmdText"></param>
        /// <param name="connection"></param>
        /// <param name="transaction"></param>
        public MonetDbCommand(string cmdText, MonetDbConnection connection, MonetDbTransaction transaction)
            : this(cmdText, connection)
        {
            Transaction = transaction;
        }

        /// <summary>
        /// Gets or sets how command results are applied to the <c>DataRow</c> when used by the 
        /// <c>Update</c> method of a <c>MonetDbDataAdapter</c>.
        /// </summary>
        public override UpdateRowSource UpdatedRowSource { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public override string CommandText { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public override int CommandTimeout { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public override CommandType CommandType { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public override bool DesignTimeVisible { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        /// <summary>
        /// 
        /// </summary>
        protected override DbParameterCollection DbParameterCollection => this.dbParameterCollection;

        /// <summary>
        /// 
        /// </summary>
        protected override DbTransaction DbTransaction { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        /// <summary>
        /// Attempts to cancels the execution of this <c>MonetDbCommand</c>.
        /// </summary>
        public override void Cancel()
        {
            this.cancellationTokenSource?.Cancel();
        }

        /// <summary>
        /// Executes an SQL statement against the <c>Connection</c> object of MonetDB data provider, 
        /// and returns the number of rows affected.
        /// </summary>
        /// <returns></returns>
        public override int ExecuteNonQuery()
        {
            var cnt = 0;
            using (var dr = new MonetDbDataReader(this.ExecuteCommand(), Connection as MonetDbConnection))
            {
                do
                {
                    cnt += dr.RecordsAffected;
                } while (dr.NextResult());
            }
            return cnt;
        }

        /// <summary>
        /// Executes the query, and returns the first column of the first row in the resultset 
        /// returned by the query. Extra columns or rows are ignored.
        /// </summary>
        /// <returns></returns>
        public override object ExecuteScalar()
        {
            using (var dr = ExecuteReader())
            {
                return dr.Read() ? dr[0] : null;
            }
        }

        /// <summary>
        /// Creates a prepared (or compiled) version of the command on the data source.
        /// </summary>
        public override void Prepare()
        {
        }

        /// <summary>
        /// Executes the <c>CommandText</c> against the <c>Connection</c> and builds an <c>IDataReader</c>.
        /// </summary>
        /// <param name="behavior"></param>
        /// <returns></returns>
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            return new MonetDbDataReader(this.ExecuteCommand(), Connection as MonetDbConnection);
        }

        /// <summary>
        /// Gets or sets the <c>IDbConnection</c> used by this instance of the <c>IDbCommand</c>.
        /// </summary>
        protected override DbConnection DbConnection
        {
            get { return _connection; }
            set { _connection = (MonetDbConnection)value; }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override DbParameter CreateDbParameter()
        {
            return new MonetDbParameter();
        }

        protected override void Dispose(bool disposing)
        {
            this.cancellationTokenSource = null;
            base.Dispose(disposing);
        }

        private IEnumerable<QueryResponseInfo> ExecuteCommand()
        {
            if (Connection == null || Connection.State != ConnectionState.Open)
            {
                throw new MonetDbException("Connection is closed");
            }

            var sb = new StringBuilder(CommandText);
            foreach (MonetDbParameter p in Parameters)
            {
                sb = ApplyParameter(sb, new KeyValuePair<string, string>(p.ParameterName, p.GetProperParameter()));
            }

            this.cancellationTokenSource = new CancellationTokenSource();
            return (Connection as MonetDbConnection).ExecuteSql(sb.ToString(), this.cancellationTokenSource.Token);
        }

        private StringBuilder ApplyParameter(StringBuilder sb, KeyValuePair<string, string> p)
        {

            var pattern = new Regex($"^{p.Key},?$");
            string[] query = sb.ToString().Split(' ');

            for(int i = 0; i <= query.Length; i++)
            {
                if(pattern.Match(query[i]).Success)
                {
                    bool comma = false;
                    if (query[i].Contains(","))
                    {
                        query[i] = query[i].Replace(',', ' ').Trim();
                        comma = true;
                    }

                    query[i] = query[i].Replace(p.Key, p.Value);

                    if(comma)
                    {
                        query[i] = query[i] + ",";
                    }

                    break;
                }
            }

            StringBuilder builder = new StringBuilder();
            return builder.Append(string.Join(" ", query));

        }
    }
}
