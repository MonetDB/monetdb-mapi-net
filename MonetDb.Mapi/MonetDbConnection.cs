/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 * 
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 * 
 * The Original Code is MonetDB .NET Client Library.
 * 
 * The Initial Developer of the Original Code is Tim Gebhardt <tim@gebhardtcomputing.com>.
 * Portions created by Tim Gebhardt are Copyright (C) 2007. All Rights Reserved.
 */

namespace MonetDb.Mapi
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Diagnostics;
    using System.IO;

    using MonetDb.Mapi.Helpers;
    using MonetDb.Mapi.Helpers.Mapi;

    /// <summary>
    /// Represents an open connection with an MonetDB server.
    /// </summary>
    public class MonetDbConnection : DbConnection
    {
        private string _host;
        private int _port;
        private string _username;
        private string _password;
        private int _minPoolConnections = 3;
        private int _maxPoolConnections = 20;
        private int _currentReplySize = -1;

        private Socket _socket;

        private Metadata _metaData;
        private readonly object _syncLock = new object();
        private bool _disposed;

        private string _connectionString;
        private ConnectionState _state;
        private string database;
        private string dataSource;
        private string serverVersion;

        #region Constructors

        /// <summary>
        /// Initializes a new connection with the MonetDB server.
        /// </summary>
        public MonetDbConnection()
        { }

        /// <summary>
        /// Initializes a new connection with the MonetDB server.
        /// </summary>
        /// <param name="connectionString">
        /// The information used to establish a connection.  
        /// See <c>ConnectionString</c> for the valid formatting of this parameter.
        /// </param>
        public MonetDbConnection(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString", "connectionString cannot be null");

            ConnectionString = connectionString;
            _state = ConnectionState.Closed;
        }

        #endregion

        /// <summary>
        /// Gets the time to wait while trying to establish a 
        /// connection before terminating the attempt and generating an error.
        /// </summary>
        public override int ConnectionTimeout
        {
            get { return 60; }
        }

        /// <summary>
        /// 
        /// </summary>
        public int ReplySize { get; set; }

        /// <summary>
        /// Gets the current state of the connection.
        /// </summary>
        public override ConnectionState State { get => _state; }

        public override string Database => this.database;

        public override string DataSource => this.dataSource;

        public override string ServerVersion => this.serverVersion;

        /// <summary>
        /// Changes the current database for an open MonetDbConnection object.
        /// </summary>
        /// <param name="databaseName">
        /// The name of the database to use in place of the current database.
        /// </param>
        public override void ChangeDatabase(string databaseName)
        {
            var reopen = false;
            if (State == ConnectionState.Open)
            {
                Close();
                reopen = true;
            }

            var connectionStringChunks = ConnectionString.Split(';');
            for (var i = 0; i < connectionStringChunks.Length; i++)
            {
                if (connectionStringChunks[i].StartsWith("database=", StringComparison.InvariantCultureIgnoreCase))
                {
                    connectionStringChunks[i] = "database=" + databaseName;
                }
            }

            ConnectionString = string.Join(";", connectionStringChunks);

            if (reopen)
            {
                Open();
            }
        }

        /// <summary>
        /// Releases the connection back to the connection pool.
        /// </summary>
        public override void Close()
        {
            if (this._socket != null)
            {
                MonetDbConnectionFactory.CloseConnection(this._socket, this.Database, () => this._socket = null);
            }

            this._state = ConnectionState.Closed;
        }

        /// <summary>
        /// Gets or sets the string used to open a database.
        /// </summary>
        /// <example>host=localhost;port=50000;username=admin;password=sa;database=demo;ssl=false;poolMinimum=3;poolMaximum=20</example>
        public override string ConnectionString
        {
            get
            {
                return _connectionString;
            }
            set
            {
                if (string.IsNullOrEmpty(value))
                {
                    throw new ArgumentNullException("value", "ConnectionString cannot be null");
                }

                _connectionString = value;
                ParseConnectionString(value);
            }
        }

        /// <summary>
        /// Opens a database connection with the settings 
        /// specified by the <c>ConnectionString</c> property 
        /// of the provider-specific Connection object.
        /// </summary>
        public override void Open()
        {
            if (this._state == ConnectionState.Open)
            {
                throw new InvalidOperationException("Connection is already open");
            }

            this._state = ConnectionState.Connecting;

            if (string.IsNullOrEmpty(ConnectionString))
            {
                this._state = ConnectionState.Closed;
                throw new InvalidOperationException("ConnectionString has not been set. Cannot connect to database.");
            }

            this._socket = MonetDbConnectionFactory.GetConnection(_host, _port, _username, _password, Database, _minPoolConnections, _maxPoolConnections);

            this._state = ConnectionState.Open;
        }

        ///// <summary>
        ///// Returns the number of rows affected in an SQL UPDATE/DELETE/INSERT query
        ///// </summary>
        ///// <returns></returns>
        //internal int GetRowsAffected()
        //{
        //    throw new NotImplementedException("this is not implemented yet");
        //    //return MapiLib.MapiRowsAffected(_socket);
        //}

        internal Metadata GetMetaData()
        {
            // create on request
            return _metaData ?? (_metaData = new Metadata(this));

            // TODO: Finish the schema extraction
            //var dt = new DataTable();
            //dt.Columns.Add("ColumnName", typeof(string));
            //dt.Columns.Add("ColumnOrdinal", typeof(int));
            //dt.Columns.Add("ColumnSize", typeof(int));
            //dt.Columns.Add("NumericPrecision");
            //dt.Columns.Add("NumericScale");
            //dt.Columns.Add("IsUnique", typeof(bool));
            //dt.Columns.Add("IsKey", typeof(bool));
            //dt.Columns.Add("BaseServerName", typeof(string));
            //dt.Columns.Add("BaseCatalogName", typeof(string));
            //dt.Columns.Add("BaseColumnName", typeof(string));
            //dt.Columns.Add("BaseSchemaName", typeof(string));
            //dt.Columns.Add("BaseTableName", typeof(string));
            //dt.Columns.Add("BaseColumnName", typeof(string));
            //dt.Columns.Add("DataType", typeof(Type));
            //dt.Columns.Add("DataTypeName", typeof(string));

            //return dt;
        }

        internal IEnumerable<QueryResponseInfo> ExecuteSql(string sql)
        {
            try
            {
#if TRACE
                Debug.WriteLine(sql, "MonetDb");
#endif
                this.PrepareExecution();
                return _socket.ExecuteSql(sql);
            }
            catch (IOException ex)
            {
                MonetDbConnectionFactory.RemoveConnection(this._socket, this.Database, () => this._socket = null);
                throw;
            }
        }

        /// <summary>
        /// Begins a database transaction with the specified <c>IsolationLevel</c> value.
        /// </summary>
        /// <param name="isolationLevel">One of the <c>IsolationLevel</c> values.</param>
        /// <returns></returns>
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            if (State != ConnectionState.Open)
                throw new InvalidOperationException("Connection is not open");

            if (isolationLevel != IsolationLevel.Serializable)
                throw new ArgumentException(string.Format(
                        "Isolation level {0} is not supported",
                        isolationLevel),
                    "isolationLevel");

            return new MonetDbTransaction(this, isolationLevel);
        }

        /// <summary>
        /// Creates and returns a Command object associated with the connection.
        /// </summary>
        /// <returns></returns>
        protected override DbCommand CreateDbCommand()
        {
            return new MonetDbCommand("", this);
        }

        protected override void Dispose(bool disposing)
        {
            if (this._disposed)
            {
                return;
            }

            if (disposing)
            {
                Close();
            }

            base.Dispose(disposing);

            this._disposed = true;
        }

        private void ParseConnectionString(string connectionString)
        {
            _host = _username = _password = database = null;
            _port = 50000;

            foreach (var setting in connectionString.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries))
            {
                var keyValue = setting.Split(new[] { '=' }, StringSplitOptions.RemoveEmptyEntries);
                if (keyValue.Length != 2)
                {
                    throw new ArgumentException(
                        string.Format("ConnectionString is not well-formed: {0}", setting),
                        "connectionString");
                }

                var key = keyValue[0].ToLowerInvariant().Trim();
                var value = keyValue[1];

                switch (key)
                {
                    case "host":
                        _host = value;
                        break;
                    case "port":
                        if (!int.TryParse(value, out _port))
                        {
                            throw new ArgumentException(
                                string.Format("Port is not a valid integer: {0}", value),
                                "connectionString");
                        }
                        break;
                    case "username":
                        _username = value;
                        break;
                    case "password":
                        _password = value;
                        break;
                    case "database":
                        database = value;
                        break;
                    case "poolminimum":
                        int tempPoolMin;
                        if (!int.TryParse(value, out tempPoolMin))
                        {
                            throw new ArgumentException(
                                string.Format("poolminimum is not a valid integer: {0}", value),
                                "connectionString");
                        }

                        if (tempPoolMin > _minPoolConnections)
                        {
                            _minPoolConnections = tempPoolMin;
                        }
                        break;
                    case "poolmaximum":
                        int tempPoolMax;
                        if (!int.TryParse(value, out tempPoolMax))
                        {
                            throw new ArgumentException(
                                string.Format("poolmaximum is not a valid integer: {0}", value),
                                "connectionString");
                        }

                        if (tempPoolMax > _maxPoolConnections)
                        {
                            _maxPoolConnections = tempPoolMax;
                        }
                        break;
                }
            }

            if (string.IsNullOrEmpty(Database))
            {
                throw new ArgumentException("Database name not specified. Please specify database.",
                    "connectionString");
            }
        }

        private void PrepareExecution()
        {
            if (this._currentReplySize != this.ReplySize)
            {
                this._socket.ExecuteControlSql("reply_size " + this._currentReplySize);
                this._currentReplySize = this.ReplySize;
            }
        }
    }
}