namespace MonetDb.Mapi
{
    using System;
    using System.Data;
    using System.Data.Common;

    /// <summary>
    /// Represents a local transaction.
    /// </summary>
    public class MonetDbTransaction : DbTransaction
    {
        private readonly MonetDbConnection _connection;
        private readonly IsolationLevel _isolation;

        private readonly object _syncLock = new object();

        /// <summary>
        /// Initializes a new transaction with the MonetDB server with this particular connection.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="isolationLevel"></param>
        internal MonetDbTransaction(MonetDbConnection connection, IsolationLevel isolationLevel)
        {
            // MonetDb only support "Serializable" isolation level
            if (isolationLevel != IsolationLevel.Serializable)
            {
                throw new ArgumentException($"Isolation level {isolationLevel} is not supported", nameof(isolationLevel));
            }

            this._connection = connection ?? throw new ArgumentNullException("connection");
            this._isolation = IsolationLevel;

            this.Start();
        }

        /// <summary>
        /// Specifies the Connection object to associate with the transaction
        /// </summary>
        protected override DbConnection DbConnection => this._connection;

        /// <summary>
        /// Specifies the <c>IsolationLevel</c> for this transaction
        /// </summary>
        public override IsolationLevel IsolationLevel
        {
            get { return _isolation; }
        }

        /// <summary>
        /// Commits the database transaction.
        /// </summary>
        public override void Commit()
        {
            lock (_syncLock)
            {
                CheckConnection();

                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = "COMMIT;";
                    command.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Rolls back a transaction from a pending state.
        /// </summary>
        public override void Rollback()
        {
            lock (_syncLock)
            {
                CheckConnection();

                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = "ROLLBACK;";
                    command.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Checks connection
        /// </summary>
        private void CheckConnection()
        {
            if (_connection == null ||
                _connection.State != ConnectionState.Open)
                throw new MonetDbException("Connection unexpectedly disposed or closed");
        }

        /// <summary>
        /// Start the database transaction
        /// </summary>
        private void Start()
        {
            lock (_syncLock)
            {
                CheckConnection();

                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = "START TRANSACTION;";
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}