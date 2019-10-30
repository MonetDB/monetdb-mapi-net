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
    using System.Timers;

    using MonetDb.Mapi.Helpers;
    using MonetDb.Mapi.Helpers.Mapi;

    /// <summary>
    /// Handles the accounting for the connections to the database.  Handles the connection
    /// pooling of the connections.
    /// </summary>
    internal static class MonetDbConnectionFactory
    {
        private static readonly Dictionary<string, ConnectionPool> connectionPools = new Dictionary<string, ConnectionPool>();
        private static readonly Timer MaintenanceTimer = new Timer(1000);

        private static void OnMaintenanceTimerElapsed(object sender, ElapsedEventArgs e)
        {
            MaintenanceTimer.Stop();
            ICollection<ConnectionPool> connections;

            lock (connectionPools)
            {
                connections = connectionPools.Values;
            }

            foreach (var pool in connections)
            {
                lock (pool)
                {
                    pool.Clear();
                }
            }

            MaintenanceTimer.Start();
        }

        static MonetDbConnectionFactory()
        {
            MaintenanceTimer.Elapsed += OnMaintenanceTimerElapsed;

            MaintenanceTimer.Start();
        }

        /// <summary>
        /// Returns a connection from the connection pool.
        /// </summary>
        /// <param name="host"></param>
        /// <param name="port"></param>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <param name="database"></param>
        /// <param name="maxConn"></param>
        /// <param name="minConn"></param>
        /// <returns></returns>
        public static Socket GetConnection(string host, int port, string username, string password, string database, int minConn, int maxConn)
        {
            if (minConn < 1)
            {
                throw new ArgumentOutOfRangeException("minConn", minConn + "", "The minimum number of connections cannot be less than 1");
            }

            if (maxConn < 1)
            {
                throw new ArgumentOutOfRangeException("maxConn", maxConn + "", "The mamimum number of connections cannot be less than 1");
            }

            if (minConn > maxConn)
            {
                throw new ArgumentException("The maximum number of connections cannot be greater than the minimum number of connections");
            }

            return GetPool(host, port, username, password, database, minConn, maxConn).Dequeue();
        }

        public static void CloseConnection(Socket socket, string database, Action callback = null)
        {
            socket.Pool?.Free(socket, callback);
        }

        public static void RemoveConnection(Socket socket, string database, Action callback = null)
        {
            socket.Pool?.Remove(socket, callback);
        }

        public static string GetConnectionPoolKey(string host, int port, string username, string database)
        {
            return string.Format("{0}_{1}_{2}_{3}", host, port, username, database);
        }

        private static ConnectionPool GetPool(string host, int port, string username, string password, string database, int minConn, int maxConn)
        {
            var key = GetConnectionPoolKey(host, port, username, database);
            ConnectionPool pool;
            lock (connectionPools)
            {
                if (!connectionPools.TryGetValue(key, out pool))
                {
                    pool = connectionPools[key] = new ConnectionPool(host, port, username, password, database, minConn, maxConn);
                }
            }

            return pool;
        }
    }
}