namespace MonetDb.Mapi.Helpers
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;

    using MonetDb.Mapi.Helpers.Mapi;

#if DEBUG
    public
#else
        internal
#endif

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
    class ConnectionPool : IDisposable
#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
    {
        private readonly object lockObj = new object();
        public readonly ConcurrentQueue<Socket> Active = new ConcurrentQueue<Socket>();
        public readonly List<Socket> Busy = new List<Socket>();

        private readonly int min;
        private readonly int max;
        private readonly string password;

        /// <summary>
        /// Construct a connection pool
        /// </summary>
        /// <param name="host"></param>
        /// <param name="port"></param>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <param name="database"></param>
        /// <param name="min"></param>
        /// <param name="max"></param>
        public ConnectionPool(string host, int port, string username, string password, string database, int min, int max)
        {
            this.min = min;
            this.max = max;
            this.Host = host;
            this.Port = port;
            this.Username = username;
            this.password = password;
            this.Database = database;

            this.Init();
        }

        /// <summary>
        /// Dispose of ConnectionPool
        /// </summary>
        ~ConnectionPool()
        {
            this.Dispose();
        }

        /// <summary>
        /// Host Name of connection
        /// </summary>
        public string Host { get; set; }

        /// <summary>
        /// Port number
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        /// User Name of user
        /// </summary>
        public string Username { get; set; }

        /// <summary>
        /// Database name 
        /// </summary>
        public string Database { get; set; }

        /// <summary>
        /// Removes and returns the socket at the beginning of the queue.
        /// </summary>
        /// <returns>The first socket in the queue, null if fails</returns>
        public Socket Dequeue()
        {
            lock (this.lockObj)
            {
                this.Init();
                if (this.Active.TryDequeue(out var socket))
                {
                    this.Busy.Add(socket);
                    return socket;
                }
            }

            return null;
        }
        
        /// <summary>
        /// Free Socket Connection
        /// </summary>
        /// <param name="socket"></param>
        /// <returns></returns>
        public bool Free(Socket socket)
        {
            lock (this.lockObj)
            {
                if (this.Busy.Remove(socket))
                {
                    this.Active.Enqueue(socket);
                    return true;
                }

                return false;
            }
        }

        /// <summary>
        /// Remove Socket from connection pool
        /// </summary>
        /// <param name="socket"></param>
        /// <returns></returns>
        public bool Remove(Socket socket)
        {
            lock (this.lockObj)
            {
                return this.Busy.Remove(socket);
            }
        }

        /// <summary>
        /// Close all sockets of current connection
        /// </summary>
        public void Clear()
        {
            lock (this.lockObj)
            {
                for (var i = 0; i < this.Active.Count + this.Busy.Count - this.min && this.Active.Count > 0; i++)
                {
                    if (this.Active.TryPeek(out var socket))
                    {
                        if (socket.Created > DateTime.Now.AddMinutes(5))
                        {
                            socket.Close();
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Dispose of socket
        /// </summary>
        public void Dispose()
        {
            while (this.Active.TryDequeue(out var socket))
            {
                socket.Close();
            }
        }

        private void Init()
        {
            for (var i = this.Active.Count + this.Busy.Count; i < this.min || (this.Active.Count == 0 && i < this.max); i++)
            {
                var socket = new Socket(this);
                try
                {
                    socket.Connect(password);
                    this.Active.Enqueue(socket);
                }
                catch (IOException ex)
                {
                    try
                    {
                        socket?.Dispose();
                    }
                    catch
                    {
                    }

                    throw new MonetDbException(ex, "Problem connecting to the MonetDB server.");
                }
            }
        }
    }
}