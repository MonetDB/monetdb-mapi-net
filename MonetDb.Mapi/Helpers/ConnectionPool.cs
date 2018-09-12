namespace MonetDb.Mapi.Helpers
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;

    using MonetDb.Mapi.Helpers.Mapi;

    public class ConnectionPool : IDisposable
    {
        private readonly object lockObj = new object();
        private readonly ConcurrentQueue<Socket> Active = new ConcurrentQueue<Socket>();
        private readonly List<Socket> Busy = new List<Socket>();

        private readonly int min;
        private readonly int max;
        private readonly string host;
        private readonly int port;
        private readonly string username;
        private readonly string password;
        private readonly string database;

        public ConnectionPool(string host, int port, string username, string password, string database, int min, int max)
        {
            this.min = min;
            this.max = max;
            this.host = host;
            this.port = port;
            this.username = username;
            this.password = password;
            this.database = database;

            this.Init();
        }

        ~ConnectionPool()
        {
            this.Dispose();
        }

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

        public void Free(Socket socket)
        {
            lock (this.lockObj)
            {
                if (!this.Busy.Remove(socket))
                {
                    throw new Exception("Socket is not busy");
                }

                this.Active.Enqueue(socket);
            }
        }

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
                try
                {
                    var socket = new Socket();
                    socket.Connect(host, port, username, password, database);
                    this.Active.Enqueue(socket);
                }
                catch (IOException ex)
                {
                    throw new MonetDbException(ex, "Problem connecting to the MonetDB server.");
                }

            }
        }
    }
}