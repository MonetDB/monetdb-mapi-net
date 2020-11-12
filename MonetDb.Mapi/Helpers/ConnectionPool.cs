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
        class ConnectionPool : IDisposable
    {
        private readonly object lockObj = new object();
        private readonly ConcurrentQueue<Socket> Active = new ConcurrentQueue<Socket>();
        private readonly List<Socket> Busy = new List<Socket>();

        private readonly int min;
        private readonly int max;
        private readonly string password;

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

        ~ConnectionPool()
        {
            this.Dispose();
        }

        public string Host { get; set; }

        public int Port { get; set; }

        public string Username { get; set; }

        public string Database { get; set; }

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

        public bool Remove(Socket socket)
        {
            lock (this.lockObj)
            {
                return this.Busy.Remove(socket);
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