namespace MonetDb.Mapi.Helpers.Mapi
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net.Sockets;

    using MonetDb.Mapi.Enums;
    using MonetDb.Mapi.Helpers.Mapi.Protocols;

    /// <summary>
    /// MapiSocket is a class for talking to a MonetDB server with the MAPI protocol.
    /// MAPI is a line oriented protocol that talks UTF8 so we wrap a TCP socket with
    /// StreamReader and StreamWriter streams to handle conversion.
    /// 
    /// MapiSocket logs into the MonetDB server, since the socket is worthless if it's
    /// not logged in.
    /// </summary>
    
#if DEBUG
    public
#else
        internal
#endif
        sealed class Socket : IDisposable
    {
        private const int MAXQUERYSIZE = 1020; // 1024

        private TcpClient _socket;

        private StreamReader fromDatabase;

        private StreamWriter toDatabase;

        public readonly DateTime Created;

        public Socket(ConnectionPool pool)
        {
            Created = DateTime.Now;

            this.Pool = pool;

            this.Host = pool.Host;
            this.Port = pool.Port;
            this.Username = pool.Username;
            this.Database = pool.Database;

            // register protocols
            MapiProtocolFactory.Register<MapiProtocolVersion8>(8);
            MapiProtocolFactory.Register<MapiProtocolVersion9>(9);
        }

        public NeedMore NeedMore { get; set; } = new NeedMore();

        internal long ProcessId { get; set; }

        public void Close()
        {
            Dispose();
        }

        /// <summary>
        /// Connect with password.  Returns a list of any warnings from the server.
        /// </summary>
        /// <param name="password"></param>
        /// <returns></returns>
        public IList<string> Connect(string password)
        {
            return this.Connect(this.Host, this.Port, this.Username, password, this.Database, true);
        }

        /// <summary>
        /// Connects to a given host.  Returns a list of any warnings from the server.
        /// </summary>
        /// <param name="host"></param>
        /// <param name="port"></param>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        private IList<string> Connect(string host, int port, string username, string password, string database, bool makeConnection)
        {
            this.Database = database;
            this.Host = host;
            this.Port = port;
            this.Username = username;

            if (makeConnection)
            {
                this._socket = new TcpClient(this.Host, this.Port)
                {
                    NoDelay = true,
                    ReceiveTimeout = 60 * 2 * 1000,
                    SendBufferSize = 60 * 2 * 1000
                };

                this.fromDatabase = new StreamReader(new Stream(this._socket.GetStream()));
                this.toDatabase = new StreamWriter(new Stream(this._socket.GetStream()))
                {
                    NewLine = "\n"
                };
            }

            var challenge = fromDatabase.ReadLine();

            // wait till the prompt
            this.WaitForPrompt();
            //FromDatabase.ReadLine();

            var response = GetChallengeResponse(challenge, username, password, "sql", database, null);

            this.toDatabase.WriteLine(response);
            this.toDatabase.Flush();

            var temp = fromDatabase.ReadLine();
            var redirects = new List<string>();
            var warnings = new List<string>();

            while (temp != ".")
            {
                if (string.IsNullOrEmpty(temp))
                    throw new MonetDbException("Connection to the server was lost");

                switch ((DbLineType)temp[0])
                {
                    case DbLineType.Error:
                        throw new MonetDbException(temp.Substring(1));

                    case DbLineType.Info:
                        warnings.Add(temp.Substring(1));
                        break;

                    case DbLineType.Redirect:
                        redirects.Add(temp.Substring(1));
                        break;
                }

                temp = fromDatabase.ReadLine();
            }

            if (redirects.Count <= 0)
            {
#if TRACE
                foreach (var w in warnings)
                {
                    Console.WriteLine("MonetDB: " + w);
                }
#endif
                return warnings;
            }

            return FollowRedirects(redirects, username, password);
        }

        public ConnectionPool Pool { get; set; }

        public string Database { get; set; }

        public string Host { get; private set; }

        public int Port { get; private set; }

        public string Username { get; private set; }

        public void Dispose()
        {
            if (toDatabase != null && _socket.Connected)
            {
                this.toDatabase.Close();
            }

            if (fromDatabase != null && _socket.Connected)
            {
                this.fromDatabase.Close();
            }

            this._socket.Close();
        }

        internal IEnumerable<QueryResponseInfo> ExecuteSql(string sql)
        {
            if (!this.NeedMore)
            {
                this.toDatabase.Write("s");
            }

            int n;
            for (int i = 0; i < sql.Length;)
            {
                n = i + MAXQUERYSIZE;
                if (n > sql.Length)
                {
                    this.toDatabase.WriteLine(sql.Substring(i).TrimEnd(';') + ";");
                    this.toDatabase.Flush();
                    break;
                }
                else
                {
                    this.toDatabase.Write(sql.Substring(i, MAXQUERYSIZE));
                    ((Stream)this.toDatabase.BaseStream).NeedMore = true;
                    this.toDatabase.Flush();
                }

                i = n;
            }

            this.ProcessId = 0;
            return new ResultEnumerator(this, fromDatabase).GetResults();
        }

        public void CancelRequest()
        {
            if (MonetDbEnviroments.CommandCloseStrategy == CommandCloseStrategy.TerminateSession)
            {
                // TODO: send cancel query to MonetDb Server
                MonetDbConnectionFactory.RemoveConnection(this, this.Database);
                this.Dispose();
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void WaitForPrompt()
        {
            while (true)
            {
                var line = this.fromDatabase.ReadLine();
                if (line == null)
                {
                    throw new IOException("Connection to server lost!");
                }

                switch ((byte)line[0])
                {
                    case (byte)DbLineType.Prompt:
                        return;

                    case (byte)DbLineType.Error:
                        throw new MonetDbException(line.Substring(1));
                }
            }
        }

        internal void ExecuteControlSql(string sql)
        {
            toDatabase.WriteLine("X" + sql);
            toDatabase.Flush();

            this.WaitForPrompt();
        }

        /// <summary>
        /// Returns a response string that we should send to the MonetDB server upon initial connection.
        /// The challenge string is sent from the server in the format (without quotes) "challenge:servertype:protocolversion:"
        /// 
        /// For now we only support protocol version 8.
        /// </summary>
        /// <param name="challengeString">initial string sent from server to challenge against</param>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <param name="language"></param>
        /// <param name="database"></param>
        /// <param name="hash">the hash method to use, or null for all supported hashes</param>
        /// <returns></returns>
        private string GetChallengeResponse(
            string challengeString,
            string username, string password,
            string language, string database,
            string hash)
        {
            var tokens = challengeString.Split(':');

            if (tokens.Length <= 4)
            {
                throw new MonetDbException(string.Format(
                    "Server challenge unusable! Challenge contains too few tokens: {0}",
                    challengeString));
            }

            int version;

            if (!int.TryParse(tokens[2], out version))
                throw new MonetDbException("Unknown Mapi protocol {0}", tokens[2]);

            // get Mapi protocol instance
            var protocol = MapiProtocolFactory.GetProtocol(version);

            if (protocol == null)
                throw new MonetDbException("Unsupported protocol version {0}", version);

            return protocol.BuildChallengeResponse(username, password,
                language, tokens,
                database, hash);
        }

        /// <summary>
        /// We try the first url to redirect to.  It's not great, but realistically
        /// we shouldn't get too many redirect urls to redirect to.  Returns all the
        /// new warnings from the new connection.
        /// </summary>
        /// <param name="redirectUrls"></param>
        /// <param name="user"></param>
        /// <param name="password"></param>
        private IList<string> FollowRedirects(IReadOnlyList<string> redirectUrls, string user, string password)
        {
            var suri = redirectUrls[0];
            if (!suri.StartsWith("mapi:"))
            {
                throw new MonetDbException($"Unknown Mapi redirect {suri}");
            }

            var uri = new Uri(suri.Substring(5));
            var merovingian = uri.Scheme == "merovingian";
            var host = uri.Host;
            var port = merovingian ? this.Port : uri.Port;
            var database = uri.Query.TrimStart('?').Split('&')
                .Select(x => x.Split('='))
                .Where(x => x[0] == "database")
                .Select(x => x[1])
                .FirstOrDefault() ?? this.Database;
#if TRACE
            Console.WriteLine($"MonetDB: Redirect to {uri} {database}");
#endif
            return Connect(host, port, user, password, database, !merovingian);
        }
    }
}