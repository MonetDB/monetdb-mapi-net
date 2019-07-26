namespace UnitTests
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    using MonetDb.Mapi;
    using MonetDb.Mapi.Helpers.Mapi;

    [TestClass]
    public class UnitTest
    {
        private const string TestConnectionString =
            "host=127.0.0.1;port=50000;username=monetdb;password=monetdb;database=demo;";

        public TestContext TestContext { get; set; }

        [TestMethod]
        public void TestConnect()
        {
            var connection = new MonetDbConnection();
            Assert.IsTrue(connection.State == ConnectionState.Closed);

            Assert.ThrowsException<InvalidOperationException>(() => connection.Open());

            connection = new MonetDbConnection(TestConnectionString);
            connection.Open();
            Assert.IsTrue(connection.State == ConnectionState.Open);
            Assert.AreEqual(connection.Database, "demo");
            connection.Close();
            Assert.IsTrue(connection.State == ConnectionState.Closed);
            Assert.AreEqual(connection.Database, "demo");

            try
            {
                connection = new MonetDbConnection(TestConnectionString.Replace("ssl=false", "ssl=true"));
                connection.Open();
            }
            catch (MonetDbException ex)
            {
                if (ex.Message.IndexOf("not supported", StringComparison.InvariantCultureIgnoreCase) < 0)
                    throw;
            }
            finally
            {
                connection.Close();
            }
        }

        [TestMethod]
        public void TestConnectMalformed1()
        {
            Assert.ThrowsException<ArgumentException>(() =>
            {
                new MonetDbConnection(TestConnectionString.Replace("port=50000", "port=asb"));
            });
        }

        [TestMethod]
        public void TestConnectMalformed2()
        {
            Assert.ThrowsException<ArgumentException>(() =>
            {
                new MonetDbConnection(TestConnectionString.Replace("port=50000", "port"));
            });
        }

        [TestMethod]
        public void TestConnectWrongDatabase()
        {
            new MonetDbConnection("host=localhost;port=50000;username=monetdb;password=monetdb;database=wrong");
        }

        [TestMethod]
        public void TestConnectNoDatabase()
        {
            Assert.ThrowsException<ArgumentException>(() =>
            {
                new MonetDbConnection("host=localhost;port=50000;username=monetdb;password=monetdb");
            });
        }

        [TestMethod]
        public void TestConnectDoubleOpen()
        {
            Assert.ThrowsException<InvalidOperationException>(() =>
            {
                var conn = new MonetDbConnection(TestConnectionString);
                conn.Open();
                conn.Open();
            });
        }

        [TestMethod]
        public void TestChangeDatabase()
        {
            Assert.ThrowsException<MonetDbException>(() =>
            {
                var conn = new MonetDbConnection(TestConnectionString);
                conn.Open();
                Assert.IsTrue(conn.State == ConnectionState.Open);

                conn.ChangeDatabase("demo2");
            },
                "This should throw a message that the database doesn't exist, but it's successfully changing the database name and reconnecting if it's doing so");
        }

        [TestMethod]
        public void TestConnectionPooling()
        {
            //This test is intended to be run through a debugger and see if the connection pooling is 
            //dynamically creating and destroying the connection pools.
            //Only run this test, because the other tests will mess up the connection pool settings...
            //I know it's not very TDD and this is a code smell, but this is pretty standard fare for
            //database connectivity.
            var modifiedConnString = TestConnectionString + "poolminimum=1;poolmaximum=5;";
            var connections = new MonetDbConnection[5];

            for (var i = 0; i < connections.Length; i++)
            {
                connections[i] = new MonetDbConnection(modifiedConnString);
                connections[i].Open();
            }

            foreach (var connection in connections)
            {
                connection.Close();
            }
        }

        [TestMethod]
        public void TestCreateInsertSelectDropTable()
        {
            // random table name
            var tableName = Guid.NewGuid().ToString();

            // random integer value
            var value = new Random().Next();

            // SQL scripts
            var createScript = string.Format("CREATE TABLE \"{0}\" (id int);", tableName);
            var insertScript = string.Format("INSERT INTO \"{0}\" VALUES({1});", tableName, value);
            var selectScript = string.Format("SELECT * FROM \"{0}\";", tableName);
            var dropScript = string.Format("DROP TABLE \"{0}\";", tableName);

            using (var connection = new MonetDbConnection(TestConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    // create table
                    command.CommandText = createScript;
                    command.ExecuteNonQuery();

                    // insert into
                    command.CommandText = insertScript;
                    // rows affected 0 or 1
                    Assert.IsTrue(new[] { 0, 1 }.Contains(command.ExecuteNonQuery()));

                    // select from
                    command.CommandText = selectScript;
                    var value2 = (int)command.ExecuteScalar();
                    Assert.AreEqual(value, value2);

                    // drop table
                    command.CommandText = dropScript;
                    command.ExecuteNonQuery();
                }
            }
        }

        [TestMethod]
        public void TestLexer()
        {
            var combinations = GetCombination(new[] {
                "123",
                "2.32",
                "-5.3",
                "+2",
                "-2.007e10",
                "true",
                "null",
                "2018-08-20",
                "2018-08-20 10:00:00.000000"
            }).ToArray();

            var sw = Stopwatch.StartNew();
            foreach (var item in combinations)
            {
                var str = string.Join(",", item);
                var parsed = Lexer.Parse($"[{str}]", '[', ']').ToArray();
                Assert.IsTrue(item.SequenceEqual(parsed));

                str = string.Join("\t,\t", item);
                parsed = Lexer.Parse($"[\t{str}\t]", '[', ']').ToArray();
                Assert.IsTrue(item.SequenceEqual(parsed));
            }

            sw.Stop();

            this.TestContext.WriteLine($"TestLexer: {combinations.Length} combinations, {sw.ElapsedMilliseconds} ms");
        }

        [TestMethod]
        public void TestParallel()
        {
            Parallel.For(0, 100, new ParallelOptions { MaxDegreeOfParallelism = 2 }, i =>
            {
                using (var connection = new MonetDbConnection(TestConnectionString))
                {
                    connection.Open();

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = $"SELECT {i} as n";

                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Assert.AreEqual(i, reader.GetInt32(0));
                            }
                        }
                    }
                }
            });
        }

        [TestMethod]
        public void TestQuotes()
        {
            using (var connection = new MonetDbConnection(TestConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"SELECT 'q \"q' as n";

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            Assert.AreEqual("q \"q", reader.GetString(0));
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void ConnectionStringBuilderTest()
        {
            var cs = new MonetDbConnectionStringBuilder();
            Assert.AreEqual(cs.ConnectionString, cs.ToString());
            Assert.AreEqual(cs.ConnectionString, "HOST=localhost;PORT=50000;POOLMINIMUM=100;POOLMAXIMUM=0");

            cs.Host = "localhost";
            cs.Port = 12345;
            cs.Username = "testUser";
            cs.Password = "testPwd";
            Assert.AreEqual(cs.ConnectionString, cs.ToString());
            Assert.AreEqual(cs.ConnectionString, "HOST=localhost;PORT=12345;POOLMINIMUM=100;POOLMAXIMUM=0;USERNAME=testUser;PASSWORD=testPwd");

            cs = new MonetDbConnectionStringBuilder
            {
                Host = "localhost",
                Port = 12345,
                Username = "testUser",
                Password = "testPwd"
            };
            Assert.AreEqual(cs.ConnectionString, cs.ToString());
            Assert.AreEqual(cs.ConnectionString, "HOST=localhost;PORT=12345;POOLMINIMUM=100;POOLMAXIMUM=0;USERNAME=testUser;PASSWORD=testPwd");
            
            cs = new MonetDbConnectionStringBuilder
            {
                ConnectionString = "HOST=localhost;PORT=12345;POOLMINIMUM=100;POOLMAXIMUM=0;USERNAME=testUser;PASSWORD=testPwd"
            };
            Assert.AreEqual("localhost", cs.Host);
            Assert.AreEqual(12345, cs.Port);
            Assert.AreEqual("testUser", cs.Username);
            Assert.AreEqual("testPwd", cs.Password);
        }

        [TestMethod]
        public void LargeQueryTest()
        {
            var n = 50;
            var cols = new string[n];
            for (int i = 0; i < n; i++)
            {
                cols[i] = "col";
                for (int k = 0; k <= i; k++)
                {
                    cols[i] += k;
                }
            }

            var sql = "SELECT '" + string.Join("', '", cols) + "';";

            var length = sql.Length;

            using (var connection = new MonetDbConnection(TestConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = sql;

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            for (int i = 0; i < n; i++)
                            {
                                Assert.AreEqual(cols[i], reader.GetString(i));
                            }
                        }
                    }
                }
            }
        }

        [TestMethod]
        public void BulkLoadSTDINTest()
        {
            // random table name
            var tableName = Guid.NewGuid().ToString();

            var id = 1;
            var data = new bool[10].ToDictionary(x => id++, x => Guid.NewGuid().ToString());

            // SQL scripts
            var createScript = string.Format("CREATE TABLE \"{0}\" (id int, t text);", tableName);
            var copyScript = new StringBuilder($"COPY 10 records INTO \"{tableName}\" (id,t) FROM STDIN DELIMITERS ',','\\n';");
            var selectScript = string.Format("SELECT * FROM \"{0}\";", tableName);
            var dropScript = string.Format("DROP TABLE \"{0}\";", tableName);

            using (var connection = new MonetDbConnection(TestConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    // create table
                    command.CommandText = createScript;
                    command.ExecuteNonQuery();

                    try
                    {
                        command.CommandText = copyScript.ToString();
                        command.ExecuteNonQuery();

                        copyScript.Clear();
                        foreach (var row in data)
                        {
                            copyScript.AppendLine($"{row.Key},{row.Value}");
                        }

                        // copyScript.Append("^D");

                        command.CommandText = copyScript.ToString();
                        command.ExecuteNonQuery();

                        // select from
                        command.CommandText = selectScript;
                        var reader = command.ExecuteReader();
                        while (reader.Read())
                        {
                            id = reader.GetInt32(0);
                            var t = reader.GetString(1);
                        }
                    }
                    finally
                    {
                        // drop table
                        command.CommandText = dropScript;
                        command.ExecuteNonQuery();
                    }
                }
            }
        }

        [TestMethod]
        public void TestMultiLine()
        {
            using (var connection = new MonetDbConnection(TestConnectionString))
            {
                connection.Open();

                Assert.ThrowsException<MonetDbException>(() =>
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = @"
SELECT 1 as n
fail";

                        using (var reader = command.ExecuteReader())
                        {
                            reader.Read();
                        }
                    }
                });

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
SELECT 1 as n
UNION ALL
SELECT 2 as n";

                    using (var reader = command.ExecuteReader())
                    {
                        reader.Read();
                        Assert.AreEqual(1, reader.GetInt32(0));

                        reader.Read();
                        Assert.AreEqual(2, reader.GetInt32(0));
                    }
                }
            }
        }

        [TestMethod]
        public void TestSchemaTable()
        {
            return;
            var query = "SELECT * FROM env();";

            using (var connection = new MonetDbConnection(TestConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    // create table
                    command.CommandText = query;

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var schema = reader.GetSchemaTable();

                            Assert.IsNotNull(schema);
                            Assert.IsTrue(schema.Columns.Contains("name"));
                            Assert.IsTrue(schema.Columns.Contains("value"));
                            Assert.AreEqual(typeof(string), schema.Columns["name"].DataType);
                            Assert.AreEqual(typeof(string), schema.Columns["value"].DataType);
                        }
                    }
                }
            }
        }

        private static IEnumerable<IEnumerable<T>> GetCombination<T>(T[] list)
        {
            if (list.Length == 1)
            {
                yield return list;
            }
            else
            {
                for (int i = 0; i < list.Length; i++)
                {
                    var subList = new T[list.Length - 1];
                    int si = 0;
                    for (; si < i; si++)
                    {
                        subList[si] = list[si];
                    }

                    for (si++; si < list.Length; si++)
                    {
                        subList[si - 1] = list[si];
                    }

                    foreach (var sub in GetCombination(subList))
                    {
                        yield return new List<T> { list[i] }.Concat(sub);
                    }
                }
            }
        }
    }
}