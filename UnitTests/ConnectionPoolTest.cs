using Microsoft.VisualStudio.TestTools.UnitTesting;
using MonetDb.Mapi;
using MonetDb.Mapi.Helpers;
using System;

#if DEBUG
namespace UnitTests
{

    [TestClass]
    public class ConnectionPoolTest
    {
        public static ConnectionPool _pool;
        public static int _maxSockets = 2;

        private const string TestConnectionString =
            "host=127.0.0.1;port=50000;username=monetdb;password=monetdb;database=demo;";


        [AssemblyInitialize]
        public static void AssemblyInit(TestContext context)
        {
            ConnectionPool pool = new ConnectionPool(host: "localhost",
                port: 50000,
                username: "monetdb",
                password: "monetdb",
                database: "demo",
                min: _maxSockets,
                max: _maxSockets
            ); ;

            _pool = pool;
        }


        [TestMethod]  
        public void CheckIfPoolCanBeConstructed()
        {
            Assert.IsNotNull(_pool);
        }

        [TestMethod]
        public void CheckIfSocketsWereAdded()
        {
            Assert.IsTrue(_pool.Active.Count > 0);
        }

        [TestMethod] 
        public void CheckIfPoolsAreCorrectlyConstructed()
        {
            var modifiedConnString = TestConnectionString + "poolminimum=1;poolmaximum=16;";
            var connections = new MonetDbConnection[16];

            for (var i = 0; i < connections.Length; i++)
            {
                connections[i] = new MonetDbConnection(modifiedConnString);
                connections[i].Open();
                var cmd = new MonetDbCommand("select 1", connections[i]);
                cmd.ExecuteScalar();
            }

            foreach (var connection in connections)
            {
                connection.Close();
            }

            Assert.AreEqual(16, connections.Length);
        }
       
        [TestMethod]
        public void CheckIfPoppedSocketIsNotInActive()
        {
            _pool.Dequeue();

            Assert.IsTrue(_pool.Active.Count == _maxSockets - 1);
        }

        [TestMethod]
        public void CheckIfPoolIsCleared()
        {
            _pool.Dispose();

            Assert.IsTrue(_pool.Active.Count == 0);
        }
    }
}
#endif
