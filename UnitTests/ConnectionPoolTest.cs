using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;
using MonetDb.Mapi.Helpers;
using MonetDb.Mapi.Helpers.Mapi;

namespace UnitTests
{
    [TestClass]
    public class ConnectionPoolTest
    {
        public static ConnectionPool _pool;
        public static int _maxSockets = 2;

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
