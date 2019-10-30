using System;
using System.Collections.Generic;

using Microsoft.AspNetCore.Mvc;

using MonetDb.Mapi;

namespace WebApi.Controllers
{
    [Route("api/[controller]")]
    public class SampleDataController : Controller
    {
        private static string[] Summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };

        [HttpPost("[action]")]
        public object Execute([FromBody]Request request)
        {
            try
            {
                var csb = new MonetDbConnectionStringBuilder
                {
                    Host = request.Host,
                    Port = request.Port,
                    Username = request.Username,
                    Password = request.Password,
                    Database = request.Database
                };

                using (var connection = new MonetDbConnection(csb.ToString()))
                {
                    connection.Open();

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = request.Query;

                        using (var reader = command.ExecuteReader())
                        {
                            var result = new Result(reader.FieldCount);
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                result.Meta[i] = reader.GetName(i);
                            }

                            while (reader.Read())
                            {
                                var row = new object[reader.FieldCount];
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    row[i] = reader.GetValue(i);
                                }

                                result.Data.Add(row);
                            }

                            return result;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                return new
                {
                    Error = ex.Message + Environment.NewLine + ex.StackTrace
                };
            }
        }

        public class Result
        {
            public Result(int count)
            {
                this.Meta = new string[count];
                this.Data = new List<object[]>();
            }

            public string[] Meta { get; set; }

            public List<object[]> Data { get; set; }
        }

        public class Request
        {
            public string Host { get; set; }

            public int Port { get; set; }

            public string Username { get; set; }

            public string Password { get; set; }

            public string Database { get; set; }

            public string Query { get; set; }
        }
    }
}