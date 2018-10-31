namespace MonetDb.Mapi.Helpers.Mapi
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    using MonetDb.Mapi.Extensions;

    /// <summary>
    /// This class process the stream into enumerated list of the MonetDBQueryResponseInfo objects which represent executed
    /// statements in the batch. IEnumerable is used to facilitate lazy execution and eliminate the need in materialization
    /// of the results returned by the server.
    /// </summary>
    internal class ResultEnumerator
    {
        private string _temp;
        private StreamReader _stream;
#if DEBUG
        private int count;
#endif

        public ResultEnumerator(StreamReader stream)
        {
            _stream = stream;
        }

        private IEnumerable<List<string>> GetRows()
        {
            while (_temp[0] == '[')
            {
                yield return SplitDataInColumns(_temp);
#if DEBUG
                this.count++;
#endif
                _temp = _stream.ReadLine();

                if (_temp == null)
                    throw new IOException("Cannot read closed stream");
            }
        }

        private static IEnumerable<string> SplitCommaTabs(string s)
        {
            return s.Split(',').Select(v => v.Trim(' ', '\t'));
        }

        private static IEnumerable<string> ExtractValuesList(string s, string start, string end)
        {
            var startIndex = s.IndexOf(start, StringComparison.Ordinal);
            var endIndex = s.IndexOf(end, StringComparison.Ordinal);
            return SplitCommaTabs(s.Substring(startIndex + 1, endIndex - startIndex - 1));
        }

        private static KeyValuePair<string, List<string>> SplitColumnInfoLine(string s)
        {
            return new KeyValuePair<string, List<string>>(s.Substring(s.IndexOf('#') + 1).Trim(), new List<string>(ExtractValuesList(s, "%", "#")));
        }

        private static List<string> SplitDataInColumns(string s)
        {
            return new List<string>(Lexer.Parse(s, '[', ']'));
        }

        private static List<MonetDbColumnInfo> GetColumnInfo(List<string> headerInfo)
        {
            var list = new List<MonetDbColumnInfo>();
            var infoLines = headerInfo.Select(SplitColumnInfoLine);

            foreach (var infoLine in infoLines)
            {
                if (list.Count == 0)
                    list.AddRange(infoLine.Value.Select(ci => new MonetDbColumnInfo()));

                for (var i = 0; i < infoLine.Value.Count; i++)
                {
                    switch (infoLine.Key)
                    {
                        case "table_name":
                            list[i].TableName = infoLine.Value[i];
                            break;
                        case "name":
                            list[i].Name = infoLine.Value[i];
                            break;
                        case "type":
                            list[i].DataType = infoLine.Value[i];
                            break;
                        case "length":
                            list[i].Length = int.Parse(infoLine.Value[i]);
                            break;
                    }
                }
            }

            headerInfo.Clear();
            return list;
        }

        public IEnumerable<QueryResponseInfo> GetResults()
        {
            int count = 0;
            bool first = true;
            var headerInfo = new List<string>();
            while (count > -1)
            {
                this._temp = this._stream.ReadLine();
                if (this._temp == null)
                {
                    throw new IOException("Unexpected end of stream");
                }

                switch (this._temp[0])
                {
                    case '!':
                        var error = "Error! " + this._temp;
                        while (true)
                        {
                            this._temp = this._stream.ReadLine();
                            if (this._temp == ".")
                            {
                                throw new MonetDbException(error);
                            }

                            error += Environment.NewLine + this._temp.Substring(1);
                        }

                    case '%':
                    case '#':
                        headerInfo.Add(_temp);
                        break;

                    case '&':
                        break;

                    case '[':
                        var ri = this._temp.ToQueryResponseInfo();
                        ri.Columns = GetColumnInfo(headerInfo);
                        ri.Data = GetRows();
                        yield return ri;
                        count++;
                        break;

                    default:
                        if (!first)
                        {
                            if (count == 0)
                            {
                                yield return new QueryResponseInfo
                                {
                                    Data = new List<List<string>>(),
                                    Columns = new List<MonetDbColumnInfo>()
                                };
                            }

                            count = -1;
                        }

                        break;
                }

                first = false;
            }

            _stream = null;
        }
    }
}
