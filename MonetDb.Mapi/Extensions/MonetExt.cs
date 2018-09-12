namespace MonetDb.Mapi.Extensions
{
    using MonetDb.Mapi.Helpers.Mapi;

    internal static class MonetExt
    {
        public static QueryResponseInfo ToQueryResponseInfo(this string info)
        {
            var sParts = info.Substring(1).Split(' ');

            switch (sParts[0])
            {
                case "1":
                case "5":
                    return new QueryResponseInfo
                    {
                        Id = int.Parse(sParts[1]),
                        TupleCount = int.Parse(sParts[2]),
                        ColumnCount = int.Parse(sParts[3]),
                        RowCount = int.Parse(sParts[4])
                    };
            }

            return new QueryResponseInfo();
        }
    }
}