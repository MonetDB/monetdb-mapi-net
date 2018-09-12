namespace MonetDb.Mapi.Helpers.Mapi
{
    using System.Collections.Generic;

    internal class QueryResponseInfo
    {
        private int? columnCount;

        public int Id;
        public int ColumnCount
        {
            get
            {
                if (this.columnCount == null)
                {
                    if (this.Columns != null)
                    {
                        return this.Columns.Count;
                    }
                    else
                    {
                        return 0;
                    }
                }
                else
                {
                    return this.columnCount.Value;
                }
            }

            set
            {
                this.columnCount = value;
            }
        }

        public int RowCount { get; set; }

        public int TupleCount { get; set; }

        public int RecordsAffected { get; set; }

        public List<MonetDbColumnInfo> Columns { get; set; }

        public IEnumerable<List<string>> Data { get; set; }
    }
}