namespace MonetDb.Mapi
{
    using System;
    using System.Data;
    using System.Data.Common;

    internal class MonetDbParameter : DbParameter
    {
        internal string GetProperParameter()
        {
            if (this.Value == DBNull.Value)
            {
                return "NULL";
            }

            //  If it is a string then let's sanitize the quotes and enclose the string in quotes
            if (this.Value is string stringValue)
            {
                return "'" + stringValue.Replace("'", "''") + "'";
            }
            else if (this.Value is DateTime dateTime)
            {
                return "'" + dateTime.ToString("yyyy-MM-dd HH:mm:ss") + "'";
            }

            return this.Value.ToString();
        }

        public override void ResetDbType()
        {
            throw new NotImplementedException();
        }

        #region IDbDataParameter Members

        public override byte Precision { get; set; }

        public override byte Scale { get; set; }

        public override int Size { get; set; }

        #endregion

        #region IDataParameter Members

        public override DbType DbType { get; set; }

        public override ParameterDirection Direction { get; set; }

        public override bool IsNullable { get; set; }

        public override string ParameterName { get; set; }

        public override string SourceColumn { get; set; }

        public override DataRowVersion SourceVersion { get; set; }

        public override object Value { get; set; }

        public override bool SourceColumnNullMapping { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        #endregion
    }
}