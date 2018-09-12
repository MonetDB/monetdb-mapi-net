namespace MonetDb.Mapi
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;

    internal class MonetDbParameterCollection : List<IDbDataParameter>, IDataParameterCollection
    {
        public bool Contains(string parameterName)
        {
            return this.Any(param => param.ParameterName.Equals(parameterName));
        }

        public int IndexOf(string parameterName)
        {
            return this.FindIndex(param => param.ParameterName.Equals(parameterName));
        }

        public void RemoveAt(string parameterName)
        {
            var index = this.IndexOf(parameterName);
            if (index > -1)
            {
                this.RemoveAt(index);
            }
        }

        public object this[string parameterName]
        {
            get
            {
                return this.FirstOrDefault(param => param.ParameterName.Equals(parameterName));
            }
            set
            {
                var index = this.IndexOf(parameterName);
                if (index > -1)
                {
                    this[index] = (IDbDataParameter)value;
                }
                else
                {
                    Add((IDbDataParameter)value);
                }
            }
        }
    }

    internal class MonetDbParameter : IDbDataParameter
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

        #region IDbDataParameter Members

        public byte Precision { get; set; }

        public byte Scale { get; set; }

        public int Size { get; set; }

        #endregion

        #region IDataParameter Members

        public DbType DbType { get; set; }

        public ParameterDirection Direction { get; set; }

        public bool IsNullable { get; set; }

        public string ParameterName { get; set; }

        public string SourceColumn { get; set; }

        public DataRowVersion SourceVersion { get; set; }

        public object Value { get; set; }

        #endregion
    }
}