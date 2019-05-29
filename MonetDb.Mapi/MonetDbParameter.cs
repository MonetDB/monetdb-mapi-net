namespace MonetDb.Mapi
{
    using System;
    using System.Data;
    using System.Data.Common;
    using System.Globalization;

    internal class MonetDbParameter : DbParameter
    {
        private static readonly NumberFormatInfo numberFormatInfo = new NumberFormatInfo
        {
            CurrencyDecimalDigits = NumberFormatInfo.InvariantInfo.CurrencyDecimalDigits,
            CurrencyDecimalSeparator = NumberFormatInfo.InvariantInfo.CurrencyDecimalSeparator,
            CurrencyGroupSeparator = NumberFormatInfo.InvariantInfo.CurrencyGroupSeparator,
            CurrencyGroupSizes = NumberFormatInfo.InvariantInfo.CurrencyGroupSizes,
            CurrencyNegativePattern = NumberFormatInfo.InvariantInfo.CurrencyNegativePattern,
            CurrencyPositivePattern = NumberFormatInfo.InvariantInfo.CurrencyPositivePattern,
            CurrencySymbol = NumberFormatInfo.InvariantInfo.CurrencySymbol,
            NaNSymbol = NumberFormatInfo.InvariantInfo.NaNSymbol,
            NegativeInfinitySymbol = NumberFormatInfo.InvariantInfo.NegativeInfinitySymbol,
            NegativeSign = NumberFormatInfo.InvariantInfo.NegativeSign,
            NumberDecimalDigits = NumberFormatInfo.InvariantInfo.NumberDecimalDigits,
            NumberDecimalSeparator = ".",
            NumberGroupSeparator = NumberFormatInfo.InvariantInfo.NumberGroupSeparator,
            NumberGroupSizes = NumberFormatInfo.InvariantInfo.NumberGroupSizes,
            NumberNegativePattern = NumberFormatInfo.InvariantInfo.NumberNegativePattern,
            PercentDecimalDigits = NumberFormatInfo.InvariantInfo.PercentDecimalDigits,
            PercentDecimalSeparator = ".",
            PercentGroupSeparator = NumberFormatInfo.InvariantInfo.PercentGroupSeparator,
            PercentGroupSizes = NumberFormatInfo.InvariantInfo.PercentGroupSizes,
            PercentNegativePattern = NumberFormatInfo.InvariantInfo.PercentNegativePattern,
            PercentPositivePattern = NumberFormatInfo.InvariantInfo.PercentPositivePattern,
            PercentSymbol = NumberFormatInfo.InvariantInfo.PercentSymbol,
            PerMilleSymbol = NumberFormatInfo.InvariantInfo.PerMilleSymbol,
            PositiveInfinitySymbol = NumberFormatInfo.InvariantInfo.PositiveInfinitySymbol,
            PositiveSign = NumberFormatInfo.InvariantInfo.PositiveSign,
        };

        internal string GetProperParameter()
        {
            if (this.Value == DBNull.Value)
            {
                return "NULL";
            }

            //  If it is a string then let's sanitize the quotes and enclose the string in quotes
            if (this.Value is string stringValue)
            {
                return "'" + stringValue.Replace("'", "''").Replace("\\", "\\\\") + "'";
            }
            else if (this.Value is DateTime dateTime)
            {
                return "'" + dateTime.ToString("yyyy-MM-dd HH:mm:ss") + "'";
            }
            else if (this.Value is double doubleVal)
            {
                return doubleVal.ToString("G17", numberFormatInfo);
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