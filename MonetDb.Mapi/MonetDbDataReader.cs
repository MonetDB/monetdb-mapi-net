namespace MonetDb.Mapi
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Globalization;
    using System.Linq;

    using MonetDb.Mapi.Enums;
    using MonetDb.Mapi.Extensions;
    using MonetDb.Mapi.Helpers;
    using MonetDb.Mapi.Helpers.Mapi;
    using MonetDb.Mapi.Models;

    /// <summary>
    /// Provides a means of reading one or more forward-only streams of result sets obtained by executing a command on a MonetDB server
    /// </summary>
    public class MonetDbDataReader : DbDataReader
    {
        private QueryResponseInfo _responseInfo;
        private readonly IEnumerator<QueryResponseInfo> _responeInfoEnumerator;
        private IEnumerator<List<string>> _enumerator;
        private readonly MonetDbConnection _connection;

        private Metadata _metadata;
        private DataTable _schemaTable;

        internal MonetDbDataReader(IEnumerable<QueryResponseInfo> ri, MonetDbConnection connection)
        {
            this._metadata = null;
            this._schemaTable = null;

            this._connection = connection;
            this._responeInfoEnumerator = ri.GetEnumerator();
            NextResult();
        }

        private DataTable GenerateSchemaTable()
        {
            if (this._metadata == null)
            {
                this._metadata = new Metadata(this._connection);
            }

            // schema table always must be named as "SchemaTable"
            var table = new DataTable("SchemaTable");

            // create table schema columns
            table.Columns.Add(nameof(SchemaTableColumns.ColumnName), typeof(string));
            table.Columns.Add(nameof(SchemaTableColumns.ColumnOrdinal), typeof(int));
            table.Columns.Add(nameof(SchemaTableColumns.ColumnSize), typeof(int));
            table.Columns.Add(nameof(SchemaTableColumns.NumericPrecision), typeof(int));
            table.Columns.Add(nameof(SchemaTableColumns.NumericScale), typeof(int));
            table.Columns.Add(nameof(SchemaTableColumns.DataType), typeof(Type));
            table.Columns.Add(nameof(SchemaTableColumns.ProviderType), typeof(int));
            table.Columns.Add(nameof(SchemaTableColumns.ProviderSpecificDataType), typeof(DbType));
            table.Columns.Add(nameof(SchemaTableColumns.IsLong), typeof(bool));
            table.Columns.Add(nameof(SchemaTableColumns.AllowDbNull), typeof(bool));
            table.Columns.Add(nameof(SchemaTableColumns.IsReadOnly), typeof(bool));
            table.Columns.Add(nameof(SchemaTableColumns.IsRowVersion), typeof(bool));
            table.Columns.Add(nameof(SchemaTableColumns.IsUnique), typeof(bool));
            table.Columns.Add(nameof(SchemaTableColumns.IsKey), typeof(bool));
            table.Columns.Add(nameof(SchemaTableColumns.IsAutoincrement), typeof(bool));
            table.Columns.Add(nameof(SchemaTableColumns.BaseSchemaName), typeof(string));
            table.Columns.Add(nameof(SchemaTableColumns.BaseCatalogName), typeof(string));
            table.Columns.Add(nameof(SchemaTableColumns.BaseTableName), typeof(string));
            table.Columns.Add(nameof(SchemaTableColumns.BaseColumnName), typeof(string));
            table.Columns.Add(nameof(SchemaTableColumns.DataTypeName), typeof(string));

            // fill table
            for (var fieldIndex = 0; fieldIndex < FieldCount; fieldIndex++)
            {
                // get column name
                var columnName = GetName(fieldIndex);

                // get column size
                var columnSize = this._responseInfo.Columns[fieldIndex].Length;

                //// get column precision
                var numericPrecision = 0;
                //DieQueryError();

                //// get column scale
                var numericScale = 0;
                //DieQueryError();

                // get data type
                var providerType = this._responseInfo.Columns[fieldIndex].DataType;

                var dbType = providerType.GetDbType();
                var providerSpecificDataType = dbType;
                var systemType = providerType.GetSystemType();

                // is binary large object
                var blobs = new[] { "blob" };
                var isLong = blobs.Contains(providerType);

                // is nullable
                // TODO: retreive information from sys.columns table
                var allowDbNull = true;

                // is read only
                // MonetDB does not support cursor updates, so
                // nothing is writable.
                var isReadOnly = true;

                // is rowid
                var isRowVersion = false;

                // is unique
                var isUnique = false;

                // is key
                var isKey = false;

                // is nullable
                var isAutoincrement = providerType.Equals("oid");

                // get column base table name. Result contains both schema and table name
                // so, we need split them
                var baseFullTableName = this._responseInfo.Columns[fieldIndex].TableName;

                var baseTableName = baseFullTableName;
                var baseSchemaName = string.Empty;

                if (baseFullTableName.IndexOf("."
                    , StringComparison.InvariantCultureIgnoreCase) != -1)
                {
                    var s = baseFullTableName.Split('.');

                    // get column base schema name
                    baseSchemaName = s[0];
                    baseTableName = s[1];
                }

                var columnInfo = new ColumnInfo
                {
                    // Catalog = this._metadata.GetEnvironmentVariable("gdk_dbname"),
                    CharOctetLength = 0,
                    ColumnSize = columnSize,
                    DataType = providerType,
                    DefaultValue = null,
                    Name = string.Empty,
                    Nullable = allowDbNull, // Actually, we don't know about this
                    Ordinal = fieldIndex, // In schema table we must always use results field index
                    Radix = 10,
                    Remarks = string.Empty,
                    Scale = numericScale,
                    Schema = baseSchemaName,
                    Table = baseTableName
                };

                // get additional info
                // isUnique = this._metadata.IsColumnUniqueKey("", columnInfo.Schema, columnInfo.Table, columnInfo.Name);
                // isKey = this._metadata.IsColumnPrimaryKey("", columnInfo.Schema, columnInfo.Table, columnInfo.Name);

                table.Rows.Add(
                        columnName,
                        fieldIndex,
                        columnSize,
                        numericPrecision,
                        numericScale,
                        systemType,
                        providerSpecificDataType.To<int>(),
                        providerSpecificDataType,
                        isLong,
                        columnInfo.Nullable,
                        isReadOnly,
                        isRowVersion,
                        isUnique,
                        isKey,
                        isAutoincrement,
                        columnInfo.Schema,
                        columnInfo.Catalog,
                        columnInfo.Table,
                        columnInfo.Name,
                        providerType
                    );
            }

            return table;
        }

        #region IDataReader Members

        /// <summary>
        /// Closes the IDataReader object
        /// </summary>
        public override void Close()
        {
            Dispose();
        }

        /// <summary>
        /// Gets a value indicating the depth of nesting for the current row.
        /// </summary>
        public override int Depth
        {
            get { throw new Exception("The method or operation is not implemented."); }
        }

        /// <summary>
        /// Returns a <c>DataTable</c> that describes the column metadata of the <c>IDataReader</c>.
        /// </summary>
        /// <returns></returns>
        public override DataTable GetSchemaTable()
        {
            return this._schemaTable ?? (this._schemaTable = GenerateSchemaTable());
        }

        /// <summary>
        /// Gets a value indicating whether the data reader is closed.
        /// </summary>
        public override bool IsClosed
        {
            get { return false; }
        }

        /// <summary>
        /// Advances the data reader to the next result, when reading the results of batch SQL statements.
        /// </summary>
        /// <returns></returns>
        public override bool NextResult()
        {
            var flag = this._responeInfoEnumerator.MoveNext();
            this.GetEnumerator();

            // we need to regenerate schema table for next result set
            if (this._schemaTable != null)
            {
                this._schemaTable = GenerateSchemaTable();
            }

            return flag;
        }

        /// <summary>
        /// Advances the <c>IDataReader</c> to the next record.
        /// </summary>
        /// <returns></returns>
        public override bool Read()
        {
            return this._enumerator.MoveNext();
        }

        /// <summary>
        /// Gets the number of rows changed, inserted, or deleted by execution of the SQL statement.
        /// </summary>
        public override int RecordsAffected
        {
            get { return this._responseInfo.RecordsAffected; }
        }

        #endregion

        #region IDisposable Members

        /// <summary>
        /// 
        /// </summary>
        /// <param name="disposing"></param>
        protected override void Dispose(bool disposing)
        {
           this._enumerator.Dispose();
            this._responeInfoEnumerator.Dispose();
        }

        #endregion

        #region IDataRecord Members

        /// <summary>
        /// Gets the number of columns in the current row.
        /// </summary>
        public override int FieldCount
        {
            get { return this._responseInfo.ColumnCount; }
        }

        /// <summary>
        /// 
        /// </summary>
        public override bool HasRows
        {
            get
            {
                return this.GetEnumerator().Current != null;
            }
        }

        /// <summary>
        /// Gets the value of the specified column as a Boolean.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override bool GetBoolean(int i)
        {
            return bool.Parse(_enumerator.Current[i]);
        }

        /// <summary>
        /// Gets the 8-bit unsigned integer value of the specified column.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override byte GetByte(int i)
        {
            return (byte)GetInt16(i);
        }

        /// <summary>
        /// Reads a stream of bytes from the specified column offset into the buffer as an array, starting at the given buffer offset.
        /// </summary>
        /// <param name="i"></param>
        /// <param name="fieldOffset"></param>
        /// <param name="buffer"></param>
        /// <param name="bufferoffset"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        /// <summary>
        /// Gets the character value of the specified column.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override char GetChar(int i)
        {
            return this._enumerator.Current[i][0];
        }

        /// <summary>
        /// Reads a stream of characters from the specified column offset into the buffer as an array, starting at the given buffer offset.
        /// </summary>
        /// <param name="i"></param>
        /// <param name="fieldoffset"></param>
        /// <param name="buffer"></param>
        /// <param name="bufferoffset"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        /// <summary>
        /// Gets the data type information for the specified field.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override string GetDataTypeName(int i)
        {
            return this._responseInfo.Columns[i].DataType;
        }

        /// <summary>
        /// Gets the date and time data value of the specified field.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override DateTime GetDateTime(int i)
        {
            return DateTime.Parse(_enumerator.Current[i]);
        }

        /// <summary>
        /// Gets the fixed-position numeric value of the specified field.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override decimal GetDecimal(int i)
        {
            return decimal.Parse(_enumerator.Current[i], CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Gets the double-precision floating point number of the specified field.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override double GetDouble(int i)
        {
            return double.Parse(_enumerator.Current[i], CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Gets the single-precision floating point number of the specified field.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override float GetFloat(int i)
        {
            return float.Parse(_enumerator.Current[i], CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Returns the GUID value of the specified field.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override Guid GetGuid(int i)
        {
            return new Guid(_enumerator.Current[i]);
        }

        /// <summary>
        /// Gets the 16-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override short GetInt16(int i)
        {
            return short.Parse(_enumerator.Current[i]);
        }

        /// <summary>
        /// Gets the 32-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override int GetInt32(int i)
        {
            return int.Parse(_enumerator.Current[i], CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Gets the 64-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override long GetInt64(int i)
        {
            return long.Parse(_enumerator.Current[i], CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Gets the name for the field to find.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override string GetName(int i)
        {
            return this._responseInfo.Columns[i].Name;
        }

        /// <summary>
        /// Return the index of the named field.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public override int GetOrdinal(string name)
        {
            return this._responseInfo.Columns.FindIndex(ci => (ci.Name == name));
        }

        /// <summary>
        /// Gets the string value of the specified field.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override string GetString(int i)
        {
            if (this.IsDBNull(i))
            {
                return null;
            }

            return this._enumerator.Current[i].Substring(1,this._enumerator.Current[i].Length - 2);
        }

        /// <summary>
        /// Return the value of the specified field.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override object GetValue(int i)
        {
            if (this.IsDBNull(i))
            {
                return DBNull.Value;
            }

            switch (this._responseInfo.Columns[i].DataType)
            {
                case "tinyint":
                    return this.GetByte(i);

                case "smallint":
                    return this.GetInt16(i);

                case "int":
                case "integer":
                    return this.GetInt32(i);

                case "bigint":
                    return this.GetInt64(i);

                case "boolean":
                    return this.GetBoolean(i);

                case "character":
                case "char":
                    return this.GetChar(i);

                case "double":
                case "double precision":
                case "numeric":
                case "dec":
                case "decimal":
                case "float":
                case "real":
                    return this.GetFloat(i);

                case "daytime":
                case "time":
                case "time with time zone":
                case "date":
                case "timestamp":
                case "timestamp with time zone":
                    return this.GetDateTime(i);

                case "varchar":
                case "text":
                case "varchar varying":
                case "character large object":
                case "string":
                case "clob":
                default:
                    return this.GetString(i);
            }
        }

        /// <summary>
        /// Gets all the attribute fields in the collection for the current record.
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        public override int GetValues(object[] values)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        /// <summary>
        /// Return whether the specified field is set to null.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override bool IsDBNull(int i)
        {
            return this._enumerator.Current[i] == "NULL";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override IEnumerator GetEnumerator()
        {
            this._responseInfo = this._responeInfoEnumerator.Current;
            return this._enumerator = this._responseInfo.Data.GetEnumerator();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override Type GetFieldType(int ordinal)
        {
            return this._responseInfo.Columns[ordinal].DataType.GetSystemType();
        }

        /// <summary>
        /// Gets the specified column by column name.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public override object this[string name]
        {
            get { return GetValue(GetOrdinal(name)); }
        }

        /// <summary>
        /// Gets the specified column by column index
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        public override object this[int i]
        {
            get { return GetValue(i); }
        }

        #endregion
    }
}