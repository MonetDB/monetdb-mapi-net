namespace MonetDb.Mapi.Enums
{
    using System.Data;

    internal enum SchemaTableColumns
    {
        /// <summary>
        /// The name of the column; this might not be unique. 
        /// If the column name cannot be determined, 
        /// a null value is returned. 
        /// This name always reflects the most 
        /// recent naming of the column in the current view or command text. 
        /// </summary>
        ColumnName,

        /// <summary>
        /// The zero-based ordinal of the column. This column cannot contain a null value.
        /// </summary>
        ColumnOrdinal,

        /// <summary>
        /// The maximum possible length of a value in the column. 
        /// For columns that use a fixed-length data type, 
        /// this is the size of the data type.
        /// </summary>
        ColumnSize,

        /// <summary>
        /// If <see cref="DbType"/> is a numeric data type, 
        /// this is the maximum precision of the column. 
        /// The precision depends on the definition of the column. 
        /// If DbType is not a numeric data type, 
        /// do not use the data in this column. 
        /// </summary>
        NumericPrecision,

        /// <summary>
        /// If <see cref="DbType"/> is <see cref="DbType.Decimal"/>, 
        /// the number of digits to the right of the decimal point. 
        /// Otherwise, this is a null value. 
        /// </summary>
        NumericScale,

        /// <summary>
        /// Maps to the .NET Framework type of the column.
        /// </summary>
        DataType,

        /// <summary>
        /// The MonetDb column data type name
        /// </summary>
        ProviderType,

        /// <summary>
        /// Returns the provider-specific data type 
        /// of the column based on the Type System Version 
        /// keyword in the connection string.
        /// </summary>
        ProviderSpecificDataType,

        /// <summary>
        /// <code>true</code> if the column contains a 
        /// Binary Long Object (BLOB) that contains very long data. 
        /// The definition of very long data is driver-specific. 
        /// </summary>
        IsLong,

        /// <summary>
        /// <code>true</code> if the consumer can 
        /// set the column to a null value or 
        /// if the driver cannot determine whether 
        /// the consumer can set the column to a null value. 
        /// Otherwise, <code>false</code>. A column may contain null values, 
        /// even if it cannot be set to a null value.
        /// </summary>
        AllowDbNull,

        /// <summary>
        /// <code>true</code> if the column cannot be modified; otherwise <code>false</code>.
        /// </summary>
        /// <remarks>MonetDB does not support cursor updates, so nothing is writable 
        /// and value is always <code>true</code>.
        /// </remarks>
        IsReadOnly,

        /// <summary>
        /// Set if the column contains a persistent row identifier 
        /// that cannot be written to, 
        /// and has no meaningful value except to identity the row. 
        /// </summary>
        IsRowVersion,

        /// <summary>
        /// <code>true</code>: No two rows in the base table 
        /// (the table returned in BaseTableName) 
        /// can have the same value in this column. 
        /// IsUnique is guaranteed to be <code>true</code> 
        /// if the column represents a key by itself 
        /// or if there is a constraint of type UNIQUE 
        /// that applies only to this column. 
        /// <code>false</code>: The column can contain duplicate 
        /// values in the base table. 
        /// The default for this column is <code>false</code>. 
        /// </summary>
        IsUnique,

        /// <summary>
        /// <code>true</code>: The column is one of a set 
        /// of columns in the rowset that, 
        /// taken together, uniquely identify the row. 
        /// The set of columns with IsKey set to <code>true</code> 
        /// must uniquely identify a row in the rowset. 
        /// There is no requirement that this set of columns 
        /// is a minimal set of columns. 
        /// This set of columns may be generated from 
        /// a base table primary key, a unique constraint, or a unique index. 
        /// <code>false</code>: The column is not required to uniquely identify the row. 
        /// </summary>
        IsKey,

        /// <summary>
        /// <code>true</code> if the column assigns 
        /// values to new rows in fixed increments; 
        /// otherwise <code>false</code>. 
        /// The default for this column is <code>false</code>.
        /// </summary>
        IsAutoincrement,

        /// <summary>
        /// The name of the schema in the 
        /// data source that contains the column. 
        /// NULL if the base catalog name cannot be determined. 
        /// The default for this column is a null value. 
        /// </summary>
        BaseSchemaName,

        /// <summary>
        /// The name of the catalog in the data 
        /// store that contains the column. 
        /// NULL if the base catalog name cannot be determined. 
        /// The default for this column is a null value.
        /// </summary>
        /// <remarks>
        /// MonetDB only handles one catalog (dbfarm) at a time
        /// </remarks>
        BaseCatalogName,

        /// <summary>
        /// The name of the table or view in 
        /// the data store that contains the column. 
        /// A null value if the base table name cannot be determined. 
        /// The default of this column is a null value. 
        /// </summary>
        BaseTableName,

        /// <summary>
        /// The name of the column in the data store. 
        /// This might be different from the column 
        /// name returned in the ColumnName column if an alias was used. 
        /// A null value if the base column name cannot 
        /// be determined or if the rowset column is derived, 
        /// but not identical to, a column in the data store. 
        /// The default for this column is a null value. 
        /// </summary>
        BaseColumnName,

        /// <summary>
        /// Returns a string representing the MonetDb column data type of the specified column.
        /// </summary>
        DataTypeName
    }
}