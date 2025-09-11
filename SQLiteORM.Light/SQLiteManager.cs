using SQLiteORM.Attributes;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Xml.Linq;

namespace SQLiteORM
{
    public static class SQLiteManager
    {
        private static string _oDefaultDbPath;
        private const string _DefaultDbFileName = "Database.sqlite";
        private const string _ConfigFileName = "AppConfig.xml";
        private static bool _bTablesChecked = false;
        private static readonly object _oConfigLock = new object();
        private static readonly object _oTableCheckLock = new object();

        // کش‌ها با استفاده از Lazy<T> و قفل‌های جداگانه
        private static readonly Dictionary<Type, Lazy<bool>> _oTableCheckCache = new Dictionary<Type, Lazy<bool>>();
        private static readonly object _oTableCheckCacheLock = new object();

        private static readonly Dictionary<Type, Lazy<PropertyInfo[]>> _oPropertyCache = new Dictionary<Type, Lazy<PropertyInfo[]>>();
        private static readonly object _oPropertyCacheLock = new object();

        private static readonly Dictionary<Type, Lazy<string>> _oTableNameCache = new Dictionary<Type, Lazy<string>>();
        private static readonly object _oTableNameCacheLock = new object();

        private static readonly Dictionary<Type, Lazy<PropertyInfo>> _oPrimaryKeyCache = new Dictionary<Type, Lazy<PropertyInfo>>();
        private static readonly object _oPrimaryKeyCacheLock = new object();

        private static readonly Dictionary<string, object> _oTableLocks = new Dictionary<string, object>();
        private static readonly object _oTableLocksLock = new object();

        private static readonly Dictionary<Type, Lazy<List<IndexInfo>>> _oIndexCache = new Dictionary<Type, Lazy<List<IndexInfo>>>();
        private static readonly object _oIndexCacheLock = new object();

        private static readonly Dictionary<Type, Lazy<CachedInsertInfo>> _oInsertInfoCache = new Dictionary<Type, Lazy<CachedInsertInfo>>();
        private static readonly object _oInsertInfoCacheLock = new object();

        private static readonly Dictionary<Type, Lazy<CachedUpdateInfo>> _oUpdateInfoCache = new Dictionary<Type, Lazy<CachedUpdateInfo>>();
        private static readonly object _oUpdateInfoCacheLock = new object();

        private static readonly Dictionary<Type, Lazy<Dictionary<string, ColumnMapping>>> _oColumnMappingCache = new Dictionary<Type, Lazy<Dictionary<string, ColumnMapping>>>();
        private static readonly object _oColumnMappingCacheLock = new object();

        private static readonly Dictionary<Type, Lazy<Func<SQLiteDataReader, object>>> _oEntityCreators = new Dictionary<Type, Lazy<Func<SQLiteDataReader, object>>>();
        private static readonly object _oEntityCreatorsLock = new object();

        private static readonly Dictionary<Type, Lazy<Func<object, object>>> _oValueConvertersCache = new Dictionary<Type, Lazy<Func<object, object>>>();
        private static readonly object _oValueConvertersCacheLock = new object();

        private static readonly List<SQLiteConnection> _oConnectionPool = new List<SQLiteConnection>();
        private static readonly object _oConnectionPoolLock = new object();
        private const int MaxPoolSize = 100;
        private static int _iConnectionCount = 0;

        static SQLiteManager()
        {
            LoadDatabaseConfig();
        }

        private class CachedInsertInfo
        {
            public string Sql { get; set; }
            public PropertyInfo[] Properties { get; set; }
            public string[] ParameterNames { get; set; }
        }

        private class CachedUpdateInfo
        {
            public string Sql { get; set; }
            public PropertyInfo[] Properties { get; set; }
            public string[] ParameterNames { get; set; }
        }

        private class ColumnMapping
        {
            public PropertyInfo Property { get; set; }
            public Func<object, object> ValueConverter { get; set; }
            public int ColumnIndex { get; set; }
        }

        private class IndexInfo
        {
            public string Name { get; set; }
            public bool IsUnique { get; set; }
            public List<string> Properties { get; set; }
            public bool AutoCreate { get; set; }
        }

        private static void LoadDatabaseConfig()
        {
            lock (_oConfigLock)
            {
                try
                {
                    string oDllDirectory = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) ?? throw new InvalidOperationException("Unable to determine assembly location");
                    string sConfigPath = Path.Combine(oDllDirectory, _ConfigFileName);
                    if (!File.Exists(sConfigPath))
                    {
                        sConfigPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, _ConfigFileName);
                    }
                    if (!File.Exists(sConfigPath))
                    {
                        CreateDefaultConfig(sConfigPath);
                    }
                    XDocument oDoc = XDocument.Load(sConfigPath);
                    string sDatabaseName = oDoc.Root?.Element("DatabaseName")?.Value ?? _DefaultDbFileName;
                    string sDatabasePath = oDoc.Root?.Element("DatabasePath")?.Value;
                    if (string.IsNullOrWhiteSpace(sDatabaseName))
                        throw new InvalidOperationException("Database name not specified in config file");
                    if (string.IsNullOrWhiteSpace(sDatabasePath) || sDatabasePath == "." ||
                        sDatabasePath.Equals("default", StringComparison.OrdinalIgnoreCase))
                    {
                        sDatabasePath = AppDomain.CurrentDomain.BaseDirectory;
                    }
                    _oDefaultDbPath = Path.Combine(sDatabasePath, sDatabaseName);
                    EnsureDatabaseFileExists();
                    CheckAllTableStructures();
                }
                catch (Exception oEx)
                {
                    throw new InvalidOperationException("Failed to load database configuration: " + oEx.Message, oEx);
                }
            }
        }

        public static void SetDefaultDatabasePath(string sDbFileName, string sDbPath = null)
        {
            if (string.IsNullOrWhiteSpace(sDbFileName))
            {
                throw new ArgumentNullException(nameof(sDbFileName));
            }
            lock (_oConfigLock)
            {
                string sBasePath = sDbPath ?? AppDomain.CurrentDomain.BaseDirectory;
                _oDefaultDbPath = Path.Combine(sBasePath, sDbFileName);
                EnsureDatabaseFileExists();
                CheckAllTableStructures();
            }
        }

        private static void CheckAllTableStructures()
        {
            if (_bTablesChecked) return;
            lock (_oTableCheckLock)
            {
                if (_bTablesChecked) return;
                var lModelTypes = new List<Type>();
                var aAssemblies = AppDomain.CurrentDomain.GetAssemblies();
                for (int i = 0; i < aAssemblies.Length; i++)
                {
                    try
                    {
                        var aTypes = aAssemblies[i].GetTypes();
                        for (int j = 0; j < aTypes.Length; j++)
                        {
                            var oType = aTypes[j];
                            if (oType != null && oType.GetCustomAttributes(typeof(TableAttribute), false).Length > 0)
                            {
                                lModelTypes.Add(oType);
                            }
                        }
                    }
                    catch (ReflectionTypeLoadException oEx)
                    {
                        var aTypes = oEx.Types;
                        for (int j = 0; j < aTypes.Length; j++)
                        {
                            var oType = aTypes[j];
                            if (oType != null && oType.GetCustomAttributes(typeof(TableAttribute), false).Length > 0)
                            {
                                lModelTypes.Add(oType);
                            }
                        }
                    }
                }
                for (int i = 0; i < lModelTypes.Count; i++)
                {
                    CheckTableStructure(lModelTypes[i]);
                }
                _bTablesChecked = true;
            }
        }

        private static void CheckTableStructure(Type oModelType)
        {
            var aTableAttrs = oModelType.GetCustomAttributes(typeof(TableAttribute), false);
            if (aTableAttrs.Length == 0) return;
            var oTableAttr = (TableAttribute)aTableAttrs[0];
            string sTableName = GetFullTableName(oTableAttr);

            object oTableLock = GetTableLock(sTableName);
            lock (oTableLock)
            {
                if (IsTableChecked(oModelType))
                    return;

                PropertyInfo[] aProperties = GetCachedProperties(oModelType);
                bool bTableExists = false;
                using (var oConnection = GetConnection())
                {
                    oConnection.Open();
                    using (var oCommand = new SQLiteCommand("SELECT 1 FROM sqlite_master WHERE type='table' AND name=@tableName", oConnection))
                    {
                        oCommand.Parameters.AddWithValue("@tableName", sTableName);
                        bTableExists = oCommand.ExecuteScalar() != null;
                    }
                }
                if (!bTableExists)
                {
                    CreateTable(oModelType, sTableName, aProperties.ToList());
                    InvalidateColumnMappingCache(oModelType);
                }
                else
                {
                    var lExistingColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    using (var oConnection = GetConnection())
                    {
                        oConnection.Open();
                        using (var oCommand = new SQLiteCommand($"PRAGMA table_info('{sTableName}')", oConnection))
                        using (var oReader = oCommand.ExecuteReader())
                        {
                            while (oReader.Read())
                            {
                                lExistingColumns.Add(oReader["name"].ToString());
                            }
                        }
                    }
                    var lMissingProperties = new List<PropertyInfo>();
                    for (int i = 0; i < aProperties.Length; i++)
                    {
                        if (!lExistingColumns.Contains(aProperties[i].Name))
                        {
                            lMissingProperties.Add(aProperties[i]);
                        }
                    }
                    if (lMissingProperties.Count > 0)
                    {
                        AddMissingColumns(sTableName, lMissingProperties);
                        InvalidateColumnMappingCache(oModelType);
                    }
                }
                CheckAndCreateIndexes(oModelType, sTableName);
                MarkTableAsChecked(oModelType);
            }
        }

        private static object GetTableLock(string sTableName)
        {
            lock (_oTableLocksLock)
            {
                if (!_oTableLocks.TryGetValue(sTableName, out object oLock))
                {
                    oLock = new object();
                    _oTableLocks[sTableName] = oLock;
                }
                return oLock;
            }
        }

        private static bool IsTableChecked(Type oModelType)
        {
            lock (_oTableCheckCacheLock)
            {
                if (_oTableCheckCache.TryGetValue(oModelType, out Lazy<bool> oLazy))
                {
                    return oLazy.Value;
                }
                return false;
            }
        }

        private static void MarkTableAsChecked(Type oModelType)
        {
            lock (_oTableCheckCacheLock)
            {
                if (!_oTableCheckCache.ContainsKey(oModelType))
                {
                    _oTableCheckCache[oModelType] = new Lazy<bool>(() => true, LazyThreadSafetyMode.ExecutionAndPublication);
                }
            }
        }

        private static void InvalidateColumnMappingCache(Type oModelType)
        {
            lock (_oColumnMappingCacheLock)
            {
                _oColumnMappingCache.Remove(oModelType);
            }
        }

        private static void CreateTable(Type oModelType, string sTableName, List<PropertyInfo> lProperties)
        {
            var oBuilder = new StringBuilder();
            oBuilder.Append($"CREATE TABLE {sTableName} (");
            PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(oModelType);
            bool bFirst = true;
            for (int i = 0; i < lProperties.Count; i++)
            {
                var oProp = lProperties[i];
                if (!bFirst) oBuilder.Append(", ");
                bFirst = false;
                string sColumnType = GetSQLiteType(oProp.PropertyType);
                oBuilder.Append($"{oProp.Name} {sColumnType}");
                if (oProp == oPrimaryKey)
                {
                    if (IsAutoIncrementType(oProp.PropertyType))
                    {
                        oBuilder.Append(" PRIMARY KEY AUTOINCREMENT");
                    }
                    else
                    {
                        oBuilder.Append(" PRIMARY KEY");
                    }
                }
                else if (!IsNullableType(oProp.PropertyType))
                {
                    oBuilder.Append(" NOT NULL");
                }
            }
            oBuilder.Append(")");
            ExecuteNonQuery(oBuilder.ToString());
        }

        private static void CheckAndCreateIndexes(Type oModelType, string sTableName)
        {
            var lIndexInfos = GetCachedIndexes(oModelType);
            using (var oConnection = GetConnection())
            {
                oConnection.Open();
                var lExistingIndexes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                using (var oCommand = new SQLiteCommand(
                    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=@tableName", oConnection))
                {
                    oCommand.Parameters.AddWithValue("@tableName", sTableName);
                    using (var oReader = oCommand.ExecuteReader())
                    {
                        while (oReader.Read())
                        {
                            lExistingIndexes.Add(oReader["name"].ToString());
                        }
                    }
                }
                for (int i = 0; i < lIndexInfos.Count; i++)
                {
                    var oIndexInfo = lIndexInfos[i];
                    if (oIndexInfo.AutoCreate && !lExistingIndexes.Contains(oIndexInfo.Name))
                    {
                        string sUnique = oIndexInfo.IsUnique ? "UNIQUE" : "";
                        var oColumnBuilder = new StringBuilder();
                        for (int j = 0; j < oIndexInfo.Properties.Count; j++)
                        {
                            if (j > 0) oColumnBuilder.Append(", ");
                            oColumnBuilder.Append(oIndexInfo.Properties[j]);
                        }
                        string sSql = $"CREATE {sUnique} INDEX {oIndexInfo.Name} ON {sTableName} ({oColumnBuilder})";
                        ExecuteNonQuery(sSql);
                    }
                }
            }
        }

        private static List<IndexInfo> GetCachedIndexes(Type oType)
        {
            lock (_oIndexCacheLock)
            {
                if (!_oIndexCache.TryGetValue(oType, out Lazy<List<IndexInfo>> oLazy))
                {
                    oLazy = new Lazy<List<IndexInfo>>(() =>
                    {
                        var lIndexInfos = new List<IndexInfo>();
                        var aProperties = GetCachedProperties(oType);
                        for (int i = 0; i < aProperties.Length; i++)
                        {
                            var oProp = aProperties[i];
                            var aIndexAttrs = oProp.GetCustomAttributes(typeof(IndexAttribute), false);
                            for (int j = 0; j < aIndexAttrs.Length; j++)
                            {
                                var oIndexAttr = (IndexAttribute)aIndexAttrs[j];
                                if (oIndexAttr.AutoCreate)
                                {
                                    string sIndexName = string.IsNullOrEmpty(oIndexAttr.Name)
                                        ? $"IX_{GetCachedTableName(oType)}_{oProp.Name}"
                                        : oIndexAttr.Name;
                                    lIndexInfos.Add(new IndexInfo
                                    {
                                        Name = sIndexName,
                                        IsUnique = oIndexAttr.IsUnique,
                                        Properties = new List<string> { oProp.Name },
                                        AutoCreate = oIndexAttr.AutoCreate
                                    });
                                }
                            }
                        }
                        var oGroupedIndexes = new Dictionary<string, List<IndexInfo>>(StringComparer.OrdinalIgnoreCase);
                        for (int i = 0; i < lIndexInfos.Count; i++)
                        {
                            var oIndexInfo = lIndexInfos[i];
                            if (!string.IsNullOrEmpty(oIndexInfo.Name))
                            {
                                List<IndexInfo> lGroup;
                                if (!oGroupedIndexes.TryGetValue(oIndexInfo.Name, out lGroup))
                                {
                                    lGroup = new List<IndexInfo>();
                                    oGroupedIndexes[oIndexInfo.Name] = lGroup;
                                }
                                lGroup.Add(oIndexInfo);
                            }
                        }
                        foreach (var lGroup in oGroupedIndexes.Values)
                        {
                            if (lGroup.Count > 1)
                            {
                                var oFirst = lGroup[0];
                                var lAllProperties = new List<string>();
                                for (int i = 0; i < lGroup.Count; i++)
                                {
                                    lAllProperties.AddRange(lGroup[i].Properties);
                                }
                                lAllProperties.Sort(StringComparer.OrdinalIgnoreCase);
                                oFirst.Properties = lAllProperties;
                                for (int i = 1; i < lGroup.Count; i++)
                                {
                                    lIndexInfos.Remove(lGroup[i]);
                                }
                            }
                        }
                        return lIndexInfos;
                    }, LazyThreadSafetyMode.ExecutionAndPublication);

                    _oIndexCache[oType] = oLazy;
                }
                return oLazy.Value;
            }
        }

        private static bool IsAutoIncrementType(Type oType)
        {
            oType = Nullable.GetUnderlyingType(oType) ?? oType;
            return oType == typeof(int) || oType == typeof(long);
        }

        private static PropertyInfo GetPrimaryKeyProperty(Type oType)
        {
            lock (_oPrimaryKeyCacheLock)
            {
                if (!_oPrimaryKeyCache.TryGetValue(oType, out Lazy<PropertyInfo> oLazy))
                {
                    oLazy = new Lazy<PropertyInfo>(() =>
                    {
                        var aProperties = GetCachedProperties(oType);
                        for (int i = 0; i < aProperties.Length; i++)
                        {
                            if (aProperties[i].GetCustomAttributes(typeof(IdentityKeyAttribute), false).Length > 0)
                            {
                                return aProperties[i];
                            }
                        }
                        for (int i = 0; i < aProperties.Length; i++)
                        {
                            if (string.Equals(aProperties[i].Name, "Id", StringComparison.OrdinalIgnoreCase))
                            {
                                return aProperties[i];
                            }
                        }
                        throw new InvalidOperationException($"No primary key defined for type {oType.Name}. Use [IdentityKey] attribute or name a property 'Id'.");
                    }, LazyThreadSafetyMode.ExecutionAndPublication);

                    _oPrimaryKeyCache[oType] = oLazy;
                }
                return oLazy.Value;
            }
        }

        private static void AddMissingColumns(string sTableName, List<PropertyInfo> lMissingProperties)
        {
            using (var oConnection = GetConnection())
            {
                oConnection.Open();
                using (var oTransaction = oConnection.BeginTransaction())
                {
                    try
                    {
                        for (int i = 0; i < lMissingProperties.Count; i++)
                        {
                            var oProp = lMissingProperties[i];
                            string sColumnType = GetSQLiteType(oProp.PropertyType);
                            string sColumnDefinition = $"{oProp.Name} {sColumnType}";
                            if (!IsNullableType(oProp.PropertyType))
                                sColumnDefinition += " NOT NULL";
                            string sAlterSql = $"ALTER TABLE {sTableName} ADD COLUMN {sColumnDefinition}";
                            using (var oCommand = new SQLiteCommand(sAlterSql, oConnection, oTransaction))
                            {
                                oCommand.ExecuteNonQuery();
                            }
                        }
                        oTransaction.Commit();
                    }
                    catch
                    {
                        oTransaction.Rollback();
                        throw;
                    }
                }
            }
        }

        private static PropertyInfo[] GetCachedProperties(Type oType)
        {
            lock (_oPropertyCacheLock)
            {
                if (!_oPropertyCache.TryGetValue(oType, out Lazy<PropertyInfo[]> oLazy))
                {
                    oLazy = new Lazy<PropertyInfo[]>(() =>
                    {
                        var aProperties = oType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
                        var lResult = new List<PropertyInfo>();
                        for (int i = 0; i < aProperties.Length; i++)
                        {
                            if (aProperties[i].GetCustomAttributes(typeof(IgnoreAttribute), false).Length == 0)
                            {
                                lResult.Add(aProperties[i]);
                            }
                        }
                        return lResult.ToArray();
                    }, LazyThreadSafetyMode.ExecutionAndPublication);

                    _oPropertyCache[oType] = oLazy;
                }
                return oLazy.Value;
            }
        }

        private static string GetCachedTableName(Type oType)
        {
            lock (_oTableNameCacheLock)
            {
                if (!_oTableNameCache.TryGetValue(oType, out Lazy<string> oLazy))
                {
                    oLazy = new Lazy<string>(() =>
                    {
                        var aTableAttrs = oType.GetCustomAttributes(typeof(TableAttribute), false);
                        if (aTableAttrs.Length == 0)
                        {
                            return oType.Name;
                        }
                        var oTableAttr = (TableAttribute)aTableAttrs[0];
                        return GetFullTableName(oTableAttr);
                    }, LazyThreadSafetyMode.ExecutionAndPublication);

                    _oTableNameCache[oType] = oLazy;
                }
                return oLazy.Value;
            }
        }

        private static string GetCachedTableName<T>()
        {
            return GetCachedTableName(typeof(T));
        }

        private static string GetFullTableName(TableAttribute oTableAttr)
        {
            return oTableAttr.Name;
        }

        private static void CreateDefaultConfig(string sConfigPath)
        {
            XDocument oDoc = new XDocument(
                new XElement("DatabaseConfig",
                    new XElement("DatabaseName", _DefaultDbFileName),
                    new XElement("DatabasePath", ".")
                )
            );
            oDoc.Save(sConfigPath);
        }

        private static void EnsureDatabaseFileExists()
        {
            lock (_oConfigLock)
            {
                string sDirectory = Path.GetDirectoryName(_oDefaultDbPath);
                if (!string.IsNullOrEmpty(sDirectory) && !Directory.Exists(sDirectory))
                    Directory.CreateDirectory(sDirectory);
                if (!File.Exists(_oDefaultDbPath))
                    SQLiteConnection.CreateFile(_oDefaultDbPath);
            }
        }

        private static SQLiteConnection GetConnection(string sDbPath = null)
        {
            string sPath = sDbPath ?? _oDefaultDbPath;
            SQLiteConnection oConnection = null;

            lock (_oConnectionPoolLock)
            {
                if (_oConnectionPool.Count > 0)
                {
                    oConnection = _oConnectionPool[0];
                    _oConnectionPool.RemoveAt(0);
                }
            }

            if (oConnection != null)
            {
                oConnection.ConnectionString = $"Data Source={sPath};Version=3;Journal Mode=WAL;";
                return oConnection;
            }

            if (Interlocked.Increment(ref _iConnectionCount) <= MaxPoolSize)
            {
                return new SQLiteConnection($"Data Source={sPath};Version=3;Journal Mode=WAL;");
            }
            else
            {
                Interlocked.Decrement(ref _iConnectionCount);
                return new SQLiteConnection($"Data Source={sPath};Version=3;Journal Mode=WAL;");
            }
        }

        private static void ReturnConnection(SQLiteConnection oConnection)
        {
            if (oConnection != null)
            {
                try
                {
                    if (oConnection.State != System.Data.ConnectionState.Closed)
                        oConnection.Close();

                    lock (_oConnectionPoolLock)
                    {
                        _oConnectionPool.Add(oConnection);
                    }
                }
                catch { /* Ignore errors */ }
            }
        }

        private static bool IsNullableType(Type oType)
        {
            return oType.IsClass || Nullable.GetUnderlyingType(oType) != null;
        }

        private static string GetSQLiteType(Type oType)
        {
            if (Nullable.GetUnderlyingType(oType) != null)
                oType = Nullable.GetUnderlyingType(oType);
            if (oType == typeof(int) || oType == typeof(long) || oType == typeof(short) || oType == typeof(byte) || oType == typeof(bool))
                return "INTEGER";
            if (oType == typeof(float) || oType == typeof(double))
                return "REAL";
            if (oType == typeof(decimal) || oType == typeof(DateTime) || oType == typeof(Guid) || oType == typeof(TimeSpan) || oType == typeof(string))
                return "TEXT";
            if (oType == typeof(byte[]))
                return "BLOB";
            return "TEXT";
        }

        private static void ExecuteNonQuery(string sSql, string sDbPath = null, Dictionary<string, object> oParameters = null)
        {
            SQLiteConnection oConnection = null;
            try
            {
                oConnection = GetConnection(sDbPath);
                oConnection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, oConnection))
                {
                    if (oParameters != null)
                    {
                        foreach (KeyValuePair<string, object> oParam in oParameters)
                            oCommand.Parameters.AddWithValue(oParam.Key, oParam.Value ?? DBNull.Value);
                    }
                    oCommand.ExecuteNonQuery();
                }
            }
            finally
            {
                if (oConnection != null)
                    ReturnConnection(oConnection);
            }
        }

        private static T ExecuteScalar<T>(string sSql, string sDbPath = null, Dictionary<string, object> oParameters = null)
        {
            SQLiteConnection oConnection = null;
            try
            {
                oConnection = GetConnection(sDbPath);
                oConnection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, oConnection))
                {
                    if (oParameters != null)
                    {
                        foreach (KeyValuePair<string, object> oParam in oParameters)
                            oCommand.Parameters.AddWithValue(oParam.Key, oParam.Value ?? DBNull.Value);
                    }
                    var oResult = oCommand.ExecuteScalar();
                    return oResult != null ? (T)Convert.ChangeType(oResult, typeof(T)) : default(T);
                }
            }
            finally
            {
                if (oConnection != null)
                    ReturnConnection(oConnection);
            }
        }

        private static CachedInsertInfo GetCachedInsertInfo<T>()
        {
            lock (_oInsertInfoCacheLock)
            {
                if (!_oInsertInfoCache.TryGetValue(typeof(T), out Lazy<CachedInsertInfo> oLazy))
                {
                    oLazy = new Lazy<CachedInsertInfo>(() =>
                    {
                        PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(typeof(T));
                        PropertyInfo[] aProperties = GetCachedProperties(typeof(T))
                            .Where(p => p != oPrimaryKey || !IsAutoIncrementType(p.PropertyType)).ToArray();
                        string sTableName = GetCachedTableName(typeof(T));
                        var oColumns = new StringBuilder();
                        var oParameters = new StringBuilder();
                        var aParameterNames = new string[aProperties.Length];
                        for (int i = 0; i < aProperties.Length; i++)
                        {
                            if (i > 0)
                            {
                                oColumns.Append(", ");
                                oParameters.Append(", ");
                            }
                            oColumns.Append(aProperties[i].Name);
                            aParameterNames[i] = "@" + aProperties[i].Name;
                            oParameters.Append(aParameterNames[i]);
                        }
                        string sSql = $"INSERT INTO {sTableName} ({oColumns}) VALUES ({oParameters}); SELECT last_insert_rowid();";
                        return new CachedInsertInfo { Sql = sSql, Properties = aProperties, ParameterNames = aParameterNames };
                    }, LazyThreadSafetyMode.ExecutionAndPublication);

                    _oInsertInfoCache[typeof(T)] = oLazy;
                }
                return oLazy.Value;
            }
        }

        public static int Insert<T>(T oEntity, string sDbPath = null) where T : class, new()
        {
            if (oEntity == null)
                throw new ArgumentNullException(nameof(oEntity));
            CachedInsertInfo oInsertInfo = GetCachedInsertInfo<T>();
            PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(typeof(T));
            SQLiteConnection oConnection = null;
            try
            {
                oConnection = GetConnection(sDbPath);
                oConnection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(oInsertInfo.Sql, oConnection))
                {
                    for (int i = 0; i < oInsertInfo.Properties.Length; i++)
                    {
                        var oProp = oInsertInfo.Properties[i];
                        object oValue = oProp.GetValue(oEntity, null) ?? DBNull.Value;
                        if (oProp.PropertyType == typeof(decimal) && oValue is decimal dVal)
                        {
                            oValue = dVal.ToString(System.Globalization.CultureInfo.InvariantCulture);
                        }
                        oCommand.Parameters.AddWithValue(oInsertInfo.ParameterNames[i], oValue);
                    }
                    object oResult = oCommand.ExecuteScalar();
                    int iGeneratedId = Convert.ToInt32(oResult);
                    if (oPrimaryKey != null && oPrimaryKey.CanWrite && IsAutoIncrementType(oPrimaryKey.PropertyType))
                    {
                        oPrimaryKey.SetValue(oEntity, iGeneratedId, null);
                    }
                    return iGeneratedId;
                }
            }
            finally
            {
                if (oConnection != null)
                    ReturnConnection(oConnection);
            }
        }

        public static int InsertList<T>(IEnumerable<T> lEntities, string sDbPath = null) where T : class, new()
        {
            if (lEntities == null)
                throw new ArgumentNullException(nameof(lEntities));
            if (!lEntities.Any())
                return 0;
            CachedInsertInfo oInsertInfo = GetCachedInsertInfo<T>();
            PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(typeof(T));
            SQLiteConnection oConnection = null;
            try
            {
                oConnection = GetConnection(sDbPath);
                oConnection.Open();
                using (var oTransaction = oConnection.BeginTransaction())
                {
                    int iCount = 0;
                    try
                    {
                        using (SQLiteCommand oCommand = new SQLiteCommand(oInsertInfo.Sql, oConnection, oTransaction))
                        {
                            oCommand.Prepare();
                            foreach (var oEntity in lEntities)
                            {
                                oCommand.Parameters.Clear();
                                for (int i = 0; i < oInsertInfo.Properties.Length; i++)
                                {
                                    var oProp = oInsertInfo.Properties[i];
                                    object oValue = oProp.GetValue(oEntity, null) ?? DBNull.Value;
                                    if (oProp.PropertyType == typeof(decimal) && oValue is decimal dVal)
                                    {
                                        oValue = dVal.ToString(System.Globalization.CultureInfo.InvariantCulture);
                                    }
                                    oCommand.Parameters.AddWithValue(oInsertInfo.ParameterNames[i], oValue);
                                }
                                object oResult = oCommand.ExecuteScalar();
                                int iGeneratedId = Convert.ToInt32(oResult);
                                if (oPrimaryKey != null && oPrimaryKey.CanWrite && IsAutoIncrementType(oPrimaryKey.PropertyType))
                                {
                                    oPrimaryKey.SetValue(oEntity, iGeneratedId, null);
                                }
                                iCount++;
                            }
                        }
                        oTransaction.Commit();
                        return iCount;
                    }
                    catch
                    {
                        oTransaction.Rollback();
                        throw;
                    }
                }
            }
            finally
            {
                if (oConnection != null)
                    ReturnConnection(oConnection);
            }
        }

        private static CachedUpdateInfo GetCachedUpdateInfo<T>()
        {
            lock (_oUpdateInfoCacheLock)
            {
                if (!_oUpdateInfoCache.TryGetValue(typeof(T), out Lazy<CachedUpdateInfo> oLazy))
                {
                    oLazy = new Lazy<CachedUpdateInfo>(() =>
                    {
                        PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(typeof(T));
                        PropertyInfo[] aProperties = GetCachedProperties(typeof(T)).Where(p => p != oPrimaryKey).ToArray();
                        string sTableName = GetCachedTableName(typeof(T));
                        var oSetClause = new StringBuilder();
                        var aParameterNames = new string[aProperties.Length];
                        for (int i = 0; i < aProperties.Length; i++)
                        {
                            if (i > 0) oSetClause.Append(", ");
                            oSetClause.Append(aProperties[i].Name).Append(" = @").Append(aProperties[i].Name);
                            aParameterNames[i] = "@" + aProperties[i].Name;
                        }
                        string sSql = $"UPDATE {sTableName} SET {oSetClause} WHERE {oPrimaryKey.Name} = @PrimaryKey";
                        return new CachedUpdateInfo { Sql = sSql, Properties = aProperties, ParameterNames = aParameterNames };
                    }, LazyThreadSafetyMode.ExecutionAndPublication);

                    _oUpdateInfoCache[typeof(T)] = oLazy;
                }
                return oLazy.Value;
            }
        }

        public static bool Update<T>(T oEntity, string sDbPath = null) where T : class, new()
        {
            if (oEntity == null)
                throw new ArgumentNullException(nameof(oEntity));
            CachedUpdateInfo oUpdateInfo = GetCachedUpdateInfo<T>();
            PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(typeof(T));
            SQLiteConnection oConnection = null;
            try
            {
                oConnection = GetConnection(sDbPath);
                oConnection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(oUpdateInfo.Sql, oConnection))
                {
                    for (int i = 0; i < oUpdateInfo.Properties.Length; i++)
                    {
                        var oProp = oUpdateInfo.Properties[i];
                        object oValue = oProp.GetValue(oEntity, null) ?? DBNull.Value;
                        if (oProp.PropertyType == typeof(decimal) && oValue is decimal dVal)
                        {
                            oValue = dVal.ToString(System.Globalization.CultureInfo.InvariantCulture);
                        }
                        oCommand.Parameters.AddWithValue(oUpdateInfo.ParameterNames[i], oValue);
                    }
                    object oPrimaryKeyValue = oPrimaryKey.GetValue(oEntity, null) ?? DBNull.Value;
                    oCommand.Parameters.AddWithValue("@PrimaryKey", oPrimaryKeyValue);
                    return oCommand.ExecuteNonQuery() > 0;
                }
            }
            finally
            {
                if (oConnection != null)
                    ReturnConnection(oConnection);
            }
        }

        public static int UpdateList<T>(IEnumerable<T> lEntities, string sDbPath = null) where T : class, new()
        {
            if (lEntities == null)
                throw new ArgumentNullException(nameof(lEntities));
            if (!lEntities.Any())
                return 0;
            CachedUpdateInfo oUpdateInfo = GetCachedUpdateInfo<T>();
            PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(typeof(T));
            SQLiteConnection oConnection = null;
            try
            {
                oConnection = GetConnection(sDbPath);
                oConnection.Open();
                using (var oTransaction = oConnection.BeginTransaction())
                {
                    int iCount = 0;
                    try
                    {
                        using (SQLiteCommand oCommand = new SQLiteCommand(oUpdateInfo.Sql, oConnection, oTransaction))
                        {
                            oCommand.Prepare();
                            foreach (var oEntity in lEntities)
                            {
                                oCommand.Parameters.Clear();
                                for (int i = 0; i < oUpdateInfo.Properties.Length; i++)
                                {
                                    var oProp = oUpdateInfo.Properties[i];
                                    object oValue = oProp.GetValue(oEntity, null) ?? DBNull.Value;
                                    if (oProp.PropertyType == typeof(decimal) && oValue is decimal dVal)
                                    {
                                        oValue = dVal.ToString(System.Globalization.CultureInfo.InvariantCulture);
                                    }
                                    oCommand.Parameters.AddWithValue(oUpdateInfo.ParameterNames[i], oValue);
                                }
                                object oPrimaryKeyValue = oPrimaryKey.GetValue(oEntity, null) ?? DBNull.Value;
                                oCommand.Parameters.AddWithValue("@PrimaryKey", oPrimaryKeyValue);
                                iCount += oCommand.ExecuteNonQuery();
                            }
                        }
                        oTransaction.Commit();
                        return iCount;
                    }
                    catch
                    {
                        oTransaction.Rollback();
                        throw;
                    }
                }
            }
            finally
            {
                if (oConnection != null)
                    ReturnConnection(oConnection);
            }
        }

        public static bool Delete<T>(int iId, string sDbPath = null) where T : class, new()
        {
            string sTableName = GetCachedTableName<T>();
            PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(typeof(T));
            string sSql = $"DELETE FROM {sTableName} WHERE {oPrimaryKey.Name} = @PrimaryKey";
            SQLiteConnection oConnection = null;
            try
            {
                oConnection = GetConnection(sDbPath);
                oConnection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, oConnection))
                {
                    oCommand.Parameters.AddWithValue("@PrimaryKey", iId);
                    return oCommand.ExecuteNonQuery() > 0;
                }
            }
            finally
            {
                if (oConnection != null)
                    ReturnConnection(oConnection);
            }
        }

        public static int DeleteList<T>(IEnumerable<int> aIds, string sDbPath = null) where T : class, new()
        {
            if (aIds == null)
                throw new ArgumentNullException(nameof(aIds));
            if (!aIds.Any())
                return 0;
            string sTableName = GetCachedTableName<T>();
            PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(typeof(T));
            SQLiteConnection oConnection = null;
            try
            {
                oConnection = GetConnection(sDbPath);
                oConnection.Open();
                using (var oTransaction = oConnection.BeginTransaction())
                {
                    int iCount = 0;
                    try
                    {
                        using (SQLiteCommand oCommand = new SQLiteCommand($"DELETE FROM {sTableName} WHERE {oPrimaryKey.Name} = @PrimaryKey", oConnection, oTransaction))
                        {
                            oCommand.Prepare();
                            foreach (int iId in aIds)
                            {
                                oCommand.Parameters.Clear();
                                oCommand.Parameters.AddWithValue("@PrimaryKey", iId);
                                iCount += oCommand.ExecuteNonQuery();
                            }
                        }
                        oTransaction.Commit();
                        return iCount;
                    }
                    catch
                    {
                        oTransaction.Rollback();
                        throw;
                    }
                }
            }
            finally
            {
                if (oConnection != null)
                    ReturnConnection(oConnection);
            }
        }

        public static bool DeleteAll<T>(Expression<Func<T, bool>> oPredicate, string sDbPath = null) where T : class, new()
        {
            string sTableName = GetCachedTableName<T>();
            var oConverter = new ExpressionToSql<T>();
            var oSqlWithParams = oConverter.ConvertToSqlWithParameters(oPredicate);
            string sSql = $"DELETE FROM {sTableName} WHERE {oSqlWithParams.Sql}";
            SQLiteConnection oConnection = null;
            try
            {
                oConnection = GetConnection(sDbPath);
                oConnection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, oConnection))
                {
                    foreach (var oParam in oSqlWithParams.Parameters)
                    {
                        oCommand.Parameters.AddWithValue(oParam.Key, oParam.Value ?? DBNull.Value);
                    }
                    return oCommand.ExecuteNonQuery() > 0;
                }
            }
            finally
            {
                if (oConnection != null)
                    ReturnConnection(oConnection);
            }
        }

        private static Dictionary<string, ColumnMapping> GetColumnMappings(Type oType)
        {
            lock (_oColumnMappingCacheLock)
            {
                if (!_oColumnMappingCache.TryGetValue(oType, out Lazy<Dictionary<string, ColumnMapping>> oLazy))
                {
                    oLazy = new Lazy<Dictionary<string, ColumnMapping>>(() =>
                    {
                        var aProperties = GetCachedProperties(oType);
                        string sTableName = GetCachedTableName(oType);
                        var oMappings = new Dictionary<string, ColumnMapping>(StringComparer.OrdinalIgnoreCase);
                        using (var oConnection = GetConnection())
                        {
                            oConnection.Open();
                            using (var oCommand = new SQLiteCommand($"PRAGMA table_info({sTableName})", oConnection))
                            using (var oReader = oCommand.ExecuteReader())
                            {
                                var lColumnNames = new List<string>();
                                while (oReader.Read())
                                {
                                    lColumnNames.Add(oReader["name"].ToString());
                                }
                                for (int i = 0; i < aProperties.Length; i++)
                                {
                                    var oProp = aProperties[i];
                                    int iIndex = lColumnNames.IndexOf(oProp.Name);
                                    if (iIndex == -1) continue;
                                    oMappings[oProp.Name] = new ColumnMapping
                                    {
                                        Property = oProp,
                                        ValueConverter = CreateValueConverter(oProp.PropertyType),
                                        ColumnIndex = iIndex
                                    };
                                }
                            }
                        }
                        return oMappings;
                    }, LazyThreadSafetyMode.ExecutionAndPublication);

                    _oColumnMappingCache[oType] = oLazy;
                }
                return oLazy.Value;
            }
        }

        private static Func<SQLiteDataReader, object> GetEntityCreator<T>()
        {
            lock (_oEntityCreatorsLock)
            {
                if (!_oEntityCreators.TryGetValue(typeof(T), out Lazy<Func<SQLiteDataReader, object>> oLazy))
                {
                    oLazy = new Lazy<Func<SQLiteDataReader, object>>(() =>
                    {
                        var oReaderParam = Expression.Parameter(typeof(SQLiteDataReader), "reader");
                        var oEntityVar = Expression.Variable(typeof(T), "entity");
                        var oNewExpr = Expression.New(typeof(T));
                        var oAssignEntity = Expression.Assign(oEntityVar, oNewExpr);
                        var lBlockExpressions = new List<Expression> { oAssignEntity };
                        var oColumnMappings = GetColumnMappings(typeof(T));
                        foreach (var oMapping in oColumnMappings.Values)
                        {
                            var oProp = oMapping.Property;
                            var oIndexExpr = Expression.Constant(oMapping.ColumnIndex);
                            var oIsDBNullMethod = typeof(SQLiteDataReader).GetMethod("IsDBNull", new[] { typeof(int) });
                            var oIsDBNullExpr = Expression.Call(oReaderParam, oIsDBNullMethod, oIndexExpr);
                            var oGetValueMethod = typeof(SQLiteDataReader).GetMethod("GetValue", new[] { typeof(int) });
                            var oValueExpr = Expression.Call(oReaderParam, oGetValueMethod, oIndexExpr);
                            var oConverter = oMapping.ValueConverter;
                            var oConvertedValueExpr = Expression.Call(
                                Expression.Constant(oConverter),
                                typeof(Func<object, object>).GetMethod("Invoke"),
                                oValueExpr);
                            var oTypedValueExpr = Expression.Convert(oConvertedValueExpr, oProp.PropertyType);
                            var oDefaultValue = Expression.Default(oProp.PropertyType);
                            var oConditionExpr = Expression.Condition(oIsDBNullExpr, oDefaultValue, oTypedValueExpr);
                            var oPropertyExpr = Expression.Property(oEntityVar, oProp);
                            var oAssignExpr = Expression.Assign(oPropertyExpr, oConditionExpr);
                            lBlockExpressions.Add(oAssignExpr);
                        }
                        lBlockExpressions.Add(oEntityVar);
                        var oBlock = Expression.Block(new[] { oEntityVar }, lBlockExpressions);
                        return Expression.Lambda<Func<SQLiteDataReader, object>>(oBlock, oReaderParam).Compile();
                    }, LazyThreadSafetyMode.ExecutionAndPublication);

                    _oEntityCreators[typeof(T)] = oLazy;
                }
                return oLazy.Value;
            }
        }

        public static T GetById<T>(int iId, string sDbPath = null) where T : class, new()
        {
            PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(typeof(T));
            if (oPrimaryKey == null)
            {
                throw new InvalidOperationException($"Type {typeof(T).Name} does not have a primary key property");
            }
            string sTableName = GetCachedTableName<T>();
            string sSql = $"SELECT * FROM {sTableName} WHERE {oPrimaryKey.Name} = @Id";
            var oEntityCreator = GetEntityCreator<T>();
            SQLiteConnection oConnection = null;
            try
            {
                oConnection = GetConnection(sDbPath);
                oConnection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, oConnection))
                {
                    oCommand.Parameters.AddWithValue("@Id", iId);
                    using (SQLiteDataReader oReader = oCommand.ExecuteReader())
                    {
                        if (oReader.Read())
                        {
                            return (T)oEntityCreator(oReader);
                        }
                    }
                }
            }
            finally
            {
                if (oConnection != null)
                    ReturnConnection(oConnection);
            }
            return null;
        }

        public static IEnumerable<T> GetAll<T>(string sDbPath = null) where T : class, new()
        {
            string sTableName = GetCachedTableName<T>();
            string sSql = $"SELECT * FROM {sTableName}";
            var oEntityCreator = GetEntityCreator<T>();
            var lResults = new List<T>();
            SQLiteConnection oConnection = null;
            try
            {
                oConnection = GetConnection(sDbPath);
                oConnection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, oConnection))
                {
                    using (SQLiteDataReader oReader = oCommand.ExecuteReader())
                    {
                        while (oReader.Read())
                        {
                            lResults.Add((T)oEntityCreator(oReader));
                        }
                    }
                }
            }
            finally
            {
                if (oConnection != null)
                    ReturnConnection(oConnection);
            }
            return lResults;
        }

        public static T GetSingle<T>(Expression<Func<T, bool>> oPredicate, string sDbPath = null) where T : class, new()
        {
            string sTableName = GetCachedTableName<T>();
            var oConverter = new ExpressionToSql<T>();
            var oSqlWithParams = oConverter.ConvertToSqlWithParameters(oPredicate);
            string sSql = $"SELECT * FROM {sTableName}";
            if (!string.IsNullOrEmpty(oSqlWithParams.Sql) && oSqlWithParams.Sql != "1=1")
            {
                sSql += $" WHERE {oSqlWithParams.Sql}";
            }
            sSql += " LIMIT 1";
            var oEntityCreator = GetEntityCreator<T>();
            SQLiteConnection oConnection = null;
            try
            {
                oConnection = GetConnection(sDbPath);
                oConnection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, oConnection))
                {
                    foreach (var oParam in oSqlWithParams.Parameters)
                    {
                        oCommand.Parameters.AddWithValue(oParam.Key, oParam.Value ?? DBNull.Value);
                    }
                    using (SQLiteDataReader oReader = oCommand.ExecuteReader())
                    {
                        if (oReader.Read())
                        {
                            return (T)oEntityCreator(oReader);
                        }
                    }
                }
            }
            finally
            {
                if (oConnection != null)
                    ReturnConnection(oConnection);
            }
            return null;
        }

        public static IEnumerable<T> Find<T>(Expression<Func<T, bool>> oPredicate, string sDbPath = null) where T : class, new()
        {
            string sTableName = GetCachedTableName<T>();
            var oConverter = new ExpressionToSql<T>();
            var oSqlWithParams = oConverter.ConvertToSqlWithParameters(oPredicate);
            string sSql = $"SELECT * FROM {sTableName}";
            if (!string.IsNullOrEmpty(oSqlWithParams.Sql) && oSqlWithParams.Sql != "1=1")
            {
                sSql += $" WHERE {oSqlWithParams.Sql}";
            }
            var oEntityCreator = GetEntityCreator<T>();
            var lResults = new List<T>();
            SQLiteConnection oConnection = null;
            try
            {
                oConnection = GetConnection(sDbPath);
                oConnection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, oConnection))
                {
                    foreach (var oParam in oSqlWithParams.Parameters)
                    {
                        oCommand.Parameters.AddWithValue(oParam.Key, oParam.Value ?? DBNull.Value);
                    }
                    using (SQLiteDataReader oReader = oCommand.ExecuteReader())
                    {
                        while (oReader.Read())
                        {
                            lResults.Add((T)oEntityCreator(oReader));
                        }
                    }
                }
            }
            finally
            {
                if (oConnection != null)
                    ReturnConnection(oConnection);
            }
            return lResults;
        }

        public static int Count<T>(Expression<Func<T, bool>> oPredicate, string sDbPath = null) where T : class, new()
        {
            string sTableName = GetCachedTableName<T>();
            var oConverter = new ExpressionToSql<T>();
            var oSqlWithParams = oConverter.ConvertToSqlWithParameters(oPredicate);
            string sSql = $"SELECT COUNT(*) FROM {sTableName}";
            if (!string.IsNullOrEmpty(oSqlWithParams.Sql) && oSqlWithParams.Sql != "1=1")
            {
                sSql += $" WHERE {oSqlWithParams.Sql}";
            }
            SQLiteConnection oConnection = null;
            try
            {
                oConnection = GetConnection(sDbPath);
                oConnection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, oConnection))
                {
                    foreach (var oParam in oSqlWithParams.Parameters)
                    {
                        oCommand.Parameters.AddWithValue(oParam.Key, oParam.Value ?? DBNull.Value);
                    }
                    return Convert.ToInt32(oCommand.ExecuteScalar());
                }
            }
            finally
            {
                if (oConnection != null)
                    ReturnConnection(oConnection);
            }
        }

        public static bool RemoveColumn(string sTableName, string sColumnName, string sDbPath = null)
        {
            try
            {
                SQLiteConnection oConnection = null;
                try
                {
                    oConnection = GetConnection(sDbPath);
                    oConnection.Open();
                    var lColumns = new List<string>();
                    using (var oCommand = new SQLiteCommand($"PRAGMA table_info({sTableName})", oConnection))
                    using (var oReader = oCommand.ExecuteReader())
                    {
                        while (oReader.Read())
                        {
                            string sName = oReader["name"].ToString();
                            if (!sName.Equals(sColumnName, StringComparison.OrdinalIgnoreCase))
                            {
                                lColumns.Add(sName);
                            }
                        }
                    }
                    if (lColumns.Count == 0)
                        throw new InvalidOperationException("No columns to keep");
                    string sTempTableName = $"{sTableName}_temp";
                    var oBuilder = new StringBuilder();
                    oBuilder.Append("CREATE TABLE ").Append(sTempTableName).Append(" AS SELECT ");
                    for (int i = 0; i < lColumns.Count; i++)
                    {
                        if (i > 0) oBuilder.Append(", ");
                        oBuilder.Append(lColumns[i]);
                    }
                    oBuilder.Append(" FROM ").Append(sTableName);
                    string sCreateTempTableSql = oBuilder.ToString();
                    ExecuteNonQuery(sCreateTempTableSql, sDbPath);
                    ExecuteNonQuery($"DROP TABLE {sTableName}", sDbPath);
                    ExecuteNonQuery($"ALTER TABLE {sTempTableName} RENAME TO {sTableName}", sDbPath);

                    // Clear column mapping cache for all types that map to this table
                    lock (_oTableNameCacheLock)
                    {
                        foreach (var oKvp in _oTableNameCache)
                        {
                            if (oKvp.Value.Value.Equals(sTableName, StringComparison.OrdinalIgnoreCase))
                            {
                                InvalidateColumnMappingCache(oKvp.Key);
                            }
                        }
                    }

                    return true;
                }
                finally
                {
                    if (oConnection != null)
                        ReturnConnection(oConnection);
                }
            }
            catch (Exception oEx)
            {
                throw new InvalidOperationException($"Error removing column {sColumnName} from table {sTableName}: {oEx.Message}", oEx);
            }
        }

        public static bool Upsert<T>(T oEntity, string sDbPath = null) where T : class, new()
        {
            if (oEntity == null)
                throw new ArgumentNullException(nameof(oEntity));
            string sTableName = GetCachedTableName<T>();
            PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(typeof(T));
            if (oPrimaryKey == null)
                throw new InvalidOperationException("Upsert requires a primary key property.");
            PropertyInfo[] aProperties = GetCachedProperties(typeof(T));
            var aNonPkProperties = aProperties.Where(p => p != oPrimaryKey).ToArray();
            object oPrimaryKeyValue = oPrimaryKey.GetValue(oEntity, null);
            if (oPrimaryKeyValue == null || (oPrimaryKeyValue is int i && i <= 0))
            {
                Insert(oEntity, sDbPath);
                return true;
            }
            bool bUpdated = Update(oEntity, sDbPath);
            if (!bUpdated)
            {
                try
                {
                    Insert(oEntity, sDbPath);
                    return true;
                }
                catch (SQLiteException oEx) when (oEx.Message.Contains("constraint") || oEx.Message.Contains("UNIQUE"))
                {
                    return Update(oEntity, sDbPath);
                }
            }
            return bUpdated;
        }

        #region Index Management Methods
        public static bool RemoveIndex(string sIndexName, string sDbPath = null)
        {
            try
            {
                string sSql = $"DROP INDEX IF EXISTS {sIndexName}";
                ExecuteNonQuery(sSql, sDbPath);
                return true;
            }
            catch (Exception oEx)
            {
                throw new InvalidOperationException($"Error removing index {sIndexName}: {oEx.Message}", oEx);
            }
        }

        public static bool RemoveIndexesForTable(string sTableName, string sDbPath = null)
        {
            try
            {
                SQLiteConnection oConnection = null;
                try
                {
                    oConnection = GetConnection(sDbPath);
                    oConnection.Open();
                    var lIndexNames = new List<string>();
                    using (var oCommand = new SQLiteCommand(
                        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=@tableName", oConnection))
                    {
                        oCommand.Parameters.AddWithValue("@tableName", sTableName);
                        using (var oReader = oCommand.ExecuteReader())
                        {
                            while (oReader.Read())
                            {
                                lIndexNames.Add(oReader["name"].ToString());
                            }
                        }
                    }
                    for (int i = 0; i < lIndexNames.Count; i++)
                    {
                        string sSql = $"DROP INDEX {lIndexNames[i]}";
                        ExecuteNonQuery(sSql, sDbPath);
                    }
                    return true;
                }
                finally
                {
                    if (oConnection != null)
                        ReturnConnection(oConnection);
                }
            }
            catch (Exception oEx)
            {
                throw new InvalidOperationException($"Error removing indexes for table {sTableName}: {oEx.Message}", oEx);
            }
        }

        public static bool RemoveIndexesForProperty<T>(Expression<Func<T, object>> oPropertySelector, string sDbPath = null)
        {
            try
            {
                string sPropertyName = GetPropertyName(oPropertySelector);
                string sTableName = GetCachedTableName<T>();
                string sIndexName = $"IX_{sTableName}_{sPropertyName}";
                return RemoveIndex(sIndexName, sDbPath);
            }
            catch (Exception oEx)
            {
                throw new InvalidOperationException($"Error removing index for property: {oEx.Message}", oEx);
            }
        }

        public static List<string> GetIndexesForTable(string sTableName, string sDbPath = null)
        {
            var lIndexNames = new List<string>();
            SQLiteConnection oConnection = null;
            try
            {
                oConnection = GetConnection(sDbPath);
                oConnection.Open();
                using (var oCommand = new SQLiteCommand(
                    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=@tableName", oConnection))
                {
                    oCommand.Parameters.AddWithValue("@tableName", sTableName);
                    using (var oReader = oCommand.ExecuteReader())
                    {
                        while (oReader.Read())
                        {
                            lIndexNames.Add(oReader["name"].ToString());
                        }
                    }
                }
            }
            finally
            {
                if (oConnection != null)
                    ReturnConnection(oConnection);
            }
            return lIndexNames;
        }

        public static bool IndexExists(string sIndexName, string sDbPath = null)
        {
            SQLiteConnection oConnection = null;
            try
            {
                oConnection = GetConnection(sDbPath);
                oConnection.Open();
                using (var oCommand = new SQLiteCommand(
                    "SELECT 1 FROM sqlite_master WHERE type='index' AND name=@indexName", oConnection))
                {
                    oCommand.Parameters.AddWithValue("@indexName", sIndexName);
                    return oCommand.ExecuteScalar() != null;
                }
            }
            finally
            {
                if (oConnection != null)
                    ReturnConnection(oConnection);
            }
        }

        private static string GetPropertyName<T>(Expression<Func<T, object>> oPropertySelector)
        {
            var oBody = oPropertySelector.Body;
            MemberExpression oMemberExpression;
            if (oBody is MemberExpression)
            {
                oMemberExpression = (MemberExpression)oBody;
            }
            else if (oBody is UnaryExpression)
            {
                oMemberExpression = (MemberExpression)((UnaryExpression)oBody).Operand;
            }
            else
            {
                throw new ArgumentException("Invalid property expression");
            }
            return oMemberExpression.Member.Name;
        }
        #endregion

        private static Func<object, object> CreateValueConverter(Type oTargetType)
        {
            lock (_oValueConvertersCacheLock)
            {
                if (!_oValueConvertersCache.TryGetValue(oTargetType, out Lazy<Func<object, object>> oLazy))
                {
                    oLazy = new Lazy<Func<object, object>>(() =>
                    {
                        Type oUnderlyingType = Nullable.GetUnderlyingType(oTargetType) ?? oTargetType;
                        if (oUnderlyingType == typeof(bool))
                            return oValue => oValue == null ? null : (object)(Convert.ToInt64(oValue) != 0);
                        else if (oUnderlyingType == typeof(byte[]))
                            return oValue => oValue;
                        else if (oUnderlyingType == typeof(Guid))
                            return oValue => oValue == null ? null : (object)Guid.Parse(oValue.ToString());
                        else if (oUnderlyingType == typeof(TimeSpan))
                            return oValue => oValue == null ? null : (object)TimeSpan.Parse(oValue.ToString());
                        else if (oUnderlyingType == typeof(decimal))
                            return oValue => oValue == null ? null : (object)decimal.Parse(oValue.ToString(), System.Globalization.CultureInfo.InvariantCulture);
                        else if (oUnderlyingType == typeof(int))
                            return oValue => oValue == null ? null : (object)Convert.ToInt32(oValue);
                        else if (oUnderlyingType == typeof(long))
                            return oValue => oValue == null ? null : (object)Convert.ToInt64(oValue);
                        else if (oUnderlyingType == typeof(short))
                            return oValue => oValue == null ? null : (object)Convert.ToInt16(oValue);
                        else if (oUnderlyingType == typeof(byte))
                            return oValue => oValue == null ? null : (object)Convert.ToByte(oValue);
                        else if (oUnderlyingType == typeof(float))
                            return oValue => oValue == null ? null : (object)Convert.ToSingle(oValue);
                        else if (oUnderlyingType == typeof(double))
                            return oValue => oValue == null ? null : (object)Convert.ToDouble(oValue);
                        else if (oUnderlyingType == typeof(string))
                            return oValue => oValue == null ? null : (object)oValue.ToString();
                        else
                            return oValue => oValue == null ? null : Convert.ChangeType(oValue, oUnderlyingType);
                    }, LazyThreadSafetyMode.ExecutionAndPublication);

                    _oValueConvertersCache[oTargetType] = oLazy;
                }
                return oLazy.Value;
            }
        }
    }
}