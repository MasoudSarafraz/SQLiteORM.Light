using SQLiteORM.Attributes;
using System;
using System.Collections;
using System.Collections.Concurrent;
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
        private static bool _TablesChecked = false;
        private static readonly object _ConfigLock = new object();
        private static readonly object _TableCheckLock = new object();
        private static readonly ConcurrentDictionary<Type, bool> _TableCheckCache = new ConcurrentDictionary<Type, bool>();
        private static readonly ConcurrentDictionary<Type, PropertyInfo[]> _PropertyCache = new ConcurrentDictionary<Type, PropertyInfo[]>();
        private static readonly ConcurrentDictionary<Type, string> _TableNameCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, PropertyInfo> _PrimaryKeyCache = new ConcurrentDictionary<Type, PropertyInfo>();
        private static readonly ConcurrentDictionary<string, object> _TableLocks = new ConcurrentDictionary<string, object>();
        private static readonly ConcurrentDictionary<Type, List<IndexInfo>> _IndexCache = new ConcurrentDictionary<Type, List<IndexInfo>>();
        private static readonly ConcurrentDictionary<Type, CachedInsertInfo> _InsertInfoCache = new ConcurrentDictionary<Type, CachedInsertInfo>();
        private static readonly ConcurrentDictionary<Type, CachedUpdateInfo> _UpdateInfoCache = new ConcurrentDictionary<Type, CachedUpdateInfo>();
        private static readonly ConcurrentDictionary<Type, Dictionary<string, ColumnMapping>> _ColumnMappingCache = new ConcurrentDictionary<Type, Dictionary<string, ColumnMapping>>();
        private static readonly ConcurrentDictionary<Type, Func<SQLiteDataReader, object>> _EntityCreators = new ConcurrentDictionary<Type, Func<SQLiteDataReader, object>>();
        private static readonly ConcurrentBag<SQLiteConnection> _ConnectionPool = new ConcurrentBag<SQLiteConnection>();
        private static readonly ConcurrentDictionary<Type, Func<object, object>> _ValueConvertersCache = new ConcurrentDictionary<Type, Func<object, object>>();
        private const int MaxPoolSize = 100;
        private static int _ConnectionCount = 0;

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
            lock (_ConfigLock)
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
            lock (_ConfigLock)
            {
                string sBasePath = sDbPath ?? AppDomain.CurrentDomain.BaseDirectory;
                _oDefaultDbPath = Path.Combine(sBasePath, sDbFileName);
                EnsureDatabaseFileExists();
                CheckAllTableStructures();
            }
        }

        private static void CheckAllTableStructures()
        {
            if (_TablesChecked) return;
            lock (_TableCheckLock)
            {
                if (_TablesChecked) return;
                var aModelTypes = new List<Type>();
                var aAssemblies = AppDomain.CurrentDomain.GetAssemblies();
                for (int i = 0; i < aAssemblies.Length; i++)
                {
                    try
                    {
                        var aTypes = aAssemblies[i].GetTypes();
                        for (int j = 0; j < aTypes.Length; j++)
                        {
                            var type = aTypes[j];
                            if (type != null && type.GetCustomAttributes(typeof(TableAttribute), false).Length > 0)
                            {
                                aModelTypes.Add(type);
                            }
                        }
                    }
                    catch (ReflectionTypeLoadException oEx)
                    {
                        var aTypes = oEx.Types;
                        for (int j = 0; j < aTypes.Length; j++)
                        {
                            var type = aTypes[j];
                            if (type != null && type.GetCustomAttributes(typeof(TableAttribute), false).Length > 0)
                            {
                                aModelTypes.Add(type);
                            }
                        }
                    }
                }
                for (int i = 0; i < aModelTypes.Count; i++)
                {
                    CheckTableStructure(aModelTypes[i]);
                }
                _TablesChecked = true;
            }
        }

        private static void CheckTableStructure(Type oModelType)
        {
            var aTableAttrs = oModelType.GetCustomAttributes(typeof(TableAttribute), false);
            if (aTableAttrs.Length == 0) return;
            var oTableAttr = (TableAttribute)aTableAttrs[0];
            string sTableName = GetFullTableName(oTableAttr);
            var oTableLock = _TableLocks.GetOrAdd(sTableName, new object());
            lock (oTableLock)
            {
                if (_TableCheckCache.TryGetValue(oModelType, out bool bChecked) && bChecked)
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
                    _ColumnMappingCache.TryRemove(oModelType, out _);
                }
                else
                {
                    var aExistingColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    using (var oConnection = GetConnection())
                    {
                        oConnection.Open();
                        using (var oCommand = new SQLiteCommand($"PRAGMA table_info('{sTableName}')", oConnection))
                        using (var oReader = oCommand.ExecuteReader())
                        {
                            while (oReader.Read())
                            {
                                aExistingColumns.Add(oReader["name"].ToString());
                            }
                        }
                    }
                    var aMissingProperties = new List<PropertyInfo>();
                    for (int i = 0; i < aProperties.Length; i++)
                    {
                        if (!aExistingColumns.Contains(aProperties[i].Name))
                        {
                            aMissingProperties.Add(aProperties[i]);
                        }
                    }
                    if (aMissingProperties.Count > 0)
                    {
                        AddMissingColumns(sTableName, aMissingProperties);
                        _ColumnMappingCache.TryRemove(oModelType, out _);
                    }
                }
                CheckAndCreateIndexes(oModelType, sTableName);
                _TableCheckCache[oModelType] = true;
            }
        }

        private static void CreateTable(Type oModelType, string sTableName, List<PropertyInfo> aProperties)
        {
            var oBuilder = new StringBuilder();
            oBuilder.Append($"CREATE TABLE {sTableName} (");
            PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(oModelType);
            bool bFirst = true;
            for (int i = 0; i < aProperties.Count; i++)
            {
                var oProp = aProperties[i];
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
            var aIndexInfos = GetCachedIndexes(oModelType);
            using (var oConnection = GetConnection())
            {
                oConnection.Open();
                var aExistingIndexes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                using (var oCommand = new SQLiteCommand(
                    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=@tableName", oConnection))
                {
                    oCommand.Parameters.AddWithValue("@tableName", sTableName);
                    using (var oReader = oCommand.ExecuteReader())
                    {
                        while (oReader.Read())
                        {
                            aExistingIndexes.Add(oReader["name"].ToString());
                        }
                    }
                }
                for (int i = 0; i < aIndexInfos.Count; i++)
                {
                    var oIndexInfo = aIndexInfos[i];
                    if (oIndexInfo.AutoCreate && !aExistingIndexes.Contains(oIndexInfo.Name))
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
            return _IndexCache.GetOrAdd(oType, type =>
            {
                var aIndexInfos = new List<IndexInfo>();
                var aProperties = GetCachedProperties(type);
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
                                ? $"IX_{GetCachedTableName(type)}_{oProp.Name}"
                                : oIndexAttr.Name;
                            aIndexInfos.Add(new IndexInfo
                            {
                                Name = sIndexName,
                                IsUnique = oIndexAttr.IsUnique,
                                Properties = new List<string> { oProp.Name },
                                AutoCreate = oIndexAttr.AutoCreate
                            });
                        }
                    }
                }

                var aGroupedIndexes = new Dictionary<string, List<IndexInfo>>(StringComparer.OrdinalIgnoreCase);
                for (int i = 0; i < aIndexInfos.Count; i++)
                {
                    var indexInfo = aIndexInfos[i];
                    if (!string.IsNullOrEmpty(indexInfo.Name))
                    {
                        List<IndexInfo> group;
                        if (!aGroupedIndexes.TryGetValue(indexInfo.Name, out group))
                        {
                            group = new List<IndexInfo>();
                            aGroupedIndexes[indexInfo.Name] = group;
                        }
                        group.Add(indexInfo);
                    }
                }

                foreach (var group in aGroupedIndexes.Values)
                {
                    if (group.Count > 1)
                    {
                        var oFirst = group[0];
                        var allProperties = new List<string>();
                        for (int i = 0; i < group.Count; i++)
                        {
                            allProperties.AddRange(group[i].Properties);
                        }
                        allProperties.Sort(StringComparer.OrdinalIgnoreCase);
                        oFirst.Properties = allProperties;

                        for (int i = 1; i < group.Count; i++)
                        {
                            aIndexInfos.Remove(group[i]);
                        }
                    }
                }

                return aIndexInfos;
            });
        }

        private static bool IsAutoIncrementType(Type oType)
        {
            oType = Nullable.GetUnderlyingType(oType) ?? oType;
            return oType == typeof(int) || oType == typeof(long);
        }

        private static PropertyInfo GetPrimaryKeyProperty(Type oType)
        {
            return _PrimaryKeyCache.GetOrAdd(oType, type =>
            {
                var aProperties = GetCachedProperties(type);
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
                throw new InvalidOperationException($"No primary key defined for type {type.Name}. Use [IdentityKey] attribute or name a property 'Id'.");
            });
        }

        private static void AddMissingColumns(string sTableName, List<PropertyInfo> aMissingProperties)
        {
            using (var oConnection = GetConnection())
            {
                oConnection.Open();
                using (var oTransaction = oConnection.BeginTransaction())
                {
                    try
                    {
                        for (int i = 0; i < aMissingProperties.Count; i++)
                        {
                            var oProp = aMissingProperties[i];
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
            return _PropertyCache.GetOrAdd(oType, type =>
            {
                var aProperties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
                var aResult = new List<PropertyInfo>();
                for (int i = 0; i < aProperties.Length; i++)
                {
                    if (aProperties[i].GetCustomAttributes(typeof(IgnoreAttribute), false).Length == 0)
                    {
                        aResult.Add(aProperties[i]);
                    }
                }
                return aResult.ToArray();
            });
        }

        private static string GetCachedTableName(Type oType)
        {
            return _TableNameCache.GetOrAdd(oType, type =>
            {
                var aTableAttrs = type.GetCustomAttributes(typeof(TableAttribute), false);
                if (aTableAttrs.Length == 0)
                {
                    return type.Name;
                }
                var oTableAttr = (TableAttribute)aTableAttrs[0];
                return GetFullTableName(oTableAttr);
            });
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
            lock (_ConfigLock)
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
            SQLiteConnection connection;
            if (_ConnectionPool.TryTake(out connection))
            {
                connection.ConnectionString = $"Data Source={sPath};Version=3;Journal Mode=WAL;";
                return connection;
            }
            if (Interlocked.Increment(ref _ConnectionCount) <= MaxPoolSize)
            {
                return new SQLiteConnection($"Data Source={sPath};Version=3;Journal Mode=WAL;");
            }
            else
            {
                Interlocked.Decrement(ref _ConnectionCount);
                return new SQLiteConnection($"Data Source={sPath};Version=3;Journal Mode=WAL;");
            }
        }

        private static void ReturnConnection(SQLiteConnection connection)
        {
            if (connection != null)
            {
                try
                {
                    if (connection.State != System.Data.ConnectionState.Closed)
                        connection.Close();
                    _ConnectionPool.Add(connection);
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
            SQLiteConnection connection = null;
            try
            {
                connection = GetConnection(sDbPath);
                connection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, connection))
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
                if (connection != null)
                    ReturnConnection(connection);
            }
        }

        private static T ExecuteScalar<T>(string sSql, string sDbPath = null, Dictionary<string, object> oParameters = null)
        {
            SQLiteConnection connection = null;
            try
            {
                connection = GetConnection(sDbPath);
                connection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, connection))
                {
                    if (oParameters != null)
                    {
                        foreach (KeyValuePair<string, object> oParam in oParameters)
                            oCommand.Parameters.AddWithValue(oParam.Key, oParam.Value ?? DBNull.Value);
                    }
                    var result = oCommand.ExecuteScalar();
                    return result != null ? (T)Convert.ChangeType(result, typeof(T)) : default(T);
                }
            }
            finally
            {
                if (connection != null)
                    ReturnConnection(connection);
            }
        }

        private static CachedInsertInfo GetCachedInsertInfo<T>()
        {
            return _InsertInfoCache.GetOrAdd(typeof(T), type =>
            {
                PropertyInfo primaryKey = GetPrimaryKeyProperty(type);
                PropertyInfo[] properties = GetCachedProperties(type)
                    .Where(p => p != primaryKey || !IsAutoIncrementType(p.PropertyType)).ToArray();
                string tableName = GetCachedTableName(type);
                var columns = new StringBuilder();
                var parameters = new StringBuilder();
                var parameterNames = new string[properties.Length];
                for (int i = 0; i < properties.Length; i++)
                {
                    if (i > 0)
                    {
                        columns.Append(", ");
                        parameters.Append(", ");
                    }
                    columns.Append(properties[i].Name);
                    parameterNames[i] = "@" + properties[i].Name;
                    parameters.Append(parameterNames[i]);
                }
                string sql = $"INSERT INTO {tableName} ({columns}) VALUES ({parameters}); SELECT last_insert_rowid();";
                return new CachedInsertInfo { Sql = sql, Properties = properties, ParameterNames = parameterNames };
            });
        }

        public static int Insert<T>(T oEntity, string sDbPath = null) where T : class, new()
        {
            if (oEntity == null)
                throw new ArgumentNullException(nameof(oEntity));
            CachedInsertInfo insertInfo = GetCachedInsertInfo<T>();
            PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(typeof(T));
            SQLiteConnection connection = null;
            try
            {
                connection = GetConnection(sDbPath);
                connection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(insertInfo.Sql, connection))
                {
                    for (int i = 0; i < insertInfo.Properties.Length; i++)
                    {
                        var oProp = insertInfo.Properties[i];
                        object oValue = oProp.GetValue(oEntity, null) ?? DBNull.Value;
                        if (oProp.PropertyType == typeof(decimal) && oValue is decimal dVal)
                        {
                            oValue = dVal.ToString(System.Globalization.CultureInfo.InvariantCulture);
                        }
                        oCommand.Parameters.AddWithValue(insertInfo.ParameterNames[i], oValue);
                    }
                    object result = oCommand.ExecuteScalar();
                    int iGeneratedId = Convert.ToInt32(result);
                    if (oPrimaryKey != null && oPrimaryKey.CanWrite && IsAutoIncrementType(oPrimaryKey.PropertyType))
                    {
                        oPrimaryKey.SetValue(oEntity, iGeneratedId, null);
                    }
                    return iGeneratedId;
                }
            }
            finally
            {
                if (connection != null)
                    ReturnConnection(connection);
            }
        }

        public static int InsertList<T>(IEnumerable<T> oEntities, string sDbPath = null) where T : class, new()
        {
            if (oEntities == null)
                throw new ArgumentNullException(nameof(oEntities));
            if (!oEntities.Any())
                return 0;
            CachedInsertInfo insertInfo = GetCachedInsertInfo<T>();
            PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(typeof(T));
            SQLiteConnection connection = null;
            try
            {
                connection = GetConnection(sDbPath);
                connection.Open();
                using (var oTransaction = connection.BeginTransaction())
                {
                    int iCount = 0;
                    try
                    {
                        using (SQLiteCommand oCommand = new SQLiteCommand(insertInfo.Sql, connection, oTransaction))
                        {
                            oCommand.Prepare();
                            foreach (var oEntity in oEntities)
                            {
                                oCommand.Parameters.Clear();
                                for (int i = 0; i < insertInfo.Properties.Length; i++)
                                {
                                    var oProp = insertInfo.Properties[i];
                                    object oValue = oProp.GetValue(oEntity, null) ?? DBNull.Value;
                                    if (oProp.PropertyType == typeof(decimal) && oValue is decimal dVal)
                                    {
                                        oValue = dVal.ToString(System.Globalization.CultureInfo.InvariantCulture);
                                    }
                                    oCommand.Parameters.AddWithValue(insertInfo.ParameterNames[i], oValue);
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
                if (connection != null)
                    ReturnConnection(connection);
            }
        }

        private static CachedUpdateInfo GetCachedUpdateInfo<T>()
        {
            return _UpdateInfoCache.GetOrAdd(typeof(T), type =>
            {
                PropertyInfo primaryKey = GetPrimaryKeyProperty(type);
                PropertyInfo[] properties = GetCachedProperties(type).Where(p => p != primaryKey).ToArray();
                string tableName = GetCachedTableName(type);
                var setClause = new StringBuilder();
                var parameterNames = new string[properties.Length];
                for (int i = 0; i < properties.Length; i++)
                {
                    if (i > 0) setClause.Append(", ");
                    setClause.Append(properties[i].Name).Append(" = @").Append(properties[i].Name);
                    parameterNames[i] = "@" + properties[i].Name;
                }
                string sql = $"UPDATE {tableName} SET {setClause} WHERE {primaryKey.Name} = @PrimaryKey";
                return new CachedUpdateInfo { Sql = sql, Properties = properties, ParameterNames = parameterNames };
            });
        }

        public static bool Update<T>(T oEntity, string sDbPath = null) where T : class, new()
        {
            if (oEntity == null)
                throw new ArgumentNullException(nameof(oEntity));
            CachedUpdateInfo updateInfo = GetCachedUpdateInfo<T>();
            PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(typeof(T));
            SQLiteConnection connection = null;
            try
            {
                connection = GetConnection(sDbPath);
                connection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(updateInfo.Sql, connection))
                {
                    for (int i = 0; i < updateInfo.Properties.Length; i++)
                    {
                        var oProp = updateInfo.Properties[i];
                        object oValue = oProp.GetValue(oEntity, null) ?? DBNull.Value;
                        if (oProp.PropertyType == typeof(decimal) && oValue is decimal dVal)
                        {
                            oValue = dVal.ToString(System.Globalization.CultureInfo.InvariantCulture);
                        }
                        oCommand.Parameters.AddWithValue(updateInfo.ParameterNames[i], oValue);
                    }
                    object oPrimaryKeyValue = oPrimaryKey.GetValue(oEntity, null) ?? DBNull.Value;
                    oCommand.Parameters.AddWithValue("@PrimaryKey", oPrimaryKeyValue);
                    return oCommand.ExecuteNonQuery() > 0;
                }
            }
            finally
            {
                if (connection != null)
                    ReturnConnection(connection);
            }
        }

        public static int UpdateList<T>(IEnumerable<T> oEntities, string sDbPath = null) where T : class, new()
        {
            if (oEntities == null)
                throw new ArgumentNullException(nameof(oEntities));
            if (!oEntities.Any())
                return 0;
            CachedUpdateInfo updateInfo = GetCachedUpdateInfo<T>();
            PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(typeof(T));
            SQLiteConnection connection = null;
            try
            {
                connection = GetConnection(sDbPath);
                connection.Open();
                using (var oTransaction = connection.BeginTransaction())
                {
                    int iCount = 0;
                    try
                    {
                        using (SQLiteCommand oCommand = new SQLiteCommand(updateInfo.Sql, connection, oTransaction))
                        {
                            oCommand.Prepare();
                            foreach (var oEntity in oEntities)
                            {
                                oCommand.Parameters.Clear();
                                for (int i = 0; i < updateInfo.Properties.Length; i++)
                                {
                                    var oProp = updateInfo.Properties[i];
                                    object oValue = oProp.GetValue(oEntity, null) ?? DBNull.Value;
                                    if (oProp.PropertyType == typeof(decimal) && oValue is decimal dVal)
                                    {
                                        oValue = dVal.ToString(System.Globalization.CultureInfo.InvariantCulture);
                                    }
                                    oCommand.Parameters.AddWithValue(updateInfo.ParameterNames[i], oValue);
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
                if (connection != null)
                    ReturnConnection(connection);
            }
        }

        public static bool Delete<T>(int iId, string sDbPath = null) where T : class, new()
        {
            string sTableName = GetCachedTableName<T>();
            PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(typeof(T));
            string sSql = $"DELETE FROM {sTableName} WHERE {oPrimaryKey.Name} = @PrimaryKey";
            SQLiteConnection connection = null;
            try
            {
                connection = GetConnection(sDbPath);
                connection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, connection))
                {
                    oCommand.Parameters.AddWithValue("@PrimaryKey", iId);
                    return oCommand.ExecuteNonQuery() > 0;
                }
            }
            finally
            {
                if (connection != null)
                    ReturnConnection(connection);
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
            SQLiteConnection connection = null;
            try
            {
                connection = GetConnection(sDbPath);
                connection.Open();
                using (var oTransaction = connection.BeginTransaction())
                {
                    int iCount = 0;
                    try
                    {
                        using (SQLiteCommand oCommand = new SQLiteCommand($"DELETE FROM {sTableName} WHERE {oPrimaryKey.Name} = @PrimaryKey", connection, oTransaction))
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
                if (connection != null)
                    ReturnConnection(connection);
            }
        }

        public static bool DeleteAll<T>(Expression<Func<T, bool>> oPredicate, string sDbPath = null) where T : class, new()
        {
            string sTableName = GetCachedTableName<T>();
            var oConverter = new ExpressionToSql<T>();
            var oSqlWithParams = oConverter.ConvertToSqlWithParameters(oPredicate);
            string sSql = $"DELETE FROM {sTableName} WHERE {oSqlWithParams.Sql}";
            SQLiteConnection connection = null;
            try
            {
                connection = GetConnection(sDbPath);
                connection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, connection))
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
                if (connection != null)
                    ReturnConnection(connection);
            }
        }

        private static Dictionary<string, ColumnMapping> GetColumnMappings(Type type)
        {
            return _ColumnMappingCache.GetOrAdd(type, t =>
            {
                var properties = GetCachedProperties(t);
                var tableName = GetCachedTableName(t);
                var mappings = new Dictionary<string, ColumnMapping>(StringComparer.OrdinalIgnoreCase);

                using (var connection = GetConnection())
                {
                    connection.Open();
                    using (var command = new SQLiteCommand($"PRAGMA table_info({tableName})", connection))
                    using (var reader = command.ExecuteReader())
                    {
                        var columnNames = new List<string>();
                        while (reader.Read())
                        {
                            columnNames.Add(reader["name"].ToString());
                        }

                        for (int i = 0; i < properties.Length; i++)
                        {
                            var prop = properties[i];
                            int index = columnNames.IndexOf(prop.Name);
                            if (index == -1) continue;

                            mappings[prop.Name] = new ColumnMapping
                            {
                                Property = prop,
                                ValueConverter = CreateValueConverter(prop.PropertyType),
                                ColumnIndex = index
                            };
                        }
                    }
                }
                return mappings;
            });
        }

        private static Func<SQLiteDataReader, object> GetEntityCreator<T>()
        {
            return _EntityCreators.GetOrAdd(typeof(T), type =>
            {
                var readerParam = Expression.Parameter(typeof(SQLiteDataReader), "reader");
                var entityVar = Expression.Variable(type, "entity");
                var newExpr = Expression.New(type);
                var assignEntity = Expression.Assign(entityVar, newExpr);
                var blockExpressions = new List<Expression> { assignEntity };

                var columnMappings = GetColumnMappings(type);

                foreach (var mapping in columnMappings.Values)
                {
                    var prop = mapping.Property;
                    var indexExpr = Expression.Constant(mapping.ColumnIndex);
                    var isDBNullMethod = typeof(SQLiteDataReader).GetMethod("IsDBNull", new[] { typeof(int) });
                    var isDBNullExpr = Expression.Call(readerParam, isDBNullMethod, indexExpr);
                    var getValueMethod = typeof(SQLiteDataReader).GetMethod("GetValue", new[] { typeof(int) });
                    var valueExpr = Expression.Call(readerParam, getValueMethod, indexExpr);
                    var converter = mapping.ValueConverter;
                    var convertedValueExpr = Expression.Call(
                        Expression.Constant(converter),
                        typeof(Func<object, object>).GetMethod("Invoke"),
                        valueExpr);
                    var typedValueExpr = Expression.Convert(convertedValueExpr, prop.PropertyType);
                    var defaultValue = Expression.Default(prop.PropertyType);
                    var conditionExpr = Expression.Condition(isDBNullExpr, defaultValue, typedValueExpr);
                    var propertyExpr = Expression.Property(entityVar, prop);
                    var assignExpr = Expression.Assign(propertyExpr, conditionExpr);
                    blockExpressions.Add(assignExpr);
                }

                blockExpressions.Add(entityVar);
                var block = Expression.Block(new[] { entityVar }, blockExpressions);
                return Expression.Lambda<Func<SQLiteDataReader, object>>(block, readerParam).Compile();
            });
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
            var entityCreator = GetEntityCreator<T>();
            SQLiteConnection connection = null;
            try
            {
                connection = GetConnection(sDbPath);
                connection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, connection))
                {
                    oCommand.Parameters.AddWithValue("@Id", iId);
                    using (SQLiteDataReader oReader = oCommand.ExecuteReader())
                    {
                        if (oReader.Read())
                        {
                            return (T)entityCreator(oReader);
                        }
                    }
                }
            }
            finally
            {
                if (connection != null)
                    ReturnConnection(connection);
            }
            return null;
        }

        public static IEnumerable<T> GetAll<T>(string sDbPath = null) where T : class, new()
        {
            string sTableName = GetCachedTableName<T>();
            string sSql = $"SELECT * FROM {sTableName}";
            var entityCreator = GetEntityCreator<T>();
            var results = new List<T>();
            SQLiteConnection connection = null;
            try
            {
                connection = GetConnection(sDbPath);
                connection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, connection))
                {
                    using (SQLiteDataReader oReader = oCommand.ExecuteReader())
                    {
                        while (oReader.Read())
                        {
                            results.Add((T)entityCreator(oReader));
                        }
                    }
                }
            }
            finally
            {
                if (connection != null)
                    ReturnConnection(connection);
            }
            return results;
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
            var entityCreator = GetEntityCreator<T>();
            SQLiteConnection connection = null;
            try
            {
                connection = GetConnection(sDbPath);
                connection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, connection))
                {
                    foreach (var oParam in oSqlWithParams.Parameters)
                    {
                        oCommand.Parameters.AddWithValue(oParam.Key, oParam.Value ?? DBNull.Value);
                    }
                    using (SQLiteDataReader oReader = oCommand.ExecuteReader())
                    {
                        if (oReader.Read())
                        {
                            return (T)entityCreator(oReader);
                        }
                    }
                }
            }
            finally
            {
                if (connection != null)
                    ReturnConnection(connection);
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
            var entityCreator = GetEntityCreator<T>();
            var results = new List<T>();
            SQLiteConnection connection = null;
            try
            {
                connection = GetConnection(sDbPath);
                connection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, connection))
                {
                    foreach (var oParam in oSqlWithParams.Parameters)
                    {
                        oCommand.Parameters.AddWithValue(oParam.Key, oParam.Value ?? DBNull.Value);
                    }
                    using (SQLiteDataReader oReader = oCommand.ExecuteReader())
                    {
                        while (oReader.Read())
                        {
                            results.Add((T)entityCreator(oReader));
                        }
                    }
                }
            }
            finally
            {
                if (connection != null)
                    ReturnConnection(connection);
            }
            return results;
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
            SQLiteConnection connection = null;
            try
            {
                connection = GetConnection(sDbPath);
                connection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, connection))
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
                if (connection != null)
                    ReturnConnection(connection);
            }
        }

        public static bool RemoveColumn(string sTableName, string sColumnName, string sDbPath = null)
        {
            try
            {
                SQLiteConnection connection = null;
                try
                {
                    connection = GetConnection(sDbPath);
                    connection.Open();
                    var aColumns = new List<string>();
                    using (var oCommand = new SQLiteCommand($"PRAGMA table_info({sTableName})", connection))
                    using (var oReader = oCommand.ExecuteReader())
                    {
                        while (oReader.Read())
                        {
                            string sName = oReader["name"].ToString();
                            if (!sName.Equals(sColumnName, StringComparison.OrdinalIgnoreCase))
                            {
                                aColumns.Add(sName);
                            }
                        }
                    }
                    if (aColumns.Count == 0)
                        throw new InvalidOperationException("No columns to keep");
                    string sTempTableName = $"{sTableName}_temp";
                    var oBuilder = new StringBuilder();
                    oBuilder.Append("CREATE TABLE ").Append(sTempTableName).Append(" AS SELECT ");
                    for (int i = 0; i < aColumns.Count; i++)
                    {
                        if (i > 0) oBuilder.Append(", ");
                        oBuilder.Append(aColumns[i]);
                    }
                    oBuilder.Append(" FROM ").Append(sTableName);
                    string sCreateTempTableSql = oBuilder.ToString();
                    ExecuteNonQuery(sCreateTempTableSql, sDbPath);
                    ExecuteNonQuery($"DROP TABLE {sTableName}", sDbPath);
                    ExecuteNonQuery($"ALTER TABLE {sTempTableName} RENAME TO {sTableName}", sDbPath);

                    // Clear column mapping cache for all types that map to this table
                    foreach (var kvp in _TableNameCache)
                    {
                        if (kvp.Value.Equals(sTableName, StringComparison.OrdinalIgnoreCase))
                        {
                            _ColumnMappingCache.TryRemove(kvp.Key, out _);
                        }
                    }

                    return true;
                }
                finally
                {
                    if (connection != null)
                        ReturnConnection(connection);
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
                catch (SQLiteException ex) when (ex.Message.Contains("constraint") || ex.Message.Contains("UNIQUE"))
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
                SQLiteConnection connection = null;
                try
                {
                    connection = GetConnection(sDbPath);
                    connection.Open();
                    var aIndexNames = new List<string>();
                    using (var oCommand = new SQLiteCommand(
                        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=@tableName", connection))
                    {
                        oCommand.Parameters.AddWithValue("@tableName", sTableName);
                        using (var oReader = oCommand.ExecuteReader())
                        {
                            while (oReader.Read())
                            {
                                aIndexNames.Add(oReader["name"].ToString());
                            }
                        }
                    }
                    for (int i = 0; i < aIndexNames.Count; i++)
                    {
                        string sSql = $"DROP INDEX {aIndexNames[i]}";
                        ExecuteNonQuery(sSql, sDbPath);
                    }
                    return true;
                }
                finally
                {
                    if (connection != null)
                        ReturnConnection(connection);
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
            var aIndexNames = new List<string>();
            SQLiteConnection connection = null;
            try
            {
                connection = GetConnection(sDbPath);
                connection.Open();
                using (var oCommand = new SQLiteCommand(
                    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=@tableName", connection))
                {
                    oCommand.Parameters.AddWithValue("@tableName", sTableName);
                    using (var oReader = oCommand.ExecuteReader())
                    {
                        while (oReader.Read())
                        {
                            aIndexNames.Add(oReader["name"].ToString());
                        }
                    }
                }
            }
            finally
            {
                if (connection != null)
                    ReturnConnection(connection);
            }
            return aIndexNames;
        }

        public static bool IndexExists(string sIndexName, string sDbPath = null)
        {
            SQLiteConnection connection = null;
            try
            {
                connection = GetConnection(sDbPath);
                connection.Open();
                using (var oCommand = new SQLiteCommand(
                    "SELECT 1 FROM sqlite_master WHERE type='index' AND name=@indexName", connection))
                {
                    oCommand.Parameters.AddWithValue("@indexName", sIndexName);
                    return oCommand.ExecuteScalar() != null;
                }
            }
            finally
            {
                if (connection != null)
                    ReturnConnection(connection);
            }
        }

        private static string GetPropertyName<T>(Expression<Func<T, object>> oPropertySelector)
        {
            var body = oPropertySelector.Body;
            MemberExpression memberExpression;
            if (body is MemberExpression)
            {
                memberExpression = (MemberExpression)body;
            }
            else if (body is UnaryExpression)
            {
                memberExpression = (MemberExpression)((UnaryExpression)body).Operand;
            }
            else
            {
                throw new ArgumentException("Invalid property expression");
            }
            return memberExpression.Member.Name;
        }
        #endregion

        private static Func<object, object> CreateValueConverter(Type targetType)
        {
            return _ValueConvertersCache.GetOrAdd(targetType, type =>
            {
                Type underlyingType = Nullable.GetUnderlyingType(type) ?? type;
                if (underlyingType == typeof(bool))
                    return value => value == null ? null : (object)(Convert.ToInt64(value) != 0);
                else if (underlyingType == typeof(byte[]))
                    return value => value;
                else if (underlyingType == typeof(Guid))
                    return value => value == null ? null : (object)Guid.Parse(value.ToString());
                else if (underlyingType == typeof(TimeSpan))
                    return value => value == null ? null : (object)TimeSpan.Parse(value.ToString());
                else if (underlyingType == typeof(decimal))
                    return value => value == null ? null : (object)decimal.Parse(value.ToString(), System.Globalization.CultureInfo.InvariantCulture);
                else if (underlyingType == typeof(int))
                    return value => value == null ? null : (object)Convert.ToInt32(value);
                else if (underlyingType == typeof(long))
                    return value => value == null ? null : (object)Convert.ToInt64(value);
                else if (underlyingType == typeof(short))
                    return value => value == null ? null : (object)Convert.ToInt16(value);
                else if (underlyingType == typeof(byte))
                    return value => value == null ? null : (object)Convert.ToByte(value);
                else if (underlyingType == typeof(float))
                    return value => value == null ? null : (object)Convert.ToSingle(value);
                else if (underlyingType == typeof(double))
                    return value => value == null ? null : (object)Convert.ToDouble(value);
                else if (underlyingType == typeof(string))
                    return value => value == null ? null : (object)value.ToString();
                else
                    return value => value == null ? null : Convert.ChangeType(value, underlyingType);
            });
        }
    }
}