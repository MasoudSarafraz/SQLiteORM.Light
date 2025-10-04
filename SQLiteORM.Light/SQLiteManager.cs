using SQLiteORM.Attributes;
using System;
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
    #region Helper Classes
    internal class CachedInsertInfo
    {
        public string Sql { get; set; }
        public PropertyInfo[] Properties { get; set; }
        public string[] ParameterNames { get; set; }
    }

    internal class CachedUpdateInfo
    {
        public string Sql { get; set; }
        public PropertyInfo[] Properties { get; set; }
        public string[] ParameterNames { get; set; }
    }

    internal class ColumnMapping
    {
        public PropertyInfo Property { get; set; }
        public Func<object, object> ValueConverter { get; set; }
        public int ColumnIndex { get; set; }
    }

    internal class IndexInfo
    {
        public string Name { get; set; }
        public bool IsUnique { get; set; }
        public List<string> Properties { get; set; }
        public bool AutoCreate { get; set; }
    }
    #endregion

    #region Core Internal Classes

    internal static class SQLiteConfiguration
    {
        internal static string DefaultDbPath;
        private const string DefaultDbFileName = "Database.sqlite";
        private const string ConfigFileName = "AppConfig.xml";
        private static readonly object ConfigLock = new object();
        private static bool TablesChecked = false;
        private static readonly object TableCheckLock = new object();

        internal static void Initialize()
        {
            lock (ConfigLock)
            {
                try
                {
                    string sConfigPath = GetConfigPath();
                    if (!File.Exists(sConfigPath)) CreateDefaultConfig(sConfigPath);
                    XDocument oDoc = XDocument.Load(sConfigPath);
                    string sDatabaseName = oDoc.Root?.Element("DatabaseName")?.Value ?? DefaultDbFileName;
                    string sDatabasePath = oDoc.Root?.Element("DatabasePath")?.Value;
                    if (string.IsNullOrWhiteSpace(sDatabaseName)) throw new InvalidOperationException("Database name not specified in config file");
                    if (string.IsNullOrWhiteSpace(sDatabasePath) || sDatabasePath == "." || sDatabasePath.Equals("default", StringComparison.OrdinalIgnoreCase)) sDatabasePath = AppDomain.CurrentDomain.BaseDirectory;
                    DefaultDbPath = Path.Combine(sDatabasePath, sDatabaseName);
                    EnsureDatabaseFileExists();
                    CheckAllTableStructures();
                }
                catch (Exception oEx) { throw new InvalidOperationException("Failed to load database configuration: " + oEx.Message, oEx); }
            }
        }

        internal static void SetDefaultDatabasePath(string sDbFileName, string sDbPath = null)
        {
            if (string.IsNullOrWhiteSpace(sDbFileName)) throw new ArgumentNullException(nameof(sDbFileName));
            lock (ConfigLock)
            {
                string sBasePath = sDbPath ?? AppDomain.CurrentDomain.BaseDirectory;
                DefaultDbPath = Path.Combine(sBasePath, sDbFileName);
                EnsureDatabaseFileExists();
                CheckAllTableStructures();
            }
        }

        private static void CheckAllTableStructures()
        {
            if (TablesChecked) return;
            lock (TableCheckLock)
            {
                if (TablesChecked) return;
                var lModelTypes = new List<Type>();
                var aAssemblies = AppDomain.CurrentDomain.GetAssemblies();
                for (int i = 0; i < aAssemblies.Length; i++)
                {
                    try
                    {
                        var aTypes = aAssemblies[i].GetTypes();
                        for (int j = 0; j < aTypes.Length; j++) if (aTypes[j] != null && aTypes[j].GetCustomAttributes(typeof(TableAttribute), false).Length > 0) lModelTypes.Add(aTypes[j]);
                    }
                    catch (ReflectionTypeLoadException oEx)
                    {
                        var aTypes = oEx.Types;
                        for (int j = 0; j < aTypes.Length; j++) if (aTypes[j] != null && aTypes[j].GetCustomAttributes(typeof(TableAttribute), false).Length > 0) lModelTypes.Add(aTypes[j]);
                    }
                }
                for (int i = 0; i < lModelTypes.Count; i++)
                {
                    SQLiteSchemaManager.CheckTableStructure(lModelTypes[i]);
                }
                TablesChecked = true;
            }
        }

        private static string GetConfigPath()
        {
            string oDllDirectory = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) ?? throw new InvalidOperationException("Unable to determine assembly location");
            string sConfigPath = Path.Combine(oDllDirectory, ConfigFileName);
            return File.Exists(sConfigPath) ? sConfigPath : Path.Combine(AppDomain.CurrentDomain.BaseDirectory, ConfigFileName);
        }

        private static void CreateDefaultConfig(string sConfigPath) => new XDocument(new XElement("DatabaseConfig", new XElement("DatabaseName", DefaultDbFileName), new XElement("DatabasePath", "."))).Save(sConfigPath);

        private static void EnsureDatabaseFileExists()
        {
            string sDirectory = Path.GetDirectoryName(DefaultDbPath);
            if (!string.IsNullOrEmpty(sDirectory) && !Directory.Exists(sDirectory)) Directory.CreateDirectory(sDirectory);
            if (!File.Exists(DefaultDbPath)) System.Data.SQLite.SQLiteConnection.CreateFile(DefaultDbPath);
        }
    }

    internal static class SQLiteConnectionManager
    {
        private static readonly List<SQLiteConnection> ConnectionPool = new List<SQLiteConnection>();
        private static readonly object ConnectionPoolLock = new object();
        private const int MaxPoolSize = 100;
        private static int ConnectionCount = 0;

        internal static SQLiteConnection GetConnection(string sDbPath = null)
        {
            string sPath = sDbPath ?? SQLiteConfiguration.DefaultDbPath;
            lock (ConnectionPoolLock)
            {
                if (ConnectionPool.Count > 0)
                {
                    var oConnection = ConnectionPool[0];
                    ConnectionPool.RemoveAt(0);
                    oConnection.ConnectionString = $"Data Source={sPath};Version=3;Journal Mode=WAL;";
                    return oConnection;
                }
            }
            if (Interlocked.Increment(ref ConnectionCount) <= MaxPoolSize) return new SQLiteConnection($"Data Source={sPath};Version=3;Journal Mode=WAL;");
            Interlocked.Decrement(ref ConnectionCount);
            return new SQLiteConnection($"Data Source={sPath};Version=3;Journal Mode=WAL;");
        }

        internal static void ReturnConnection(SQLiteConnection oConnection)
        {
            if (oConnection != null)
            {
                try
                {
                    if (oConnection.State != System.Data.ConnectionState.Closed) oConnection.Close();
                    lock (ConnectionPoolLock)
                    {
                        ConnectionPool.Add(oConnection);
                    }
                }
                catch { /* Ignore errors */ }
            }
        }
    }

    internal static class SQLiteMetadataCache
    {
        private static readonly Dictionary<Type, Lazy<bool>> TableCheckCache = new Dictionary<Type, Lazy<bool>>();
        private static readonly object TableCheckCacheLock = new object();
        private static readonly Dictionary<Type, Lazy<PropertyInfo[]>> PropertyCache = new Dictionary<Type, Lazy<PropertyInfo[]>>();
        private static readonly object PropertyCacheLock = new object();
        private static readonly Dictionary<Type, Lazy<string>> TableNameCache = new Dictionary<Type, Lazy<string>>();
        private static readonly object TableNameCacheLock = new object();
        private static readonly Dictionary<Type, Lazy<PropertyInfo>> PrimaryKeyCache = new Dictionary<Type, Lazy<PropertyInfo>>();
        private static readonly object PrimaryKeyCacheLock = new object();
        private static readonly Dictionary<Type, Lazy<List<IndexInfo>>> IndexCache = new Dictionary<Type, Lazy<List<IndexInfo>>>();
        private static readonly object IndexCacheLock = new object();
        private static readonly Dictionary<Type, Lazy<CachedInsertInfo>> InsertInfoCache = new Dictionary<Type, Lazy<CachedInsertInfo>>();
        private static readonly object InsertInfoCacheLock = new object();
        private static readonly Dictionary<Type, Lazy<CachedUpdateInfo>> UpdateInfoCache = new Dictionary<Type, Lazy<CachedUpdateInfo>>();
        private static readonly object UpdateInfoCacheLock = new object();
        private static readonly Dictionary<Type, Lazy<Dictionary<string, ColumnMapping>>> ColumnMappingCache = new Dictionary<Type, Lazy<Dictionary<string, ColumnMapping>>>();
        private static readonly object ColumnMappingCacheLock = new object();
        private static readonly Dictionary<Type, Lazy<Func<SQLiteDataReader, object>>> EntityCreators = new Dictionary<Type, Lazy<Func<SQLiteDataReader, object>>>();
        private static readonly object EntityCreatorsLock = new object();
        private static readonly Dictionary<Type, Lazy<Func<object, object>>> ValueConvertersCache = new Dictionary<Type, Lazy<Func<object, object>>>();
        private static readonly object ValueConvertersCacheLock = new object();
        private static readonly Dictionary<string, object> TableLocks = new Dictionary<string, object>();
        private static readonly object TableLocksLock = new object();

        internal static bool IsTableChecked(Type oModelType)
        {
            lock (TableCheckCacheLock) { return TableCheckCache.TryGetValue(oModelType, out var oLazy) && oLazy.Value; }
        }

        internal static void MarkTableAsChecked(Type oModelType)
        {
            lock (TableCheckCacheLock)
            {
                if (!TableCheckCache.ContainsKey(oModelType))
                {
                    TableCheckCache[oModelType] = new Lazy<bool>(() => true, LazyThreadSafetyMode.ExecutionAndPublication);
                }
            }
        }

        internal static void InvalidateColumnMappingCache(Type oModelType)
        {
            lock (ColumnMappingCacheLock) { ColumnMappingCache.Remove(oModelType); }
        }

        internal static PropertyInfo[] GetCachedProperties(Type oType)
        {
            lock (PropertyCacheLock)
            {
                if (!PropertyCache.TryGetValue(oType, out var oLazy))
                {
                    oLazy = new Lazy<PropertyInfo[]>(() => oType.GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.GetCustomAttributes(typeof(IgnoreAttribute), false).Length == 0).ToArray(), LazyThreadSafetyMode.ExecutionAndPublication);
                    PropertyCache[oType] = oLazy;
                }
                return oLazy.Value;
            }
        }

        internal static string GetCachedTableName(Type oType)
        {
            lock (TableNameCacheLock)
            {
                if (!TableNameCache.TryGetValue(oType, out var oLazy))
                {
                    oLazy = new Lazy<string>(() =>
                    {
                        var aTableAttrs = (TableAttribute[])oType.GetCustomAttributes(typeof(TableAttribute), false);
                        return aTableAttrs.Length == 0 ? oType.Name : aTableAttrs[0].Name;
                    }, LazyThreadSafetyMode.ExecutionAndPublication);
                    TableNameCache[oType] = oLazy;
                }
                return oLazy.Value;
            }
        }

        internal static PropertyInfo GetPrimaryKeyProperty(Type oType)
        {
            lock (PrimaryKeyCacheLock)
            {
                if (!PrimaryKeyCache.TryGetValue(oType, out var oLazy))
                {
                    oLazy = new Lazy<PropertyInfo>(() =>
                    {
                        var aProperties = GetCachedProperties(oType);
                        for (int i = 0; i < aProperties.Length; i++)
                        {
                            if (aProperties[i].GetCustomAttributes(typeof(IdentityKeyAttribute), false).Length > 0) return aProperties[i];
                        }
                        for (int i = 0; i < aProperties.Length; i++)
                        {
                            if (string.Equals(aProperties[i].Name, "Id", StringComparison.OrdinalIgnoreCase)) return aProperties[i];
                        }
                        throw new InvalidOperationException($"No primary key defined for type {oType.Name}. Use [IdentityKey] attribute or name a property 'Id'.");
                    }, LazyThreadSafetyMode.ExecutionAndPublication);
                    PrimaryKeyCache[oType] = oLazy;
                }
                return oLazy.Value;
            }
        }

        internal static List<IndexInfo> GetCachedIndexes(Type oType)
        {
            lock (IndexCacheLock)
            {
                if (!IndexCache.TryGetValue(oType, out var oLazy))
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
                                if (oIndexAttr.AutoCreate) lIndexInfos.Add(new IndexInfo { Name = string.IsNullOrEmpty(oIndexAttr.Name) ? $"IX_{GetCachedTableName(oType)}_{oProp.Name}" : oIndexAttr.Name, IsUnique = oIndexAttr.IsUnique, Properties = new List<string> { oProp.Name }, AutoCreate = oIndexAttr.AutoCreate });
                            }
                        }
                        return lIndexInfos;
                    }, LazyThreadSafetyMode.ExecutionAndPublication);
                    IndexCache[oType] = oLazy;
                }
                return oLazy.Value;
            }
        }

        internal static CachedInsertInfo GetCachedInsertInfo<T>()
        {
            lock (InsertInfoCacheLock)
            {
                if (!InsertInfoCache.TryGetValue(typeof(T), out var oLazy))
                {
                    oLazy = new Lazy<CachedInsertInfo>(() =>
                    {
                        var oPrimaryKey = GetPrimaryKeyProperty(typeof(T));
                        var aProperties = GetCachedProperties(typeof(T)).Where(p => p != oPrimaryKey || !IsAutoIncrementType(p.PropertyType)).ToArray();
                        var oColumns = new StringBuilder(aProperties.Length * 20);
                        var oParameters = new StringBuilder(aProperties.Length * 10);
                        var aParameterNames = new string[aProperties.Length];
                        for (int i = 0; i < aProperties.Length; i++)
                        {
                            if (i > 0) { oColumns.Append(", "); oParameters.Append(", "); }
                            oColumns.Append(aProperties[i].Name);
                            aParameterNames[i] = "@" + aProperties[i].Name;
                            oParameters.Append(aParameterNames[i]);
                        }
                        return new CachedInsertInfo { Sql = $"INSERT INTO {GetCachedTableName(typeof(T))} ({oColumns}) VALUES ({oParameters}); SELECT last_insert_rowid();", Properties = aProperties, ParameterNames = aParameterNames };
                    }, LazyThreadSafetyMode.ExecutionAndPublication);
                    InsertInfoCache[typeof(T)] = oLazy;
                }
                return oLazy.Value;
            }
        }

        internal static CachedUpdateInfo GetCachedUpdateInfo<T>()
        {
            lock (UpdateInfoCacheLock)
            {
                if (!UpdateInfoCache.TryGetValue(typeof(T), out var oLazy))
                {
                    oLazy = new Lazy<CachedUpdateInfo>(() =>
                    {
                        var oPrimaryKey = GetPrimaryKeyProperty(typeof(T));
                        var aProperties = GetCachedProperties(typeof(T)).Where(p => p != oPrimaryKey).ToArray();
                        var oSetClause = new StringBuilder(aProperties.Length * 25);
                        var aParameterNames = new string[aProperties.Length];
                        for (int i = 0; i < aProperties.Length; i++)
                        {
                            if (i > 0) oSetClause.Append(", ");
                            oSetClause.Append(aProperties[i].Name).Append(" = @").Append(aProperties[i].Name);
                            aParameterNames[i] = "@" + aProperties[i].Name;
                        }
                        return new CachedUpdateInfo { Sql = $"UPDATE {GetCachedTableName(typeof(T))} SET {oSetClause} WHERE {oPrimaryKey.Name} = @PrimaryKey", Properties = aProperties, ParameterNames = aParameterNames };
                    }, LazyThreadSafetyMode.ExecutionAndPublication);
                    UpdateInfoCache[typeof(T)] = oLazy;
                }
                return oLazy.Value;
            }
        }

        internal static Dictionary<string, ColumnMapping> GetColumnMappings(Type oType)
        {
            lock (ColumnMappingCacheLock)
            {
                if (!ColumnMappingCache.TryGetValue(oType, out var oLazy))
                {
                    oLazy = new Lazy<Dictionary<string, ColumnMapping>>(() =>
                    {
                        var aProperties = GetCachedProperties(oType);
                        string sTableName = GetCachedTableName(oType);
                        var oMappings = new Dictionary<string, ColumnMapping>(StringComparer.OrdinalIgnoreCase);
                        using (var oConnection = SQLiteConnectionManager.GetConnection())
                        {
                            oConnection.Open();
                            using (var oCommand = new SQLiteCommand($"PRAGMA table_info({sTableName})", oConnection))
                            using (var oReader = oCommand.ExecuteReader())
                            {
                                var lColumnNames = new List<string>();
                                while (oReader.Read()) lColumnNames.Add(oReader["name"].ToString());
                                for (int i = 0; i < aProperties.Length; i++)
                                {
                                    var oProp = aProperties[i];
                                    int iIndex = lColumnNames.IndexOf(oProp.Name);
                                    if (iIndex != -1) oMappings[oProp.Name] = new ColumnMapping { Property = oProp, ValueConverter = CreateValueConverter(oProp.PropertyType), ColumnIndex = iIndex };
                                }
                            }
                        }
                        return oMappings;
                    }, LazyThreadSafetyMode.ExecutionAndPublication);
                    ColumnMappingCache[oType] = oLazy;
                }
                return oLazy.Value;
            }
        }

        internal static Func<SQLiteDataReader, object> GetEntityCreator<T>()
        {
            lock (EntityCreatorsLock)
            {
                if (!EntityCreators.TryGetValue(typeof(T), out var oLazy))
                {
                    oLazy = new Lazy<Func<SQLiteDataReader, object>>(() =>
                    {
                        var oReaderParam = Expression.Parameter(typeof(SQLiteDataReader), "reader");
                        var oEntityVar = Expression.Variable(typeof(T), "entity");
                        var oAssignEntity = Expression.Assign(oEntityVar, Expression.New(typeof(T)));
                        var lBlockExpressions = new List<Expression> { oAssignEntity };
                        foreach (var oMapping in GetColumnMappings(typeof(T)).Values)
                        {
                            var oProp = oMapping.Property;
                            var oIndexExpr = Expression.Constant(oMapping.ColumnIndex);
                            var oIsDBNullExpr = Expression.Call(oReaderParam, typeof(SQLiteDataReader).GetMethod("IsDBNull", new[] { typeof(int) }), oIndexExpr);
                            var oValueExpr = Expression.Call(oReaderParam, typeof(SQLiteDataReader).GetMethod("GetValue", new[] { typeof(int) }), oIndexExpr);
                            var oConvertedValueExpr = Expression.Call(Expression.Constant(oMapping.ValueConverter), typeof(Func<object, object>).GetMethod("Invoke"), oValueExpr);
                            var oTypedValueExpr = Expression.Convert(oConvertedValueExpr, oProp.PropertyType);
                            var oConditionExpr = Expression.Condition(oIsDBNullExpr, Expression.Default(oProp.PropertyType), oTypedValueExpr);
                            lBlockExpressions.Add(Expression.Assign(Expression.Property(oEntityVar, oProp), oConditionExpr));
                        }
                        lBlockExpressions.Add(oEntityVar);
                        return Expression.Lambda<Func<SQLiteDataReader, object>>(Expression.Block(new[] { oEntityVar }, lBlockExpressions), oReaderParam).Compile();
                    }, LazyThreadSafetyMode.ExecutionAndPublication);
                    EntityCreators[typeof(T)] = oLazy;
                }
                return oLazy.Value;
            }
        }

        internal static Func<object, object> CreateValueConverter(Type oTargetType)
        {
            lock (ValueConvertersCacheLock)
            {
                if (!ValueConvertersCache.TryGetValue(oTargetType, out var oLazy))
                {
                    oLazy = new Lazy<Func<object, object>>(() =>
                    {
                        Type oUnderlyingType = Nullable.GetUnderlyingType(oTargetType) ?? oTargetType;
                        if (oUnderlyingType == typeof(bool)) return oValue => oValue == null ? null : (object)(Convert.ToInt64(oValue) != 0);
                        if (oUnderlyingType == typeof(byte[])) return oValue => oValue;
                        if (oUnderlyingType == typeof(Guid)) return oValue => oValue == null ? null : (object)Guid.Parse(oValue.ToString());
                        if (oUnderlyingType == typeof(TimeSpan)) return oValue => oValue == null ? null : (object)TimeSpan.Parse(oValue.ToString());
                        if (oUnderlyingType == typeof(decimal)) return oValue => oValue == null ? null : (object)decimal.Parse(oValue.ToString(), System.Globalization.CultureInfo.InvariantCulture);
                        if (oUnderlyingType == typeof(int)) return oValue => oValue == null ? null : (object)Convert.ToInt32(oValue);
                        if (oUnderlyingType == typeof(long)) return oValue => oValue == null ? null : (object)Convert.ToInt64(oValue);
                        if (oUnderlyingType == typeof(string)) return oValue => oValue == null ? null : (object)oValue.ToString();
                        return oValue => oValue == null ? null : Convert.ChangeType(oValue, oUnderlyingType);
                    }, LazyThreadSafetyMode.ExecutionAndPublication);
                    ValueConvertersCache[oTargetType] = oLazy;
                }
                return oLazy.Value;
            }
        }

        internal static bool IsAutoIncrementType(Type oType) => (Nullable.GetUnderlyingType(oType) ?? oType) == typeof(int) || (Nullable.GetUnderlyingType(oType) ?? oType) == typeof(long);

        internal static bool IsNullableType(Type oType) => oType.IsClass || Nullable.GetUnderlyingType(oType) != null;

        internal static string GetSQLiteType(Type oType)
        {
            oType = Nullable.GetUnderlyingType(oType) ?? oType;
            if (oType == typeof(int) || oType == typeof(long) || oType == typeof(short) || oType == typeof(byte) || oType == typeof(bool)) return "INTEGER";
            if (oType == typeof(float) || oType == typeof(double)) return "REAL";
            if (oType == typeof(decimal) || oType == typeof(DateTime) || oType == typeof(Guid) || oType == typeof(TimeSpan) || oType == typeof(string)) return "TEXT";
            if (oType == typeof(byte[])) return "BLOB";
            return "TEXT";
        }

        internal static object GetTableLock(string sTableName)
        {
            lock (TableLocksLock)
            {
                if (!TableLocks.TryGetValue(sTableName, out var oLock))
                {
                    oLock = new object();
                    TableLocks[sTableName] = oLock;
                }
                return oLock;
            }
        }
    }

    internal static class SQLiteSchemaManager
    {
        internal static void CheckTableStructure(Type oModelType)
        {
            string sTableName = SQLiteMetadataCache.GetCachedTableName(oModelType);
            object oTableLock = SQLiteMetadataCache.GetTableLock(sTableName);
            lock (oTableLock)
            {
                if (SQLiteMetadataCache.IsTableChecked(oModelType)) return;
                PropertyInfo[] aProperties = SQLiteMetadataCache.GetCachedProperties(oModelType);
                bool bTableExists = TableExists(sTableName);
                if (!bTableExists)
                {
                    CreateTable(oModelType, sTableName, aProperties);
                    SQLiteMetadataCache.InvalidateColumnMappingCache(oModelType);
                }
                else
                {
                    var lMissingProperties = GetMissingProperties(sTableName, aProperties);
                    if (lMissingProperties.Count > 0)
                    {
                        AddMissingColumns(sTableName, lMissingProperties);
                        SQLiteMetadataCache.InvalidateColumnMappingCache(oModelType);
                    }
                }
                CheckAndCreateIndexes(oModelType, sTableName);
                SQLiteMetadataCache.MarkTableAsChecked(oModelType);
            }
        }

        private static bool TableExists(string sTableName)
        {
            using (var oConnection = SQLiteConnectionManager.GetConnection())
            {
                oConnection.Open();
                using (var oCommand = new SQLiteCommand("SELECT 1 FROM sqlite_master WHERE type='table' AND name=@tableName", oConnection))
                {
                    oCommand.Parameters.AddWithValue("@tableName", sTableName);
                    return oCommand.ExecuteScalar() != null;
                }
            }
        }

        private static List<PropertyInfo> GetMissingProperties(string sTableName, PropertyInfo[] aProperties)
        {
            var lExistingColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            using (var oConnection = SQLiteConnectionManager.GetConnection())
            {
                oConnection.Open();
                using (var oCommand = new SQLiteCommand($"PRAGMA table_info('{sTableName}')", oConnection))
                using (var oReader = oCommand.ExecuteReader()) while (oReader.Read()) lExistingColumns.Add(oReader["name"].ToString());
            }
            var lMissingProperties = new List<PropertyInfo>();
            for (int i = 0; i < aProperties.Length; i++) if (!lExistingColumns.Contains(aProperties[i].Name)) lMissingProperties.Add(aProperties[i]);
            return lMissingProperties;
        }

        private static void CreateTable(Type oModelType, string sTableName, PropertyInfo[] aProperties)
        {
            var oBuilder = new StringBuilder(aProperties.Length * 30);
            oBuilder.Append($"CREATE TABLE {sTableName} (");
            PropertyInfo oPrimaryKey = SQLiteMetadataCache.GetPrimaryKeyProperty(oModelType);
            for (int i = 0; i < aProperties.Length; i++)
            {
                var oProp = aProperties[i];
                if (i > 0) oBuilder.Append(", ");
                string sColumnType = SQLiteMetadataCache.GetSQLiteType(oProp.PropertyType);
                oBuilder.Append($"{oProp.Name} {sColumnType}");
                if (oProp == oPrimaryKey) oBuilder.Append(SQLiteMetadataCache.IsAutoIncrementType(oProp.PropertyType) ? " PRIMARY KEY AUTOINCREMENT" : " PRIMARY KEY");
                else if (!SQLiteMetadataCache.IsNullableType(oProp.PropertyType)) oBuilder.Append(" NOT NULL");
            }
            oBuilder.Append(")");
            ExecuteNonQuery(oBuilder.ToString());
        }

        private static void AddMissingColumns(string sTableName, List<PropertyInfo> lMissingProperties)
        {
            using (var oConnection = SQLiteConnectionManager.GetConnection())
            {
                oConnection.Open();
                using (var oTransaction = oConnection.BeginTransaction())
                {
                    try
                    {
                        for (int i = 0; i < lMissingProperties.Count; i++)
                        {
                            var oProp = lMissingProperties[i];
                            string sColumnType = SQLiteMetadataCache.GetSQLiteType(oProp.PropertyType);
                            string sColumnDefinition = $"{oProp.Name} {sColumnType}";
                            if (!SQLiteMetadataCache.IsNullableType(oProp.PropertyType)) sColumnDefinition += " NOT NULL";
                            ExecuteNonQuery($"ALTER TABLE {sTableName} ADD COLUMN {sColumnDefinition}", null, null, oConnection, oTransaction);
                        }
                        oTransaction.Commit();
                    }
                    catch { oTransaction.Rollback(); throw; }
                }
            }
        }

        private static void CheckAndCreateIndexes(Type oModelType, string sTableName)
        {
            var lIndexInfos = SQLiteMetadataCache.GetCachedIndexes(oModelType);
            var lExistingIndexes = GetExistingIndexes(sTableName);
            for (int i = 0; i < lIndexInfos.Count; i++)
            {
                var oIndexInfo = lIndexInfos[i];
                if (oIndexInfo.AutoCreate && !lExistingIndexes.Contains(oIndexInfo.Name))
                {
                    string sUnique = oIndexInfo.IsUnique ? "UNIQUE" : "";
                    string sColumns = string.Join(", ", oIndexInfo.Properties);
                    ExecuteNonQuery($"CREATE {sUnique} INDEX {oIndexInfo.Name} ON {sTableName} ({sColumns})");
                }
            }
        }

        private static HashSet<string> GetExistingIndexes(string sTableName)
        {
            var lExistingIndexes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            using (var oConnection = SQLiteConnectionManager.GetConnection())
            {
                oConnection.Open();
                using (var oCommand = new SQLiteCommand("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=@tableName", oConnection))
                {
                    oCommand.Parameters.AddWithValue("@tableName", sTableName);
                    using (var oReader = oCommand.ExecuteReader()) while (oReader.Read()) lExistingIndexes.Add(oReader["name"].ToString());
                }
            }
            return lExistingIndexes;
        }

        internal static bool RemoveColumn(string sTableName, string sColumnName, string sDbPath = null)
        {
            try
            {
                var lColumns = GetTableColumns(sTableName).Where(n => !n.Equals(sColumnName, StringComparison.OrdinalIgnoreCase)).ToList();
                if (lColumns.Count == 0) throw new InvalidOperationException("No columns to keep");
                string sTempTableName = $"{sTableName}_temp";
                string sCreateTempTableSql = $"CREATE TABLE {sTempTableName} AS SELECT {string.Join(", ", lColumns)} FROM {sTableName}";
                ExecuteNonQuery(sCreateTempTableSql, sDbPath);
                ExecuteNonQuery($"DROP TABLE {sTableName}", sDbPath);
                ExecuteNonQuery($"ALTER TABLE {sTempTableName} RENAME TO {sTableName}", sDbPath);
                return true;
            }
            catch (Exception oEx) { throw new InvalidOperationException($"Error removing column {sColumnName} from table {sTableName}: {oEx.Message}", oEx); }
        }

        internal static bool RemoveIndex(string sIndexName, string sDbPath = null) { ExecuteNonQuery($"DROP INDEX IF EXISTS {sIndexName}", sDbPath); return true; }

        internal static bool RemoveIndexesForTable(string sTableName, string sDbPath = null)
        {
            foreach (string sIndexName in GetIndexesForTable(sTableName, sDbPath)) ExecuteNonQuery($"DROP INDEX {sIndexName}", sDbPath);
            return true;
        }

        internal static bool RemoveIndexesForProperty<T>(Expression<Func<T, object>> oPropertySelector, string sDbPath = null)
        {
            string sPropertyName = GetPropertyName(oPropertySelector);
            string sTableName = SQLiteMetadataCache.GetCachedTableName(typeof(T));
            return RemoveIndex($"IX_{sTableName}_{sPropertyName}", sDbPath);
        }

        internal static List<string> GetIndexesForTable(string sTableName, string sDbPath = null)
        {
            var lIndexNames = new List<string>();
            using (var oConnection = SQLiteConnectionManager.GetConnection(sDbPath))
            {
                oConnection.Open();
                using (var oCommand = new SQLiteCommand("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=@tableName", oConnection))
                {
                    oCommand.Parameters.AddWithValue("@tableName", sTableName);
                    using (var oReader = oCommand.ExecuteReader()) while (oReader.Read()) lIndexNames.Add(oReader["name"].ToString());
                }
            }
            return lIndexNames;
        }

        internal static bool IndexExists(string sIndexName, string sDbPath = null)
        {
            using (var oConnection = SQLiteConnectionManager.GetConnection(sDbPath))
            {
                oConnection.Open();
                using (var oCommand = new SQLiteCommand("SELECT 1 FROM sqlite_master WHERE type='index' AND name=@indexName", oConnection))
                {
                    oCommand.Parameters.AddWithValue("@indexName", sIndexName);
                    return oCommand.ExecuteScalar() != null;
                }
            }
        }

        private static List<string> GetTableColumns(string sTableName)
        {
            var lColumns = new List<string>();
            using (var oConnection = SQLiteConnectionManager.GetConnection())
            {
                oConnection.Open();
                using (var oCommand = new SQLiteCommand($"PRAGMA table_info({sTableName})", oConnection))
                using (var oReader = oCommand.ExecuteReader()) while (oReader.Read()) lColumns.Add(oReader["name"].ToString());
            }
            return lColumns;
        }

        private static string GetPropertyName<T>(Expression<Func<T, object>> oPropertySelector)
        {
            MemberExpression oMemberExpression = oPropertySelector.Body as MemberExpression ?? ((UnaryExpression)oPropertySelector.Body).Operand as MemberExpression;
            return oMemberExpression?.Member.Name ?? throw new ArgumentException("Invalid property expression");
        }

        private static void ExecuteNonQuery(string sSql, string sDbPath = null, Dictionary<string, object> oParameters = null, SQLiteConnection oConnection = null, SQLiteTransaction oTransaction = null)
        {
            bool bOwnConnection = false;
            if (oConnection == null)
            {
                oConnection = SQLiteConnectionManager.GetConnection(sDbPath);
                oConnection.Open();
                bOwnConnection = true;
            }
            try
            {
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, oConnection, oTransaction))
                {
                    if (oParameters != null) foreach (var oParam in oParameters) oCommand.Parameters.AddWithValue(oParam.Key, oParam.Value ?? DBNull.Value);
                    oCommand.ExecuteNonQuery();
                }
            }
            finally { if (bOwnConnection) SQLiteConnectionManager.ReturnConnection(oConnection); }
        }
    }

    internal static class SQLiteDataAccess
    {
        internal static int Insert<T>(T oEntity, string sDbPath = null) where T : class, new()
        {
            if (oEntity == null) throw new ArgumentNullException(nameof(oEntity));
            var oInsertInfo = SQLiteMetadataCache.GetCachedInsertInfo<T>();
            var oPrimaryKey = SQLiteMetadataCache.GetPrimaryKeyProperty(typeof(T));
            using (var oConnection = SQLiteConnectionManager.GetConnection(sDbPath))
            {
                oConnection.Open();
                using (var oCommand = new SQLiteCommand(oInsertInfo.Sql, oConnection))
                {
                    var aProperties = oInsertInfo.Properties;
                    var aParameterNames = oInsertInfo.ParameterNames;
                    for (int i = 0; i < aProperties.Length; i++)
                    {
                        var oProp = aProperties[i];
                        object oValue = oProp.GetValue(oEntity, null) ?? DBNull.Value;
                        if (oProp.PropertyType == typeof(decimal) && oValue is decimal dVal) oValue = dVal.ToString(System.Globalization.CultureInfo.InvariantCulture);
                        oCommand.Parameters.AddWithValue(aParameterNames[i], oValue);
                    }
                    object oResult = oCommand.ExecuteScalar();
                    int iGeneratedId = Convert.ToInt32(oResult);
                    if (oPrimaryKey != null && oPrimaryKey.CanWrite && SQLiteMetadataCache.IsAutoIncrementType(oPrimaryKey.PropertyType)) oPrimaryKey.SetValue(oEntity, iGeneratedId, null);
                    return iGeneratedId;
                }
            }
        }

        internal static int InsertList<T>(IEnumerable<T> lEntities, string sDbPath = null) where T : class, new()
        {
            if (lEntities == null) throw new ArgumentNullException(nameof(lEntities));
            var oEntityList = lEntities as IList<T> ?? lEntities.ToList();
            if (oEntityList.Count == 0) return 0;
            var oInsertInfo = SQLiteMetadataCache.GetCachedInsertInfo<T>();
            var oPrimaryKey = SQLiteMetadataCache.GetPrimaryKeyProperty(typeof(T));
            int iCount = 0;
            using (var oConnection = SQLiteConnectionManager.GetConnection(sDbPath))
            {
                oConnection.Open();
                using (var oTransaction = oConnection.BeginTransaction())
                {
                    try
                    {
                        using (var oCommand = new SQLiteCommand(oInsertInfo.Sql, oConnection, oTransaction))
                        {
                            oCommand.Prepare();
                            var aProperties = oInsertInfo.Properties;
                            var aParameterNames = oInsertInfo.ParameterNames;
                            for (int j = 0; j < oEntityList.Count; j++)
                            {
                                var oEntity = oEntityList[j];
                                oCommand.Parameters.Clear();
                                for (int i = 0; i < aProperties.Length; i++)
                                {
                                    var oProp = aProperties[i];
                                    object oValue = oProp.GetValue(oEntity, null) ?? DBNull.Value;
                                    if (oProp.PropertyType == typeof(decimal) && oValue is decimal dVal) oValue = dVal.ToString(System.Globalization.CultureInfo.InvariantCulture);
                                    oCommand.Parameters.AddWithValue(aParameterNames[i], oValue);
                                }
                                object oResult = oCommand.ExecuteScalar();
                                int iGeneratedId = Convert.ToInt32(oResult);
                                if (oPrimaryKey != null && oPrimaryKey.CanWrite && SQLiteMetadataCache.IsAutoIncrementType(oPrimaryKey.PropertyType)) oPrimaryKey.SetValue(oEntity, iGeneratedId, null);
                                iCount++;
                            }
                        }
                        oTransaction.Commit();
                        return iCount;
                    }
                    catch { oTransaction.Rollback(); throw; }
                }
            }
        }

        internal static bool Update<T>(T oEntity, string sDbPath = null) where T : class, new()
        {
            if (oEntity == null) throw new ArgumentNullException(nameof(oEntity));
            var oUpdateInfo = SQLiteMetadataCache.GetCachedUpdateInfo<T>();
            var oPrimaryKey = SQLiteMetadataCache.GetPrimaryKeyProperty(typeof(T));
            using (var oConnection = SQLiteConnectionManager.GetConnection(sDbPath))
            {
                oConnection.Open();
                using (var oCommand = new SQLiteCommand(oUpdateInfo.Sql, oConnection))
                {
                    var aProperties = oUpdateInfo.Properties;
                    var aParameterNames = oUpdateInfo.ParameterNames;
                    for (int i = 0; i < aProperties.Length; i++)
                    {
                        var oProp = aProperties[i];
                        object oValue = oProp.GetValue(oEntity, null) ?? DBNull.Value;
                        if (oProp.PropertyType == typeof(decimal) && oValue is decimal dVal) oValue = dVal.ToString(System.Globalization.CultureInfo.InvariantCulture);
                        oCommand.Parameters.AddWithValue(aParameterNames[i], oValue);
                    }
                    object oPrimaryKeyValue = oPrimaryKey.GetValue(oEntity, null) ?? DBNull.Value;
                    oCommand.Parameters.AddWithValue("@PrimaryKey", oPrimaryKeyValue);
                    return oCommand.ExecuteNonQuery() > 0;
                }
            }
        }

        internal static int UpdateList<T>(IEnumerable<T> lEntities, string sDbPath = null) where T : class, new()
        {
            if (lEntities == null) throw new ArgumentNullException(nameof(lEntities));
            var oEntityList = lEntities as IList<T> ?? lEntities.ToList();
            if (oEntityList.Count == 0) return 0;
            var oUpdateInfo = SQLiteMetadataCache.GetCachedUpdateInfo<T>();
            var oPrimaryKey = SQLiteMetadataCache.GetPrimaryKeyProperty(typeof(T));
            int iCount = 0;
            using (var oConnection = SQLiteConnectionManager.GetConnection(sDbPath))
            {
                oConnection.Open();
                using (var oTransaction = oConnection.BeginTransaction())
                {
                    try
                    {
                        using (var oCommand = new SQLiteCommand(oUpdateInfo.Sql, oConnection, oTransaction))
                        {
                            oCommand.Prepare();
                            var aProperties = oUpdateInfo.Properties;
                            var aParameterNames = oUpdateInfo.ParameterNames;
                            for (int j = 0; j < oEntityList.Count; j++)
                            {
                                var oEntity = oEntityList[j];
                                oCommand.Parameters.Clear();
                                for (int i = 0; i < aProperties.Length; i++)
                                {
                                    var oProp = aProperties[i];
                                    object oValue = oProp.GetValue(oEntity, null) ?? DBNull.Value;
                                    if (oProp.PropertyType == typeof(decimal) && oValue is decimal dVal) oValue = dVal.ToString(System.Globalization.CultureInfo.InvariantCulture);
                                    oCommand.Parameters.AddWithValue(aParameterNames[i], oValue);
                                }
                                object oPrimaryKeyValue = oPrimaryKey.GetValue(oEntity, null) ?? DBNull.Value;
                                oCommand.Parameters.AddWithValue("@PrimaryKey", oPrimaryKeyValue);
                                iCount += oCommand.ExecuteNonQuery();
                            }
                        }
                        oTransaction.Commit();
                        return iCount;
                    }
                    catch { oTransaction.Rollback(); throw; }
                }
            }
        }

        internal static bool Delete<T>(int iId, string sDbPath = null) where T : class, new()
        {
            string sTableName = SQLiteMetadataCache.GetCachedTableName(typeof(T));
            var oPrimaryKey = SQLiteMetadataCache.GetPrimaryKeyProperty(typeof(T));
            return ExecuteNonQuery($"DELETE FROM {sTableName} WHERE {oPrimaryKey.Name} = @PrimaryKey", sDbPath, new Dictionary<string, object> { { "@PrimaryKey", iId } }) > 0;
        }

        internal static int DeleteList<T>(IEnumerable<int> aIds, string sDbPath = null) where T : class, new()
        {
            if (aIds == null) throw new ArgumentNullException(nameof(aIds));
            var oIdList = aIds as IList<int> ?? aIds.ToList();
            if (oIdList.Count == 0) return 0;
            string sTableName = SQLiteMetadataCache.GetCachedTableName(typeof(T));
            var oPrimaryKey = SQLiteMetadataCache.GetPrimaryKeyProperty(typeof(T));
            int iCount = 0;
            using (var oConnection = SQLiteConnectionManager.GetConnection(sDbPath))
            {
                oConnection.Open();
                using (var oTransaction = oConnection.BeginTransaction())
                {
                    try
                    {
                        using (var oCommand = new SQLiteCommand($"DELETE FROM {sTableName} WHERE {oPrimaryKey.Name} = @PrimaryKey", oConnection, oTransaction))
                        {
                            oCommand.Prepare();
                            for (int i = 0; i < oIdList.Count; i++)
                            {
                                oCommand.Parameters.Clear();
                                oCommand.Parameters.AddWithValue("@PrimaryKey", oIdList[i]);
                                iCount += oCommand.ExecuteNonQuery();
                            }
                        }
                        oTransaction.Commit();
                        return iCount;
                    }
                    catch { oTransaction.Rollback(); throw; }
                }
            }
        }

        internal static bool DeleteAll<T>(Expression<Func<T, bool>> oPredicate, string sDbPath = null) where T : class, new()
        {
            string sTableName = SQLiteMetadataCache.GetCachedTableName(typeof(T));
            var oSqlWithParams = new ExpressionToSql<T>().ConvertToSqlWithParameters(oPredicate);
            return ExecuteNonQuery($"DELETE FROM {sTableName} WHERE {oSqlWithParams.Sql}", sDbPath, oSqlWithParams.Parameters) > 0;
        }

        internal static T GetById<T>(int iId, string sDbPath = null) where T : class, new()
        {
            var oPrimaryKey = SQLiteMetadataCache.GetPrimaryKeyProperty(typeof(T));
            string sTableName = SQLiteMetadataCache.GetCachedTableName(typeof(T));
            string sSql = $"SELECT * FROM {sTableName} WHERE {oPrimaryKey.Name} = @Id";
            var oEntityCreator = SQLiteMetadataCache.GetEntityCreator<T>();
            using (var oConnection = SQLiteConnectionManager.GetConnection(sDbPath))
            {
                oConnection.Open();
                using (var oCommand = new SQLiteCommand(sSql, oConnection))
                {
                    oCommand.Parameters.AddWithValue("@Id", iId);
                    using (var oReader = oCommand.ExecuteReader()) if (oReader.Read()) return (T)oEntityCreator(oReader);
                }
            }
            return null;
        }

        internal static IEnumerable<T> GetAll<T>(string sDbPath = null) where T : class, new()
        {
            string sTableName = SQLiteMetadataCache.GetCachedTableName(typeof(T));
            string sSql = $"SELECT * FROM {sTableName}";
            var oEntityCreator = SQLiteMetadataCache.GetEntityCreator<T>();
            var lResults = new List<T>();
            using (var oConnection = SQLiteConnectionManager.GetConnection(sDbPath))
            {
                oConnection.Open();
                using (var oCommand = new SQLiteCommand(sSql, oConnection))
                {
                    using (var oReader = oCommand.ExecuteReader()) while (oReader.Read()) lResults.Add((T)oEntityCreator(oReader));
                }
            }
            return lResults;
        }

        internal static T GetSingle<T>(Expression<Func<T, bool>> oPredicate, string sDbPath = null) where T : class, new()
        {
            string sTableName = SQLiteMetadataCache.GetCachedTableName(typeof(T));
            var oSqlWithParams = new ExpressionToSql<T>().ConvertToSqlWithParameters(oPredicate);
            string sSql = $"SELECT * FROM {sTableName}";
            if (!string.IsNullOrEmpty(oSqlWithParams.Sql) && oSqlWithParams.Sql != "1=1") sSql += $" WHERE {oSqlWithParams.Sql}";
            sSql += " LIMIT 1";
            var oEntityCreator = SQLiteMetadataCache.GetEntityCreator<T>();
            using (var oConnection = SQLiteConnectionManager.GetConnection(sDbPath))
            {
                oConnection.Open();
                using (var oCommand = new SQLiteCommand(sSql, oConnection))
                {
                    foreach (var oParam in oSqlWithParams.Parameters) oCommand.Parameters.AddWithValue(oParam.Key, oParam.Value ?? DBNull.Value);
                    using (var oReader = oCommand.ExecuteReader()) if (oReader.Read()) return (T)oEntityCreator(oReader);
                }
            }
            return null;
        }

        internal static IEnumerable<T> Find<T>(Expression<Func<T, bool>> oPredicate, string sDbPath = null) where T : class, new()
        {
            string sTableName = SQLiteMetadataCache.GetCachedTableName(typeof(T));
            var oSqlWithParams = new ExpressionToSql<T>().ConvertToSqlWithParameters(oPredicate);
            string sSql = $"SELECT * FROM {sTableName}";
            if (!string.IsNullOrEmpty(oSqlWithParams.Sql) && oSqlWithParams.Sql != "1=1") sSql += $" WHERE {oSqlWithParams.Sql}";
            var oEntityCreator = SQLiteMetadataCache.GetEntityCreator<T>();
            var lResults = new List<T>();
            using (var oConnection = SQLiteConnectionManager.GetConnection(sDbPath))
            {
                oConnection.Open();
                using (var oCommand = new SQLiteCommand(sSql, oConnection))
                {
                    foreach (var oParam in oSqlWithParams.Parameters) oCommand.Parameters.AddWithValue(oParam.Key, oParam.Value ?? DBNull.Value);
                    using (var oReader = oCommand.ExecuteReader()) while (oReader.Read()) lResults.Add((T)oEntityCreator(oReader));
                }
            }
            return lResults;
        }

        internal static int Count<T>(Expression<Func<T, bool>> oPredicate, string sDbPath = null) where T : class, new()
        {
            string sTableName = SQLiteMetadataCache.GetCachedTableName(typeof(T));
            var oSqlWithParams = new ExpressionToSql<T>().ConvertToSqlWithParameters(oPredicate);
            string sSql = $"SELECT COUNT(*) FROM {sTableName}";
            if (!string.IsNullOrEmpty(oSqlWithParams.Sql) && oSqlWithParams.Sql != "1=1") sSql += $" WHERE {oSqlWithParams.Sql}";
            using (var oConnection = SQLiteConnectionManager.GetConnection(sDbPath))
            {
                oConnection.Open();
                using (var oCommand = new SQLiteCommand(sSql, oConnection))
                {
                    foreach (var oParam in oSqlWithParams.Parameters) oCommand.Parameters.AddWithValue(oParam.Key, oParam.Value ?? DBNull.Value);
                    return Convert.ToInt32(oCommand.ExecuteScalar());
                }
            }
        }

        internal static bool Upsert<T>(T oEntity, string sDbPath = null) where T : class, new()
        {
            if (oEntity == null) throw new ArgumentNullException(nameof(oEntity));
            var oPrimaryKey = SQLiteMetadataCache.GetPrimaryKeyProperty(typeof(T));
            object oPrimaryKeyValue = oPrimaryKey.GetValue(oEntity, null);
            if (oPrimaryKeyValue == null || (oPrimaryKeyValue is int i && i <= 0)) { Insert(oEntity, sDbPath); return true; }
            bool bUpdated = Update(oEntity, sDbPath);
            if (!bUpdated)
            {
                try { Insert(oEntity, sDbPath); return true; }
                catch (SQLiteException oEx) when (oEx.Message.Contains("constraint") || oEx.Message.Contains("UNIQUE")) { return Update(oEntity, sDbPath); }
            }
            return bUpdated;
        }

        private static int ExecuteNonQuery(string sSql, string sDbPath, Dictionary<string, object> oParameters)
        {
            using (var oConnection = SQLiteConnectionManager.GetConnection(sDbPath))
            {
                oConnection.Open();
                using (var oCommand = new SQLiteCommand(sSql, oConnection))
                {
                    if (oParameters != null) foreach (var oParam in oParameters) oCommand.Parameters.AddWithValue(oParam.Key, oParam.Value ?? DBNull.Value);
                    return oCommand.ExecuteNonQuery();
                }
            }
        }
    }
    #endregion

    public static class SQLiteManager
    {
        static SQLiteManager() => SQLiteConfiguration.Initialize();

        public static void SetDefaultDatabasePath(string sDbFileName, string sDbPath = null) => SQLiteConfiguration.SetDefaultDatabasePath(sDbFileName, sDbPath);

        public static int Insert<T>(T oEntity, string sDbPath = null) where T : class, new() => SQLiteDataAccess.Insert(oEntity, sDbPath);

        public static int InsertList<T>(IEnumerable<T> lEntities, string sDbPath = null) where T : class, new() => SQLiteDataAccess.InsertList(lEntities, sDbPath);

        public static bool Update<T>(T oEntity, string sDbPath = null) where T : class, new() => SQLiteDataAccess.Update(oEntity, sDbPath);

        public static int UpdateList<T>(IEnumerable<T> lEntities, string sDbPath = null) where T : class, new() => SQLiteDataAccess.UpdateList(lEntities, sDbPath);

        public static bool Delete<T>(int iId, string sDbPath = null) where T : class, new() => SQLiteDataAccess.Delete<T>(iId, sDbPath);

        public static int DeleteList<T>(IEnumerable<int> aIds, string sDbPath = null) where T : class, new() => SQLiteDataAccess.DeleteList<T>(aIds, sDbPath);

        public static bool DeleteAll<T>(Expression<Func<T, bool>> oPredicate, string sDbPath = null) where T : class, new() => SQLiteDataAccess.DeleteAll(oPredicate, sDbPath);

        public static T GetById<T>(int iId, string sDbPath = null) where T : class, new() => SQLiteDataAccess.GetById<T>(iId, sDbPath);

        public static IEnumerable<T> GetAll<T>(string sDbPath = null) where T : class, new() => SQLiteDataAccess.GetAll<T>(sDbPath);

        public static T GetSingle<T>(Expression<Func<T, bool>> oPredicate, string sDbPath = null) where T : class, new() => SQLiteDataAccess.GetSingle(oPredicate, sDbPath);

        public static IEnumerable<T> Find<T>(Expression<Func<T, bool>> oPredicate, string sDbPath = null) where T : class, new() => SQLiteDataAccess.Find(oPredicate, sDbPath);

        public static int Count<T>(Expression<Func<T, bool>> oPredicate, string sDbPath = null) where T : class, new() => SQLiteDataAccess.Count(oPredicate, sDbPath);

        public static bool Upsert<T>(T oEntity, string sDbPath = null) where T : class, new() => SQLiteDataAccess.Upsert(oEntity, sDbPath);

        public static bool RemoveColumn(string sTableName, string sColumnName, string sDbPath = null) => SQLiteSchemaManager.RemoveColumn(sTableName, sColumnName, sDbPath);
        public static bool RemoveIndex(string sIndexName, string sDbPath = null) => SQLiteSchemaManager.RemoveIndex(sIndexName, sDbPath);
        public static bool RemoveIndexesForTable(string sTableName, string sDbPath = null) => SQLiteSchemaManager.RemoveIndexesForTable(sTableName, sDbPath);
        public static bool RemoveIndexesForProperty<T>(Expression<Func<T, object>> oPropertySelector, string sDbPath = null) => SQLiteSchemaManager.RemoveIndexesForProperty(oPropertySelector, sDbPath);
        public static List<string> GetIndexesForTable(string sTableName, string sDbPath = null) => SQLiteSchemaManager.GetIndexesForTable(sTableName, sDbPath);
        public static bool IndexExists(string sIndexName, string sDbPath = null) => SQLiteSchemaManager.IndexExists(sIndexName, sDbPath);
    }
}