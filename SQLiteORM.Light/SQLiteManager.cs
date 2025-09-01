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
        private static readonly object _configLock = new object();
        private static readonly object _tableCheckLock = new object();

        // کش‌ها برای بهبود عملکرد
        private static readonly ConcurrentDictionary<Type, bool> _TableCheckCache = new ConcurrentDictionary<Type, bool>();
        private static readonly ConcurrentDictionary<Type, PropertyInfo[]> _PropertyCache = new ConcurrentDictionary<Type, PropertyInfo[]>();
        private static readonly ConcurrentDictionary<Type, string> _TableNameCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, PropertyInfo> _PrimaryKeyCache = new ConcurrentDictionary<Type, PropertyInfo>();
        private static readonly ConcurrentDictionary<string, object> _tableLocks = new ConcurrentDictionary<string, object>();
        private static readonly ConcurrentDictionary<Type, List<IndexInfo>> _IndexCache = new ConcurrentDictionary<Type, List<IndexInfo>>();

        static SQLiteManager()
        {
            LoadDatabaseConfig();
        }

        private static void LoadDatabaseConfig()
        {
            lock (_configLock)
            {
                try
                {
                    string sConfigPath = Path.Combine(
                        AppDomain.CurrentDomain.BaseDirectory,
                        _ConfigFileName);

                    if (!File.Exists(sConfigPath))
                    {
                        CreateDefaultConfig(sConfigPath);
                    }

                    XDocument oDoc = XDocument.Load(sConfigPath);
                    string sDatabaseName = oDoc.Root?.Element("DatabaseName")?.Value ?? _DefaultDbFileName;
                    string sDatabasePath = oDoc.Root?.Element("DatabasePath")?.Value;

                    if (string.IsNullOrWhiteSpace(sDatabaseName))
                        throw new InvalidOperationException("Database name not specified in config file");

                    if (string.IsNullOrWhiteSpace(sDatabasePath) || sDatabasePath == "." || sDatabasePath.Equals("default", StringComparison.OrdinalIgnoreCase))
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

        private static void CheckAllTableStructures()
        {
            if (_TablesChecked) return;

            lock (_tableCheckLock)
            {
                if (_TablesChecked) return;

                var aModelTypes = AppDomain.CurrentDomain.GetAssemblies()
                    .SelectMany(a => a.GetTypes())
                    .Where(t => t.GetCustomAttributes(typeof(TableAttribute), false).Length > 0)
                    .ToList();

                foreach (var oType in aModelTypes)
                {
                    CheckTableStructure(oType);
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

            // گرفتن قفل مخصوص این جدول
            var oTableLock = _tableLocks.GetOrAdd(sTableName, new object());

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

                    var aMissingProperties = aProperties
                        .Where(p => !aExistingColumns.Contains(p.Name))
                        .ToList();

                    if (aMissingProperties.Any())
                    {
                        AddMissingColumns(sTableName, aMissingProperties);
                    }
                }

                // بررسی و ایجاد ایندکس‌ها
                CheckAndCreateIndexes(oModelType, sTableName);

                _TableCheckCache[oModelType] = true;
            }
        }

        private static void CreateTable(Type oModelType, string sTableName, List<PropertyInfo> aProperties)
        {
            var aColumns = new List<string>();
            PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(oModelType);

            foreach (var oProp in aProperties)
            {
                string sColumnType = GetSQLiteType(oProp.PropertyType);
                string sColumnDefinition = $"{oProp.Name} {sColumnType}"; // حذف براکت از نام ستون

                if (oProp == oPrimaryKey)
                {
                    if (IsAutoIncrementType(oProp.PropertyType))
                    {
                        sColumnDefinition += " PRIMARY KEY AUTOINCREMENT";
                    }
                    else
                    {
                        sColumnDefinition += " PRIMARY KEY";
                    }
                }
                else if (!IsNullableType(oProp.PropertyType))
                {
                    sColumnDefinition += " NOT NULL";
                }
                aColumns.Add(sColumnDefinition);
            }

            string sCreateTableSql = $"CREATE TABLE {sTableName} ({string.Join(", ", aColumns)})"; // حذف براکت از نام جدول
            ExecuteNonQuery(sCreateTableSql);
        }

        private static void CheckAndCreateIndexes(Type oModelType, string sTableName)
        {
            var aIndexInfos = GetCachedIndexes(oModelType);

            using (var oConnection = GetConnection())
            {
                oConnection.Open();

                // دریافت ایندکس‌های موجود
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

                // ایجاد ایندکس‌های جدید
                foreach (var oIndexInfo in aIndexInfos.Where(i => i.AutoCreate && !aExistingIndexes.Contains(i.Name)))
                {
                    string sUnique = oIndexInfo.IsUnique ? "UNIQUE" : "";
                    string sColumns = string.Join(", ", oIndexInfo.Properties.Select(p => $"{p}")); // حذف براکت
                    string sSql = $"CREATE {sUnique} INDEX {oIndexInfo.Name} ON {sTableName} ({sColumns})"; // حذف براکت

                    ExecuteNonQuery(sSql);
                }
            }
        }

        private static List<IndexInfo> GetCachedIndexes(Type oType)
        {
            return _IndexCache.GetOrAdd(oType, type =>
            {
                var aIndexInfos = new List<IndexInfo>();
                var aProperties = GetCachedProperties(type);

                // ایندکس‌های تکی
                foreach (var oProp in aProperties)
                {
                    var aIndexAttrs = oProp.GetCustomAttributes(typeof(IndexAttribute), false)
                        .Cast<IndexAttribute>()
                        .Where(attr => attr.AutoCreate);

                    foreach (var oIndexAttr in aIndexAttrs)
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

                // ایندکس‌های ترکیبی (بر اساس نام)
                var aGroupedIndexes = aIndexInfos
                    .Where(i => !string.IsNullOrEmpty(i.Name))
                    .GroupBy(i => i.Name)
                    .Where(g => g.Count() > 1);

                foreach (var oGroup in aGroupedIndexes)
                {
                    var oFirst = oGroup.First();
                    oFirst.Properties = oGroup
                        .OrderBy(i => i.Properties.First())
                        .SelectMany(i => i.Properties)
                        .ToList();

                    aIndexInfos.RemoveAll(i => i.Name == oFirst.Name && i != oFirst);
                }

                return aIndexInfos;
            });
        }

        private static bool IsAutoIncrementType(Type type)
        {
            type = Nullable.GetUnderlyingType(type) ?? type;
            return type == typeof(int) || type == typeof(long);
        }

        private static PropertyInfo GetPrimaryKeyProperty(Type oType)
        {
            return _PrimaryKeyCache.GetOrAdd(oType, type =>
            {
                var aProperties = GetCachedProperties(type);
                var oIdentityKeyProperty = aProperties.FirstOrDefault(p =>
                    p.GetCustomAttributes(typeof(IdentityKeyAttribute), false).Length > 0);
                if (oIdentityKeyProperty != null)
                    return oIdentityKeyProperty;

                var oIdProperty = aProperties.FirstOrDefault(p =>
                    string.Equals(p.Name, "Id", StringComparison.OrdinalIgnoreCase));
                if (oIdProperty != null)
                    return oIdProperty;

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
                        foreach (var oProp in aMissingProperties)
                        {
                            string sColumnType = GetSQLiteType(oProp.PropertyType);
                            string sColumnDefinition = $"{oProp.Name} {sColumnType}"; // حذف براکت

                            if (!IsNullableType(oProp.PropertyType))
                                sColumnDefinition += " NOT NULL";

                            string sAlterSql = $"ALTER TABLE {sTableName} ADD COLUMN {sColumnDefinition}"; // حذف براکت

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
                type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Where(p => p.GetCustomAttributes(typeof(IgnoreAttribute), false).Length == 0)
                    .ToArray());
        }

        private static string GetCachedTableName(Type oType)
        {
            return _TableNameCache.GetOrAdd(oType, type =>
            {
                var aTableAttrs = type.GetCustomAttributes(typeof(TableAttribute), false);
                if (aTableAttrs.Length == 0)
                    return type.Name;

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
            // در SQLite از schema پشتیبانی نمی‌شود، بنابراین فقط نام جدول را برمی‌گردانیم
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

        public static void SetDefaultDatabasePath(string sDbFileName, string sDbPath = null)
        {
            if (string.IsNullOrWhiteSpace(sDbFileName))
                throw new ArgumentNullException(nameof(sDbFileName));

            lock (_configLock)
            {
                string sBasePath = sDbPath ?? AppDomain.CurrentDomain.BaseDirectory;
                _oDefaultDbPath = Path.Combine(sBasePath, sDbFileName);
                EnsureDatabaseFileExists();
                CheckAllTableStructures();
            }
        }

        private static void EnsureDatabaseFileExists()
        {
            lock (_configLock)
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
            return new SQLiteConnection($"Data Source={sPath};Version=3;Journal Mode=WAL;");
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
            using (SQLiteConnection oConnection = GetConnection(sDbPath))
            {
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
        }

        private static T ExecuteScalar<T>(string sSql, string sDbPath = null, Dictionary<string, object> oParameters = null)
        {
            using (SQLiteConnection oConnection = GetConnection(sDbPath))
            {
                oConnection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, oConnection))
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
        }

        public static int Insert<T>(T oEntity, string sDbPath = null) where T : class, new()
        {
            if (oEntity == null)
                throw new ArgumentNullException(nameof(oEntity));

            string sTableName = GetCachedTableName<T>();
            PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(typeof(T));
            PropertyInfo[] aProperties = GetCachedProperties(typeof(T))
                .Where(p => p != oPrimaryKey || !IsAutoIncrementType(p.PropertyType))
                .ToArray();

            string sColumns = string.Join(", ", aProperties.Select(p => $"{p.Name}")); // حذف براکت
            string sParameters = string.Join(", ", aProperties.Select(p => "@" + p.Name));
            string sSql = $"INSERT INTO {sTableName} ({sColumns}) VALUES ({sParameters}); SELECT last_insert_rowid();"; // حذف براکت

            using (SQLiteConnection oConnection = GetConnection(sDbPath))
            {
                oConnection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, oConnection))
                {
                    foreach (PropertyInfo oProp in aProperties)
                    {
                        object oValue = oProp.GetValue(oEntity, null) ?? DBNull.Value;
                        if (oProp.PropertyType == typeof(decimal) && oValue is decimal dVal)
                        {
                            oValue = dVal.ToString(System.Globalization.CultureInfo.InvariantCulture);
                        }
                        oCommand.Parameters.AddWithValue("@" + oProp.Name, oValue);
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
        }

        public static int InsertList<T>(IEnumerable<T> oEntities, string sDbPath = null) where T : class, new()
        {
            if (oEntities == null)
                throw new ArgumentNullException(nameof(oEntities));
            if (!oEntities.Any())
                return 0;

            string sTableName = GetCachedTableName<T>();
            PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(typeof(T));
            PropertyInfo[] aProperties = GetCachedProperties(typeof(T))
                .Where(p => p != oPrimaryKey || !IsAutoIncrementType(p.PropertyType))
                .ToArray();

            string sColumns = string.Join(", ", aProperties.Select(p => $"{p.Name}")); // حذف براکت
            string sParameters = string.Join(", ", aProperties.Select(p => "@" + p.Name));
            string sSql = $"INSERT INTO {sTableName} ({sColumns}) VALUES ({sParameters}); SELECT last_insert_rowid();"; // حذف براکت

            using (SQLiteConnection oConnection = GetConnection(sDbPath))
            {
                oConnection.Open();
                using (var oTransaction = oConnection.BeginTransaction())
                {
                    int iCount = 0;
                    try
                    {
                        foreach (var oEntity in oEntities)
                        {
                            using (SQLiteCommand oCommand = new SQLiteCommand(sSql, oConnection, oTransaction))
                            {
                                foreach (PropertyInfo oProp in aProperties)
                                {
                                    object oValue = oProp.GetValue(oEntity, null) ?? DBNull.Value;
                                    if (oProp.PropertyType == typeof(decimal) && oValue is decimal dVal)
                                    {
                                        oValue = dVal.ToString(System.Globalization.CultureInfo.InvariantCulture);
                                    }
                                    oCommand.Parameters.AddWithValue("@" + oProp.Name, oValue);
                                }

                                object result = oCommand.ExecuteScalar();
                                int iGeneratedId = Convert.ToInt32(result);

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
        }

        public static bool Update<T>(T oEntity, string sDbPath = null) where T : class, new()
        {
            if (oEntity == null)
                throw new ArgumentNullException(nameof(oEntity));

            string sTableName = GetCachedTableName<T>();
            PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(typeof(T));
            if (oPrimaryKey == null)
                throw new InvalidOperationException("Entity must have a primary key property");

            PropertyInfo[] aProperties = GetCachedProperties(typeof(T))
                .Where(p => p != oPrimaryKey)
                .ToArray();

            string sSetClause = string.Join(", ", aProperties.Select(p => $"{p.Name} = @{p.Name}")); // حذف براکت
            string sSql = $"UPDATE {sTableName} SET {sSetClause} WHERE {oPrimaryKey.Name} = @PrimaryKey"; // حذف براکت

            using (SQLiteConnection oConnection = GetConnection(sDbPath))
            {
                oConnection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, oConnection))
                {
                    foreach (PropertyInfo oProp in aProperties)
                    {
                        object oValue = oProp.GetValue(oEntity, null) ?? DBNull.Value;
                        if (oProp.PropertyType == typeof(decimal) && oValue is decimal dVal)
                        {
                            oValue = dVal.ToString(System.Globalization.CultureInfo.InvariantCulture);
                        }
                        oCommand.Parameters.AddWithValue("@" + oProp.Name, oValue);
                    }

                    object oPrimaryKeyValue = oPrimaryKey.GetValue(oEntity, null) ?? DBNull.Value;
                    oCommand.Parameters.AddWithValue("@PrimaryKey", oPrimaryKeyValue);

                    return oCommand.ExecuteNonQuery() > 0;
                }
            }
        }

        public static int UpdateList<T>(IEnumerable<T> oEntities, string sDbPath = null) where T : class, new()
        {
            if (oEntities == null)
                throw new ArgumentNullException(nameof(oEntities));
            if (!oEntities.Any())
                return 0;

            string sTableName = GetCachedTableName<T>();
            PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(typeof(T));
            if (oPrimaryKey == null)
                throw new InvalidOperationException("Entity must have a primary key property");

            PropertyInfo[] aProperties = GetCachedProperties(typeof(T))
                .Where(p => p != oPrimaryKey)
                .ToArray();

            string sSetClause = string.Join(", ", aProperties.Select(p => $"{p.Name} = @{p.Name}")); // حذف براکت
            string sSql = $"UPDATE {sTableName} SET {sSetClause} WHERE {oPrimaryKey.Name} = @PrimaryKey"; // حذف براکت

            using (SQLiteConnection oConnection = GetConnection(sDbPath))
            {
                oConnection.Open();
                using (var oTransaction = oConnection.BeginTransaction())
                {
                    int iCount = 0;
                    try
                    {
                        foreach (var oEntity in oEntities)
                        {
                            using (SQLiteCommand oCommand = new SQLiteCommand(sSql, oConnection, oTransaction))
                            {
                                foreach (PropertyInfo oProp in aProperties)
                                {
                                    object oValue = oProp.GetValue(oEntity, null) ?? DBNull.Value;
                                    if (oProp.PropertyType == typeof(decimal) && oValue is decimal dVal)
                                    {
                                        oValue = dVal.ToString(System.Globalization.CultureInfo.InvariantCulture);
                                    }
                                    oCommand.Parameters.AddWithValue("@" + oProp.Name, oValue);
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
        }

        public static bool Delete<T>(int iId, string sDbPath = null) where T : class, new()
        {
            string sTableName = GetCachedTableName<T>();
            PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(typeof(T));
            string sSql = $"DELETE FROM {sTableName} WHERE {oPrimaryKey.Name} = @PrimaryKey"; // حذف براکت

            using (SQLiteConnection oConnection = GetConnection(sDbPath))
            {
                oConnection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, oConnection))
                {
                    oCommand.Parameters.AddWithValue("@PrimaryKey", iId);
                    return oCommand.ExecuteNonQuery() > 0;
                }
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

            using (SQLiteConnection oConnection = GetConnection(sDbPath))
            {
                oConnection.Open();
                using (var oTransaction = oConnection.BeginTransaction())
                {
                    int iCount = 0;
                    try
                    {
                        foreach (int iId in aIds)
                        {
                            string sSql = $"DELETE FROM {sTableName} WHERE {oPrimaryKey.Name} = @PrimaryKey"; // حذف براکت
                            using (SQLiteCommand oCommand = new SQLiteCommand(sSql, oConnection, oTransaction))
                            {
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
        }

        public static bool DeleteAll<T>(Expression<Func<T, bool>> oPredicate, string sDbPath = null) where T : class, new()
        {
            string sTableName = GetCachedTableName<T>();
            var oConverter = new ExpressionToSql<T>();
            string sWhereClause = oConverter.Convert(oPredicate);
            string sSql = $"DELETE FROM {sTableName} WHERE {sWhereClause}"; // حذف براکت

            using (SQLiteConnection oConnection = GetConnection(sDbPath))
            {
                oConnection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, oConnection))
                {
                    return oCommand.ExecuteNonQuery() > 0;
                }
            }
        }

        public static T GetById<T>(int iId, string sDbPath = null) where T : class, new()
        {
            PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(typeof(T));
            if (oPrimaryKey == null)
                throw new InvalidOperationException($"Type {typeof(T).Name} does not have a primary key property");

            ParameterExpression oParameter = Expression.Parameter(typeof(T), "e");
            MemberExpression oPropertyAccess = Expression.Property(oParameter, oPrimaryKey);
            ConstantExpression oConstantValue = Expression.Constant(iId);
            BinaryExpression oEquality = Expression.Equal(oPropertyAccess, oConstantValue);
            Expression<Func<T, bool>> oLambda = Expression.Lambda<Func<T, bool>>(oEquality, oParameter);

            return Find<T>(oLambda, sDbPath).FirstOrDefault();
        }

        public static IEnumerable<T> GetAll<T>(string sDbPath = null) where T : class, new()
        {
            return Find<T>(e => true, sDbPath);
        }

        public static T GetSingle<T>(Expression<Func<T, bool>> oPredicate, string sDbPath = null) where T : class, new()
        {
            return Find<T>(oPredicate, sDbPath).FirstOrDefault();
        }

        public static IEnumerable<T> Find<T>(Expression<Func<T, bool>> oPredicate, string sDbPath = null) where T : class, new()
        {
            string sTableName = GetCachedTableName<T>();
            var oConverter = new ExpressionToSql<T>();
            string sWhereClause = oConverter.Convert(oPredicate);
            string sSql = $"SELECT * FROM {sTableName}"; // حذف براکت

            if (!string.IsNullOrEmpty(sWhereClause) && sWhereClause != "1=1")
            {
                sSql += $" WHERE {sWhereClause}";
            }

            List<T> oResults = new List<T>();
            PropertyInfo[] aProperties = GetCachedProperties(typeof(T));

            using (SQLiteConnection oConnection = GetConnection(sDbPath))
            {
                oConnection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, oConnection))
                {
                    using (SQLiteDataReader oReader = oCommand.ExecuteReader())
                    {
                        while (oReader.Read())
                        {
                            T oEntity = new T();
                            for (int iIndex = 0; iIndex < oReader.FieldCount; iIndex++)
                            {
                                string sFieldName = oReader.GetName(iIndex);
                                PropertyInfo oProp = Array.Find(aProperties, p =>
                                    p.Name.Equals(sFieldName, StringComparison.OrdinalIgnoreCase));

                                if (oProp != null && !oReader.IsDBNull(iIndex))
                                {
                                    try
                                    {
                                        object oValue = oReader.GetValue(iIndex);
                                        if (oProp.PropertyType == typeof(bool) && oValue is long lVal)
                                            oValue = lVal != 0;
                                        else if (oProp.PropertyType == typeof(byte[]))
                                            oValue = oValue;
                                        else if (oProp.PropertyType == typeof(Guid) && oValue is string sGuid)
                                            oValue = Guid.Parse(sGuid);
                                        else if (oProp.PropertyType == typeof(TimeSpan) && oValue is string sTimeSpan)
                                            oValue = TimeSpan.Parse(sTimeSpan);
                                        else if (oProp.PropertyType == typeof(decimal) && oValue is string sDecimal)
                                            oValue = decimal.Parse(sDecimal, System.Globalization.CultureInfo.InvariantCulture);
                                        else
                                            oValue = Convert.ChangeType(oValue, Nullable.GetUnderlyingType(oProp.PropertyType) ?? oProp.PropertyType);

                                        oProp.SetValue(oEntity, oValue, null);
                                    }
                                    catch (Exception oEx)
                                    {
                                        throw new InvalidOperationException($"Failed to set value for property {oProp.Name}: {oEx.Message}", oEx);
                                    }
                                }
                            }
                            oResults.Add(oEntity);
                        }
                    }
                }
            }
            return oResults;
        }

        public static int Count<T>(Expression<Func<T, bool>> oPredicate, string sDbPath = null) where T : class, new()
        {
            string sTableName = GetCachedTableName<T>();
            var oConverter = new ExpressionToSql<T>();
            string sWhereClause = oConverter.Convert(oPredicate);
            string sSql = $"SELECT COUNT(*) FROM {sTableName}"; // حذف براکت

            if (!string.IsNullOrEmpty(sWhereClause) && sWhereClause != "1=1")
            {
                sSql += $" WHERE {sWhereClause}";
            }

            using (SQLiteConnection oConnection = GetConnection(sDbPath))
            {
                oConnection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, oConnection))
                {
                    return Convert.ToInt32(oCommand.ExecuteScalar());
                }
            }
        }

        public static bool RemoveColumn(string sTableName, string sColumnName, string sDbPath = null)
        {
            try
            {
                using (SQLiteConnection oConnection = GetConnection(sDbPath))
                {
                    oConnection.Open();
                    var aColumns = new List<string>();
                    using (var oCommand = new SQLiteCommand($"PRAGMA table_info({sTableName})", oConnection)) // حذف براکت
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
                    string sCreateTempTableSql = $"CREATE TABLE {sTempTableName} AS SELECT {string.Join(", ", aColumns.Select(c => $"{c}"))} FROM {sTableName}"; // حذف براکت
                    ExecuteNonQuery(sCreateTempTableSql, sDbPath);
                    ExecuteNonQuery($"DROP TABLE {sTableName}", sDbPath); // حذف براکت
                    ExecuteNonQuery($"ALTER TABLE {sTempTableName} RENAME TO {sTableName}", sDbPath); // حذف براکت

                    return true;
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
                string sSql = $"DROP INDEX IF EXISTS {sIndexName}"; // حذف براکت
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
                using (SQLiteConnection oConnection = GetConnection(sDbPath))
                {
                    oConnection.Open();
                    var aIndexNames = new List<string>();

                    // پیدا کردن تمام ایندکس‌های مربوط به جدول
                    using (var oCommand = new SQLiteCommand(
                        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=@tableName", oConnection))
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

                    // حذف همه ایندکس‌ها
                    foreach (string sIndexName in aIndexNames)
                    {
                        string sSql = $"DROP INDEX {sIndexName}"; // حذف براکت
                        ExecuteNonQuery(sSql, sDbPath);
                    }

                    return true;
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

            using (SQLiteConnection oConnection = GetConnection(sDbPath))
            {
                oConnection.Open();
                using (var oCommand = new SQLiteCommand(
                    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=@tableName", oConnection))
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

            return aIndexNames;
        }

        public static bool IndexExists(string sIndexName, string sDbPath = null)
        {
            using (SQLiteConnection oConnection = GetConnection(sDbPath))
            {
                oConnection.Open();
                using (var oCommand = new SQLiteCommand(
                    "SELECT 1 FROM sqlite_master WHERE type='index' AND name=@indexName", oConnection))
                {
                    oCommand.Parameters.AddWithValue("@indexName", sIndexName);
                    return oCommand.ExecuteScalar() != null;
                }
            }
        }

        // متد کمکی برای گرفتن نام پراپرتی از expression
        private static string GetPropertyName<T>(Expression<Func<T, object>> oPropertySelector)
        {
            if (oPropertySelector.Body is MemberExpression oMember)
                return oMember.Member.Name;

            if (oPropertySelector.Body is UnaryExpression oUnary && oUnary.Operand is MemberExpression oMemberOperand)
                return oMemberOperand.Member.Name;

            throw new ArgumentException("Invalid property expression");
        }

        #endregion
        // کلاس برای نگهداری اطلاعات ایندکس
        private class IndexInfo
        {
            public string Name { get; set; }
            public bool IsUnique { get; set; }
            public List<string> Properties { get; set; }
            public bool AutoCreate { get; set; }
        }
    }
}