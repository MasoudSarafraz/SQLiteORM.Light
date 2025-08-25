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
using System.Xml.Linq;

namespace SQLiteORM
{

    public static class SQLiteManager
    {
        private static string _oDefaultDbPath;
        private const string _DefaultDbFileName = "Database.sqlite";
        private const string _ConfigFileName = "AppConfig.xml";
        private static bool _TablesChecked = false;

        // حفظ شد: کش‌ها برای بهبود عملکر
        private static readonly ConcurrentDictionary<Type, bool> _TableCheckCache = new ConcurrentDictionary<Type, bool>();
        private static readonly ConcurrentDictionary<Type, PropertyInfo[]> _PropertyCache = new ConcurrentDictionary<Type, PropertyInfo[]>();
        private static readonly ConcurrentDictionary<Type, string> _TableNameCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, PropertyInfo> _PrimaryKeyCache = new ConcurrentDictionary<Type, PropertyInfo>();

        // private static readonly object _dbLock = new object();
        //private static readonly ConcurrentDictionary<string, object> _tableLocks = new ConcurrentDictionary<string, object>();

        static SQLiteManager()
        {
            LoadDatabaseConfig();
        }

        private static void LoadDatabaseConfig()
        {
            //// lock (_configLock) - این بخش معمولاً یک‌بار در استارت‌آپ اجرا می‌شود و نیازی به lock ندارد.
            try
            {
                string sConfigPath = Path.Combine(
                    Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) ??
                    throw new InvalidOperationException("Unable to determine executable path"),
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
                    sDatabasePath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) ??
                        throw new InvalidOperationException("Unable to determine executable path");
                }

                _oDefaultDbPath = Path.Combine(sDatabasePath, sDatabaseName);
                EnsureDatabaseFileExists();
                CheckAllTableStructures(); // این بخش نیز معمولاً یک‌بار اجرا می‌شود.
            }
            catch (Exception oEx)
            {
                throw new InvalidOperationException("Failed to load database configuration: " + oEx.Message, oEx);
            }
        }

        private static void CheckAllTableStructures()
        {
            if (_TablesChecked) return;
            var aModelTypes = AppDomain.CurrentDomain.GetAssemblies()
                .SelectMany(a => a.GetTypes())
                .Where(t => t.GetCustomAttributes(typeof(TableAttribute), false).Length > 0)
                .ToList();

            // Parallel.ForEach - به دلیل I/O و پیچیدگی هم‌زمانی
            foreach (var oType in aModelTypes)
            {
                CheckTableStructure(oType); // بدون lock
            }
            _TablesChecked = true;
        }

        private static void CheckTableStructure(Type oModelType)
        {
            //  lock(oTableLock) - دیتابیس خود مدیریت می‌کند
            if (_TableCheckCache.TryGetValue(oModelType, out bool bChecked) && bChecked)
                return;

            var aTableAttrs = oModelType.GetCustomAttributes(typeof(TableAttribute), false);
            if (aTableAttrs.Length == 0) return;
            var oTableAttr = (TableAttribute)aTableAttrs[0];
            string sTableName = oTableAttr.Name;
            PropertyInfo[] aProperties = GetCachedProperties(oModelType);

            bool bTableExists = false;
            using (var oConnection = GetConnection())
            {
                oConnection.Open();

                using (var oCommand = new SQLiteCommand($"SELECT 1 FROM sqlite_master WHERE type='table' AND name='{sTableName}'", oConnection))
                {
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
            _TableCheckCache[oModelType] = true;
        }

        private static void CreateTable(Type oModelType, string sTableName, List<PropertyInfo> aProperties)
        {
            var aColumns = new List<string>();
            PropertyInfo oPrimaryKey = GetPrimaryKeyProperty(oModelType);

            foreach (var oProp in aProperties)
            {
                string sColumnType = GetSQLiteType(oProp.PropertyType);
                string sColumnDefinition = $"[{oProp.Name}] {sColumnType}";

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


            string sCreateTableSql = $"CREATE TABLE IF NOT EXISTS [{sTableName}] ({string.Join(", ", aColumns)})";
            ExecuteNonQuery(sCreateTableSql); // بدون lock
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
            var oAlterCommands = new List<string>();
            foreach (var oProp in aMissingProperties)
            {
                string sColumnType = GetSQLiteType(oProp.PropertyType);
                string sColumnDefinition = $"[{oProp.Name}] {sColumnType}";
                var oPrimaryKey = GetPrimaryKeyProperty(oProp.DeclaringType);
                if (!IsNullableType(oProp.PropertyType) && oProp != oPrimaryKey)
                    sColumnDefinition += " NOT NULL";
                oAlterCommands.Add($"ALTER TABLE [{sTableName}] ADD COLUMN {sColumnDefinition}");
            }
            if (oAlterCommands.Any())
            {
                using (var oConnection = GetConnection())
                {
                    oConnection.Open();
                    using (var oTransaction = oConnection.BeginTransaction())
                    {
                        try
                        {
                            foreach (string sCommand in oAlterCommands)
                            {
                                using (var oCmd = new SQLiteCommand(sCommand, oConnection, oTransaction))
                                {
                                    oCmd.ExecuteNonQuery();
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
        }

        private static PropertyInfo[] GetCachedProperties(Type oType)
        {
            return _PropertyCache.GetOrAdd(oType, type =>
                type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Where(p => p.GetCustomAttributes(typeof(IgnoreAttribute), false).Length == 0)
                    .ToArray());
        }

        private static string GetCachedTableName<T>()
        {
            return _TableNameCache.GetOrAdd(typeof(T), type =>
            {
                var aTableAttrs = type.GetCustomAttributes(typeof(TableAttribute), false);
                return aTableAttrs.Length > 0
                    ? ((TableAttribute)aTableAttrs[0]).Name
                    : type.Name;
            });
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
            string sBasePath = sDbPath ?? Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) ??
                throw new InvalidOperationException("Unable to determine executable path");
            _oDefaultDbPath = Path.Combine(sBasePath, sDbFileName);
            EnsureDatabaseFileExists();
            CheckAllTableStructures();
        }

        private static void EnsureDatabaseFileExists()
        {
            string sDirectory = Path.GetDirectoryName(_oDefaultDbPath);
            if (!string.IsNullOrEmpty(sDirectory) && !Directory.Exists(sDirectory))
                Directory.CreateDirectory(sDirectory);
            if (!File.Exists(_oDefaultDbPath))
                SQLiteConnection.CreateFile(_oDefaultDbPath);
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
            if (oType == typeof(int))
                return "INTEGER";
            if (oType == typeof(long))
                return "INTEGER";
            if (oType == typeof(short))
                return "INTEGER";
            if (oType == typeof(byte))
                return "INTEGER";
            if (oType == typeof(float))
                return "REAL";
            if (oType == typeof(double))
                return "REAL";
            if (oType == typeof(decimal))
                return "TEXT";
            if (oType == typeof(bool))
                return "INTEGER";
            if (oType == typeof(DateTime))
                return "TEXT";
            if (oType == typeof(byte[]))
                return "BLOB";
            if (oType == typeof(Guid))
                return "TEXT";
            if (oType == typeof(TimeSpan))
                return "TEXT";
            if (oType == typeof(string))
                return "TEXT";
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
            string sColumns = string.Join(", ", aProperties.Select(p => $"[{p.Name}]"));
            string sParameters = string.Join(", ", aProperties.Select(p => "@" + p.Name));
            string sSql = $"INSERT INTO [{sTableName}] ({sColumns}) VALUES ({sParameters}); SELECT last_insert_rowid();";

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
            string sColumns = string.Join(", ", aProperties.Select(p => $"[{p.Name}]"));
            string sParameters = string.Join(", ", aProperties.Select(p => "@" + p.Name));
            string sSql = $"INSERT INTO [{sTableName}] ({sColumns}) VALUES ({sParameters}); SELECT last_insert_rowid();";

            using (SQLiteConnection oConnection = GetConnection(sDbPath))
            {
                oConnection.Open();
                using (var oTransaction = oConnection.BeginTransaction()) // تراکنش برای عملکرد بهتر
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
            string sSetClause = string.Join(", ", aProperties.Select(p => $"[{p.Name}] = @{p.Name}"));
            string sSql = $"UPDATE [{sTableName}] SET {sSetClause} WHERE [{oPrimaryKey.Name}] = @PrimaryKey";

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

            string sSetClause = string.Join(", ", aProperties.Select(p => $"[{p.Name}] = @{p.Name}"));
            string sSql = $"UPDATE [{sTableName}] SET {sSetClause} WHERE [{oPrimaryKey.Name}] = @PrimaryKey";

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
            string sSql = $"DELETE FROM [{sTableName}] WHERE [{oPrimaryKey.Name}] = @PrimaryKey";
            using (SQLiteConnection oConnection = GetConnection(sDbPath)) // اتصال موقت
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
            var aIdParams = aIds.Select((id, index) => $"@id{index}").ToArray();
            string sSql = $"DELETE FROM [{sTableName}] WHERE [{oPrimaryKey.Name}] IN ({string.Join(",", aIdParams)})";
            using (SQLiteConnection oConnection = GetConnection(sDbPath))
            {
                oConnection.Open();
                using (SQLiteCommand oCommand = new SQLiteCommand(sSql, oConnection))
                {
                    for (int i = 0; i < aIds.Count(); i++)
                    {
                        oCommand.Parameters.AddWithValue($"@id{i}", aIds.ElementAt(i));
                    }
                    return oCommand.ExecuteNonQuery();
                }
            }
        }

        public static bool DeleteAll<T>(Expression<Func<T, bool>> oPredicate, string sDbPath = null) where T : class, new()
        {
            string sTableName = GetCachedTableName<T>();
            var oConverter = new ExpressionToSqlConverter<T>();
            string sWhereClause = oConverter.Convert(oPredicate);
            string sSql = $"DELETE FROM [{sTableName}] WHERE {sWhereClause}";
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
            var oConverter = new ExpressionToSqlConverter<T>();
            string sWhereClause = oConverter.Convert(oPredicate);
            string sSql = $"SELECT * FROM [{sTableName}]";
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
            var oConverter = new ExpressionToSqlConverter<T>();
            string sWhereClause = oConverter.Convert(oPredicate);
            string sSql = $"SELECT COUNT(*) FROM [{sTableName}]";
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
                    using (var oCommand = new SQLiteCommand($"PRAGMA table_info([{sTableName}])", oConnection))
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
                    string sCreateTempTableSql = $"CREATE TABLE [{sTempTableName}] AS SELECT {string.Join(", ", aColumns.Select(c => $"[{c}]"))} FROM [{sTableName}]";
                    ExecuteNonQuery(sCreateTempTableSql, sDbPath);
                    ExecuteNonQuery($"DROP TABLE [{sTableName}]", sDbPath);
                    ExecuteNonQuery($"ALTER TABLE [{sTempTableName}] RENAME TO [{sTableName}]", sDbPath);
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

    }
    public class ExpressionToSqlConverter<T>
    {
        public string Convert(Expression<Func<T, bool>> oExpression)
        {
            if (oExpression == null)
                return "1=1";
            return Visit(oExpression.Body);
        }

        private string Visit(Expression oNode)
        {
            switch (oNode.NodeType)
            {
                case ExpressionType.Equal:
                    return VisitBinary((BinaryExpression)oNode, "=");
                case ExpressionType.NotEqual:
                    return VisitBinary((BinaryExpression)oNode, "<>");
                case ExpressionType.GreaterThan:
                    return VisitBinary((BinaryExpression)oNode, ">");
                case ExpressionType.GreaterThanOrEqual:
                    return VisitBinary((BinaryExpression)oNode, ">=");
                case ExpressionType.LessThan:
                    return VisitBinary((BinaryExpression)oNode, "<");
                case ExpressionType.LessThanOrEqual:
                    return VisitBinary((BinaryExpression)oNode, "<=");
                case ExpressionType.AndAlso:
                    return VisitBinary((BinaryExpression)oNode, "AND");
                case ExpressionType.OrElse:
                    return VisitBinary((BinaryExpression)oNode, "OR");
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                    return VisitBinaryArithmetic(oNode, "+");
                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                    return VisitBinaryArithmetic(oNode, "-");
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                    return VisitBinaryArithmetic(oNode, "*");
                case ExpressionType.Divide:
                    return VisitBinaryArithmetic(oNode, "/");
                case ExpressionType.Modulo:
                    return VisitBinaryArithmetic(oNode, "%");
                case ExpressionType.And:
                    return VisitBinaryBitwise(oNode, "&");
                case ExpressionType.Or:
                    return VisitBinaryBitwise(oNode, "|");
                case ExpressionType.ExclusiveOr:
                    return VisitBinaryBitwise(oNode, "~");
                case ExpressionType.Coalesce:
                    return VisitCoalesce((BinaryExpression)oNode);
                case ExpressionType.MemberAccess:
                    return VisitMember((MemberExpression)oNode);
                case ExpressionType.Constant:
                    return VisitConstant((ConstantExpression)oNode);
                case ExpressionType.Call:
                    return VisitMethodCall((MethodCallExpression)oNode);
                case ExpressionType.Convert:
                    return VisitConvert((UnaryExpression)oNode);
                case ExpressionType.Not:
                    return VisitNot((UnaryExpression)oNode);
                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                    return VisitUnaryMinus((UnaryExpression)oNode);
                case ExpressionType.Conditional:
                    return VisitConditional((ConditionalExpression)oNode);
                default:
                    throw new NotSupportedException($"Expression type '{oNode.NodeType}' is not supported");
            }
        }

        private string VisitBinary(BinaryExpression oNode, string oOperatorStr)
        {
            string oLeft = Visit(oNode.Left);
            string oRight = Visit(oNode.Right);
            if (oRight == "NULL")
                return oOperatorStr == "=" ? $"{oLeft} IS NULL" : $"{oLeft} IS NOT NULL";
            if (oLeft == "NULL")
                return oOperatorStr == "=" ? $"{oRight} IS NULL" : $"{oRight} IS NOT NULL";
            return $"({oLeft} {oOperatorStr} {oRight})";
        }

        private string VisitBinaryArithmetic(Expression oNode, string oOperatorStr)
        {
            BinaryExpression oBinary = (BinaryExpression)oNode;
            string oLeft = Visit(oBinary.Left);
            string oRight = Visit(oBinary.Right);
            return $"({oLeft} {oOperatorStr} {oRight})";
        }

        private string VisitBinaryBitwise(Expression oNode, string oOperatorStr)
        {
            BinaryExpression oBinary = (BinaryExpression)oNode;
            string oLeft = Visit(oBinary.Left);
            string oRight = Visit(oBinary.Right);
            return $"({oLeft} {oOperatorStr} {oRight})";
        }

        private string VisitCoalesce(BinaryExpression oNode)
        {
            string oLeft = Visit(oNode.Left);
            string oRight = Visit(oNode.Right);
            return $"COALESCE({oLeft}, {oRight})";
        }

        private string VisitMember(MemberExpression oNode)
        {
            if (oNode.Expression is ParameterExpression oParamExpr &&
                oParamExpr.Type == typeof(T))
            {
                return $"[{oNode.Member.Name}]";
            }
            if (oNode.Expression is MemberExpression oInnerMember &&
                oInnerMember.Type == typeof(DateTime) &&
                oNode.Member.DeclaringType == typeof(DateTime))
            {
                string oInner = Visit(oInnerMember);
                switch (oNode.Member.Name)
                {
                    case "Year": return $"CAST(STRFTIME('%Y', {oInner}) AS INTEGER)";
                    case "Month": return $"CAST(STRFTIME('%m', {oInner}) AS INTEGER)";
                    case "Day": return $"CAST(STRFTIME('%d', {oInner}) AS INTEGER)";
                    case "Hour": return $"CAST(STRFTIME('%H', {oInner}) AS INTEGER)";
                    case "Minute": return $"CAST(STRFTIME('%M', {oInner}) AS INTEGER)";
                    case "Second": return $"CAST(STRFTIME('%S', {oInner}) AS INTEGER)";
                    case "DayOfWeek": return $"CAST(STRFTIME('%w', {oInner}) AS INTEGER)";
                    case "DayOfYear": return $"CAST(STRFTIME('%j', {oInner}) AS INTEGER)";
                    default:
                        throw new NotSupportedException($"DateTime property {oNode.Member.Name} is not supported");
                }
            }
            try
            {
                object oValue = Expression.Lambda(oNode).Compile().DynamicInvoke();
                return VisitConstant(Expression.Constant(oValue, oNode.Type));
            }
            catch
            {
                throw new NotSupportedException($"Member '{oNode.Member.Name}' cannot be evaluated");
            }
        }

        private string VisitConstant(ConstantExpression oNode)
        {
            if (oNode.Value == null)
                return "NULL";
            if (oNode.Value is IEnumerable oEnumerable &&
                !(oNode.Value is string) &&
                !(oNode.Value is byte[]))
            {
                var oSb = new StringBuilder();
                oSb.Append('(');
                bool bFirst = true;
                foreach (var oItem in oEnumerable)
                {
                    if (!bFirst) oSb.Append(", ");
                    oSb.Append(VisitConstant(Expression.Constant(oItem)));
                    bFirst = false;
                }
                if (bFirst) oSb.Append("NULL");
                oSb.Append(')');
                return oSb.ToString();
            }
            return FormatValue(oNode.Value);
        }

        private string FormatValue(object oValue)
        {
            if (oValue == null) return "NULL";
            if (oValue is string sStr)
                return $"'{sStr.Replace("'", "''")}'";
            if (oValue is bool bBool)
                return bBool ? "1" : "0";
            if (oValue is DateTime dt)
                return $"'{dt:yyyy-MM-dd HH:mm:ss}'";
            if (oValue is byte[] aBytes)
                return "X'" + BitConverter.ToString(aBytes).Replace("-", "") + "'";
            if (oValue is Guid guid)
                return $"'{guid}'";
            if (oValue is TimeSpan timeSpan)
                return $"'{timeSpan}'";
            return oValue.ToString();
        }

        private string VisitMethodCall(MethodCallExpression oNode)
        {
            if (oNode.Object != null && oNode.Object is MethodCallExpression)
            {
                string oInnerValue = Visit(oNode.Object);
                switch (oNode.Method.Name)
                {
                    case "Trim": return $"TRIM({oInnerValue})";
                    case "ToUpper": return $"UPPER({oInnerValue})";
                    case "ToLower": return $"LOWER({oInnerValue})";
                    default:
                        throw new NotSupportedException($"Method '{oNode.Method.Name}' is not supported");
                }
            }
            if (oNode.Method.DeclaringType == typeof(string))
            {
                string sMember = oNode.Object != null ? Visit(oNode.Object) : null;
                string[] aArgs = oNode.Arguments.Select(Visit).ToArray();
                switch (oNode.Method.Name)
                {
                    case "Contains": return $"{sMember} LIKE '%' || {aArgs[0]} || '%'";
                    case "StartsWith": return $"{sMember} LIKE {aArgs[0]} || '%'";
                    case "EndsWith": return $"{sMember} LIKE '%' || {aArgs[0]}";
                    case "Equals": return $"{sMember} = {aArgs[0]}";
                    case "Trim": return $"TRIM({sMember})";
                    case "ToUpper": return $"UPPER({sMember})";
                    case "ToLower": return $"LOWER({sMember})";
                    case "Replace": return $"REPLACE({sMember}, {aArgs[0]}, {aArgs[1]})";
                    case "Substring":
                        if (aArgs.Length == 1)
                            return $"SUBSTR({sMember}, {aArgs[0]} + 1)";
                        else if (aArgs.Length == 2)
                            return $"SUBSTR({sMember}, {aArgs[0]} + 1, {aArgs[1]})";
                        else
                            throw new NotSupportedException("Substring with more than 2 parameters not supported");
                    case "get_Length": return $"LENGTH({sMember})";
                    case "IndexOf":
                        if (aArgs.Length == 1)
                            return $"INSTR({sMember}, {aArgs[0]}) - 1";
                        else
                            throw new NotSupportedException("IndexOf with parameters not supported");
                    default:
                        throw new NotSupportedException($"String method '{oNode.Method.Name}' is not supported");
                }
            }
            if (oNode.Method.DeclaringType == typeof(Enumerable))
            {
                switch (oNode.Method.Name)
                {
                    case "Contains": return HandleEnumerableContains(oNode);
                    default:
                        throw new NotSupportedException($"Enumerable method '{oNode.Method.Name}' is not supported");
                }
            }
            if (oNode.Method.DeclaringType == typeof(Math))
            {
                string[] aArgs = oNode.Arguments.Select(Visit).ToArray();
                switch (oNode.Method.Name)
                {
                    case "Abs": return $"ABS({aArgs[0]})";
                    case "Round":
                        if (aArgs.Length == 1)
                            return $"ROUND({aArgs[0]})";
                        else if (aArgs.Length == 2)
                            return $"ROUND({aArgs[0]}, {aArgs[1]})";
                        else
                            throw new NotSupportedException("Round with more than 2 parameters not supported");
                    case "Ceiling": return $"CEIL({aArgs[0]})";
                    case "Floor": return $"FLOOR({aArgs[0]})";
                    default:
                        throw new NotSupportedException($"Math method '{oNode.Method.Name}' is not supported");
                }
            }
            if (oNode.Method.Name == "Contains" && oNode.Object != null)
            {
                return HandleInstanceContains(oNode);
            }
            throw new NotSupportedException($"Method '{oNode.Method.Name}' is not supported");
        }

        private string HandleEnumerableContains(MethodCallExpression oNode)
        {
            object oCollectionValue;
            try
            {
                oCollectionValue = Expression.Lambda(oNode.Arguments[0]).Compile().DynamicInvoke();
            }
            catch
            {
                throw new NotSupportedException("Collection in Contains must be evaluable");
            }
            string sValues = VisitConstant(Expression.Constant(oCollectionValue));
            string sItem = Visit(oNode.Arguments[1]);
            return $"{sItem} IN {sValues}";
        }

        private string HandleInstanceContains(MethodCallExpression oNode)
        {
            object oCollectionValue;
            try
            {
                oCollectionValue = Expression.Lambda(oNode.Object).Compile().DynamicInvoke();
            }
            catch
            {
                throw new NotSupportedException("Collection in Contains must be evaluable");
            }
            string sValues = VisitConstant(Expression.Constant(oCollectionValue));
            string sItem = Visit(oNode.Arguments[0]);
            return $"{sItem} IN {sValues}";
        }

        private string VisitConvert(UnaryExpression oNode)
        {
            return Visit(oNode.Operand);
        }

        private string VisitNot(UnaryExpression oNode)
        {
            string oOperand = Visit(oNode.Operand);
            if (oNode.Type == typeof(bool) || oNode.Type == typeof(bool?))
                return $"NOT ({oOperand})";
            else
                return $"~({oOperand})";
        }

        private string VisitUnaryMinus(UnaryExpression oNode)
        {
            string oOperand = Visit(oNode.Operand);
            return $"-({oOperand})";
        }

        private string VisitConditional(ConditionalExpression oNode)
        {
            string sTest = Visit(oNode.Test);
            string sIfTrue = Visit(oNode.IfTrue);
            string sIfFalse = Visit(oNode.IfFalse);
            return $"(CASE WHEN {sTest} THEN {sIfTrue} ELSE {sIfFalse} END)";
        }
    }
}