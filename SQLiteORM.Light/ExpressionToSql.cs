using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;

namespace SQLiteORM
{
    internal sealed class SqlWithParameters
    {
        public string Sql { get; set; }
        public Dictionary<string, object> Parameters { get; set; }
        public SqlWithParameters()
        {
            Parameters = new Dictionary<string, object>();
        }
    }

    internal sealed class ExpressionToSql<T>
    {
        // جایگزینی ConcurrentDictionary با Dictionary و Lazy
        private static readonly Dictionary<WeakReference, Lazy<SqlWithParameters>> _oCache =
            new Dictionary<WeakReference, Lazy<SqlWithParameters>>();
        private static readonly object _oCacheLock = new object();

        // محدود کردن اندازه کش برای جلوگیری از رشد بی‌نهایت
        private const int MaxCacheSize = 1000;
        private static int _iCacheCount = 0;

        private static readonly Dictionary<MemberExpression, Lazy<Func<object>>> _oMemberCache =
            new Dictionary<MemberExpression, Lazy<Func<object>>>();
        private static readonly object _oMemberCacheLock = new object();

        private static readonly Dictionary<MethodInfo, Lazy<string>> _oMethodCache =
            new Dictionary<MethodInfo, Lazy<string>>();
        private static readonly object _oMethodCacheLock = new object();

        private static readonly Dictionary<Type, Lazy<Func<object, string>>> _oValueFormatterCache =
            new Dictionary<Type, Lazy<Func<object, string>>>();
        private static readonly object _oValueFormatterCacheLock = new object();

        private int _iParamIndex = 0;

        public SqlWithParameters ConvertToSqlWithParameters(Expression<Func<T, bool>> oExpression)
        {
            if (oExpression == null)
                return new SqlWithParameters { Sql = "1=1" };

            CheckCacheSize();

            // ایجاد WeakReference برای Expression
            var oWeakRef = new WeakReference(oExpression.Body);

            lock (_oCacheLock)
            {
                if (!_oCache.TryGetValue(oWeakRef, out Lazy<SqlWithParameters> oLazy))
                {
                    oLazy = new Lazy<SqlWithParameters>(() =>
                    {
                        Expression oBody;
                        if (oWeakRef.IsAlive && oWeakRef.Target is Expression oExpr)
                        {
                            oBody = oExpr;
                        }
                        else
                        {
                            _iParamIndex = 0;
                            var oResult = new SqlWithParameters();
                            oResult.Sql = Visit(oExpression.Body, oResult);
                            return oResult;
                        }
                        _iParamIndex = 0;
                        var oNewResult = new SqlWithParameters();
                        oNewResult.Sql = Visit(oBody, oNewResult);
                        return oNewResult;
                    }, LazyThreadSafetyMode.ExecutionAndPublication);

                    _oCache[oWeakRef] = oLazy;
                    Interlocked.Increment(ref _iCacheCount);
                }
                return oLazy.Value;
            }
        }

        private void CheckCacheSize()
        {
            if (_iCacheCount >= MaxCacheSize)
            {
                lock (_oCacheLock)
                {
                    if (_iCacheCount >= MaxCacheSize)
                    {
                        var lKeysToRemove = new List<WeakReference>();
                        foreach (var oKvp in _oCache)
                        {
                            if (!oKvp.Key.IsAlive)
                            {
                                lKeysToRemove.Add(oKvp.Key);
                            }
                        }
                        foreach (var oKey in lKeysToRemove)
                        {
                            _oCache.Remove(oKey);
                            Interlocked.Decrement(ref _iCacheCount);
                        }

                        if (_iCacheCount >= MaxCacheSize)
                        {
                            var oKeys = _oCache.Keys.ToList();
                            var iRemoveCount = (int)(oKeys.Count * 0.2);
                            var oRandom = new Random();
                            for (int i = 0; i < iRemoveCount; i++)
                            {
                                var iIndex = oRandom.Next(oKeys.Count);
                                var oKey = oKeys[iIndex];
                                _oCache.Remove(oKey);
                                oKeys.RemoveAt(iIndex);
                                Interlocked.Decrement(ref _iCacheCount);
                            }
                        }
                    }
                }
            }
        }

        [Obsolete("Use ConvertToSqlWithParameters for better security and performance. This will remove in the next version")]
        public string Convert(Expression<Func<T, bool>> oExpression)
        {
            return ConvertToSqlWithParameters(oExpression).Sql;
        }

        private string Visit(Expression oExpr, SqlWithParameters oResult)
        {
            if (oExpr == null) return "NULL";
            try
            {
                switch (oExpr.NodeType)
                {
                    case ExpressionType.Equal:
                        return VisitBinary((BinaryExpression)oExpr, "=", oResult);
                    case ExpressionType.NotEqual:
                        return VisitBinary((BinaryExpression)oExpr, "<>", oResult);
                    case ExpressionType.GreaterThan:
                        return VisitBinary((BinaryExpression)oExpr, ">", oResult);
                    case ExpressionType.GreaterThanOrEqual:
                        return VisitBinary((BinaryExpression)oExpr, ">=", oResult);
                    case ExpressionType.LessThan:
                        return VisitBinary((BinaryExpression)oExpr, "<", oResult);
                    case ExpressionType.LessThanOrEqual:
                        return VisitBinary((BinaryExpression)oExpr, "<=", oResult);
                    case ExpressionType.AndAlso:
                        return VisitBinary((BinaryExpression)oExpr, "AND", oResult);
                    case ExpressionType.OrElse:
                        return VisitBinary((BinaryExpression)oExpr, "OR", oResult);
                    case ExpressionType.Add:
                    case ExpressionType.AddChecked:
                        return VisitBinaryArithmetic(oExpr, "+", oResult);
                    case ExpressionType.Subtract:
                    case ExpressionType.SubtractChecked:
                        return VisitBinaryArithmetic(oExpr, "-", oResult);
                    case ExpressionType.Multiply:
                    case ExpressionType.MultiplyChecked:
                        return VisitBinaryArithmetic(oExpr, "*", oResult);
                    case ExpressionType.Divide:
                        return VisitBinaryArithmetic(oExpr, "/", oResult);
                    case ExpressionType.Modulo:
                        return VisitBinaryArithmetic(oExpr, "%", oResult);
                    case ExpressionType.And:
                        return VisitBinaryBitwise(oExpr, "&", oResult);
                    case ExpressionType.Or:
                        return VisitBinaryBitwise(oExpr, "|", oResult);
                    case ExpressionType.ExclusiveOr:
                        return VisitBinaryBitwise(oExpr, "^", oResult);
                    case ExpressionType.Coalesce:
                        return VisitCoalesce((BinaryExpression)oExpr, oResult);
                    case ExpressionType.MemberAccess:
                        return VisitMember((MemberExpression)oExpr, oResult);
                    case ExpressionType.Constant:
                        return VisitConstant((ConstantExpression)oExpr, oResult);
                    case ExpressionType.Call:
                        return VisitMethodCall((MethodCallExpression)oExpr, oResult);
                    case ExpressionType.Convert:
                        return VisitConvert((UnaryExpression)oExpr, oResult);
                    case ExpressionType.Not:
                        return VisitNot((UnaryExpression)oExpr, oResult);
                    case ExpressionType.Negate:
                    case ExpressionType.NegateChecked:
                        return VisitUnaryMinus((UnaryExpression)oExpr, oResult);
                    case ExpressionType.Conditional:
                        return VisitConditional((ConditionalExpression)oExpr, oResult);
                    case ExpressionType.Invoke:
                        return VisitInvoke((InvocationExpression)oExpr, oResult);
                    default:
                        throw new NotSupportedException($"Expression type '{oExpr.NodeType}' is not supported.");
                }
            }
            catch (Exception oEx)
            {
                throw new InvalidOperationException($"Error processing expression: {oExpr}. See inner exception for details.", oEx);
            }
        }

        private string VisitInvoke(InvocationExpression oNode, SqlWithParameters oResult)
        {
            // ساده‌سازی فراخوانی Expression با ارزیابی آن
            try
            {
                var oLambda = Expression.Lambda(oNode);
                var oCompiled = oLambda.Compile();
                var oValue = oCompiled.DynamicInvoke();
                return VisitConstant(Expression.Constant(oValue), oResult);
            }
            catch (Exception oEx)
            {
                throw new NotSupportedException("Expression invocation could not be evaluated.", oEx);
            }
        }

        private string VisitBinary(BinaryExpression oNode, string sOperatorStr, SqlWithParameters oResult)
        {
            string sLeft = Visit(oNode.Left, oResult);
            string sRight = Visit(oNode.Right, oResult);
            if (sRight == "NULL")
                return sOperatorStr == "=" ? $"{sLeft} IS NULL" : $"{sLeft} IS NOT NULL";
            if (sLeft == "NULL")
                return sOperatorStr == "=" ? $"{sRight} IS NULL" : $"{sRight} IS NOT NULL";
            return $"({sLeft} {sOperatorStr} {sRight})";
        }

        private string VisitBinaryArithmetic(Expression oNode, string sOperatorStr, SqlWithParameters oResult)
        {
            BinaryExpression oBinary = (BinaryExpression)oNode;
            string sLeft = Visit(oBinary.Left, oResult);
            string sRight = Visit(oBinary.Right, oResult);
            return $"({sLeft} {sOperatorStr} {sRight})";
        }

        private string VisitBinaryBitwise(Expression oExpression, string sOperatorStr, SqlWithParameters oResult)
        {
            BinaryExpression oBinary = (BinaryExpression)oExpression;
            string sLeft = Visit(oBinary.Left, oResult);
            string sRight = Visit(oBinary.Right, oResult);
            return $"({sLeft} {sOperatorStr} {sRight})";
        }

        private string VisitCoalesce(BinaryExpression oNode, SqlWithParameters oResult)
        {
            string sLeft = Visit(oNode.Left, oResult);
            string sRight = Visit(oNode.Right, oResult);
            return $"COALESCE({sLeft}, {sRight})";
        }

        private string VisitMember(MemberExpression oNode, SqlWithParameters oResult)
        {
            if (oNode.Expression is ParameterExpression oParamExpr && oParamExpr.Type == typeof(T))
            {
                return $"[{oNode.Member.Name}]";
            }

            if (oNode.Expression is MemberExpression oInnerMember &&
                oInnerMember.Type == typeof(DateTime) &&
                oNode.Member.DeclaringType == typeof(DateTime))
            {
                string sInner = Visit(oInnerMember, oResult);
                string sMemberName = oNode.Member.Name;
                switch (sMemberName)
                {
                    case "Year": return $"CAST(STRFTIME('%Y', {sInner}) AS INTEGER)";
                    case "Month": return $"CAST(STRFTIME('%m', {sInner}) AS INTEGER)";
                    case "Day": return $"CAST(STRFTIME('%d', {sInner}) AS INTEGER)";
                    case "Hour": return $"CAST(STRFTIME('%H', {sInner}) AS INTEGER)";
                    case "Minute": return $"CAST(STRFTIME('%M', {sInner}) AS INTEGER)";
                    case "Second": return $"CAST(STRFTIME('%S', {sInner}) AS INTEGER)";
                    case "DayOfWeek": return $"CAST(STRFTIME('%w', {sInner}) AS INTEGER)";
                    case "DayOfYear": return $"CAST(STRFTIME('%j', {sInner}) AS INTEGER)";
                    case "Date": return $"DATE({sInner})";
                    default:
                        throw new NotSupportedException($"DateTime property '{sMemberName}' is not supported.");
                }
            }

            if (oNode.Member.DeclaringType != null &&
                oNode.Member.DeclaringType.IsGenericType &&
                oNode.Member.DeclaringType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                if (oNode.Member.Name == "HasValue")
                {
                    string sInner = Visit(oNode.Expression, oResult);
                    return $"({sInner} IS NOT NULL)";
                }
                if (oNode.Member.Name == "Value")
                {
                    return Visit(oNode.Expression, oResult);
                }
            }

            try
            {
                var oCompiled = GetCompiledMemberExpression(oNode);
                var oValue = oCompiled();
                return VisitConstant(Expression.Constant(oValue, oNode.Type), oResult);
            }
            catch (Exception oEx)
            {
                throw new InvalidOperationException($"Could not evaluate member expression: {oNode.Member.Name}", oEx);
            }
        }

        private Func<object> GetCompiledMemberExpression(MemberExpression oNode)
        {
            lock (_oMemberCacheLock)
            {
                if (!_oMemberCache.TryGetValue(oNode, out Lazy<Func<object>> oLazy))
                {
                    oLazy = new Lazy<Func<object>>(() =>
                    {
                        var oLambda = Expression.Lambda<Func<object>>(Expression.Convert(oNode, typeof(object)));
                        return oLambda.Compile();
                    }, LazyThreadSafetyMode.ExecutionAndPublication);

                    _oMemberCache[oNode] = oLazy;
                }
                return oLazy.Value;
            }
        }

        private string VisitConstant(ConstantExpression oExpr, SqlWithParameters oResult)
        {
            if (oExpr.Value == null)
                return "NULL";

            if (oExpr.Value is IEnumerable oEnumerable &&
                !(oExpr.Value is string) &&
                !(oExpr.Value is byte[]))
            {
                var oSb = new StringBuilder();
                oSb.Append('(');
                bool bFirst = true;
                foreach (var oItem in oEnumerable)
                {
                    if (!bFirst) oSb.Append(", ");
                    oSb.Append(VisitConstant(Expression.Constant(oItem), oResult));
                    bFirst = false;
                }
                if (bFirst) oSb.Append("NULL");
                oSb.Append(')');
                return oSb.ToString();
            }

            string sParamName = $"@p{_iParamIndex++}";
            oResult.Parameters[sParamName] = oExpr.Value;
            return sParamName;
        }

        private string FormatValue(object oValue)
        {
            if (oValue == null) return "NULL";

            var oFormatter = GetValueFormatter(oValue.GetType());
            return oFormatter(oValue);
        }

        private Func<object, string> GetValueFormatter(Type oType)
        {
            lock (_oValueFormatterCacheLock)
            {
                if (!_oValueFormatterCache.TryGetValue(oType, out Lazy<Func<object, string>> oLazy))
                {
                    oLazy = new Lazy<Func<object, string>>(() =>
                    {
                        if (oType == typeof(string))
                            return oVal => $"'{((string)oVal).Replace("'", "''")}'";
                        if (oType == typeof(bool))
                            return oVal => (bool)oVal ? "1" : "0";
                        if (oType == typeof(DateTime))
                            return oVal => $"'{((DateTime)oVal):yyyy-MM-dd HH:mm:ss}'";
                        if (oType == typeof(byte[]))
                            return oVal => "X'" + BitConverter.ToString((byte[])oVal).Replace("-", "") + "'";
                        if (oType == typeof(Guid))
                            return oVal => $"'{oVal}'";
                        if (oType == typeof(TimeSpan))
                            return oVal => $"'{oVal}'";
                        if (typeof(IFormattable).IsAssignableFrom(oType))
                            return oVal => ((IFormattable)oVal).ToString(null, CultureInfo.InvariantCulture);
                        return oVal => oVal.ToString();
                    }, LazyThreadSafetyMode.ExecutionAndPublication);

                    _oValueFormatterCache[oType] = oLazy;
                }
                return oLazy.Value;
            }
        }

        private string VisitMethodCall(MethodCallExpression oEx, SqlWithParameters oResult)
        {
            if (oEx.Object == null)
            {
                var oMethodKey = oEx.Method;
                var sCachedMethod = GetCachedMethod(oMethodKey);

                if (sCachedMethod != null)
                {
                    string sTarget = Visit(oEx.Arguments[0], oResult);
                    switch (sCachedMethod)
                    {
                        case "NULL_OR_EMPTY":
                            return $"({sTarget} IS NULL OR {sTarget} = '')";
                        case "NULL_OR_WHITE_SPACE":
                            return $"({sTarget} IS NULL OR TRIM({sTarget}) = '')";
                    }
                }
            }

            if (oEx.Object != null && oEx.Object.Type == typeof(string))
            {
                string sTarget = Visit(oEx.Object, oResult);
                string[] aArgs = oEx.Arguments.Select(arg => Visit(arg, oResult)).ToArray();
                string sMethodName = oEx.Method.Name;
                switch (sMethodName)
                {
                    case "Contains": return $"{sTarget} LIKE '%' || {aArgs[0]} || '%'";
                    case "StartsWith": return $"{sTarget} LIKE {aArgs[0]} || '%'";
                    case "EndsWith": return $"{sTarget} LIKE '%' || {aArgs[0]}";
                    case "Equals": return $"{sTarget} = {aArgs[0]}";
                    case "Trim": return $"TRIM({sTarget})";
                    case "ToUpper": return $"UPPER({sTarget})";
                    case "ToLower": return $"LOWER({sTarget})";
                    case "Replace": return $"REPLACE({sTarget}, {aArgs[0]}, {aArgs[1]})";
                    case "Substring":
                        if (aArgs.Length == 1)
                            return $"SUBSTR({sTarget}, {aArgs[0]} + 1)";
                        else if (aArgs.Length == 2)
                            return $"SUBSTR({sTarget}, {aArgs[0]} + 1, {aArgs[1]})";
                        else
                            throw new NotSupportedException("Substring with more than 2 parameters is not supported.");
                    case "get_Length": return $"LENGTH({sTarget})";
                    case "IndexOf":
                        if (aArgs.Length == 1)
                            return $"INSTR({sTarget}, {aArgs[0]}) - 1";
                        else
                            throw new NotSupportedException("IndexOf with parameters is not supported.");
                    default:
                        throw new NotSupportedException($"String method '{sMethodName}' is not supported.");
                }
            }

            if (oEx.Method.DeclaringType == typeof(Enumerable) && oEx.Method.Name == "Contains")
            {
                return HandleEnumerableContains(oEx, oResult);
            }

            if (oEx.Method.Name == "Contains" && oEx.Object != null)
            {
                return HandleInstanceContains(oEx, oResult);
            }

            if (oEx.Method.DeclaringType == typeof(Math))
            {
                string[] aArgs = oEx.Arguments.Select(arg => Visit(arg, oResult)).ToArray();
                string sMethodName = oEx.Method.Name;
                switch (sMethodName)
                {
                    case "Abs": return $"ABS({aArgs[0]})";
                    case "Round":
                        if (aArgs.Length == 1)
                            return $"ROUND({aArgs[0]})";
                        else if (aArgs.Length == 2)
                            return $"ROUND({aArgs[0]}, {aArgs[1]})";
                        else
                            throw new NotSupportedException("Round with more than 2 parameters is not supported.");
                    case "Ceiling": return $"CEIL({aArgs[0]})";
                    case "Floor": return $"FLOOR({aArgs[0]})";
                    default:
                        throw new NotSupportedException($"Math method '{sMethodName}' is not supported.");
                }
            }

            if (oEx.Object is MethodCallExpression)
            {
                string sInner = Visit(oEx.Object, oResult);
                string sMethodName = oEx.Method.Name;
                switch (sMethodName)
                {
                    case "Trim": return $"TRIM({sInner})";
                    case "ToUpper": return $"UPPER({sInner})";
                    case "ToLower": return $"LOWER({sInner})";
                    default:
                        throw new NotSupportedException($"Chained method '{sMethodName}' is not supported.");
                }
            }

            throw new NotSupportedException($"Method '{oEx.Method.Name}' on type '{(oEx.Method.DeclaringType != null ? oEx.Method.DeclaringType.Name : "unknown")}' is not supported.");
        }

        private string GetCachedMethod(MethodInfo oMethod)
        {
            lock (_oMethodCacheLock)
            {
                if (!_oMethodCache.TryGetValue(oMethod, out Lazy<string> oLazy))
                {
                    oLazy = new Lazy<string>(() =>
                    {
                        if (oMethod.DeclaringType == typeof(string))
                        {
                            if (oMethod.Name == "IsNullOrEmpty") return "NULL_OR_EMPTY";
                            if (oMethod.Name == "IsNullOrWhiteSpace") return "NULL_OR_WHITE_SPACE";
                        }
                        return null;
                    }, LazyThreadSafetyMode.ExecutionAndPublication);

                    _oMethodCache[oMethod] = oLazy;
                }
                return oLazy.Value;
            }
        }

        private string HandleEnumerableContains(MethodCallExpression oExpression, SqlWithParameters oResult)
        {
            try
            {
                var oCollection = Expression.Lambda(oExpression.Arguments[0]).Compile().DynamicInvoke();
                if (oCollection == null)
                    return "1=0";
                string sValues = VisitConstant(Expression.Constant(oCollection), oResult);
                string sItem = Visit(oExpression.Arguments[1], oResult);
                return $"{sItem} IN {sValues}";
            }
            catch (Exception oEx)
            {
                throw new NotSupportedException("Collection in Enumerable.Contains must be evaluable.", oEx);
            }
        }

        private string HandleInstanceContains(MethodCallExpression oNode, SqlWithParameters oResult)
        {
            try
            {
                var oCollection = Expression.Lambda(oNode.Object).Compile().DynamicInvoke();
                if (oCollection == null)
                    return "1=0";
                string sValues = VisitConstant(Expression.Constant(oCollection), oResult);
                string sItem = Visit(oNode.Arguments[0], oResult);
                return $"{sItem} IN {sValues}";
            }
            catch (Exception oEx)
            {
                throw new NotSupportedException("Collection in instance.Contains must be evaluable.", oEx);
            }
        }

        private string VisitConvert(UnaryExpression oExpr, SqlWithParameters oResult)
        {
            return Visit(oExpr.Operand, oResult);
        }

        private string VisitNot(UnaryExpression oNode, SqlWithParameters oResult)
        {
            string sOperand = Visit(oNode.Operand, oResult);
            if (oNode.Type == typeof(bool) || oNode.Type == typeof(bool?))
                return $"NOT ({sOperand})";
            else
                return $"~({sOperand})";
        }

        private string VisitUnaryMinus(UnaryExpression oExpression, SqlWithParameters oResult)
        {
            string sOperand = Visit(oExpression.Operand, oResult);
            return $"-({sOperand})";
        }

        private string VisitConditional(ConditionalExpression oNode, SqlWithParameters oResult)
        {
            string sTest = Visit(oNode.Test, oResult);
            string sIfTrue = Visit(oNode.IfTrue, oResult);
            string sIfFalse = Visit(oNode.IfFalse, oResult);
            return $"(CASE WHEN {sTest} THEN {sIfTrue} ELSE {sIfFalse} END)";
        }
    }
}