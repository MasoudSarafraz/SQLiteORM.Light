using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

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
        // استفاده از WeakReference
        private static readonly ConcurrentDictionary<WeakReference, SqlWithParameters> _Cache =new ConcurrentDictionary<WeakReference, SqlWithParameters>();
        // محدود کردن اندازه کش برای جلوگیری از رشد بی‌نهایت
        private const int MaxCacheSize = 1000;
        private static int _cacheCount = 0;
        private static readonly ConcurrentDictionary<MemberExpression, Func<object>> _MemberCache =new ConcurrentDictionary<MemberExpression, Func<object>>();
        private static readonly ConcurrentDictionary<MethodInfo, string> _MethodCache =new ConcurrentDictionary<MethodInfo, string>();
        private static readonly ConcurrentDictionary<Type, Func<object, string>> _ValueFormatterCache =new ConcurrentDictionary<Type, Func<object, string>>();
        private int _ParameterIndex = 0;
        public SqlWithParameters ConvertToSqlWithParameters(Expression<Func<T, bool>> oExpression)
        {
            if (oExpression == null)
                return new SqlWithParameters { Sql = "1=1" };
            CheckCacheSize();
            // ایجاد WeakReference برای Expression
            var oWeakRef = new WeakReference(oExpression.Body);
            return _Cache.GetOrAdd(oWeakRef, wr =>
            {
                Expression oBody;
                if (wr.IsAlive && wr.Target is Expression oExpr)
                {
                    oBody = oExpr;
                }
                else
                {
                    _ParameterIndex = 0;
                    var oResult = new SqlWithParameters();
                    oResult.Sql = Visit(oExpression.Body, oResult);
                    return oResult;
                }
                _ParameterIndex = 0;
                var oNewResult = new SqlWithParameters();
                oNewResult.Sql = Visit(oBody, oNewResult);
                return oNewResult;
            });
        }

        private void CheckCacheSize()
        {
            if (_cacheCount >= MaxCacheSize)
            {
                var keysToRemove = new List<WeakReference>();
                foreach (var kvp in _Cache)
                {
                    if (!kvp.Key.IsAlive)
                    {
                        keysToRemove.Add(kvp.Key);
                    }
                }

                foreach (var key in keysToRemove)
                {
                    SqlWithParameters _;
                    _Cache.TryRemove(key, out _);
                    _cacheCount--;
                }
                if (_cacheCount >= MaxCacheSize)
                {
                    var oKeys = _Cache.Keys.ToList();
                    var iRemoveCount = (int)(oKeys.Count * 0.2);
                    var oRandom = new Random();

                    for (int i = 0; i < iRemoveCount; i++)
                    {
                        var index = oRandom.Next(oKeys.Count);
                        var key = oKeys[index];
                        SqlWithParameters _;
                        _Cache.TryRemove(key, out _);
                        oKeys.RemoveAt(index);
                        _cacheCount--;
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
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error processing expression: {oExpr}. See inner exception for details.", ex);
            }
        }

        private string VisitInvoke(InvocationExpression oNode, SqlWithParameters oResult)
        {
            // ساده‌سازی فراخوانی Expression با ارزیابی آن
            try
            {
                var lambda = Expression.Lambda(oNode);
                var compiled = lambda.Compile();
                var value = compiled.DynamicInvoke();
                return VisitConstant(Expression.Constant(value), oResult);
            }
            catch (Exception ex)
            {
                throw new NotSupportedException("Expression invocation could not be evaluated.", ex);
            }
        }

        private string VisitBinary(BinaryExpression oNode, string oOperatorStr, SqlWithParameters oResult)
        {
            string oLeft = Visit(oNode.Left, oResult);
            string oRight = Visit(oNode.Right, oResult);
            if (oRight == "NULL")
                return oOperatorStr == "=" ? $"{oLeft} IS NULL" : $"{oLeft} IS NOT NULL";
            if (oLeft == "NULL")
                return oOperatorStr == "=" ? $"{oRight} IS NULL" : $"{oRight} IS NOT NULL";
            return $"({oLeft} {oOperatorStr} {oRight})";
        }

        private string VisitBinaryArithmetic(Expression oNode, string oOperatorStr, SqlWithParameters oResult)
        {
            BinaryExpression oBinary = (BinaryExpression)oNode;
            string oLeft = Visit(oBinary.Left, oResult);
            string oRight = Visit(oBinary.Right, oResult);
            return $"({oLeft} {oOperatorStr} {oRight})";
        }

        private string VisitBinaryBitwise(Expression oExpression, string oOperatorStr, SqlWithParameters oResult)
        {
            BinaryExpression oBinary = (BinaryExpression)oExpression;
            string oLeft = Visit(oBinary.Left, oResult);
            string oRight = Visit(oBinary.Right, oResult);
            return $"({oLeft} {oOperatorStr} {oRight})";
        }

        private string VisitCoalesce(BinaryExpression oNode, SqlWithParameters oResult)
        {
            string oLeft = Visit(oNode.Left, oResult);
            string oRight = Visit(oNode.Right, oResult);
            return $"COALESCE({oLeft}, {oRight})";
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
                string oInner = Visit(oInnerMember, oResult);
                string sMemberName = oNode.Member.Name;

                switch (sMemberName)
                {
                    case "Year": return $"CAST(STRFTIME('%Y', {oInner}) AS INTEGER)";
                    case "Month": return $"CAST(STRFTIME('%m', {oInner}) AS INTEGER)";
                    case "Day": return $"CAST(STRFTIME('%d', {oInner}) AS INTEGER)";
                    case "Hour": return $"CAST(STRFTIME('%H', {oInner}) AS INTEGER)";
                    case "Minute": return $"CAST(STRFTIME('%M', {oInner}) AS INTEGER)";
                    case "Second": return $"CAST(STRFTIME('%S', {oInner}) AS INTEGER)";
                    case "DayOfWeek": return $"CAST(STRFTIME('%w', {oInner}) AS INTEGER)";
                    case "DayOfYear": return $"CAST(STRFTIME('%j', {oInner}) AS INTEGER)";
                    case "Date": return $"DATE({oInner})";
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
                    string oInner = Visit(oNode.Expression, oResult);
                    return $"({oInner} IS NOT NULL)";
                }
                if (oNode.Member.Name == "Value")
                {
                    return Visit(oNode.Expression, oResult);
                }
            }
            try
            {
                var oCompiled = _MemberCache.GetOrAdd(oNode, node =>
                {
                    var lambda = Expression.Lambda<Func<object>>(Expression.Convert(node, typeof(object)));
                    return lambda.Compile();
                });
                var oValue = oCompiled();
                return VisitConstant(Expression.Constant(oValue, oNode.Type), oResult);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Could not evaluate member expression: {oNode.Member.Name}", ex);
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
            string sParamName = $"@p{_ParameterIndex++}";
            oResult.Parameters[sParamName] = oExpr.Value;
            return sParamName;
        }

        private string FormatValue(object oValue)
        {
            if (oValue == null) return "NULL";
            var formatter = _ValueFormatterCache.GetOrAdd(oValue.GetType(), type =>
            {
                if (type == typeof(string))
                    return value => $"'{((string)value).Replace("'", "''")}'";
                if (type == typeof(bool))
                    return value => (bool)value ? "1" : "0";
                if (type == typeof(DateTime))
                    return value => $"'{((DateTime)value):yyyy-MM-dd HH:mm:ss}'";
                if (type == typeof(byte[]))
                    return value => "X'" + BitConverter.ToString((byte[])value).Replace("-", "") + "'";
                if (type == typeof(Guid))
                    return value => $"'{value}'";
                if (type == typeof(TimeSpan))
                    return value => $"'{value}'";
                if (typeof(IFormattable).IsAssignableFrom(type))
                    return value => ((IFormattable)value).ToString(null, CultureInfo.InvariantCulture);
                return value => value.ToString();
            });

            return formatter(oValue);
        }

        private string VisitMethodCall(MethodCallExpression oEx, SqlWithParameters oResult)
        {
            if (oEx.Object == null)
            {
                var methodKey = oEx.Method;
                var cachedMethod = _MethodCache.GetOrAdd(methodKey, method =>
                {
                    if (method.DeclaringType == typeof(string))
                    {
                        if (method.Name == "IsNullOrEmpty") return "NULL_OR_EMPTY";
                        if (method.Name == "IsNullOrWhiteSpace") return "NULL_OR_WHITE_SPACE";
                    }
                    return null;
                });

                if (cachedMethod != null)
                {
                    string sTarget = Visit(oEx.Arguments[0], oResult);
                    switch (cachedMethod)
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
                string oInner = Visit(oEx.Object, oResult);
                string sMethodName = oEx.Method.Name;

                switch (sMethodName)
                {
                    case "Trim": return $"TRIM({oInner})";
                    case "ToUpper": return $"UPPER({oInner})";
                    case "ToLower": return $"LOWER({oInner})";
                    default:
                        throw new NotSupportedException($"Chained method '{sMethodName}' is not supported.");
                }
            }

            throw new NotSupportedException($"Method '{oEx.Method.Name}' on type '{(oEx.Method.DeclaringType != null ? oEx.Method.DeclaringType.Name : "unknown")}' is not supported.");
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
            string oOperand = Visit(oNode.Operand, oResult);
            if (oNode.Type == typeof(bool) || oNode.Type == typeof(bool?))
                return $"NOT ({oOperand})";
            else
                return $"~({oOperand})";
        }

        private string VisitUnaryMinus(UnaryExpression oExpression, SqlWithParameters oResult)
        {
            string oOperand = Visit(oExpression.Operand, oResult);
            return $"-({oOperand})";
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