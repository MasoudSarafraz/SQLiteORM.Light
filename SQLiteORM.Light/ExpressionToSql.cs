using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace SQLiteORM
{
    public class ExpressionToSql<T>
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
