using ServerSharedData;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using static ServerSharedData.SharedUtils;

namespace LinqDbClientInternal
{
    public partial class Ldb
    {
        public ClientResult Where<T>(Expression<Func<T, bool>> predicate)
        {
            var res = new ClientResult();
            res.Type = "where";

            Stack<Oper> wstack = new Stack<Oper>();
            ParseBinExpr(predicate.Body as BinaryExpression, predicate.Parameters.ToList(), wstack, typeof(T));
            
            List<SharedOper> opers = new List<SharedOper>();            
            res.Opers = opers;
            while (wstack.Any())
            {
                var op = wstack.Pop();
                byte[] ndb = null;

                if (!op.IsDb && !op.IsOperator)
                {
                    var column = wstack.Peek();
                    op.ColumnName = column.ColumnName;
                }
                if (!op.IsOperator && op.NonDbValue != null)
                {
                    if (op.ColumnName == null)
                    {
                        throw new LinqDbException("Linqdb: error in Where statement, probably field selector is used in the expression. That's not supported, i.e. .Where(f => f.Value % 2 == 0) won't work.");
                    }

                    var def = GetTableDefinition<T>();
                    var type = StringTypeToLinqType(def.Item1[op.ColumnName]);
                    if (type == LinqDbTypes.int_)
                    {
                        ndb = BitConverter.GetBytes((int)op.NonDbValue);
                    }
                    else if (type == LinqDbTypes.long_)
                    {
                        ndb = BitConverter.GetBytes((long)op.NonDbValue);
                    }
                    else if (type == LinqDbTypes.double_)
                    {
                        ndb = BitConverter.GetBytes(Convert.ToDouble(op.NonDbValue));
                    }
                    else if (type == LinqDbTypes.decimal_)
                    {
                        ndb = DecimalConversion.ToByteArray(Convert.ToDecimal(op.NonDbValue));
                    }
                    else if (type == LinqDbTypes.string_)
                    {
                        ndb = Encoding.UTF8.GetBytes((string)op.NonDbValue);
                    }
                    else if (type == LinqDbTypes.DateTime_)
                    {
                        var ms = ((DateTime)op.NonDbValue - new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
                        ndb = BitConverter.GetBytes(ms);
                    }
                    else if (type == LinqDbTypes.binary_ || type == LinqDbTypes.vector_)
                    {
                        ndb = (byte[])op.NonDbValue;
                    }
                }

                var sop = new SharedOper()
                {
                    ColumnName = op.ColumnName,
                    IsDb = op.IsDb,
                    IsOperator = op.IsOperator,
                    NonDbValue = ndb,
                    IsResult = op.IsResult,
                    Type = (short)op.Type
                };
                opers.Add(sop);
            }
            opers.Reverse();
            for(short i = 0; i < opers.Count; i++)
            {
                opers[i].Id = i;
            }

            return res;
        }

        public void ParseBinExpr(BinaryExpression expr, List<ParameterExpression> pars, Stack<Oper> stack, Type table_type)
        {
            var op = new Oper()
            {
                IsOperator = true,
                Type = expr.NodeType
            };
            stack.Push(op);

            var left = expr.Left;
            var right = expr.Right;
            if (left is BinaryExpression)
            {
                ParseBinExpr(left as BinaryExpression, pars, stack, table_type);
            }
            else if (left.NodeType != ExpressionType.MemberAccess)
            {
                throw new LinqDbException("Linqdb: wrong .Where clause - left hand side of a binary expression must be a member access only. Types must match.");
            }
            else
            {
                op = FillOpData(left, pars, table_type);
                stack.Push(op);
            }
            if (right is BinaryExpression && IsLogicalOrComparison(right as BinaryExpression))
            {
                ParseBinExpr(right as BinaryExpression, pars, stack, table_type);
            }
            else
            {
                try
                {
                    object tmp_val = null;
                    if (right.NodeType == ExpressionType.Constant)
                    {
                        tmp_val = (right as ConstantExpression).Value;
                    }
                    else
                    {
                        tmp_val = EvaluateExpression(right);
                    }

                    op = new Oper()
                    {
                        IsDb = false,
                        IsOperator = false,
                        NonDbValue = tmp_val
                    };
                    stack.Push(op);
                    return;
                }
                catch
                {
                    throw new LinqDbException("Linqdb: error in Where clause.");
                }
            }

            object EvaluateExpression(Expression expression)
            {
                var lambda = Expression.Lambda(expression);
                var compiledLambda = lambda.Compile();
                var res = compiledLambda.DynamicInvoke();

                return res;
            }
        }

        static bool IsLogicalOrComparison(BinaryExpression expression)
        {
            return expression.NodeType == ExpressionType.AndAlso ||
                   expression.NodeType == ExpressionType.OrElse ||
                   expression.NodeType == ExpressionType.Equal ||
                   expression.NodeType == ExpressionType.NotEqual ||
                   expression.NodeType == ExpressionType.GreaterThan ||
                   expression.NodeType == ExpressionType.GreaterThanOrEqual ||
                   expression.NodeType == ExpressionType.LessThan ||
                   expression.NodeType == ExpressionType.LessThanOrEqual;
        }
        Oper FillOpData(Expression expr, List<ParameterExpression> pars, Type table_type)
        {
            var pname = pars.First().Name;
            var column_name = SharedUtils.GetPropertyNameFromBody(expr);
            var op = new Oper()
            {
                Type = expr.NodeType,
                ColumnName = column_name,
                IsOperator = false,
                IsDb = true
            };
            return op;
        }

    }

    public class Oper
    {
        public bool IsOperator { get; set; }
        public ExpressionType Type { get; set; }
        public string ColumnName { get; set; }
        public object NonDbValue { get; set; }
        public bool IsDb { get; set; }
        public short TableNumber { get; set; }
        public short ColumnNumber { get; set; }
        public bool IsResult { get; set; }
        public OperResult Result { get; set; }
        public string StringResult { get; set; }
    }

    public class OperResult
    {
        public bool Skip { get; set; }
        public bool All { get; set; }
        public HashSet<int> ResIds { get; set; }
        public bool IsOrdered { get; set; }
        public Dictionary<int, int> OrderedIds { get; set; }
        public int Id { get; set; }
        public int? OrWith { get; set; }
    }

    public class WhereInfo : BaseInfo
    {
        public Stack<Oper> Opers { get; set; }
    }

    public class BaseInfo
    {
        public int Id { get; set; }
        public int? OrWith { get; set; }
    }
}
