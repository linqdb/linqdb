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

namespace LinqDbInternal
{
    public partial class Ldb
    {
        public IDbQueryable<T> Table<T>()
        {
            return new IDbQueryable<T>() { _db = this };
        }
        public IDbQueryable<T> Table<T>(LinqdbTransactionInternal transaction)
        {
            return new IDbQueryable<T>() { _db = this, LDBTransaction = transaction };
        }
        public IDbQueryable<T> Where<T>(IDbQueryable<T> source, Expression<Func<T, bool>> predicate)
        {
            var type_name = GetTypeName<T>(source.Partition);
            CheckTableInfo<T>(source.Partition);
            Stack<Oper> stack = new Stack<Oper>();
            ParseBinExpr(predicate.Body as BinaryExpression, predicate.Parameters.ToList(), stack, typeof(T), type_name);
            if (source.LDBTree == null)
            {
                source.LDBTree = new QueryTree();
            }
            if (source.LDBTree.WhereInfo == null)
            {
                source.LDBTree.WhereInfo = new List<WhereInfo>();
            }

            var info = new WhereInfo() { Opers = stack };
            source.LDBTree.WhereInfo.Add(info);

            source.LDBTree.Prev = info;
            source.LDBTree.Prev.Id = source.LDBTree.Counter + 1;
            source.LDBTree.Counter++;
            return source;
        }

        public void ParseBinExpr(BinaryExpression expr, List<ParameterExpression> pars, Stack<Oper> stack, Type table_type, string type_name)
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
                ParseBinExpr(left as BinaryExpression, pars, stack, table_type, type_name);
            }
            else if (left.NodeType != ExpressionType.MemberAccess)
            {
                throw new LinqDbException("Linqdb: wrong .Where clause - left hand side of a binary expression must be a member access only. Types must match.");
            }
            else
            {
                op = FillOpData(left, pars, table_type, type_name);
                stack.Push(op);
            }
            if (right is BinaryExpression && IsLogicalOrComparison(right as BinaryExpression))
            {
                ParseBinExpr(right as BinaryExpression, pars, stack, table_type, type_name);
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

        Oper FillOpData(Expression expr, List<ParameterExpression> pars, Type table_type, string type_name)
        {
            var table_info = GetTableInfo(type_name);
            var pname = pars.First().Name;

            var column_name = SharedUtils.GetPropertyNameFromBody(expr);
            var op = new Oper()
            {
                Type = expr.NodeType,
                ColumnName = column_name,
                TableName = table_info.Name,
                IsOperator = false,
                IsDb = true,
                TableNumber = table_info.TableNumber,
                ColumnNumber = table_info.ColumnNumbers[column_name],
                ColumnType = table_info.Columns[column_name]
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
        public int TableNumber { get; set; }
        public short ColumnNumber { get; set; }
        public LinqDbTypes ColumnType { get; set; }
        public bool IsResult { get; set; }
        public OperResult Result { get; set; }
        public string TableName { get; set; }
    }

    public class OperResult
    {
        public bool Skip { get; set; }
        public bool All { get; set; }
        public List<int> ResIds { get; set; }
        public List<double> ResDistances { get; set; }
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
