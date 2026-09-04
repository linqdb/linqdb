using ServerSharedData;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbClientInternal
{
    public partial class Ldb
    {
        public ClientResult Intersect<T, TKey>(Expression<Func<T, TKey>> keySelector, HashSet<TKey> set)
        {
            if (set.Any(f => f == null))
            {
                MemberExpression memberExpression = (keySelector.Body as MemberExpression) ?? ((keySelector.Body as UnaryExpression).Operand as MemberExpression);
                if (!(memberExpression.Member is PropertyInfo propertyInfo))
                {
                    throw new ArgumentException("The member accessed by the expression is not a property.", nameof(keySelector));
                }
                Type propertyType = propertyInfo.PropertyType;
                bool isNullable = !propertyType.IsValueType || Nullable.GetUnderlyingType(propertyType) != null;

                if (!isNullable)
                {
                    throw new LinqDbException("Linqdb: Non-nullable type is intersected with null value.");
                }
            }

            var res = new ClientResult();
            if (typeof(TKey) == typeof(int) || Nullable.GetUnderlyingType(typeof(TKey)) == typeof(int))
            {
                res.Int_set = new HashSet<int>();
                foreach (var s in set)
                {
                    if (s == null)
                    {
                        res.Int_null = true;
                    }
                    else
                    {
                        res.Int_set.Add(Convert.ToInt32(s));
                    }
                }
            }
            else if (typeof(TKey) == typeof(double) || Nullable.GetUnderlyingType(typeof(TKey)) == typeof(double))
            {
                res.Double_set = new HashSet<double>();
                foreach (var s in set)
                {
                    if (s == null)
                    {
                        res.Double_null = true;
                    }
                    else
                    {
                        res.Double_set.Add(Convert.ToDouble(s));
                    }
                }
            }
            else if (typeof(TKey) == typeof(decimal) || Nullable.GetUnderlyingType(typeof(TKey)) == typeof(decimal))
            {
                res.Decimal_set = new HashSet<decimal>();
                foreach (var s in set)
                {
                    if (s == null)
                    {
                        res.Decimal_null = true;
                    }
                    else
                    {
                        res.Decimal_set.Add(Convert.ToDecimal(s));
                    }
                }
            }
            else if (typeof(TKey) == typeof(long) || Nullable.GetUnderlyingType(typeof(TKey)) == typeof(long))
            {
                res.Long_set = new HashSet<long>();
                foreach (var s in set)
                {
                    if (s == null)
                    {
                        res.Long_null = true;
                    }
                    else
                    {
                        res.Long_set.Add(Convert.ToInt64(s));
                    }
                }
            }
            else if (typeof(TKey) == typeof(DateTime) || Nullable.GetUnderlyingType(typeof(TKey)) == typeof(DateTime))
            {
                res.Date_set = new HashSet<double>();
                foreach (var s in set)
                {
                    if (s == null)
                    {
                        res.Date_null = true;
                    }
                    else
                    {
                        var val = (Convert.ToDateTime(s) - new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
                        res.Date_set.Add(val);
                    }
                }
            }
            else
            {
                res.String_set = new HashSet<string>();
                foreach (var s in set)
                {
                    if (s == null)
                    {
                        res.String_null = true;
                    }
                    else
                    {
                        res.String_set.Add(Convert.ToString(s));
                    }
                }
            }


            var name = SharedUtils.GetPropertyName(keySelector);
            res.Selector = name;
            res.Type = "intersect";
            return res;
        }
        //public ClientResult Intersect<T, TKey>(Expression<Func<T, TKey>> keySelector, HashSet<int> set)
        //{
        //    var res = new ClientResult();
        //    res.Int_set = new HashSet<int>();
        //    foreach (var s in set)
        //    {
        //        res.Int_set.Add(s);
        //    }
        //    var par = keySelector.Parameters.First();
        //    var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
        //    res.Selector = name;
        //    res.Type = "intersect";
        //    return res;
        //}
        //public ClientResult Intersect<T, TKey>(Expression<Func<T, TKey>> keySelector, HashSet<double?> set)
        //{
        //    var res = new ClientResult();
        //    res.Double_set = new HashSet<double>();
        //    foreach (var s in set)
        //    {
        //        if (s == null)
        //        {
        //            res.Double_null = true;
        //        }
        //        else
        //        {
        //            res.Double_set.Add((double)s);
        //        }
        //    }
        //    var par = keySelector.Parameters.First();
        //    var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
        //    res.Selector = name;
        //    res.Type = "intersect";
        //    return res;
        //}
        //public ClientResult Intersect<T, TKey>(Expression<Func<T, TKey>> keySelector, HashSet<double> set)
        //{
        //    var res = new ClientResult();
        //    res.Double_set = new HashSet<double>();
        //    foreach (var s in set)
        //    {
        //        res.Double_set.Add(s);
        //    }
        //    var par = keySelector.Parameters.First();
        //    var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
        //    res.Selector = name;
        //    res.Type = "intersect";
        //    return res;
        //}
        //public ClientResult Intersect<T, TKey>(Expression<Func<T, TKey>> keySelector, HashSet<decimal?> set)
        //{
        //    var res = new ClientResult();
        //    res.Decimal_set = new HashSet<decimal>();
        //    foreach (var s in set)
        //    {
        //        if (s == null)
        //        {
        //            res.Decimal_null = true;
        //        }
        //        else
        //        {
        //            res.Decimal_set.Add((decimal)s);
        //        }
        //    }
        //    var par = keySelector.Parameters.First();
        //    var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
        //    res.Selector = name;
        //    res.Type = "intersect";
        //    return res;
        //}
        //public ClientResult Intersect<T, TKey>(Expression<Func<T, TKey>> keySelector, HashSet<decimal> set)
        //{
        //    var res = new ClientResult();
        //    res.Decimal_set = new HashSet<decimal>();
        //    foreach (var s in set)
        //    {
        //        res.Decimal_set.Add(s);
        //    }
        //    var par = keySelector.Parameters.First();
        //    var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
        //    res.Selector = name;
        //    res.Type = "intersect";
        //    return res;
        //}
        //public ClientResult Intersect<T, TKey>(Expression<Func<T, TKey>> keySelector, HashSet<long?> set)
        //{
        //    var res = new ClientResult();
        //    res.Long_set = new HashSet<long>();
        //    foreach (var s in set)
        //    {
        //        if (s == null)
        //        {
        //            res.Long_null = true;
        //        }
        //        else
        //        {
        //            res.Long_set.Add((long)s);
        //        }
        //    }
        //    var par = keySelector.Parameters.First();
        //    var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
        //    res.Selector = name;
        //    res.Type = "intersect";
        //    return res;
        //}
        //public ClientResult Intersect<T, TKey>(Expression<Func<T, TKey>> keySelector, HashSet<long> set)
        //{
        //    var res = new ClientResult();
        //    res.Long_set = new HashSet<long>();
        //    foreach (var s in set)
        //    {
        //        res.Long_set.Add(s);
        //    }
        //    var par = keySelector.Parameters.First();
        //    var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
        //    res.Selector = name;
        //    res.Type = "intersect";
        //    return res;
        //}
        //public ClientResult Intersect<T, TKey>(Expression<Func<T, TKey>> keySelector, HashSet<DateTime?> set)
        //{
        //    var res = new ClientResult();
        //    res.Date_set = new HashSet<double>();
        //    foreach (var s in set)
        //    {
        //        if (s == null)
        //        {
        //            res.Date_null = true;
        //        }
        //        else
        //        {
        //            var val = ((DateTime)s - new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
        //            res.Date_set.Add(val);
        //        }
        //    }
        //    var par = keySelector.Parameters.First();
        //    var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
        //    res.Selector = name;
        //    res.Type = "intersect";
        //    return res;
        //}
        //public ClientResult Intersect<T, TKey>(Expression<Func<T, TKey>> keySelector, HashSet<DateTime> set)
        //{
        //    var res = new ClientResult();
        //    res.Date_set = new HashSet<double>();
        //    foreach (var s in set)
        //    {
        //        var val = (s - new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
        //        res.Date_set.Add(val);
        //    }
        //    var par = keySelector.Parameters.First();
        //    var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
        //    res.Selector = name;
        //    res.Type = "intersect";
        //    return res;
        //}
        //public ClientResult Intersect<T, TKey>(Expression<Func<T, TKey>> keySelector, HashSet<string> set)
        //{
        //    var res = new ClientResult();
        //    res.String_set = new HashSet<string>();
        //    foreach (var s in set)
        //    {
        //        if (s == null)
        //        {
        //            res.String_null = true;
        //        }
        //        else
        //        {
        //            res.String_set.Add(s);
        //        }
        //    }
        //    var par = keySelector.Parameters.First();
        //    var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
        //    res.Selector = name;
        //    res.Type = "intersect";
        //    return res;
        //}
    }
}
