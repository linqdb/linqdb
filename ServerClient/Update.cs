using ServerSharedData;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using static ServerSharedData.SharedUtils;

namespace LinqDbClientInternal
{
    public partial class Ldb
    {
        public ClientResult Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, TKey> values)
        {
            if (typeof(TKey) == typeof(int) || Nullable.GetUnderlyingType(typeof(TKey)) == typeof(int))
            {
                var data = new Dictionary<int, byte[]>();
                foreach (var val in values)
                {
                    data[val.Key] = val.Value == null ? null : BitConverter.GetBytes(Convert.ToInt32(val.Value));
                }
                return GenericUpdate<T, TKey>(keySelector, data);
            }
            else if (typeof(TKey) == typeof(double) || Nullable.GetUnderlyingType(typeof(TKey)) == typeof(double))
            {
                var data = new Dictionary<int, byte[]>();
                foreach (var val in values)
                {
                    data[val.Key] = val.Value == null ? null : BitConverter.GetBytes(Convert.ToDouble(val.Value));
                }
                return GenericUpdate<T, TKey>(keySelector, data);
            }
            else if (typeof(TKey) == typeof(decimal) || Nullable.GetUnderlyingType(typeof(TKey)) == typeof(decimal))
            {
                var data = new Dictionary<int, byte[]>();
                foreach (var val in values)
                {
                    data[val.Key] = val.Value == null ? null : DecimalConversion.ToByteArray(Convert.ToDecimal(val.Value));
                }
                return GenericUpdate<T, TKey>(keySelector, data);
            }
            else if (typeof(TKey) == typeof(long) || Nullable.GetUnderlyingType(typeof(TKey)) == typeof(long))
            {
                var data = new Dictionary<int, byte[]>();
                foreach (var val in values)
                {
                    data[val.Key] = val.Value == null ? null : BitConverter.GetBytes(Convert.ToInt64(val.Value));
                }
                return GenericUpdate<T, TKey>(keySelector, data);
            }
            else if (typeof(TKey) == typeof(DateTime) || Nullable.GetUnderlyingType(typeof(TKey)) == typeof(DateTime))
            {
                var data = new Dictionary<int, byte[]>();
                foreach (var val in values)
                {
                    data[val.Key] = val.Value == null ? null : BitConverter.GetBytes((Convert.ToDateTime(val.Value) - new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds);
                }
                return GenericUpdate<T, TKey>(keySelector, data);
            }
            else if (typeof(TKey) == typeof(string))
            {
                var data = new Dictionary<int, byte[]>();
                foreach (var val in values)
                {
                    data[val.Key] = val.Value == null ? null : Encoding.UTF8.GetBytes(Convert.ToString(val.Value));
                }
                return GenericUpdate<T, TKey>(keySelector, data);
            }
            else
            {
                var data = new Dictionary<int, byte[]>();
                foreach (var val in values)
                {
                    if (val.Value != null && val.Value is float[])
                    {
                        data[val.Key] = SharedUtils.FloatByteConverter.FloatArrayToByteArray(val.Value as float[]);
                    }
                    else
                    {
                        data[val.Key] = val.Value == null ? null : val.Value as byte[];
                    }
                }
                return GenericUpdate<T, TKey>(keySelector, data);
            }

        }
        //public ClientResult Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, int> values)
        //{
        //    var data = new Dictionary<int, byte[]>();
        //    foreach (var val in values)
        //    {
        //        data[val.Key] = BitConverter.GetBytes(val.Value);
        //    }
        //    return GenericUpdate<T, TKey>(keySelector, data);
        //}
        //public ClientResult Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, double?> values)
        //{
        //    var data = new Dictionary<int, byte[]>();
        //    foreach (var val in values)
        //    {
        //        data[val.Key] = val.Value == null ? null : BitConverter.GetBytes((double)val.Value);
        //    }
        //    return GenericUpdate<T, TKey>(keySelector, data);
        //}
        //public ClientResult Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, double> values)
        //{
        //    var data = new Dictionary<int, byte[]>();
        //    foreach (var val in values)
        //    {
        //        data[val.Key] = BitConverter.GetBytes(val.Value);
        //    }
        //    return GenericUpdate<T, TKey>(keySelector, data);
        //}
        //public ClientResult Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, decimal?> values)
        //{
        //    var data = new Dictionary<int, byte[]>();
        //    foreach (var val in values)
        //    {
        //        data[val.Key] = val.Value == null ? null : DecimalConversion.ToByteArray((decimal)val.Value);
        //    }
        //    return GenericUpdate<T, TKey>(keySelector, data);
        //}
        //public ClientResult Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, decimal> values)
        //{
        //    var data = new Dictionary<int, byte[]>();
        //    foreach (var val in values)
        //    {
        //        data[val.Key] = DecimalConversion.ToByteArray(val.Value);
        //    }
        //    return GenericUpdate<T, TKey>(keySelector, data);
        //}
        //public ClientResult Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, long?> values)
        //{
        //    var data = new Dictionary<int, byte[]>();
        //    foreach (var val in values)
        //    {
        //        data[val.Key] = val.Value == null ? null : BitConverter.GetBytes((long)val.Value);
        //    }
        //    return GenericUpdate<T, TKey>(keySelector, data);
        //}
        //public ClientResult Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, long> values)
        //{
        //    var data = new Dictionary<int, byte[]>();
        //    foreach (var val in values)
        //    {
        //        data[val.Key] = BitConverter.GetBytes(val.Value);
        //    }
        //    return GenericUpdate<T, TKey>(keySelector, data);
        //}
        //public ClientResult Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, DateTime?> values)
        //{
        //    var data = new Dictionary<int, byte[]>();
        //    foreach (var val in values)
        //    {
        //        data[val.Key] = val.Value == null ? null : BitConverter.GetBytes(((DateTime)val.Value - new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds);
        //    }
        //    return GenericUpdate<T, TKey>(keySelector, data);
        //}
        //public ClientResult Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, DateTime> values)
        //{
        //    var data = new Dictionary<int, byte[]>();
        //    foreach (var val in values)
        //    {
        //        data[val.Key] = BitConverter.GetBytes((val.Value - new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds);
        //    }
        //    return GenericUpdate<T, TKey>(keySelector, data);
        //}
        //public ClientResult Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, byte[]> values)
        //{
        //    var data = new Dictionary<int, byte[]>();
        //    foreach (var val in values)
        //    {
        //        data[val.Key] = val.Value == null ? null : val.Value;
        //    }
        //    return GenericUpdate<T, TKey>(keySelector, data);
        //}
        //public ClientResult Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, string> values)
        //{
        //    var data = new Dictionary<int, byte[]>();
        //    foreach (var val in values)
        //    {
        //        data[val.Key] = val.Value == null ? null : Encoding.UTF8.GetBytes((string)val.Value);
        //    }
        //    return GenericUpdate<T, TKey>(keySelector, data);
        //}
        public ClientResult GenericUpdate<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, byte[]> UpdateData)
        {
            var res = new ClientResult();
            StringBuilder up_sb = new StringBuilder();
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector);
            res.Selector = name;
            res.Type = "update";
            res.UpdateData = UpdateData;
            return res;
        }

        public ClientResult GenericUpdate(string keySelector, Dictionary<int, byte[]> UpdateData)
        {
            var res = new ClientResult();
            StringBuilder up_sb = new StringBuilder();
            res.Selector = keySelector;
            res.Type = "update";
            res.UpdateData = UpdateData;
            return res;
        }
    }
}
