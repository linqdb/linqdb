using ServerSharedData;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbClientInternal
{
    public partial class Ldb
    {
        public ClientResult Between<T, TKey>(Expression<Func<T, TKey>> keySelector, object from, object to, BetweenBoundariesInternal boundaries = BetweenBoundariesInternal.BothInclusive)
        {
            if (from == null || to == null)
            {
                throw new LinqDbException("Linqdb: Between parameter(s) is null");
            }

            if (typeof(TKey) == typeof(DateTime) || Nullable.GetUnderlyingType(typeof(TKey)) == typeof(DateTime))
            {
                var res = new ClientResult();
                res.Type = "between";
                res.Boundaries = (short)boundaries;
                res.From = (Convert.ToDateTime(from) - new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
                res.To = (Convert.ToDateTime(to) - new DateTime(0001, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
                var par = keySelector.Parameters.First();
                var name = SharedUtils.GetPropertyName(keySelector);
                res.Selector = name;
                return res;
            }
            else if (typeof(TKey) == typeof(decimal) || Nullable.GetUnderlyingType(typeof(TKey)) == typeof(decimal))
            {
                var res = new ClientResult();
                res.Type = "betweend";
                res.Boundaries = (short)boundaries;
                res.FromDecimal = Convert.ToDecimal(from);
                res.ToDecimal = Convert.ToDecimal(to);
                var par = keySelector.Parameters.First();
                var name = SharedUtils.GetPropertyName(keySelector);
                res.Selector = name;
                return res;
            }
            else
            {
                var res = new ClientResult();
                res.Type = "between";
                res.Boundaries = (short)boundaries;
                res.From = Convert.ToDouble(from);
                res.To = Convert.ToDouble(to);
                var par = keySelector.Parameters.First();
                var name = SharedUtils.GetPropertyName(keySelector);
                res.Selector = name;
                return res;
            }
        }
    }
    public enum BetweenBoundariesInternal : int
    {
        BothInclusive,
        FromInclusiveToExclusive,
        FromExclusiveToInclusive,
        BothExclusive
    }
}
