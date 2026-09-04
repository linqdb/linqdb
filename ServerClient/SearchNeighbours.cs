using ServerSharedData;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbClientInternal
{
    public partial class Ldb
    {
        public ClientResult SearchNeighbours<T, TKey>(Expression<Func<T, TKey>> keySelector, float[] vector, int k)
        {
            if (k <= 0)
            {
                throw new LinqDbException("Linqdb: k must be positive");
            }
            if (vector == null)
            {
                throw new LinqDbException("Linqdb: vector can't be null");
            }
            var res = new ClientResult();
            res.Type = "searchneighbours";
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector);
            res.Selector = name;
            res.Take = k;
            res.QueueData = SharedUtils.FloatByteConverter.FloatArrayToByteArray(vector);
            return res;
        }
    }
}
