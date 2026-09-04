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
        public ClientResult CreatePropertyIndex<T, TKey>(Expression<Func<T, TKey>> valuePropertySelector, bool isbitfunnel, int bitfunnel_size, int batch_size)
        {
            var res = new ClientResult();
            res.Type = "propertyindex";
            if (isbitfunnel)
            {
                res.String_null = true;
                res.Take = bitfunnel_size;
                res.Skip = batch_size;
            }
            var par = valuePropertySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(valuePropertySelector);
            res.Selector = name;
            return res;
        }

        public ClientResult RemovePropertyIndex<T, TKey>(Expression<Func<T, TKey>> valuePropertySelector, bool isbitfunnel)
        {
            var res = new ClientResult();
            res.Type = "removepropertyindex";
            if (isbitfunnel)
            {
                res.Type += "-";
            }
            var par = valuePropertySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(valuePropertySelector);
            res.Selector = name;
            return res;
        }
    }
}
