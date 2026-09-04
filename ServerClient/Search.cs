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
        public ClientResult Search<T, TKey>(Expression<Func<T, TKey>> keySelector, string search_query, bool partial, bool timeLimited, int searchTimeLimitInMs, int? start_step, int? steps)
        {
            if (start_step != null && steps == null || start_step == null && steps != null)
            {
                throw new LinqDbException("Linqdb: start_step and steps parameters must be used together.");
            }

            var res = new ClientResult();
            res.Type = "search";
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector);
            res.Selector = name;
            res.Query = search_query;
            res.Start_step = start_step;
            res.Steps = steps;
            res.Double_null = partial;
            res.Int_null = timeLimited;
            res.Take = searchTimeLimitInMs;
            return res;            
        }

        public ClientResult SearchBitfunnel<T, TKey>(Expression<Func<T, TKey>> keySelector, string search_query, int? stop_res_count = null)
        {
            var res = new ClientResult();
            res.Type = "searchbf";
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector);
            res.Selector = name;
            res.Steps = stop_res_count;
            res.Query = search_query;
            return res;
        }
    }
}
