using ServerSharedData;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        public IDbQueryable<T> SearchNeighbours<T, TKey>(IDbQueryable<T> source, Expression<Func<T, TKey>> keySelector, float[] vector, int k)
        {
            CheckTableInfo<T>(source.Partition);
            if (k <= 0)
            {
                throw new LinqDbException("Linqdb: k must be positive");
            }
            if (vector == null)
            {
                throw new LinqDbException("Linqdb: vector can't be null");
            }
            if (source.LDBTree == null)
            {
                source.LDBTree = new QueryTree();
            }
            var tree = source.LDBTree;
            if (tree.SearchInfo == null)
            {
                tree.SearchInfo = new List<SearchInfo>();
            }
            var info = new SearchInfo();
            info.Vector = vector;
            tree.SearchInfo.Add(info);
            info.Steps = k;
            info.IsVector = true;

            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector);
            var type_name = GetTypeName<T>(source.Partition);
            var table_info = GetTableInfo(type_name);
            info.TableInfo = table_info;
            info.Name = name;

            source.LDBTree.Prev = info;
            source.LDBTree.Prev.Id = source.LDBTree.Counter + 1;
            source.LDBTree.Counter++;

            return source;
        }
    }
}
