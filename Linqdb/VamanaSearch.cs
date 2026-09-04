using RocksDbSharp;
using ServerSharedData;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Principal;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        List<Tuple<int, double>> MakeVectorSearch(float[] vector, int k, TableInfo table_info, string name, ReadOptions ro)
        {
            var index = new VamanaIndex(VamanaIndex.Mode.Search, new VamanaIndex.Config(), table_info.TableNumber, table_info.ColumnNumbers[name], (object)ro,
                                        GetVectorNeighboursFromDisk, LoadVector, GetCurrentEntryPoints, GetCorpusCount);

            var res = index.Search(vector, k);

            if (res == null)
            {
                return new List<Tuple<int, double>>();
            }
            else
            {
                return res.ToList();
            }
        }
    }
}
