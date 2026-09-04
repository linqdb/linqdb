using RocksDbSharp;
using ServerSharedData;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        public List<int> GetVectorNeighboursFromDisk(int table_number, short column_number, int id, object ro)
        {
            var key = MakeVamanaVectorsNeighboursKey(table_number, column_number, id);
            byte[] res;
            if (ro == null)
            {
                res = leveld_db.Get(key);
            }
            else
            {
                res = leveld_db.Get(key: key, readOptions: (ReadOptions)ro);
            }
           
            if (res == null)
            {
                return new List<int>();
            }
            var list = SharedUtils.IntListSerializer.FromByteArray(res);
            return list;
        }

        public float[] LoadVector(int table_number, short column_number, int id, object ro)
        {
            var key = MakeBinaryValueKey(new IndexKeyInfo()
            {
                TableNumber = table_number,
                ColumnNumber = column_number,
                Id = id
            });
            byte[] val;
            if (ro == null)
            {
                val = leveld_db.Get(key);
            }
            else
            {
                val = leveld_db.Get(key: key, readOptions: (ReadOptions)ro);
            }

            if (val == null)
            {
                return null;
            }
            else if (ValsEqual(val, NullConstant))
            {
                return null;
            }
            else
            {
                var result = SharedUtils.FloatByteConverter.ByteArrayToFloatArray(val.Skip(1).ToArray());
                return result;
            }
        }

        public List<int> GetCurrentEntryPoints(int table_number, short column_number, object ro)
        {
            var key = MakeVamanaEntryPointsKey(table_number, column_number);
            byte[] res;
            if (ro == null)
            {
                res = leveld_db.Get(key);
            }
            else
            {
                res = leveld_db.Get(key: key, readOptions: (ReadOptions)ro);
            }
            if (res == null)
            {
                return new List<int>();
            }
            var list = SharedUtils.IntListSerializer.FromByteArray(res);
            return list;
        }

        public long GetCorpusCount(int table_number, short column_number, object ro)
        {
            var res = GetVamanaItemsCount(table_number, column_number, (ReadOptions)ro);
            return res;
        }

    }
}
