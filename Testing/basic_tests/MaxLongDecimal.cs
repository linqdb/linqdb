#if (SERVER || SOCKETS)
using LinqdbClient;
using ServerLogic;
#else
using LinqDb;
#endif
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.tables;

namespace Testing.basic_tests
{
    class MaxLongDecimal : ITest
    {
        public void Do(Db db)
        {
            bool dispose = false; if (db == null) { db = new Db("DATA"); dispose = true; }
#if (SERVER)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
#endif
#if (SOCKETS)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f, db); };
#endif
#if (SOCKETS || SAMEDB || INDEXES || SERVER)
            db.Table<LongClass>().Delete(new HashSet<int>(db.Table<LongClass>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            db.Table<DecimalClass>().Delete(new HashSet<int>(db.Table<DecimalClass>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif
#if (INDEXES)
            db.Table<LongClass>().CreatePropertyMemoryIndex(f => f.LongValueNonNull);
            db.Table<DecimalClass>().CreatePropertyMemoryIndex(f => f.DecValueNonNull);
#endif

            var d = new LongClass()
            {
                Id = 1,
                IntValue = 5,
                LongValueNonNull = long.MaxValue
            };
            db.Table<LongClass>().Save(d);

            d = new LongClass()
            {
                Id = 2,
                IntValue = 3,
                LongValueNonNull = long.MinValue
            };
            db.Table<LongClass>().Save(d);

            var res = db.Table<LongClass>().OrderBy(f => f.LongValueNonNull).SelectEntity();
            if (res.Count() != 2 || res[0].LongValueNonNull != long.MinValue || res[1].LongValueNonNull != long.MaxValue)
            {
                throw new Exception("Assert failure");
            }

            var dec = new DecimalClass()
            {
                Id = 1,
                DecValueNonNull = decimal.MaxValue,
                IntValue = 5
            };
            db.Table<DecimalClass>().Save(dec);
            dec = new DecimalClass()
            {
                Id = 2,
                DecValueNonNull = decimal.MinValue,
                IntValue = 10
            };
            db.Table<DecimalClass>().Save(dec);

            var resdec = db.Table<DecimalClass>().OrderBy(f => f.DecValueNonNull).SelectEntity();
            if (resdec.Count() != 2 || resdec[0].DecValueNonNull != decimal.MinValue || resdec[1].DecValueNonNull != decimal.MaxValue)
            {
                throw new Exception("Assert failure");
            }


            decimal? tmp = null;
            try
            {
                resdec = db.Table<DecimalClass>().Between(f => f.DecValueNonNull, tmp, 5m).SelectEntity();
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("Between parameter(s) is null"))
                {
                    throw new Exception("Assert failure");
                }
            }


#if (INDEXES)
            db.Table<LongClass>().RemovePropertyMemoryIndex(f => f.LongValueNonNull);
            db.Table<DecimalClass>().RemovePropertyMemoryIndex(f => f.DecValueNonNull);
#endif

#if (SERVER || SOCKETS)
            if (dispose) { Logic.Dispose(); }
#else
            if (dispose) { db.Dispose(); }
#endif
#if (!SOCKETS && !SAMEDB && !INDEXES && !SERVER)
            if(dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
#endif
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
