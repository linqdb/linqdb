using LinqDb;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.tables;


namespace Testing.basic_tests
{
#if (!SOCKETS && !SERVER && !SAMEDB && !INDEXES)
    class LowLevelVectorDelete : ITest
    {
        public void Do(Db db)
        {
            bool dispose = false; if (db == null) { db = new Db("DATA"); dispose = true; }

            db.Table<SomeVector>().Delete(new HashSet<int>(db.Table<SomeVector>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));

            var d = new SomeVector()
            {
                Id = 1,
                VectorL2 = new float[5] { 1.1f, 1.1f, 1.1f, 1.1f, 1.1f }
            };
            db.Table<SomeVector>().Save(d);
            d = new SomeVector()
            {
                Id = 2,
                VectorL2 = new float[5] { 1.2f, 1.2f, 1.2f, 1.2f, 1.2f }
            };
            db.Table<SomeVector>().Save(d);
            d = new SomeVector()
            {
                Id = 3,
                VectorL2 = new float[5] { 1.3f, 1.3f, 1.3f, 1.3f, 1.3f }
            };
            db.Table<SomeVector>().Save(d);

            d = new SomeVector()
            {
                Id = 4,
                VectorL2 = new float[5] { -1.1f, -1.1f, -1.1f, -1.1f, -1.1f }
            };
            db.Table<SomeVector>().Save(d);
            d = new SomeVector()
            {
                Id = 5,
                VectorL2 = new float[5] { -1.2f, -1.2f, -1.2f, -1.2f, -1.2f }
            };
            db.Table<SomeVector>().Save(d);
            d = new SomeVector()
            {
                Id = 6,
                VectorL2 = new float[5] { -1.3f, -1.3f, -1.3f, -1.3f, -1.3f }
            };
            db.Table<SomeVector>().Save(d);


            var level_db = db._internal_data_db;

            var keys = new List<byte[]>();
            var values = new List<byte[]>();
            using (var it = level_db.NewIterator())
            {
                it.SeekToFirst();
                while (it.Valid())
                {
                    keys.Add(it.Key());
                    values.Add(it.Value());
                    it.Next();
                }
            }

            var vkeys = keys.Where(f => f[0] == 122 || f[0] == 123).ToList();

            if (vkeys.Count() != 7)
            {
                throw new Exception("Assert failure");
            }

            db.Table<SomeVector>().Delete(new HashSet<int>() { 1, 2, 3, 4, 5, 6 });

            keys = new List<byte[]>();
            values = new List<byte[]>();
            using (var it = level_db.NewIterator())
            {
                it.SeekToFirst();
                while (it.Valid())
                {
                    keys.Add(it.Key());
                    values.Add(it.Value());
                    it.Next();
                }
            }

            vkeys = keys.Where(f => f[0] == 122 || f[0] == 123).ToList();

            if (vkeys.Count() != 0)
            {
                throw new Exception("Assert failure");
            }

            if (dispose) { db.Dispose(); }
            if(dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
#endif
}
