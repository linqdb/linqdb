#if (SERVER || SOCKETS)
using LinqdbClient;
using ServerLogic;
#else
using LinqDb;
#endif
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.tables;

namespace Testing.basic_tests
{
    class VectorSave : ITest
    {
        public void Do(Db db)
        {
            bool dispose = false;
            if (db == null)
            {
                db = new Db("DATA");
                dispose = true;
            }
#if (SERVER)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
#endif
#if (SOCKETS)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f, db); };
#endif
            db.Table<SomeVector>().Delete(new HashSet<int>(db.Table<SomeVector>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));


            db.Table<SomeVector>().Save(new SomeVector()
            { 
                Date = DateTime.Now,
                VectorL2 = new float[] { 0.01f, 0.02f, 0.03f }
            });

            var res = db.Table<SomeVector>()
                        .SelectEntity();

            if (res.Count() != 1 || res[0].VectorL2.Sum() != 0.06f)
            {
                throw new Exception("Assert failure");
            }

            var res_anonymous = db.Table<SomeVector>()
                                  .Select(f => new
                                  {
                                      f.VectorL2
                                  });


            if (res.Count() != 1 || res[0].VectorL2.Sum() != 0.06f)
            {
                throw new Exception("Assert failure");
            }

            db.Table<SomeVector>().Save(new SomeVector()
            {
                Date = DateTime.Now,
                VectorL2 = new float[] { 0.04f, 0.05f, 0.06f }
            });

            res = db.Table<SomeVector>()
                        .OrderBy(f => f.Id)
                        .SelectEntity();

            if (res.Count() != 2 || res[1].VectorL2.Sum() != 0.15f)
            {
                throw new Exception("Assert failure");
            }

            var res_anonymous2 = db.Table<SomeVector>()
                                  .OrderBy(f => f.Id)
                                  .Select(f => new
                                  {
                                      f.VectorL2,
                                      f.Date
                                  });


            if (res.Count() != 2 || res[1].VectorL2.Sum() != 0.15f)
            {
                throw new Exception("Assert failure");
            }

#if (SERVER || SOCKETS)
            if (dispose)
            {
                if(dispose) { Logic.Dispose(); }
            }
#else
            if (dispose)
            {
                if (dispose) { db.Dispose(); }
            }
#endif
#if (!SOCKETS && !SAMEDB && !INDEXES && !SERVER)
            if(dispose)
            {
                if(dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
            }
#endif
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
