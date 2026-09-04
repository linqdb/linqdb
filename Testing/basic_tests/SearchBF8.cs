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
    class SearchBF8 : ITest
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
            db.Table<SomeData>().Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif

#if INDEXES
            db.Table<SomeData>().RemoveGroupByMemoryIndex(f => f.GroupBy, z => z.Normalized);
            db.Table<SomeData>().RemoveGroupByMemoryIndex(f => f.GroupBy, z => z.Date);
            db.Table<SomeData>().RemoveGroupByMemoryIndex(f => f.GroupBy, z => z.Normalized);
            db.Table<SomeData>().RemoveGroupByMemoryIndex(f => f.GroupBy2, z => z.Normalized);
            db.Table<SomeData>().RemovePropertyMemoryIndex(f => f.PeriodId);
            db.Table<SomeData>().RemovePropertyMemoryIndex(f => f.Value);
            db.Table<SomeData>().RemovePropertyMemoryIndex(f => f.Id);
#endif

            try
            {
                db.Table<SomeData>().CreatePropertyBitFunnelMemoryIndex(f => f.StringVal, 150);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("bitfunnel_size must be a multiple of 8"))
                {
                    throw new Exception("Assert failure");
                }
            }

            try
            {
                db.Table<SomeData>().CreatePropertyMemoryIndex(f => f.PeriodId);
                db.Table<SomeData>().CreatePropertyBitFunnelMemoryIndex(f => f.StringVal, 160);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("bitfunnel index must be the first and only index built on a table."))
                {
                    throw new Exception("Assert failure");
                }
            }

            try
            {
                db.Table<SomeData>().CreatePropertyMemoryIndex(f => f.PeriodId);
                db.Table<SomeData>().RemovePropertyMemoryIndex(f => f.PeriodId);
                db.Table<SomeData>().CreatePropertyBitFunnelMemoryIndex(f => f.StringVal, 160);
            }
            catch (Exception ex)
            {
                throw new Exception("Assert failure");
            }


            try
            {
                db.Table<SomeData>().CreateGroupByMemoryIndex(f => f.PeriodId, f => f.Value);
                db.Table<SomeData>().CreatePropertyBitFunnelMemoryIndex(f => f.StringVal, 160);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("bitfunnel index must be the first and only index built on a table."))
                {
                    throw new Exception("Assert failure");
                }
            }

            try
            {
                db.Table<SomeData>().RemoveBitFunnelMemoryIndex(f => f.StringVal);
                db.Table<SomeData>().CreateGroupByMemoryIndex(f => f.PeriodId, f => f.Value);
                db.Table<SomeData>().RemoveGroupByMemoryIndex(f => f.PeriodId, f => f.Value);
                db.Table<SomeData>().CreatePropertyBitFunnelMemoryIndex(f => f.StringVal, 160);
                db.Table<SomeData>().RemoveBitFunnelMemoryIndex(f => f.StringVal);
            }
            catch (Exception ex)
            {
                throw new Exception("Assert failure");
            }


            try
            {
                db.Table<SomeData>().CreatePropertyBitFunnelMemoryIndex(f => f.StringVal, 160);
                db.Table<SomeData>().CreateGroupByMemoryIndex(f => f.PeriodId, f => f.Value);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("bitfunnel index must be the first and only index built on a table."))
                {
                    throw new Exception("Assert failure");
                }
            }

            try
            {
                db.Table<SomeData>().CreatePropertyBitFunnelMemoryIndex(f => f.PeriodId, 160);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("bitfunnel index can be created and removed on string properties only"))
                {
                    throw new Exception("Assert failure");
                }
            }

            try
            {
                db.Table<SomeData>().RemoveBitFunnelMemoryIndex(f => f.PeriodId);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("bitfunnel index can be created and removed on string properties only"))
                {
                    throw new Exception("Assert failure");
                }
            }

            try
            {
                db.Table<SomeData>().CreatePropertyMemoryIndex(f => f.StringVal);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("memory index can't be created on string properties"))
                {
                    throw new Exception("Assert failure");
                }
            }

            try
            {
                db.Table<SomeData>().RemovePropertyMemoryIndex(f => f.StringVal);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("memory index can't be removed on string properties"))
                {
                    throw new Exception("Assert failure");
                }
            }

            try
            {
                db.Table<SomeData>().CreateGroupByMemoryIndex(f => f.PeriodId, f => f.StringVal);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("memory index can't be created on string properties"))
                {
                    throw new Exception("Assert failure");
                }
            }

            try
            {
                db.Table<SomeData>().RemoveGroupByMemoryIndex(f => f.PeriodId, f => f.StringVal);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("memory index can't be removed on string properties"))
                {
                    throw new Exception("Assert failure");
                }
            }


            db.Table<SomeData>().RemovePropertyMemoryIndex(f => f.PeriodId);
            db.Table<SomeData>().RemoveBitFunnelMemoryIndex(f => f.StringVal);
#if (SERVER || SOCKETS)
            if(dispose) { Logic.Dispose(); }
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
