using LinqDbClientInternal;
using ServerSharedData;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LinqdbClient
{
    public struct DistributedId
    {
        public DistributedId(int serverId, int id)
        {
            ServerId = serverId;
            Id = id;
        }
        public int ServerId { get; set; }
        public int Id { get; set; }
    }
    public class ServerDb
    {
        public Db db { get; set; }
        public int ServerId { get; set; }
    }
    public class ILinqDbDistributedQueryable<T> where T : new()
    {
        Dictionary<int, Db> _dbs_value { get; set; }
        public Dictionary<int, Db> _dbs
        {
            get
            {
                return _dbs_value;
            }
            set
            {
                _list_dbs = value.Select(f => new ServerDb() { ServerId = f.Key, db = f.Value }).ToList();
                _dbs_value = value;

                _list_queryable = new List<Tuple<ServerDb, ILinqDbQueryable<T>>>();
                _dic_queryable = new Dictionary<int, ILinqDbQueryable<T>>();
                foreach (var db in _list_dbs)
                {
                    var _inter = new IDbQueryable<T>() { Result = new List<ClientResult>(), _db = db.db._db_internal };
                    var queryable = new ILinqDbQueryable<T>() { _internal = _inter };
                    _list_queryable.Add(new Tuple<ServerDb, ILinqDbQueryable<T>>(db, queryable));
                    _dic_queryable[db.ServerId] = queryable;
                }
            }
        }
        List<ServerDb> _list_dbs { get; set; }
        List<Tuple<ServerDb, ILinqDbQueryable<T>>> _list_queryable { get; set; }
        Dictionary<int, ILinqDbQueryable<T>> _dic_queryable { get; set; }

        internal static class ThreadSafeRandom
        {
            private static int _seed = Environment.TickCount;

            // Each thread gets its own Random, seeded uniquely via Interlocked
            private static readonly ThreadLocal<Random> _rng =
                new ThreadLocal<Random>(() => new Random(Interlocked.Increment(ref _seed)));

            public static int Next(int maxValue) => _rng.Value.Next(maxValue);
            public static int Next(int minValue, int maxValue) => _rng.Value.Next(minValue, maxValue);
        }
        ServerDb GetRandomDb()
        {
            if (_list_dbs == null || _list_dbs.Count == 0)
                throw new LinqDbException("Linqdb: no databases configured.");

            int which = ThreadSafeRandom.Next(_list_dbs.Count);
            return _list_dbs[which];
        }

        ServerDb GetDb(int ServerId)
        {
            return new ServerDb()
            {
                ServerId = ServerId,
                db = _dbs[ServerId]
            };
        }

        /// <summary>
        ///  Saves new item if Id is 0 and assigns new (Sid,Id). Updates existing item if (Sid,Id) is existing's item's (Sid,Id).
        /// </summary>
        public DistributedId Save(T item)
        {
            var prop = item.GetType().GetProperty("Id");
            if (prop == null)
            {
                throw new LinqDbException("Linqdb: type must have integer Id property");
            }
            int id = (int)prop.GetValue(item);
            var prop2 = item.GetType().GetProperty("Sid");
            if (prop2 == null)
            {
                throw new LinqDbException("Linqdb: type must have integer Sid property");
            }
            int sid = (int)prop2.GetValue(item);

            if (id == 0 && sid == 0)
            {
                var db = GetRandomDb();
                prop2.SetValue(item, db.ServerId);
                db.db._db_internal.IsDist = true;
                var local_id = db.db.Table<T>().Save(item);
                prop.SetValue(item, local_id);
                // FIX: return the actual shard ServerId, not the old sid (which was 0)
                return new DistributedId()
                {
                    ServerId = db.ServerId,
                    Id = local_id
                };
            }
            else if (sid != 0 && id != 0)
            {
                var db = GetDb(sid);
                db.db._db_internal.IsDist = true;
                db.db.Table<T>().Save(item);
                return new DistributedId()
                {
                    Id = id,
                    ServerId = sid
                };
            }
            else
            {
                throw new LinqDbException("Linqdb: Id and Sid must both be equal to zero (for new items) or both be non-zero to indicate specific item.");
            }
        }
        /// <summary>
        ///  Saves items by ditributing it to N servers.
        /// </summary>
        public void SaveBatch(List<T> items)
        {
            if (items == null || !items.Any())
            {
                return;
            }
            var grouped = new Dictionary<int, List<T>>();
            bool isNew = false;
            for (int i = 0; i < items.Count; i++)
            {
                var prop = items[i].GetType().GetProperty("Id");
                if (prop == null)
                {
                    throw new LinqDbException("Linqdb: type must have integer Id property");
                }
                int id = (int)prop.GetValue(items[i]);
                prop = items[i].GetType().GetProperty("Sid");
                if (prop == null)
                {
                    throw new LinqDbException("Linqdb: type must have integer Sid property");
                }
                int sid = (int)prop.GetValue(items[i]);
                if (id != 0 && sid == 0 || id == 0 && sid != 0)
                {
                    throw new LinqDbException("Linqdb: Id and Sid must both be equal to zero (for new items) or both be non-zero to indicate specific item.");
                }
                if (sid != 0)
                {
                    if (!grouped.ContainsKey(sid))
                    {
                        grouped[sid] = new List<T>();
                    }
                    grouped[sid].Add(items[i]);
                }
                if (i == 0)
                {
                    if (id == 0)
                    {
                        isNew = true;
                    }
                }
                else
                {
                    if (isNew && id != 0 || !isNew && id == 0)
                    {
                        throw new LinqDbException("Linqdb: Batch cannot contain both new (Id is 0) and non-new items.");
                    }
                }
            }

            var data = new List<Tuple<Db, List<T>>>();
            if (isNew)
            {
                var dataDbs = new List<Tuple<ServerDb, List<T>>>();
                var total = _list_dbs.Count;
                var size = items.Count / total + 1;
                for (int i = 0; i < total; i++)
                {
                    dataDbs.Add(new Tuple<ServerDb, List<T>>(_list_dbs[i], new List<T>(size)));
                }
                for (int i = 0; i < items.Count; i++)
                {
                    int dbId = i % total;
                    var prop = items[i].GetType().GetProperty("Sid");
                    prop.SetValue(items[i], dataDbs[dbId].Item1.ServerId);
                    dataDbs[dbId].Item2.Add(items[i]);
                }
                // FIX: move projection OUT of the loop to avoid O(n^2)
                data = dataDbs.Select(f => new Tuple<Db, List<T>>(f.Item1.db, f.Item2)).ToList();
            }
            else
            {
                data = grouped.Select(f => new Tuple<Db, List<T>>(GetDb(f.Key).db, f.Value.ToList())).ToList();
            }

            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            Parallel.ForEach(data, d =>
            {
                string db_name = d.Item1.GetIpAndPort();
                try
                {
                    d.Item1._db_internal.IsDist = true;
                    d.Item1.Table<T>().SaveBatch(d.Item2);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Save error", errors);
            }
        }
        /// <summary>
        ///  Saves any amount of items non-atomically, i.e. if it fails in the middle some items will be saved and some won't be.
        /// </summary>
        public void SaveBatchNonAtomically(List<T> items, int batchSize = 5000)
        {
            if (items == null || !items.Any())
            {
                return;
            }
            var grouped = new Dictionary<int, List<T>>();
            bool isNew = false;
            for (int i = 0; i < items.Count; i++)
            {
                var prop = items[i].GetType().GetProperty("Id");
                if (prop == null)
                {
                    throw new LinqDbException("Linqdb: type must have integer Id property");
                }
                int id = (int)prop.GetValue(items[i]);
                prop = items[i].GetType().GetProperty("Sid");
                if (prop == null)
                {
                    throw new LinqDbException("Linqdb: type must have integer Sid property");
                }
                int sid = (int)prop.GetValue(items[i]);
                if (id != 0 && sid == 0 || id == 0 && sid != 0)
                {
                    throw new LinqDbException("Linqdb: Id and Sid must both be equal to zero (for new items) or both be non-zero to indicate specific item.");
                }
                if (sid != 0)
                {
                    if (!grouped.ContainsKey(sid))
                    {
                        grouped[sid] = new List<T>();
                    }
                    grouped[sid].Add(items[i]);
                }
                if (i == 0)
                {
                    if (id == 0)
                    {
                        isNew = true;
                    }
                }
                else
                {
                    if (isNew && id != 0 || !isNew && id == 0)
                    {
                        throw new LinqDbException("Linqdb: Batch cannot contain both new (Id is 0) and non-new items.");
                    }
                }
            }
            var data = new List<Tuple<Db, List<T>>>();
            if (isNew)
            {
                var dataDbs = new List<Tuple<ServerDb, List<T>>>();
                var total = _list_dbs.Count;
                var size = items.Count / total + 1;
                for (int i = 0; i < total; i++)
                {
                    dataDbs.Add(new Tuple<ServerDb, List<T>>(_list_dbs[i], new List<T>(size)));
                }
                for (int i = 0; i < items.Count; i++)
                {
                    int dbId = i % total;
                    var prop = items[i].GetType().GetProperty("Sid");
                    prop.SetValue(items[i], dataDbs[dbId].Item1.ServerId);
                    dataDbs[dbId].Item2.Add(items[i]);
                }
                // FIX: move projection OUT of the loop to avoid O(n^2)
                data = dataDbs.Select(f => new Tuple<Db, List<T>>(f.Item1.db, f.Item2)).ToList();
            }
            else
            {
                data = grouped.Select(f => new Tuple<Db, List<T>>(GetDb(f.Key).db, f.Value.ToList())).ToList();
            }

            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            Parallel.ForEach(data, d =>
            {
                string db_name = d.Item1.GetIpAndPort();
                try
                {
                    d.Item1._db_internal.IsDist = true;
                    d.Item1.Table<T>().SaveNonAtomically(d.Item2, batchSize);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: SaveBatchNonAtomically error", errors);
            }
        }

        /// <summary>
        ///  Record count.
        /// </summary>
        public int Count()
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            int result = 0;
            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    var count = d.Item2.Count();
                    // FIX: use Interlocked to avoid coarse lock on aggregation
                    System.Threading.Interlocked.Add(ref result, count);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Count error", errors);
            }
            return result;
        }

        /// <summary>
        ///  Get selected ids without having to select them (more efficiently).
        /// </summary>
        public SelectedDistributedIds GetIds()
        {
            // Keep errors exactly as before
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();

            // Lock-free data path
            var idBag = new System.Collections.Concurrent.ConcurrentBag<DistributedId>();
            int allOk = 1; // 1 = true, 0 = false

            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    var serverId = d.Item1.ServerId;
                    var ids = d.Item2.GetIds();

                    if (!ids.AllIds)
                    {
                        // if any shard says "not all", final must be false
                        System.Threading.Interlocked.Exchange(ref allOk, 0);
                    }

                    if (ids.Ids != null)
                    {
                        foreach (var localId in ids.Ids)
                        {
                            idBag.Add(new DistributedId(serverId, localId));
                        }
                    }
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });

            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: GetIds error", errors);
            }

            return new SelectedDistributedIds
            {
                AllIds = allOk == 1,
                Ids = idBag.ToList()
            };
        }


        /// <summary>
        ///  Last search step. To be used in step-search.
        /// </summary>
        public int LastStep()
        {
            // Keep error collection as-is
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();

            // Lock-free aggregation for results
            var results = new System.Collections.Concurrent.ConcurrentBag<int>();

            Parallel.ForEach(_list_dbs, d =>
            {
                string db_name = d.db.GetIpAndPort();
                try
                {
                    var lastStep = d.db.Table<T>().LastStep();
                    results.Add(lastStep);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });

            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: LastStep error", errors);
            }

            if (!results.Any())
            {
                return -1;
            }
            return results.Max();
        }

        /// <summary>
        ///  Deletes item with given Id.
        /// </summary>
        public void Delete(DistributedId id)
        {
            var db = GetDb(id.ServerId);
            db.db.Table<T>().Delete(id.Id);
        }

        /// <summary>
        ///  Deletes items.
        /// </summary>
        public void Delete(List<DistributedId> ids)
        {
            if (ids == null || !ids.Any())
            {
                return;
            }

            var data = ids.GroupBy(f => f.ServerId).Select(f => new Tuple<Db, HashSet<int>>(GetDb(f.Key).db, new HashSet<int>(f.Select(z => z.Id).ToList()))).ToList();

            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            Parallel.ForEach(data, d =>
            {
                string db_name = d.Item1.GetIpAndPort();
                try
                {
                    d.Item1.Table<T>().Delete(d.Item2);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Delete error", errors);
            }
        }
        /// <summary>
        ///  Deletes any amount of items non-atomically, i.e. if it fails in the middle some items will be deleted and some won't be.
        /// </summary>
        public void DeleteNonAtomically(List<DistributedId> ids, int batchSize = 5000)
        {
            if (ids == null || !ids.Any())
            {
                return;
            }

            var data = ids.GroupBy(f => f.ServerId).Select(f => new Tuple<Db, HashSet<int>>(GetDb(f.Key).db, new HashSet<int>(f.Select(z => z.Id).ToList()))).ToList();

            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            Parallel.ForEach(data, d =>
            {
                string db_name = d.Item1.GetIpAndPort();
                try
                {
                    d.Item1.Table<T>().DeleteNonAtomically(d.Item2, batchSize);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: DeleteNonAtomically error", errors);
            }
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by DistributedId.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, DistributedId id, TKey value)
        {
            var db = GetDb(id.ServerId);
            db.db.Table<T>().Update(keySelector, id.Id, value);
        }

        /// <summary>
        ///  Updates items' fields with supplied values. Item is identified by DistributedId.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<DistributedId, TKey> values)
        {
            if (values == null || !values.Any())
            {
                return;
            }

            var data = values.GroupBy(f => f.Key.ServerId).Select(f => new Tuple<Db, Dictionary<int, TKey>>(GetDb(f.Key).db, f.ToDictionary(z => z.Key.Id, z => z.Value))).ToList();

            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            Parallel.ForEach(data, d =>
            {
                string db_name = d.Item1.GetIpAndPort();
                try
                {
                    d.Item1.Table<T>().Update(keySelector, d.Item2);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Update error", errors);
            }
        }
        /// <summary>
        ///  Updates items' fields with supplied values. Item is identified by DistributedId.
        /// </summary>
        public void UpdateNonAtomically<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<DistributedId, TKey> values, int batchSize = 5000)
        {
            if (values == null || !values.Any())
            {
                return;
            }

            var data = values.GroupBy(f => f.Key.ServerId).Select(f => new Tuple<Db, Dictionary<int, TKey>>(GetDb(f.Key).db, f.ToDictionary(z => z.Key.Id, z => z.Value))).ToList();

            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            Parallel.ForEach(data, d =>
            {
                string db_name = d.Item1.GetIpAndPort();
                try
                {
                    d.Item1.Table<T>().UpdateNonAtomically(keySelector, d.Item2, batchSize);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: UpdateNonAtomically error", errors);
            }
        }

        /// <summary>
        ///  Selects anonymous type using result entities. Select only what's needed as it is more efficient.
        /// </summary>
        public List<R> Select<R>(Expression<Func<T, R>> predicate)
        {
            // Keep error handling unchanged
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();

            // Lock-free data path: collect per-thread lists, merge once
            var bags = new System.Collections.Concurrent.ConcurrentBag<List<R>>();

            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    var local = d.Item2.Select(predicate).ToList();
                    bags.Add(local);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });

            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Select error", errors);
            }

            return bags.SelectMany(x => x).ToList();
        }


        /// <summary>
        ///  Selects anonymous type using result entities. Select only what's needed as it is more efficient. Statistics is used as out parameter.
        /// </summary>
        public List<R> Select<R>(Expression<Func<T, R>> predicate, LinqdbSelectStatistics statistics)
        {
            var errors = new Dictionary<string, List<Exception>>();
            object errLock = new object();

            var bags = new System.Collections.Concurrent.ConcurrentBag<List<R>>();
            var statsPerServer = new System.Collections.Concurrent.ConcurrentDictionary<int, LinqdbSelectStatistics>();

            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    var stat = new LinqdbSelectStatistics();
                    var local = d.Item2.Select(predicate, stat).ToList();
                    bags.Add(local);
                    statsPerServer[d.Item1.ServerId] = stat;
                }
                catch (Exception ex)
                {
                    lock (errLock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });

            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Select error", errors);
            }

            statistics.DistancesDist = new Dictionary<Tuple<int, int>, double>();
            foreach (var kv in statsPerServer.Where(f => f.Value.Distances != null))
            {
                foreach (var item in kv.Value.Distances)
                {
                    statistics.DistancesDist[new Tuple<int, int>(kv.Key, item.Key)] = item.Value;
                }
            }

            return bags.SelectMany(x => x).ToList();
        }

        /// <summary>
        ///  Selects anonymous type using result entities. Select only what's needed as it is more efficient.
        /// </summary>
        public List<R> SelectNonAtomically<R>(Expression<Func<T, R>> predicate, int batchSize = 3000)
        {
            var errors = new Dictionary<string, List<Exception>>();
            object errLock = new object();

            var bags = new System.Collections.Concurrent.ConcurrentBag<List<R>>();

            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    var local = d.Item2.SelectNonAtomically(predicate, batchSize).ToList();
                    bags.Add(local);
                }
                catch (Exception ex)
                {
                    lock (errLock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });

            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: SelectNonAtomically error", errors);
            }

            return bags.SelectMany(x => x).ToList();
        }

        /// <summary>
        ///  Selects entire entities using result set.
        /// </summary>
        public List<T> SelectEntity()
        {
            var errors = new Dictionary<string, List<Exception>>();
            object errLock = new object();

            var bags = new System.Collections.Concurrent.ConcurrentBag<List<T>>();

            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    var local = d.Item2.SelectEntity().ToList();
                    bags.Add(local);
                }
                catch (Exception ex)
                {
                    lock (errLock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });

            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: SelectEntity error", errors);
            }

            return bags.SelectMany(x => x).ToList();
        }

        /// <summary>
        ///  Selects entire entities using result set. Statistics is used as out parameter.
        /// </summary>
        public List<T> SelectEntity(LinqdbSelectStatistics statistics)
        {
            var errors = new Dictionary<string, List<Exception>>();
            object errLock = new object();

            var bags = new System.Collections.Concurrent.ConcurrentBag<List<T>>();
            var statsPerServer = new System.Collections.Concurrent.ConcurrentDictionary<int, LinqdbSelectStatistics>();

            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    var stat = new LinqdbSelectStatistics();
                    var local = d.Item2.SelectEntity(stat).ToList();
                    bags.Add(local);
                    statsPerServer[d.Item1.ServerId] = stat;
                }
                catch (Exception ex)
                {
                    lock (errLock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });

            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: SelectEntity error", errors);
            }

            statistics.DistancesDist = new Dictionary<Tuple<int, int>, double>();
            foreach (var kv in statsPerServer.Where(f => f.Value.Distances != null))
            {
                foreach (var item in kv.Value.Distances)
                {
                    statistics.DistancesDist[new Tuple<int, int>(kv.Key, item.Key)] = item.Value;
                }
            }

            return bags.SelectMany(x => x).ToList();
        }

        /// <summary>
        ///  Selects entire entities using result set.
        /// </summary>
        public List<T> SelectEntityNonAtomically(int batchSize = 3000)
        {
            var errors = new Dictionary<string, List<Exception>>();
            object errLock = new object();

            var bags = new System.Collections.Concurrent.ConcurrentBag<List<T>>();

            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    var local = d.Item2.SelectEntityNonAtomically(batchSize).ToList();
                    bags.Add(local);
                }
                catch (Exception ex)
                {
                    lock (errLock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });

            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: SelectEntityNonAtomically error", errors);
            }

            return bags.SelectMany(x => x).ToList();
        }

        /// <summary>
        ///  Applies between condition to the result set.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Between<TKey>(Expression<Func<T, TKey>> keySelector, TKey from, TKey to, BetweenBoundaries boundaries = BetweenBoundaries.BothInclusive)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Between(keySelector, from, to, (BetweenBoundariesInternal)(int)boundaries);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }

        /// <summary>
        ///  Applies intersect with distributed ids. Handy when used together with efficient .GetIds method.
        /// </summary>
        public ILinqDbDistributedQueryable<T> IntersectWithDistributedIds(List<DistributedId> ids)
        {
            return IntersectWithDistributedIds(new HashSet<DistributedId>(ids));
        }
        /// <summary>
        ///  Applies intersect with distributed ids. Handy when used together with efficient .GetIds method.
        /// </summary>
        public ILinqDbDistributedQueryable<T> IntersectWithDistributedIds(HashSet<DistributedId> ids)
        {
            var data = ids.GroupBy(f => f.ServerId).ToDictionary(f => f.Key, f => new HashSet<int>(f.Select(z => z.Id)));
            var keySelector = BuildKeySelector<T, int>("Id");

            foreach (var db in _list_queryable)
            {
                if (data.ContainsKey(db.Item1.ServerId))
                {
                    var result = _dic_queryable[db.Item1.ServerId]._internal._db.Intersect(keySelector, data[db.Item1.ServerId]);
                    _dic_queryable[db.Item1.ServerId]._internal.Result.Add(result);
                }
                else
                {
                    var result = _dic_queryable[db.Item1.ServerId]._internal._db.Intersect(keySelector, new HashSet<int>());
                    _dic_queryable[db.Item1.ServerId]._internal.Result.Add(result);
                }
            }

            return this;
        }

        public static Expression<Func<T, TKey>> BuildKeySelector<T, TKey>(string propertyName)
        {
            var param = Expression.Parameter(typeof(T), "f");
            var body = Expression.PropertyOrField(param, propertyName);

            if (body.Type != typeof(TKey))
            {
                throw new InvalidOperationException($"Property '{propertyName}' is not of type {typeof(TKey).Name}");
            }

            return Expression.Lambda<Func<T, TKey>>(body, param);
        }


        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, HashSet<TKey> set)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Intersect(keySelector, set);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }

        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, List<TKey> set)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Intersect(keySelector, new HashSet<TKey>(set));
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }

        /// <summary>
        ///  Full text search on a column.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Search<TKey>(Expression<Func<T, TKey>> keySelector, string search_query, int? start_step = null, int? steps = null)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Search(keySelector, search_query, false, false, 0, start_step, steps);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Full text search on a column using in-memory bitfunnel index. 
        /// </summary>
        public ILinqDbDistributedQueryable<T> SearchBitFunnel<TKey>(Expression<Func<T, TKey>> keySelector, string search_query, int? stop_res_count = null)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.SearchBitfunnel(keySelector, search_query, stop_res_count);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Full text search on a column matching beginning of a word.
        /// </summary>
        public ILinqDbDistributedQueryable<T> SearchPartial<TKey>(Expression<Func<T, TKey>> keySelector, string search_query)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Search(keySelector, search_query, true, false, 0, null, null);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Full text search on a column, limited by time period.
        /// </summary>
        public ILinqDbDistributedQueryable<T> SearchTimeLimited<TKey>(Expression<Func<T, TKey>> keySelector, string search_query, int maxSearchTimeInMs)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Search(keySelector, search_query, false, true, maxSearchTimeInMs, null, null);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        /// Vector space search for k-nearest neighbours. Uses aproximate on-disk algorithm.
        /// </summary>
        public ILinqDbDistributedQueryable<T> SearchNeighbours<TKey>(Expression<Func<T, TKey>> keySelector, float[] vector, int k)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.SearchNeighbours(keySelector, vector, k);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Applies where condition to the result set.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Where(Expression<Func<T, bool>> predicate)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Where(predicate);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Creates in-memory property index.
        /// </summary>
        public void CreatePropertyMemoryIndex<TKey>(Expression<Func<T, TKey>> valuePropertySelector)
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            var result = new List<T>();
            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    d.Item2.CreatePropertyMemoryIndex(valuePropertySelector);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: CreatePropertyMemoryIndex error", errors);
            }
        }

        /// <summary>
        ///  Creates in-memory bitfunnel index. Returns bloom filters' average set bits to all bits ratio.
        /// </summary>
        public double CreatePropertyBitFunnelMemoryIndex<TKey>(Expression<Func<T, TKey>> valuePropertySelector, int bitfunnel_size, int batch_size = 1024)
        {
            // Keep error handling unchanged
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();

            // Lock-free aggregation
            var ratios = new System.Collections.Concurrent.ConcurrentBag<double>();

            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    var r = d.Item2.CreatePropertyBitFunnelMemoryIndex(valuePropertySelector, bitfunnel_size, batch_size);
                    ratios.Add(r);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });

            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: CreatePropertyBitFunnelMemoryIndex error", errors);
            }

            if (!ratios.Any())
            {
                return -1;
            }
            return ratios.Average();
        }


        /// <summary>
        ///  Removes index from startup creation.
        /// </summary>
        public void RemoveBitFunnelMemoryIndex<TKey>(Expression<Func<T, TKey>> valuePropertySelector)
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            var ratios = new List<double>();
            var result = new List<T>();
            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    d.Item2.RemoveBitFunnelMemoryIndex(valuePropertySelector);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });

            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: RemoveBitFunnelMemoryIndex error", errors);
            }
        }

        /// <summary>
        ///  Creates in-memory group-by index, parameter is property to be aggregated.
        /// </summary>
        public void CreateGroupByMemoryIndex<TKey1, TKey2>(Expression<Func<T, TKey1>> groupPropertySelector, Expression<Func<T, TKey2>> valuePropertySelector)
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            var result = new List<T>();
            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    d.Item2.CreateGroupByMemoryIndex(groupPropertySelector, valuePropertySelector);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: CreateGroupByMemoryIndex error", errors);
            }
        }

        /// <summary>
        ///  Removes index from startup creation.
        /// </summary>
        public void RemovePropertyMemoryIndex<TKey>(Expression<Func<T, TKey>> valuePropertySelector)
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            var result = new List<T>();
            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    d.Item2.RemovePropertyMemoryIndex(valuePropertySelector);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: RemovePropertyMemoryIndex error", errors);
            }
        }
        /// <summary>
        ///  Removes group index from startup creation.
        /// </summary>
        public void RemoveGroupByMemoryIndex<TKey1, TKey2>(Expression<Func<T, TKey1>> groupPropertySelector, Expression<Func<T, TKey2>> valuePropertySelector)
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            var result = new List<T>();
            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    d.Item2.RemoveGroupByMemoryIndex(groupPropertySelector, valuePropertySelector);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: RemoveGroupByMemoryIndex error", errors);
            }
        }

        /// <summary>
        ///  Groups data by key. Group index must be created in advance.
        /// </summary>
        public ILinqDbGroupedDistributedQueryable<T, TKey> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            var list_grouped_queryable = new List<Tuple<ServerDb, ILinqDbGroupedQueryable<T, TKey>>>();
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.GroupBy(keySelector);
                db.Item2._internal.Result.Add(result);

                var _inter = new IDbGroupedQueryable<T>() { Result = db.Item2._internal.Result, _db = db.Item2._internal._db };
                list_grouped_queryable.Add(new Tuple<ServerDb, ILinqDbGroupedQueryable<T, TKey>>(db.Item1, new ILinqDbGroupedQueryable<T, TKey>() { _internal = _inter }));
            }

            return new ILinqDbGroupedDistributedQueryable<T, TKey>() { _list_grouped_queryable = list_grouped_queryable };
        }

        /// <summary>
        ///  Applies or condition to the neighbouring statements.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Or()
        {
            foreach (var db in _list_queryable)
            {
                var result = Ldb_ext.Or<T>();
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Orders data by ascending values.
        /// </summary>
        public ILinqDbOrderedDistributedQueryable<T, TKey> OrderBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            var list_ordered_queryable = new List<Tuple<ServerDb, ILinqDbOrderedQueryable<T>>>();
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.OrderBy(keySelector);
                db.Item2._internal.Result.Add(result);

                var _inter = new IDbOrderedQueryable<T>() { Result = db.Item2._internal.Result, _db = db.Item2._internal._db, Partition = db.Item2._internal.Partition };
                list_ordered_queryable.Add(new Tuple<ServerDb, ILinqDbOrderedQueryable<T>>(db.Item1, new ILinqDbOrderedQueryable<T>() { _internal = _inter }));
            }

            return new ILinqDbOrderedDistributedQueryable<T, TKey>() { _list_ordered_queryable = list_ordered_queryable, keySelector = keySelector, Asc = true };
        }
        /// <summary>
        ///  Orders data by descending values.
        /// </summary>
        public ILinqDbOrderedDistributedQueryable<T, TKey> OrderByDescending<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            var list_ordered_queryable = new List<Tuple<ServerDb, ILinqDbOrderedQueryable<T>>>();
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.OrderByDescending(keySelector);
                db.Item2._internal.Result.Add(result);

                var _inter = new IDbOrderedQueryable<T>() { Result = db.Item2._internal.Result, _db = db.Item2._internal._db, Partition = db.Item2._internal.Partition };
                list_ordered_queryable.Add(new Tuple<ServerDb, ILinqDbOrderedQueryable<T>>(db.Item1, new ILinqDbOrderedQueryable<T>() { _internal = _inter }));
            }

            return new ILinqDbOrderedDistributedQueryable<T, TKey>() { _list_ordered_queryable = list_ordered_queryable, keySelector = keySelector, Asc = false };
        }

    }

    public class ILinqDbDistributedPartitionedQueryable<T> where T : new()
    {
        public string Partition { get; set; }
        Dictionary<int, Db> _dbs_value { get; set; }
        public Dictionary<int, Db> _dbs
        {
            get
            {
                return _dbs_value;
            }
            set
            {
                _list_dbs = value.Select(f => new ServerDb() { ServerId = f.Key, db = f.Value }).ToList();
                _dbs_value = value;

                _list_queryable = new List<Tuple<ServerDb, ILinqDbQueryable<T>>>();
                _dic_queryable = new Dictionary<int, ILinqDbQueryable<T>>();
                foreach (var db in _list_dbs)
                {
                    var _inter = new IDbQueryable<T>() { Result = new List<ClientResult>(), _db = db.db._db_internal, Partition = Partition };
                    var queryable = new ILinqDbQueryable<T>() { _internal = _inter };
                    _list_queryable.Add(new Tuple<ServerDb, ILinqDbQueryable<T>>(db, queryable));
                    _dic_queryable[db.ServerId] = queryable;
                }
            }
        }
        List<ServerDb> _list_dbs { get; set; }
        List<Tuple<ServerDb, ILinqDbQueryable<T>>> _list_queryable { get; set; }
        Dictionary<int, ILinqDbQueryable<T>> _dic_queryable { get; set; }

        internal static class ThreadSafeRandom
        {
            private static int _seed = Environment.TickCount;

            // Each thread gets its own Random, seeded uniquely via Interlocked
            private static readonly ThreadLocal<Random> _rng =
                new ThreadLocal<Random>(() => new Random(Interlocked.Increment(ref _seed)));

            public static int Next(int maxValue) => _rng.Value.Next(maxValue);
            public static int Next(int minValue, int maxValue) => _rng.Value.Next(minValue, maxValue);
        }
        ServerDb GetRandomDb()
        {
            if (_list_dbs == null || _list_dbs.Count == 0)
                throw new LinqDbException("Linqdb: no databases configured.");

            int which = ThreadSafeRandom.Next(_list_dbs.Count);
            return _list_dbs[which];
        }
        ServerDb GetDb(int ServerId)
        {
            return new ServerDb()
            {
                ServerId = ServerId,
                db = _dbs[ServerId]
            };
        }

        /// <summary>
        ///  Saves new item if Id is 0 and assigns new (Sid,Id). Updates existing item if (Sid,Id) is existing's item's (Sid,Id).
        /// </summary>
        public DistributedId Save(T item)
        {
            var prop = item.GetType().GetProperty("Id");
            if (prop == null)
            {
                throw new LinqDbException("Linqdb: type must have integer Id property");
            }
            int id = (int)prop.GetValue(item);
            var prop2 = item.GetType().GetProperty("Sid");
            if (prop2 == null)
            {
                throw new LinqDbException("Linqdb: type must have integer Sid property");
            }
            int sid = (int)prop2.GetValue(item);

            if (id == 0 && sid == 0)
            {
                var db = GetRandomDb();
                prop2.SetValue(item, db.ServerId);
                db.db._db_internal.IsDist = true;
                var local_id = db.db.PartitionedTable<T>(Partition).Save(item);
                prop.SetValue(item, local_id);
                // FIX: return the actual shard ServerId
                return new DistributedId()
                {
                    ServerId = db.ServerId,
                    Id = local_id
                };
            }
            else if (sid != 0 && id != 0)
            {
                var db = GetDb(sid);
                db.db._db_internal.IsDist = true;
                db.db.PartitionedTable<T>(Partition).Save(item);
                return new DistributedId()
                {
                    Id = id,
                    ServerId = sid
                };
            }
            else
            {
                throw new LinqDbException("Linqdb: Id and Sid must both be equal to zero (for new items) or both be non-zero to indicate specific item.");
            }
        }
        /// <summary>
        ///  Saves items.
        /// </summary>
        public void SaveBatch(List<T> items)
        {
            if (items == null || !items.Any())
            {
                return;
            }
            var grouped = new Dictionary<int, List<T>>();
            bool isNew = false;
            for (int i = 0; i < items.Count; i++)
            {
                var prop = items[i].GetType().GetProperty("Id");
                if (prop == null)
                {
                    throw new LinqDbException("Linqdb: type must have integer Id property");
                }
                int id = (int)prop.GetValue(items[i]);
                prop = items[i].GetType().GetProperty("Sid");
                if (prop == null)
                {
                    throw new LinqDbException("Linqdb: type must have integer Sid property");
                }
                int sid = (int)prop.GetValue(items[i]);
                if (id != 0 && sid == 0 || id == 0 && sid != 0)
                {
                    throw new LinqDbException("Linqdb: Id and Sid must both be equal to zero (for new items) or both be non-zero to indicate specific item.");
                }
                if (sid != 0)
                {
                    if (!grouped.ContainsKey(sid))
                    {
                        grouped[sid] = new List<T>();
                    }
                    grouped[sid].Add(items[i]);
                }
                if (i == 0)
                {
                    if (id == 0)
                    {
                        isNew = true;
                    }
                }
                else
                {
                    if (isNew && id != 0 || !isNew && id == 0)
                    {
                        throw new LinqDbException("Linqdb: Batch cannot contain both new (Id is 0) and non-new items.");
                    }
                }
            }

            var data = new List<Tuple<Db, List<T>>>();
            if (isNew)
            {
                var dataDbs = new List<Tuple<ServerDb, List<T>>>();
                var total = _list_dbs.Count;
                var size = items.Count / total + 1;
                for (int i = 0; i < total; i++)
                {
                    dataDbs.Add(new Tuple<ServerDb, List<T>>(_list_dbs[i], new List<T>(size)));
                }
                for (int i = 0; i < items.Count; i++)
                {
                    int dbId = i % total;
                    var prop = items[i].GetType().GetProperty("Sid");
                    prop.SetValue(items[i], dataDbs[dbId].Item1.ServerId);
                    dataDbs[dbId].Item2.Add(items[i]);
                }
                // FIX: move projection OUT of the loop to avoid O(n^2)
                data = dataDbs.Select(f => new Tuple<Db, List<T>>(f.Item1.db, f.Item2)).ToList();
            }
            else
            {
                data = grouped.Select(f => new Tuple<Db, List<T>>(GetDb(f.Key).db, f.Value.ToList())).ToList();
            }

            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            Parallel.ForEach(data, d =>
            {
                string db_name = d.Item1.GetIpAndPort();
                try
                {
                    d.Item1._db_internal.IsDist = true;
                    d.Item1.PartitionedTable<T>(Partition).SaveBatch(d.Item2);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Save error", errors);
            }
        }
        /// <summary>
        ///  Saves any amount of items non-atomically, i.e. if it fails in the middle some items will be saved and some won't be.
        /// </summary>
        public void SaveBatchNonAtomically(List<T> items, int batchSize = 5000)
        {
            if (items == null || !items.Any())
            {
                return;
            }
            var grouped = new Dictionary<int, List<T>>();
            bool isNew = false;
            for (int i = 0; i < items.Count; i++)
            {
                var prop = items[i].GetType().GetProperty("Id");
                if (prop == null)
                {
                    throw new LinqDbException("Linqdb: type must have integer Id property");
                }
                int id = (int)prop.GetValue(items[i]);
                prop = items[i].GetType().GetProperty("Sid");
                if (prop == null)
                {
                    throw new LinqDbException("Linqdb: type must have integer Sid property");
                }
                int sid = (int)prop.GetValue(items[i]);
                if (id != 0 && sid == 0 || id == 0 && sid != 0)
                {
                    throw new LinqDbException("Linqdb: Id and Sid must both be equal to zero (for new items) or both be non-zero to indicate specific item.");
                }
                if (sid != 0)
                {
                    if (!grouped.ContainsKey(sid))
                    {
                        grouped[sid] = new List<T>();
                    }
                    grouped[sid].Add(items[i]);
                }
                if (i == 0)
                {
                    if (id == 0)
                    {
                        isNew = true;
                    }
                }
                else
                {
                    if (isNew && id != 0 || !isNew && id == 0)
                    {
                        throw new LinqDbException("Linqdb: Batch cannot contain both new (Id is 0) and non-new items.");
                    }
                }
            }

            var data = new List<Tuple<Db, List<T>>>();
            if (isNew)
            {
                var dataDbs = new List<Tuple<ServerDb, List<T>>>();
                var total = _list_dbs.Count;
                var size = items.Count / total + 1;
                for (int i = 0; i < total; i++)
                {
                    dataDbs.Add(new Tuple<ServerDb, List<T>>(_list_dbs[i], new List<T>(size)));
                }
                for (int i = 0; i < items.Count; i++)
                {
                    int dbId = i % total;
                    var prop = items[i].GetType().GetProperty("Sid");
                    prop.SetValue(items[i], dataDbs[dbId].Item1.ServerId);
                    dataDbs[dbId].Item2.Add(items[i]);
                }
                // FIX: move projection OUT of the loop to avoid O(n^2)
                data = dataDbs.Select(f => new Tuple<Db, List<T>>(f.Item1.db, f.Item2)).ToList();
            }
            else
            {
                data = grouped.Select(f => new Tuple<Db, List<T>>(GetDb(f.Key).db, f.Value.ToList())).ToList();
            }

            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            Parallel.ForEach(data, d =>
            {
                string db_name = d.Item1.GetIpAndPort();
                try
                {
                    d.Item1._db_internal.IsDist = true;
                    d.Item1.PartitionedTable<T>(Partition).SaveNonAtomically(d.Item2, batchSize);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: SaveBatchNonAtomically error", errors);
            }
        }

        /// <summary>
        ///  Record count.
        /// </summary>
        public int Count()
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            int result = 0;
            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    var count = d.Item2.Count();
                    // FIX: use Interlocked to avoid coarse lock on aggregation
                    System.Threading.Interlocked.Add(ref result, count);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Count error", errors);
            }
            return result;
        }

        /// <summary>
        ///  Get selected ids without having to select them (more efficiently).
        /// </summary>
        public SelectedDistributedIds GetIds()
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object errLock = new object();
            var bags = new System.Collections.Concurrent.ConcurrentBag<SelectedDistributedIds>();

            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    var serverId = d.Item1.ServerId;
                    var ids = d.Item2.GetIds();
                    var local = new SelectedDistributedIds()
                    {
                        AllIds = ids.AllIds,
                        Ids = ids.Ids != null && ids.Ids.Any() ? ids.Ids.Select(f => new DistributedId(serverId, f)).ToList() : new List<DistributedId>()
                    };
                    bags.Add(local);
                }
                catch (Exception ex)
                {
                    lock (errLock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: GetIds error", errors);
            }

            var col = bags.ToList();
            var res = new SelectedDistributedIds()
            {
                AllIds = col.All(f => f.AllIds),
                Ids = col.SelectMany(f => f.Ids ?? Enumerable.Empty<DistributedId>()).ToList()
            };
            return res;
        }
        /// <summary>
        ///  Last search step. To be used in step-search.
        /// </summary>
        public int LastStep()
        {
            // Keep error collection as-is
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();

            // Lock-free aggregation for results
            var results = new System.Collections.Concurrent.ConcurrentBag<int>();

            Parallel.ForEach(_list_dbs, d =>
            {
                string db_name = d.db.GetIpAndPort();
                try
                {
                    var lastStep = d.db.PartitionedTable<T>(Partition).LastStep();
                    results.Add(lastStep);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });

            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: LastStep error", errors);
            }

            if (!results.Any())
            {
                return -1;
            }
            return results.Max();
        }

        /// <summary>
        ///  Deletes item with given Id.
        /// </summary>
        public void Delete(DistributedId id)
        {
            var db = GetDb(id.ServerId);
            db.db.PartitionedTable<T>(Partition).Delete(id.Id);
        }
        /// <summary>
        ///  Deletes items.
        /// </summary>
        public void Delete(List<DistributedId> ids)
        {
            if (ids == null || !ids.Any())
            {
                return;
            }

            var data = ids.GroupBy(f => f.ServerId).Select(f => new Tuple<Db, HashSet<int>>(GetDb(f.Key).db, new HashSet<int>(f.Select(z => z.Id).ToList()))).ToList();

            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            Parallel.ForEach(data, d =>
            {
                string db_name = d.Item1.GetIpAndPort();
                try
                {
                    d.Item1.PartitionedTable<T>(Partition).Delete(d.Item2);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Delete error", errors);
            }
        }
        /// <summary>
        ///  Deletes any amount of items non-atomically, i.e. if it fails in the middle some items will be deleted and some won't be.
        /// </summary>
        public void DeleteNonAtomically(List<DistributedId> ids, int batchSize = 5000)
        {
            if (ids == null || !ids.Any())
            {
                return;
            }

            var data = ids.GroupBy(f => f.ServerId).Select(f => new Tuple<Db, HashSet<int>>(GetDb(f.Key).db, new HashSet<int>(f.Select(z => z.Id).ToList()))).ToList();

            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            Parallel.ForEach(data, d =>
            {
                string db_name = d.Item1.GetIpAndPort();
                try
                {
                    d.Item1.PartitionedTable<T>(Partition).DeleteNonAtomically(d.Item2, batchSize);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: DeleteNonAtomically error", errors);
            }
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by DistributedId.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, DistributedId id, TKey value)
        {
            var db = GetDb(id.ServerId);
            db.db.PartitionedTable<T>(Partition).Update(keySelector, id.Id, value);
        }

        /// <summary>
        ///  Updates items' fields with supplied values. Item is identified by DistributedId.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<DistributedId, TKey> values)
        {
            if (values == null || !values.Any())
            {
                return;
            }

            var data = values.GroupBy(f => f.Key.ServerId).Select(f => new Tuple<Db, Dictionary<int, TKey>>(GetDb(f.Key).db, f.ToDictionary(z => z.Key.Id, z => z.Value))).ToList();

            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            Parallel.ForEach(data, d =>
            {
                string db_name = d.Item1.GetIpAndPort();
                try
                {
                    d.Item1.PartitionedTable<T>(Partition).Update(keySelector, d.Item2);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Update error", errors);
            }
        }
        /// <summary>
        ///  Updates items' fields with supplied values. Item is identified by DistributedId.
        /// </summary>
        public void UpdateNonAtomically<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<DistributedId, TKey> values, int batchSize = 5000)
        {
            if (values == null || !values.Any())
            {
                return;
            }

            var data = values.GroupBy(f => f.Key.ServerId).Select(f => new Tuple<Db, Dictionary<int, TKey>>(GetDb(f.Key).db, f.ToDictionary(z => z.Key.Id, z => z.Value))).ToList();

            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            Parallel.ForEach(data, d =>
            {
                string db_name = d.Item1.GetIpAndPort();
                try
                {
                    d.Item1.PartitionedTable<T>(Partition).UpdateNonAtomically(keySelector, d.Item2, batchSize);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: UpdateNonAtomically error", errors);
            }
        }

        /// <summary>
        ///  Selects anonymous type using result entities. Select only what's needed as it is more efficient.
        /// </summary>
        public List<R> Select<R>(Expression<Func<T, R>> predicate)
        {
            var errors = new Dictionary<string, List<Exception>>();
            object errLock = new object();

            var bags = new System.Collections.Concurrent.ConcurrentBag<List<R>>();

            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    var local = d.Item2.Select(predicate).ToList();
                    bags.Add(local);
                }
                catch (Exception ex)
                {
                    lock (errLock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });

            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Select error", errors);
            }

            return bags.SelectMany(x => x).ToList();
        }

        /// <summary>
        ///  Selects anonymous type using result entities. Select only what's needed as it is more efficient. Statistics is used as out parameter.
        /// </summary>
        public List<R> Select<R>(Expression<Func<T, R>> predicate, LinqdbSelectStatistics statistics)
        {
            var errors = new Dictionary<string, List<Exception>>();
            object errLock = new object();

            var bags = new System.Collections.Concurrent.ConcurrentBag<List<R>>();
            var statsList = new Dictionary<int, LinqdbSelectStatistics>();

            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    LinqdbSelectStatistics stat = new LinqdbSelectStatistics();
                    var local = d.Item2.Select(predicate, stat).ToList();
                    bags.Add(local);
                    lock (errLock)
                    {
                        statsList[d.Item1.ServerId] = stat;
                    }
                }
                catch (Exception ex)
                {
                    lock (errLock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });

            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Select error", errors);
            }

            statistics.DistancesDist = new Dictionary<Tuple<int, int>, double>();
            foreach (var stat in statsList.Where(f => f.Value.Distances != null).ToList())
            {
                foreach (var item in stat.Value.Distances)
                {
                    statistics.DistancesDist[new Tuple<int, int>(stat.Key, item.Key)] = item.Value;
                }
            }

            return bags.SelectMany(x => x).ToList();
        }
        /// <summary>
        ///  Selects anonymous type using result entities. Select only what's needed as it is more efficient.
        /// </summary>
        public List<R> SelectNonAtomically<R>(Expression<Func<T, R>> predicate, int batchSize = 3000)
        {
            var errors = new Dictionary<string, List<Exception>>();
            object errLock = new object();
            var bags = new System.Collections.Concurrent.ConcurrentBag<List<R>>();

            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    var local = d.Item2.SelectNonAtomically(predicate, batchSize).ToList();
                    bags.Add(local);
                }
                catch (Exception ex)
                {
                    lock (errLock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });

            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: SelectNonAtomically error", errors);
            }

            return bags.SelectMany(x => x).ToList();
        }

        /// <summary>
        ///  Selects entire entities using result set.
        /// </summary>
        public List<T> SelectEntity()
        {
            var errors = new Dictionary<string, List<Exception>>();
            object errLock = new object();
            var bags = new System.Collections.Concurrent.ConcurrentBag<List<T>>();

            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    var local = d.Item2.SelectEntity().ToList();
                    bags.Add(local);
                }
                catch (Exception ex)
                {
                    lock (errLock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: SelectEntity error", errors);
            }

            return bags.SelectMany(x => x).ToList();
        }

        /// <summary>
        ///  Selects entire entities using result set. Statistics is used as out parameter.
        /// </summary>
        public List<T> SelectEntity(LinqdbSelectStatistics statistics)
        {
            var errors = new Dictionary<string, List<Exception>>();
            object errLock = new object();
            var bags = new System.Collections.Concurrent.ConcurrentBag<List<T>>();
            var statsList = new Dictionary<int, LinqdbSelectStatistics>();

            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    LinqdbSelectStatistics stat = new LinqdbSelectStatistics();
                    var local = d.Item2.SelectEntity(stat).ToList();
                    bags.Add(local);
                    lock (errLock)
                    {
                        statsList[d.Item1.ServerId] = stat;
                    }
                }
                catch (Exception ex)
                {
                    lock (errLock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: SelectEntity error", errors);
            }

            statistics.DistancesDist = new Dictionary<Tuple<int, int>, double>();
            foreach (var stat in statsList.Where(f => f.Value.Distances != null).ToList())
            {
                foreach (var item in stat.Value.Distances)
                {
                    statistics.DistancesDist[new Tuple<int, int>(stat.Key, item.Key)] = item.Value;
                }
            }

            return bags.SelectMany(x => x).ToList();
        }

        /// <summary>
        ///  Selects entire entities using result set.
        /// </summary>
        public List<T> SelectEntityNonAtomically(int batchSize = 3000)
        {
            var errors = new Dictionary<string, List<Exception>>();
            object errLock = new object();
            var bags = new System.Collections.Concurrent.ConcurrentBag<List<T>>();

            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    var local = d.Item2.SelectEntityNonAtomically(batchSize).ToList();
                    bags.Add(local);
                }
                catch (Exception ex)
                {
                    lock (errLock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: SelectEntityNonAtomically error", errors);
            }

            return bags.SelectMany(x => x).ToList();
        }


        /// <summary>
        ///  Applies between condition to the result set.
        /// </summary>
        public ILinqDbDistributedPartitionedQueryable<T> Between<TKey>(Expression<Func<T, TKey>> keySelector, TKey from, TKey to, BetweenBoundaries boundaries = BetweenBoundaries.BothInclusive)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Between(keySelector, from, to, (BetweenBoundariesInternal)(int)boundaries);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }

        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbDistributedPartitionedQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, HashSet<TKey> set)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Intersect(keySelector, set);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }

        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbDistributedPartitionedQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, List<TKey> set)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Intersect(keySelector, new HashSet<TKey>(set));
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }

        /// <summary>
        ///  Applies intersect with distributed ids. Handy when used together with efficient .GetIds method.
        /// </summary>
        public ILinqDbDistributedPartitionedQueryable<T> IntersectWithDistributedIds(List<DistributedId> ids)
        {
            return IntersectWithDistributedIds(new HashSet<DistributedId>(ids));
        }
        /// <summary>
        ///  Applies intersect with distributed ids. Handy when used together with efficient .GetIds method.
        /// </summary>
        public ILinqDbDistributedPartitionedQueryable<T> IntersectWithDistributedIds(HashSet<DistributedId> ids)
        {
            var data = ids.GroupBy(f => f.ServerId).ToDictionary(f => f.Key, f => new HashSet<int>(f.Select(z => z.Id)));
            var keySelector = BuildKeySelector<T, int>("Id");

            foreach (var db in _list_queryable)
            {
                if (data.ContainsKey(db.Item1.ServerId))
                {
                    var result = _dic_queryable[db.Item1.ServerId]._internal._db.Intersect(keySelector, data[db.Item1.ServerId]);
                    _dic_queryable[db.Item1.ServerId]._internal.Result.Add(result);
                }
                else
                {
                    var result = _dic_queryable[db.Item1.ServerId]._internal._db.Intersect(keySelector, new HashSet<int>());
                    _dic_queryable[db.Item1.ServerId]._internal.Result.Add(result);
                }
            }

            return this;
        }

        public static Expression<Func<T, TKey>> BuildKeySelector<T, TKey>(string propertyName)
        {
            var param = Expression.Parameter(typeof(T), "f");
            var body = Expression.PropertyOrField(param, propertyName);

            if (body.Type != typeof(TKey))
            {
                throw new InvalidOperationException($"Property '{propertyName}' is not of type {typeof(TKey).Name}");
            }

            return Expression.Lambda<Func<T, TKey>>(body, param);
        }

        /// <summary>
        ///  Full text search on a column.
        /// </summary>
        public ILinqDbDistributedPartitionedQueryable<T> Search<TKey>(Expression<Func<T, TKey>> keySelector, string search_query, int? start_step = null, int? steps = null)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Search(keySelector, search_query, false, false, 0, start_step, steps);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Full text search on a column matching beginning of a word.
        /// </summary>
        public ILinqDbDistributedPartitionedQueryable<T> SearchPartial<TKey>(Expression<Func<T, TKey>> keySelector, string search_query)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Search(keySelector, search_query, true, false, 0, null, null);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Full text search on a column, limited by time period.
        /// </summary>
        public ILinqDbDistributedPartitionedQueryable<T> SearchTimeLimited<TKey>(Expression<Func<T, TKey>> keySelector, string search_query, int maxSearchTimeInMs)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Search(keySelector, search_query, false, true, maxSearchTimeInMs, null, null);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        /// Vector space search for k-nearest neighbours. Uses aproximate on-disk algorithm.
        /// </summary>
        public ILinqDbDistributedPartitionedQueryable<T> SearchNeighbours<TKey>(Expression<Func<T, TKey>> keySelector, float[] vector, int k)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.SearchNeighbours(keySelector, vector, k);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Applies where condition to the result set.
        /// </summary>
        public ILinqDbDistributedPartitionedQueryable<T> Where(Expression<Func<T, bool>> predicate)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Where(predicate);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }


        /// <summary>
        ///  Applies or condition to the neighbouring statements.
        /// </summary>
        public ILinqDbDistributedPartitionedQueryable<T> Or()
        {
            foreach (var db in _list_queryable)
            {
                var result = Ldb_ext.Or<T>();
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Orders data by ascending values.
        /// </summary>
        public ILinqDbOrderedDistributedQueryable<T, TKey> OrderBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            var list_ordered_queryable = new List<Tuple<ServerDb, ILinqDbOrderedQueryable<T>>>();
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.OrderBy(keySelector);
                db.Item2._internal.Result.Add(result);

                var _inter = new IDbOrderedQueryable<T>() { Result = db.Item2._internal.Result, _db = db.Item2._internal._db, Partition = db.Item2._internal.Partition };
                list_ordered_queryable.Add(new Tuple<ServerDb, ILinqDbOrderedQueryable<T>>(db.Item1, new ILinqDbOrderedQueryable<T>() { _internal = _inter }));
            }

            return new ILinqDbOrderedDistributedQueryable<T, TKey>() { _list_ordered_queryable = list_ordered_queryable, keySelector = keySelector, Asc = true };
        }
        /// <summary>
        ///  Orders data by descending values.
        /// </summary>
        public ILinqDbOrderedDistributedQueryable<T, TKey> OrderByDescending<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            var list_ordered_queryable = new List<Tuple<ServerDb, ILinqDbOrderedQueryable<T>>>();
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.OrderByDescending(keySelector);
                db.Item2._internal.Result.Add(result);

                var _inter = new IDbOrderedQueryable<T>() { Result = db.Item2._internal.Result, _db = db.Item2._internal._db, Partition = db.Item2._internal.Partition };
                list_ordered_queryable.Add(new Tuple<ServerDb, ILinqDbOrderedQueryable<T>>(db.Item1, new ILinqDbOrderedQueryable<T>() { _internal = _inter }));
            }

            return new ILinqDbOrderedDistributedQueryable<T, TKey>() { _list_ordered_queryable = list_ordered_queryable, keySelector = keySelector, Asc = false };
        }

    }

    public class SelectedDistributedIds
    {
        public List<DistributedId> Ids { get; set; }
        public bool AllIds { get; set; }
    }

    public class ILinqDbGroupedDistributedQueryable<T, TKey> where T : new()
    {
        public List<Tuple<ServerDb, ILinqDbGroupedQueryable<T, TKey>>> _list_grouped_queryable { get; set; }

        public List<dynamic> Select<R>(Expression<Func<IGrouping<TKey, T>, R>> predicate)
        {
            var errors = new Dictionary<string, List<Exception>>();
            object errLock = new object();

            // Lock-free data path: gather per-thread, then merge
            var bags = new System.Collections.Concurrent.ConcurrentBag<List<R>>();

            Parallel.ForEach(_list_grouped_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    var local = d.Item2.Select(predicate).ToList();
                    bags.Add(local);
                }
                catch (Exception ex)
                {
                    lock (errLock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });

            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Select (groupby) error", errors);
            }

            var result = bags.SelectMany(x => x).ToList();

            // Step 2: Final aggregation after grouping the results
            var key = typeof(R).GetProperty("Key");
            if (key == null)
            {
                throw new LinqDbException("Linqdb: 'Key' property is mandatory in anonymous type.");
            }
            var groupedResult = result.GroupBy(f => GetKeyValue(f, "Key"));

            // Parse the expression to detect aggregations
            var parsedAggregations = ParseAggregations(predicate);

            var finalResult = groupedResult.Select(group =>
            {
                // Create a dynamic object to hold the final results
                dynamic aggregatedObject = new ExpandoObject();
                var expandoDict = (IDictionary<string, object>)aggregatedObject;

                // Set the key dynamically
                expandoDict["Key"] = group.Key;

                // Apply the detected aggregations dynamically
                foreach (var aggregation in parsedAggregations)
                {
                    var propertyName = aggregation.PropertyName;
                    var aggregationType = aggregation.AggregationType;

                    if (aggregationType == "Sum")
                    {
                        var values = group.Select(item => ConvertToComparableNumericOrDateTime(GetDynamicValue(item, propertyName))).ToList();
                        object firstNonNull = values.FirstOrDefault(v => v != null);

                        if (firstNonNull == null)
                        {
                            expandoDict[propertyName] = 0;
                        }
                        else if (firstNonNull is int)
                        {
                            int sum = 0;
                            foreach (var v in values) if (v != null) sum = checked(sum + (int)v);
                            expandoDict[propertyName] = sum;
                        }
                        else if (firstNonNull is long)
                        {
                            long sum = 0L;
                            foreach (var v in values) if (v != null) sum = checked(sum + (long)v);
                            expandoDict[propertyName] = sum;
                        }
                        else if (firstNonNull is decimal)
                        {
                            decimal sum = 0m;
                            foreach (var v in values) if (v != null) sum += (decimal)v;
                            expandoDict[propertyName] = sum;
                        }
                        else if (firstNonNull is double)
                        {
                            double sum = 0d;
                            foreach (var v in values) if (v != null) sum += (double)v;
                            expandoDict[propertyName] = sum;
                        }
                        else
                        {
                            throw new LinqDbException($"Linqdb: Sum got unsupported type '{firstNonNull.GetType().Name}'.");
                        }
                    }
                    else if (aggregationType == "Count")
                    {
                        var res = group.Select(item => ConvertToComparableNumericOrDateTime(GetDynamicValue(item, propertyName))) as IEnumerable;
                        int sum = 0;
                        foreach (var r in res)
                        {
                            if (r != null)
                            {
                                sum += (int)r;
                            }
                        }
                        expandoDict[propertyName] = sum;
                    }
                    else if (aggregationType == "Min")
                    {
                        expandoDict[propertyName] = group.Min(item => ConvertToComparableNumericOrDateTime(GetDynamicValue(item, propertyName)));
                    }
                    else if (aggregationType == "Max")
                    {
                        expandoDict[propertyName] = group.Max(item => ConvertToComparableNumericOrDateTime(GetDynamicValue(item, propertyName)));
                    }
                    else
                    {
                        throw new LinqDbException($"Linqdb: '{aggregationType}' aggreagtion operator is not supported. Supported operators are 'Count', 'Sum', 'Min' and 'Max'");
                    }
                }

                return aggregatedObject;
            }).ToList();

            return finalResult; // Return as List<dynamic>
        }

        // Helper class to represent parsed aggregation information
        private class AggregationInfo
        {
            public string PropertyName { get; set; }
            public string AggregationType { get; set; }
        }

        private List<AggregationInfo> ParseAggregations<R>(Expression<Func<IGrouping<TKey, T>, R>> predicate)
        {
            var aggregations = new List<AggregationInfo>();
            var body = predicate.Body;

            if (body is NewExpression newExpr)
            {
                for (int i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var argument = newExpr.Arguments[i];
                    var member = newExpr.Members[i];

                    if (argument is MethodCallExpression methodCall)
                    {
                        var aggregationType = methodCall.Method.Name;

                        var propertyName = member.Name;

                        aggregations.Add(new AggregationInfo
                        {
                            PropertyName = propertyName,
                            AggregationType = aggregationType
                        });
                    }
                }
            }

            return aggregations;
        }

        private object ConvertToComparableNumericOrDateTime(dynamic value)
        {
            if (value == null)
            {
                return null;
            }

            if (value is int)
            {
                return (int)value;
            }
            if (value is double)
            {
                return (double)value;
            }
            if (value is DateTime)
            {
                return (DateTime)value;
            }
            if (value is decimal)
            {
                return (decimal)value;
            }
            if (value is long)
            {
                return (long)value;
            }

            return null;
        }

        // Helper to dynamically get the value of a property
        private dynamic GetDynamicValue<R>(R obj, string propertyName)
        {
            var propertyInfo = typeof(R).GetProperty(propertyName);
            return propertyInfo?.GetValue(obj);
        }

        // Helper to get the value of the "Key" property for grouping
        private dynamic GetKeyValue<R>(R obj, string keyPropertyName)
        {
            var keyProperty = typeof(R).GetProperty(keyPropertyName);
            return keyProperty?.GetValue(obj);
        }

    }

    public class ILinqDbOrderedDistributedQueryable<T, TKey> where T : new()
    {
        public List<Tuple<ServerDb, ILinqDbOrderedQueryable<T>>> _list_ordered_queryable { get; set; }
        public Expression<Func<T, TKey>> keySelector { get; set; }
        public int TakeCount { get; set; }
        public bool Asc { get; set; }

        /// <summary>
        ///  Selects anonymous type using result entities. Select only what's needed as it is more efficient.
        /// </summary>
        public List<R> Select<R>(Expression<Func<T, R>> predicate)
        {
            var errors = new Dictionary<string, List<Exception>>();
            object errLock = new object();

            var bags = new System.Collections.Concurrent.ConcurrentBag<List<R>>();

            Parallel.ForEach(_list_ordered_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    var local = d.Item2.Select(predicate).ToList();
                    bags.Add(local);
                }
                catch (Exception ex)
                {
                    lock (errLock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Select error (orderby)", errors);
            }

            var all = bags.SelectMany(x => x).ToList();

            var expName = keySelector.Body.ToString();
            var propName = expName.Substring(expName.IndexOf('.') + 1);
            var res = OrderListByProperty(all, propName, Asc);

            if (this.TakeCount > 0)
            {
                res = res.Take(TakeCount).ToList();
            }

            return res;
        }

        List<R> OrderListByProperty<R>(List<R> list, string propertyName, bool ascending)
        {
            PropertyInfo property = typeof(R).GetProperty(propertyName);

            if (property == null)
            {
                throw new LinqDbException($"Linqdb: Property '{propertyName}' must be present in anonymous type.");
            }

            if (!property.PropertyType.IsValueType && property.PropertyType != typeof(string))
            {
                throw new ArgumentException($"Linqdb: Property '{propertyName}' must be a value type or string");
            }

            return ascending ? list.OrderBy(f => property.GetValue(f)).ToList() : list.OrderByDescending(f => property.GetValue(f)).ToList();
        }

        /// <summary>
        ///  Selects anonymous type using result entities. Select only what's needed as it is more efficient. Statistics is used as out parameter.
        /// </summary>
        public List<R> Select<R>(Expression<Func<T, R>> predicate, LinqdbSelectStatistics statistics)
        {
            var errors = new Dictionary<string, List<Exception>>();
            object errLock = new object();

            var bags = new System.Collections.Concurrent.ConcurrentBag<List<R>>();
            var statsList = new List<LinqdbSelectStatistics>();

            Parallel.ForEach(_list_ordered_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    LinqdbSelectStatistics stat = new LinqdbSelectStatistics();
                    var local = d.Item2.Select(predicate, stat).ToList();
                    bags.Add(local);
                    lock (errLock)
                    {
                        statsList.Add(stat);
                    }
                }
                catch (Exception ex)
                {
                    lock (errLock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Select error (orderby, statistics)", errors);
            }

            statistics.Total = statsList.Sum(f => f.Total);

            var all = bags.SelectMany(x => x).ToList();

            var expName = keySelector.Body.ToString();
            var propName = expName.Substring(expName.IndexOf('.') + 1);
            var res = OrderListByProperty(all, propName, Asc);

            if (this.TakeCount > 0)
            {
                res = res.Take(TakeCount).ToList();
            }

            return res;
        }

        /// <summary>
        ///  Selects entire entities using result set.
        /// </summary>
        public List<T> SelectEntity()
        {
            var errors = new Dictionary<string, List<Exception>>();
            object errLock = new object();

            var bags = new System.Collections.Concurrent.ConcurrentBag<List<T>>();

            Parallel.ForEach(_list_ordered_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    var local = d.Item2.SelectEntity().ToList();
                    bags.Add(local);
                }
                catch (Exception ex)
                {
                    lock (errLock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: SelectEntity error (orderby)", errors);
            }

            var all = bags.SelectMany(x => x).ToList();

            var expName = keySelector.Body.ToString();
            var propName = expName.Substring(expName.IndexOf('.') + 1);
            var res = OrderListByProperty(all, propName, Asc);

            if (this.TakeCount > 0)
            {
                res = res.Take(TakeCount).ToList();
            }

            return res;
        }

        /// <summary>
        ///  Selects entire entities using result set. Statistics is used as out parameter.
        /// </summary>
        public List<T> SelectEntity(LinqdbSelectStatistics statistics)
        {
            var errors = new Dictionary<string, List<Exception>>();
            object errLock = new object();

            var bags = new System.Collections.Concurrent.ConcurrentBag<List<T>>();
            var statsList = new List<LinqdbSelectStatistics>();

            Parallel.ForEach(_list_ordered_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    LinqdbSelectStatistics stat = new LinqdbSelectStatistics();
                    var local = d.Item2.SelectEntity(stat).ToList();
                    bags.Add(local);
                    lock (errLock)
                    {
                        statsList.Add(stat);
                    }
                }
                catch (Exception ex)
                {
                    lock (errLock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: SelectEntity error (orderby)", errors);
            }

            statistics.Total = statsList.Sum(f => f.Total);
            var all = bags.SelectMany(x => x).ToList();

            var expName = keySelector.Body.ToString();
            var propName = expName.Substring(expName.IndexOf('.') + 1);
            var res = OrderListByProperty(all, propName, Asc);

            if (this.TakeCount > 0)
            {
                res = res.Take(TakeCount).ToList();
            }

            return res;
        }


        /// <summary>
        ///  Takes some ordered values.
        /// </summary>
        public ILinqDbOrderedDistributedQueryable<T, TKey> Take(int count)
        {
            foreach (var db in _list_ordered_queryable)
            {
                var result = db.Item2._internal._db.Take<T>(count);
                db.Item2._internal.Result.Add(result);
            }
            TakeCount = count;
            return this;
        }

    }

    public class IDbGroupedDistributedQueryable<T>
    {
        public Ldb _db { get; set; }
        public List<ClientResult> Result = new List<ClientResult>();
    }
}

