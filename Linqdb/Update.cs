using RocksDbSharp;
using ServerSharedData;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        public void UpdateIncrement<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, int?> values, IDbQueryable<T> source)
        {
            GenericUpdateIncrement<T, TKey>(keySelector, values.ToDictionary(f => f.Key, f => (object)f.Value), source);
        }
        public void Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, TKey> values, IDbQueryable<T> source)
        {
            if (values.Any(f => f.Value == null))
            {
                MemberExpression memberExpression = (keySelector.Body as MemberExpression) ?? ((keySelector.Body as UnaryExpression).Operand as MemberExpression);
                if (!(memberExpression.Member is PropertyInfo propertyInfo))
                {
                    throw new ArgumentException("The member accessed by the expression is not a property.", nameof(keySelector));
                }
                Type propertyType = propertyInfo.PropertyType;
                bool isNullable = !propertyType.IsValueType || Nullable.GetUnderlyingType(propertyType) != null;

                if (!isNullable)
                {
                    throw new LinqDbException("Linqdb: Non-nullable type is updated with null value.");
                }
            }


            CheckTableInfo<T>(source.Partition);
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector);
            var type_name = GetTypeName<T>(source.Partition);
            var table_info = GetTableInfo(type_name);

            var info = new UpdateInfo()
            {
                TableNumber = table_info.TableNumber,
                ColumnNumber = table_info.ColumnNumbers[name],
                ColumnType = table_info.Columns[name],
                TableInfo = table_info,
                ColumnName = name
            };

            var data = new Dictionary<int, object>();
            foreach (var v in values)
            {
                data[v.Key] = v.Value;
            }
            Update<T>(info, data, table_info, source);
        }
        //public void Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, int> values, IDbQueryable<T> source)
        //{
        //    GenericUpdate<T, TKey>(keySelector, values.ToDictionary(f => f.Key, f => (object)f.Value), source, LinqDbTypes.int_);
        //}
        //public void Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, long?> values, IDbQueryable<T> source)
        //{
        //    GenericUpdate<T, TKey>(keySelector, values.ToDictionary(f => f.Key, f => (object)f.Value), source, LinqDbTypes.long_);
        //}
        //public void Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, long> values, IDbQueryable<T> source)
        //{
        //    GenericUpdate<T, TKey>(keySelector, values.ToDictionary(f => f.Key, f => (object)f.Value), source, LinqDbTypes.long_);
        //}
        //public void Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, double?> values, IDbQueryable<T> source)
        //{
        //    GenericUpdate<T, TKey>(keySelector, values.ToDictionary(f => f.Key, f => (object)f.Value), source, LinqDbTypes.double_);
        //}
        //public void Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, double> values, IDbQueryable<T> source)
        //{
        //    GenericUpdate<T, TKey>(keySelector, values.ToDictionary(f => f.Key, f => (object)f.Value), source, LinqDbTypes.double_);
        //}
        //public void Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, decimal?> values, IDbQueryable<T> source)
        //{
        //    GenericUpdate<T, TKey>(keySelector, values.ToDictionary(f => f.Key, f => (object)f.Value), source, LinqDbTypes.decimal_);
        //}
        //public void Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, decimal> values, IDbQueryable<T> source)
        //{
        //    GenericUpdate<T, TKey>(keySelector, values.ToDictionary(f => f.Key, f => (object)f.Value), source, LinqDbTypes.decimal_);
        //}
        //public void Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, DateTime?> values, IDbQueryable<T> source)
        //{
        //    GenericUpdate<T, TKey>(keySelector, values.ToDictionary(f => f.Key, f => (object)f.Value), source, LinqDbTypes.DateTime_);
        //}
        //public void Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, DateTime> values, IDbQueryable<T> source)
        //{
        //    GenericUpdate<T, TKey>(keySelector, values.ToDictionary(f => f.Key, f => (object)f.Value), source, LinqDbTypes.DateTime_);
        //}
        //public void Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, byte[]> values, IDbQueryable<T> source)
        //{
        //    GenericUpdate<T, TKey>(keySelector, values.ToDictionary(f => f.Key, f => (object)f.Value), source, LinqDbTypes.binary_);
        //}
        //public void Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, string> values, IDbQueryable<T> source)
        //{
        //    GenericUpdate<T, TKey>(keySelector, values.ToDictionary(f => f.Key, f => (object)f.Value), source, LinqDbTypes.string_);
        //}
        //public void GenericUpdate<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, object> values, IDbQueryable<T> source, LinqDbTypes type)
        //{
        //    CheckTableInfo<T>(source.Partition);
        //    var par = keySelector.Parameters.First();
        //    var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
        //    var type_name = GetTypeName<T>(source.Partition);
        //    var table_info = GetTableInfo(type_name);


        //    if (table_info.Columns[name] != type)
        //    {
        //        throw new LinqDbException("Linqdb: wrong data type for given column.");
        //    }

        //    var info = new UpdateInfo()
        //    {
        //        TableNumber = table_info.TableNumber,
        //        ColumnNumber = table_info.ColumnNumbers[name],
        //        ColumnType = table_info.Columns[name],
        //        TableInfo = table_info,
        //        ColumnName = name
        //    };

        //    var data = new Dictionary<int, object>();
        //    foreach (var v in values)
        //    {
        //        data[v.Key] = v.Value;
        //    }
        //    Update<T>(info, data, table_info, source);
        //}
        public void GenericUpdateIncrement<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, object> values, IDbQueryable<T> source)
        {
            CheckTableInfo<T>(source?.Partition);
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector);
            var type_name = GetTypeName<T>(source?.Partition);
            var table_info = GetTableInfo(type_name);
            var info = new UpdateInfo()
            {
                TableNumber = table_info.TableNumber,
                ColumnNumber = table_info.ColumnNumbers[name],
                ColumnType = table_info.Columns[name],
                TableInfo = table_info,
                ColumnName = name
            };

            var data = new Dictionary<int, object>();
            foreach (var v in values)
            {
                data[v.Key] = v.Value;
            }
            UpdateIncrement<T>(info, data, table_info, source);
        }

        public void UpdateIncrement<T>(UpdateInfo info, Dictionary<int, object> values, TableInfo table_info, IDbQueryable<T> source)
        {
            Dictionary<int, object> current_values = new Dictionary<int, object>();
            foreach (var kv in values)
            {
                current_values[kv.Key] = kv.Value;
            }
            if (current_values.Any())
            {
                UpdateBatchIncrement<T>(info, current_values, table_info, source);
            }
        }
        public void Update<T>(UpdateInfo info, Dictionary<int, object> values, TableInfo table_info, IDbQueryable<T> source)
        {
            Dictionary<int, object> current_values = new Dictionary<int, object>();
            foreach (var kv in values)
            {
                current_values[kv.Key] = kv.Value;
            }
            if (current_values.Any())
            {
                UpdateBatch<T>(info, current_values, table_info, source);
            }
        }

        public void UpdateBatchIncrement<T>(UpdateInfo info, Dictionary<int, object> values, TableInfo table_info, IDbQueryable<T> source)
        {
            var type_name = GetTypeName<T>(source?.Partition);
            var _write_lock = GetTableWriteLock(type_name);
            if (source?.LDBTransaction == null)
            {
                lock (_write_lock)
                {
                    using (WriteBatchWithConstraints batch = new WriteBatchWithConstraints())
                    {
                        Dictionary<string, KeyValuePair<byte[], HashSet<int>>> string_cache = new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>();
                        Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData, int, int>> meta_index = BuildMetaOnIndex(table_info);
                        UpdateBatch(info, values, table_info, batch, string_cache, meta_index, false);
                        WriteStringCacheToBatch(batch, string_cache, table_info, null);
                        var snapshots_dic = InsertIndexChanges(table_info, meta_index);
                        foreach (var snap in snapshots_dic)
                        {
                            var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers[snap.Key]);
                            batch.Put(skey, Encoding.UTF8.GetBytes(snap.Value));
                        }
                        leveld_db.Write(batch._writeBatch);
                    }
                }
            }
            else
            {
                info.TableInfo = table_info;
                if (!source.LDBTransaction.data_to_update.ContainsKey(type_name))
                {
                    source.LDBTransaction.data_to_update[type_name] = new List<KeyValuePair<UpdateInfo, Dictionary<int, object>>>();
                }
                source.LDBTransaction.data_to_update[type_name].Add(new KeyValuePair<UpdateInfo, Dictionary<int, object>>(info, values));
            }
        }

        public void UpdateBatch<T>(UpdateInfo info, Dictionary<int, object> values, TableInfo table_info, IDbQueryable<T> source)
        {
            if (source.LDBTransaction == null)
            {
                bool done = false;
                string error = null;
                var ilock = ModifyBatch.GetTableUpdateBatchLock(table_info.Name);
                var key = table_info.Name + "|" + info.ColumnName;
                lock (ilock)
                {
                    if (!ModifyBatch._update_batch.ContainsKey(key))
                    {
                        ModifyBatch._update_batch[key] = new UpdateData() { Callbacks = new List<Action<string>>(), values = values};
                    }
                    else
                    {
                        var vals = ModifyBatch._update_batch[key].values;
                        foreach (var v in values)
                        {
                            if (!vals.ContainsKey(v.Key))
                            {
                                vals[v.Key] = v.Value;
                            }
                        }
                    }
                    ModifyBatch._update_batch[key].Callbacks.Add(f =>
                    {
                        done = true;
                        error = f;
                    });
                }
                var type_name = GetTypeName<T>(source.Partition);
                var _write_lock = GetTableWriteLock(type_name);

                bool lockAcquired = false;
                int maxWaitMs = 60000;
                UpdateData _update_data = null;
                try
                {
                    DateTime start = DateTime.Now;
                    while (!done)
                    {
                        lockAcquired = Monitor.TryEnter(_write_lock, 0);
                        if (lockAcquired)
                        {
                            if (done)
                            {
                                Monitor.Exit(_write_lock);
                                lockAcquired = false;
                                break;
                            }
                            else
                            {
                                break;
                            }
                        }
                        Thread.Sleep(250);
                        //if ((DateTime.Now - start).TotalMilliseconds > maxWaitMs)
                        //{
                        //    throw new LinqDbException("Linqdb: Update waited too long to acquire write lock. Is the load too high?");
                        //}
                    }
                    if (done)
                    {
                        if (!string.IsNullOrEmpty(error))
                        {
                            throw new LinqDbException(error);
                        }
                        else
                        {
                            return;
                        }
                    }

                    //not done, but have write lock for the table
                    lock (ilock)
                    {
                        _update_data = ModifyBatch._update_batch[key];
                        var oval = new UpdateData();
                        ModifyBatch._update_batch.TryRemove(key, out oval);
                    }
                    if (_update_data.values.Any())
                    {
                        using (WriteBatchWithConstraints batch = new WriteBatchWithConstraints())
                        {
                            Dictionary<string, KeyValuePair<byte[], HashSet<int>>> string_cache = new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>();
                            Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData, int, int>> meta_index = BuildMetaOnIndex(table_info);
                            UpdateBatch(info, _update_data.values, table_info, batch, string_cache, meta_index, false);
                            WriteStringCacheToBatch(batch, string_cache, table_info, null);
                            var snapshots_dic = InsertIndexChanges(table_info, meta_index);
                            foreach (var snap in snapshots_dic)
                            {
                                var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers[snap.Key]);
                                batch.Put(skey, Encoding.UTF8.GetBytes(snap.Value));
                            }
                            leveld_db.Write(batch._writeBatch);
                        }
                    }
                    foreach (var cb in _update_data.Callbacks)
                    {
                        cb(null);
                    }
                }
                catch (Exception ex)
                {
                    if (_update_data != null)
                    {
                        var additionalInfo = ex.Message;
                        if (_update_data.Callbacks.Count() > 1)
                        {
                            additionalInfo += " This error could belong to another entity which happened to be in the same batch.";
                        }
                        foreach (var cb in _update_data.Callbacks)
                        {
                            cb(additionalInfo);
                        }
                    }
                    throw;
                }
                finally
                {
                    if (lockAcquired)
                    {
                        Monitor.Exit(_write_lock);
                    }
                }
            }
            else
            {
                var type_name = GetTypeName<T>(source.Partition);
                info.TableInfo = table_info;
                if (!source.LDBTransaction.data_to_update.ContainsKey(type_name))
                {
                    source.LDBTransaction.data_to_update[type_name] = new List<KeyValuePair<UpdateInfo, Dictionary<int, object>>>();
                }
                source.LDBTransaction.data_to_update[type_name].Add(new KeyValuePair<UpdateInfo, Dictionary<int, object>>(info, values));
            }
        }

        public void UpdateBatch(UpdateInfo info, Dictionary<int, object> values, TableInfo table_info, WriteBatchWithConstraints batch, Dictionary<string, KeyValuePair<byte[], HashSet<int>>> string_cache, Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData, int, int>> memory_index_meta, bool is_trans)
        {
            var vector_cache = new Dictionary<string, VamanaIndex>();
            foreach (var item in values)
            {
                var key = MakeIndexKey(new IndexKeyInfo()
                {
                    TableNumber = info.TableNumber,
                    ColumnNumber = table_info.ColumnNumbers["Id"],
                    Val = BitConverter.GetBytes(item.Key).MyReverseNoCopy(LinqDbTypes.int_),
                    Id = item.Key
                });
                var index_val = leveld_db.Get(key);
                if (index_val == null)
                {
                    continue;
                }

                object value = item.Value;

                if (info.ColumnType == LinqDbTypes.string_)
                {
                    IndexDeletedData index_deleted = null;
                    IndexNewData index_new = null;
                    IndexChangedData index_changed = null;
                    int bitfunnel_size = 0;
                    if (memory_index_meta.ContainsKey(info.ColumnName))
                    {
                        index_deleted = memory_index_meta[info.ColumnName].Item2;
                        index_new = memory_index_meta[info.ColumnName].Item1;
                        index_changed = memory_index_meta[info.ColumnName].Item3;
                        bitfunnel_size = memory_index_meta[info.ColumnName].Item4;
                    }
                    SaveStringData(batch, (string)item.Value, info.ColumnName, info.TableInfo, item.Key, string_cache, false, index_new, index_changed, bitfunnel_size);
                }
                else if (info.ColumnType == LinqDbTypes.binary_ || info.ColumnType == LinqDbTypes.vector_)
                {
                    if (info.ColumnType == LinqDbTypes.vector_ && !vector_cache.ContainsKey(info.ColumnName))
                    {
                        if (is_trans)
                        {
                            throw new LinqDbException("Linqdb: float[] properties are not supported in a transaction");
                        }
                        if (info.ColumnName.ToLower().EndsWith("l2"))
                        {
                            vector_cache[info.ColumnName] = new VamanaIndex(VamanaIndex.Mode.Write, new VamanaIndex.Config(), table_info.TableNumber, table_info.ColumnNumbers[info.ColumnName], null,
                                                                            GetVectorNeighboursFromDisk, LoadVector, GetCurrentEntryPoints, GetCorpusCount);
                        }
                    }
                    SaveBinaryColumn(batch, value, info.ColumnName, info.TableInfo, item.Key, false, vector_cache.ContainsKey(info.ColumnName) ? vector_cache[info.ColumnName] : null, info.ColumnType);
                    //SaveBinaryColumn(batch, value, info.ColumnName, info.TableInfo, item.Key, false);
                }
                else
                {
                    IndexDeletedData index_deleted = null;
                    IndexNewData index_new = null;
                    IndexChangedData index_changed = null;
                    if (memory_index_meta.ContainsKey(info.ColumnName))
                    {
                        index_deleted = memory_index_meta[info.ColumnName].Item2;
                        index_new = memory_index_meta[info.ColumnName].Item1;
                        index_changed = memory_index_meta[info.ColumnName].Item3;
                    }
                    SaveDataColumn(batch, value, info.ColumnName, info.ColumnType, info.TableInfo, item.Key, false, index_new, index_changed);
                }
            }

            //vector index
            foreach (var vc in vector_cache)
            {
                var data = vc.Value.FinalizeSession();
                if (data.NeighborsByNode != null && data.NeighborsByNode.Any())
                {
                    foreach (var n in data.NeighborsByNode)
                    {
                        var key = MakeVamanaVectorsNeighboursKey(table_info.TableNumber, table_info.ColumnNumbers[vc.Key], n.Key);
                        if (!n.Value.Any())
                        {
                            batch.Delete(key);
                        }
                        else
                        {
                            var neighbours = SharedUtils.IntListSerializer.ToByteArray(n.Value);
                            batch.Put(key, neighbours);
                        }
                    }
                }
                if (data.EntryPoints != null && data.EntryPoints.Any())
                {
                    var key = MakeVamanaEntryPointsKey(table_info.TableNumber, table_info.ColumnNumbers[vc.Key]);
                    var entries = SharedUtils.IntListSerializer.ToByteArray(data.EntryPoints);
                    batch.Put(key, entries);
                }
                //else
                //{
                //    var key = MakeVamanaEntryPointsKey(table_info.TableNumber, table_info.ColumnNumbers[vc.Key]);
                //    batch.Delete(key);
                //}
                if (data.RealVectorDelta != 0)
                {
                    IncrementVamanaItemsCount(table_info.TableNumber, table_info.ColumnNumbers[vc.Key], data.RealVectorDelta, batch);
                }
            }
            //WriteStringCacheToBatch(batch, string_cache, table_info, null);
        }
    }

    public class UpdateInfo
    {
        public int TableNumber { get; set; }
        public short ColumnNumber { get; set; }
        public LinqDbTypes ColumnType { get; set; }
        public TableInfo TableInfo { get; set; }
        public string ColumnName { get; set; }
    }
}
