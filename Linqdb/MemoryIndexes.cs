using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        //key: type name|prop name|snapshot id
        ConcurrentDictionary<string, IndexGeneric> indexes = new ConcurrentDictionary<string, IndexGeneric>();
        //key: type name, value: prop names
        ConcurrentDictionary<string, HashSet<string>> existing_indexes = new ConcurrentDictionary<string, HashSet<string>>();
        //key: type name|prop name
        ConcurrentDictionary<string, string> latest_snapshots = new ConcurrentDictionary<string, string>();
        //key: type name|prop name
        ConcurrentDictionary<string, List<Tuple<bool, string>>> snapshots_alive = new ConcurrentDictionary<string, List<Tuple<bool, string>>>();
        ConcurrentDictionary<string, DateTime> last_cleanup = new ConcurrentDictionary<string, DateTime>();
        //key: type name
        ConcurrentDictionary<string, int> compaction_count = new ConcurrentDictionary<string, int>();
        //key: type name|prop name
        ConcurrentDictionary<string, int> bitfunnel_sizes = new ConcurrentDictionary<string, int>();
        //key: type name|prop name
        ConcurrentDictionary<string, int> bitfunnel_batches = new ConcurrentDictionary<string, int>();

        public Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData, int, int>> BuildMetaOnIndex(TableInfo table_info)
        {
            var res = new Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData, int, int>>();
            if (existing_indexes.ContainsKey(table_info.Name))
            {
                foreach (var prop in existing_indexes[table_info.Name])
                {
                    if (!table_info.Columns.ContainsKey(prop))
                    {
                        throw new LinqDbException("Linqdb: Can't insert row with missing column on which in-memory index is built: " + prop);
                    }
                    else
                    {
                        int bf_size = 0;
                        int bf_batch = 0;
                        var snapshot = bitfunnel_sizes.ToArray();
                        if (snapshot.Any(f => f.Key.StartsWith($"{table_info.Name}|")))
                        {
                            var val = snapshot.First(f => f.Key.StartsWith($"{table_info.Name}|"));
                            bf_size = val.Value;

                            var snapshot_batches = bitfunnel_batches.ToArray();
                            val = snapshot_batches.First(f => f.Key.StartsWith($"{table_info.Name}|"));
                            bf_batch = val.Value;
                        }
                        res[prop] = new Tuple<IndexNewData, IndexDeletedData, IndexChangedData, int, int>(
                            new IndexNewData(prop, table_info.Columns[prop]),
                            new IndexDeletedData(prop, table_info.Columns[prop]),
                            new IndexChangedData(prop, table_info.Columns[prop]),
                            bf_size,
                            bf_batch);
                    }
                }
                return res;
            }
            else
            {
                return res;
            }
        }

        public Dictionary<string, string> InsertIndexChanges(TableInfo table_info, Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData, int, int>> changes)
        {
            var res = new Dictionary<string, string>();
            foreach (var property in changes.OrderBy(f => f.Key == "Id" ? 1 : 0))
            {
                if (existing_indexes[table_info.Name].Contains(property.Key))
                {
                    var snapshot_id = Ldb.GetNewSpnapshotId();
                    res[property.Key] = snapshot_id;
                    MakeNewPropSnapshot(table_info.Name, property.Key, snapshot_id, property.Value.Item1, property.Value.Item2, property.Value.Item3, property.Value.Item4, property.Value.Item5);
                }
            }
            if (changes.Any())
            {
                CompactData(table_info.Name);
            }
            return res;
        }

        public void MakeNewPropSnapshot(string type, string prop, string snapshot_id, IndexNewData index_new, IndexDeletedData index_deleted, IndexChangedData index_changed, int bitfunnel_size, int batch_size)
        {
            var latest_key = type + "|" + prop;
            var latest_snapshot_id = latest_snapshots[latest_key];
            var key = type + "|" + prop + "|" + latest_snapshot_id;
            var index = indexes[key];
            var ids_index = indexes[type + "|Id|" + latest_snapshots[type + "|Id"]];
            var parts = new List<IndexPart>();
            var new_index = new IndexGeneric()
            {
                ColumnName = index.ColumnName,
                ColumnType = index.ColumnType,
                GroupListMapping = index.GroupListMapping,
                IndexType = index.IndexType,
                Parts = parts,
                TypeName = index.TypeName
            };
            bool has_changed_int = index_changed != null && index_changed.IntValues != null && index_changed.IntValues.Any();
            bool has_changed_double = index_changed != null && index_changed.DoubleValues != null && index_changed.DoubleValues.Any();
            bool has_changed_decimal = index_changed != null && index_changed.DecimalValues != null && index_changed.DecimalValues.Any();
            bool has_changed_long = index_changed != null && index_changed.LongValues != null && index_changed.LongValues.Any();
            bool has_changed_bits = index_changed != null && index_changed.BitValues != null && index_changed.BitValues.Any();
            bool has_deleted = index_deleted != null && index_deleted.Ids.Any();

            bool has_new_int = index_new != null && index_new.IntValues != null && index_new.IntValues.Any();
            bool has_new_double = index_new != null && index_new.DoubleValues != null && index_new.DoubleValues.Any();
            bool has_new_decimal = index_new != null && index_new.DecimalValues != null && index_new.DecimalValues.Any();
            bool has_new_long = index_new != null && index_new.LongValues != null && index_new.LongValues.Any();
            bool has_new_bits = index_new != null && index_new.Bits != null && index_new.Bits.Any();

            switch (index.ColumnType)
            {
                case LinqDbTypes.int_:
                    int batch = 1024;
                    if (batch_size > 0)
                    {
                        batch = batch_size;
                    }
                    if (index.IndexType == IndexType.PropertyOnly) //property index only
                    {
                        for (int i = 0; i < index.Parts.Count; i++)
                        {
                            var p = index.Parts[i];
                            p.Ids = ids_index.Parts[i].IntValues;
                            IndexPart np = null;
                            HashSet<int> removed = null;
                            int icount = p.IntValues.Count;
                            for (int j = 0; j < icount; j++)
                            {
                                var cid = p.Ids[j];
                                if (has_changed_int /*&& changed_int_bloom[cid % dbloomsize]*/ && index_changed.IntValues.ContainsKey(cid))
                                {
                                    if (np == null)
                                    {
                                        np = new IndexPart()
                                        {
                                            Ids = new List<int>(p.Ids),
                                            IntValues = new List<int>(p.IntValues)
                                        };
                                    }
                                    np.IntValues[j] = index_changed.IntValues[cid];
                                }
                                if (has_deleted /*&& deleted_bloom[cid % dbloomsize]*/ && index_deleted.Ids.Contains(cid))
                                {
                                    if (removed == null)
                                    {
                                        removed = new HashSet<int>();
                                    }
                                    removed.Add((int)cid);
                                }
                            }
                            if (removed != null)
                            {
                                if (np == null)
                                {
                                    np = new IndexPart()
                                    {
                                        Ids = new List<int>(p.Ids),
                                        IntValues = new List<int>(p.IntValues)
                                    };
                                }
                                var npp = new IndexPart()
                                {
                                    Ids = new List<int>(),
                                    IntValues = new List<int>()
                                };
                                for (int z = 0; z < np.Ids.Count; z++)
                                {
                                    if (!removed.Contains((int)np.Ids[z]))
                                    {
                                        npp.Ids.Add(np.Ids[z]);
                                        npp.IntValues.Add(np.IntValues[z]);
                                    }
                                }
                                np = npp;
                            }
                            if (np != null)
                            {
                                p = np; //wow, finally!
                            }
                            parts.Add(p);
                        }
                        if (has_new_int)
                        {
                            IndexPart lastp = null;
                            if (parts.Any())
                            {
                                lastp = parts.LastOrDefault();
                            }
                            if (lastp == null)
                            {
                                lastp = new IndexPart()
                                {
                                    Ids = new List<int>(),
                                    IntValues = new List<int>()
                                };
                                parts.Add(lastp);
                            }
                            else
                            {
                                lastp = new IndexPart()
                                {
                                    Ids = new List<int>(lastp.Ids),
                                    IntValues = new List<int>(lastp.IntValues)
                                };
                                parts[parts.Count() - 1] = lastp;
                            }
                            for (int i = 0; i < index_new.IntValues.Count; i++)
                            {
                                if (lastp.Ids.Count == batch)
                                {
                                    lastp = new IndexPart()
                                    {
                                        Ids = new List<int>(),
                                        IntValues = new List<int>()
                                    };
                                    parts.Add(lastp);
                                }
                                lastp.IntValues.Add(index_new.IntValues[i]);
                                lastp.Ids.Add(index_new.Ids[i]);
                            }
                        }
                        for (int i = 0; i < index.Parts.Count(); i++)
                        {
                            index.Parts[i].Ids = null;
                        }
                    }
                    else if (index.IndexType == IndexType.GroupOnly) //group index only
                    {
                        int map = 0;
                        if (index.GroupListMapping.Any())
                        {
                            map = index.GroupListMapping.Max(f => f.Value);
                            map++;
                        }
                        for (int i = 0; i < index.Parts.Count; i++)
                        {
                            var p = index.Parts[i];
                            p.Ids = ids_index.Parts[i].IntValues;
                            IndexPart np = null;
                            HashSet<int> removed = null;
                            int icount = p.GroupValues.Count;
                            for (int j = 0; j < icount; j++)
                            {
                                int cid = p.Ids[j];
                                if (has_changed_int /*&& changed_int_bloom[cid % dbloomsize]*/ && index_changed.IntValues.ContainsKey(cid))
                                {
                                    if (np == null)
                                    {
                                        np = new IndexPart()
                                        {
                                            Ids = new List<int>(p.Ids),
                                            GroupValues = new List<int>(p.GroupValues)
                                        };
                                    }
                                    if (!index.GroupListMapping.ContainsKey(index_changed.IntValues[cid]))
                                    {
                                        index.GroupListMapping[index_changed.IntValues[cid]] = map;
                                        map++;
                                    }
                                    np.GroupValues[j] = index.GroupListMapping[index_changed.IntValues[cid]];
                                }
                                if (has_deleted /*&& deleted_bloom[cid % dbloomsize]*/ && index_deleted.Ids.Contains(cid))
                                {
                                    if (removed == null)
                                    {
                                        removed = new HashSet<int>();
                                    }
                                    removed.Add(cid);
                                }
                            }
                            if (removed != null)
                            {
                                if (np == null)
                                {
                                    np = new IndexPart()
                                    {
                                        Ids = new List<int>(p.Ids),
                                        GroupValues = new List<int>(p.GroupValues)
                                    };
                                }
                                var npp = new IndexPart()
                                {
                                    Ids = new List<int>(),
                                    GroupValues = new List<int>()
                                };
                                for (int z = 0; z < np.Ids.Count; z++)
                                {
                                    if (!removed.Contains((int)np.Ids[z]))
                                    {
                                        npp.Ids.Add(np.Ids[z]);
                                        npp.GroupValues.Add(np.GroupValues[z]);
                                    }
                                }
                                np = npp;
                            }
                            if (np != null)
                            {
                                p = np; //wow, finally!
                            }
                            parts.Add(p);
                        }
                        if (has_new_int)
                        {
                            IndexPart lastp = null;
                            if (parts.Any())
                            {
                                lastp = parts.LastOrDefault();
                            }
                            if (lastp == null)
                            {
                                lastp = new IndexPart()
                                {
                                    Ids = new List<int>(),
                                    GroupValues = new List<int>()
                                };
                                parts.Add(lastp);
                            }
                            else
                            {
                                lastp = new IndexPart()
                                {
                                    Ids = new List<int>(lastp.Ids),
                                    GroupValues = new List<int>(lastp.GroupValues)
                                };
                                parts[parts.Count() - 1] = lastp;
                            }
                            for (int i = 0; i < index_new.IntValues.Count; i++)
                            {
                                if (lastp.Ids.Count == batch)
                                {
                                    lastp = new IndexPart()
                                    {
                                        Ids = new List<int>(),
                                        GroupValues = new List<int>()
                                    };
                                    parts.Add(lastp);
                                }
                                if (!index.GroupListMapping.ContainsKey((int)index_new.IntValues[i]))
                                {
                                    index.GroupListMapping[(int)index_new.IntValues[i]] = map;
                                    map++;
                                }
                                lastp.GroupValues.Add(index.GroupListMapping[(int)index_new.IntValues[i]]);
                                lastp.Ids.Add(index_new.Ids[i]);
                            }
                        }
                        for (int i = 0; i < index.Parts.Count(); i++)
                        {
                            index.Parts[i].Ids = null;
                        }
                    }
                    else //both
                    {
                        int map = 0;
                        if (index.GroupListMapping.Any())
                        {
                            map = index.GroupListMapping.Max(f => f.Value);
                            map++;
                        }
                        for (int i = 0; i < index.Parts.Count; i++)
                        {
                            var p = index.Parts[i];
                            p.Ids = ids_index.Parts[i].IntValues;
                            IndexPart np = null;
                            HashSet<int> removed = null;
                            int icount = p.GroupValues.Count;
                            for (int j = 0; j < icount; j++)
                            {
                                int cid = p.Ids[j];
                                if (has_changed_int /*&& changed_int_bloom[cid % dbloomsize]*/ && index_changed.IntValues.ContainsKey(cid))
                                {
                                    if (np == null)
                                    {
                                        np = new IndexPart()
                                        {
                                            Ids = new List<int>(p.Ids),
                                            GroupValues = new List<int>(p.GroupValues),
                                            IntValues = new List<int>(p.IntValues)
                                        };
                                    }
                                    if (!index.GroupListMapping.ContainsKey(index_changed.IntValues[cid]))
                                    {
                                        index.GroupListMapping[index_changed.IntValues[cid]] = map;
                                        map++;
                                    }
                                    np.GroupValues[j] = index.GroupListMapping[index_changed.IntValues[cid]];
                                    np.IntValues[j] = index_changed.IntValues[cid];
                                }
                                if (has_deleted /*&& deleted_bloom[cid % dbloomsize]*/ && index_deleted.Ids.Contains(cid))
                                {
                                    if (removed == null)
                                    {
                                        removed = new HashSet<int>();
                                    }
                                    removed.Add(cid);
                                }
                            }
                            if (removed != null)
                            {
                                if (np == null)
                                {
                                    np = new IndexPart()
                                    {
                                        Ids = new List<int>(p.Ids),
                                        GroupValues = new List<int>(p.GroupValues),
                                        IntValues = new List<int>(p.IntValues)
                                    };
                                }
                                var npp = new IndexPart()
                                {
                                    Ids = new List<int>(),
                                    GroupValues = new List<int>(),
                                    IntValues = new List<int>()
                                };
                                for (int z = 0; z < np.Ids.Count; z++)
                                {
                                    if (!removed.Contains((int)np.Ids[z]))
                                    {
                                        npp.Ids.Add(np.Ids[z]);
                                        npp.GroupValues.Add(np.GroupValues[z]);
                                        npp.IntValues.Add(np.IntValues[z]);
                                    }
                                }
                                np = npp;
                            }
                            if (np != null)
                            {
                                p = np; //wow, finally!
                            }
                            parts.Add(p);
                        }
                        if (has_new_int)
                        {
                            IndexPart lastp = null;
                            if (parts.Any())
                            {
                                lastp = parts.LastOrDefault();
                            }
                            if (lastp == null)
                            {
                                lastp = new IndexPart()
                                {
                                    Ids = new List<int>(),
                                    GroupValues = new List<int>(),
                                    IntValues = new List<int>()
                                };
                                parts.Add(lastp);
                            }
                            else
                            {
                                lastp = new IndexPart()
                                {
                                    Ids = new List<int>(lastp.Ids),
                                    GroupValues = new List<int>(lastp.GroupValues),
                                    IntValues = new List<int>(lastp.IntValues)
                                };
                                parts[parts.Count() - 1] = lastp;
                            }
                            for (int i = 0; i < index_new.IntValues.Count; i++)
                            {
                                if (lastp.Ids.Count == batch)
                                {
                                    lastp = new IndexPart()
                                    {
                                        Ids = new List<int>(),
                                        GroupValues = new List<int>(),
                                        IntValues = new List<int>()
                                    };
                                    parts.Add(lastp);
                                }
                                if (!index.GroupListMapping.ContainsKey((int)index_new.IntValues[i]))
                                {
                                    index.GroupListMapping[(int)index_new.IntValues[i]] = map;
                                    map++;
                                }
                                lastp.GroupValues.Add(index.GroupListMapping[(int)index_new.IntValues[i]]);
                                lastp.IntValues.Add(index_new.IntValues[i]);
                                lastp.Ids.Add(index_new.Ids[i]);
                            }
                        }
                        for (int i = 0; i < index.Parts.Count(); i++)
                        {
                            index.Parts[i].Ids = null;
                        }
                    }
                    break;
                case LinqDbTypes.double_:
                case LinqDbTypes.DateTime_:
                    for (int i = 0; i < index.Parts.Count; i++)
                    {
                        var p = index.Parts[i];
                        p.Ids = ids_index.Parts[i].IntValues;
                        IndexPart np = null;
                        HashSet<int> removed = null;
                        int icount = p.DoubleValues.Count;
                        for (int j = 0; j < icount; j++)
                        {
                            int cid = p.Ids[j];
                            if (has_changed_double /*&& changed_double_bloom[cid % dbloomsize]*/ && index_changed.DoubleValues.ContainsKey(cid))
                            {
                                if (np == null)
                                {
                                    np = new IndexPart()
                                    {
                                        Ids = new List<int>(p.Ids),
                                        DoubleValues = new List<double>(p.DoubleValues)
                                    };
                                }
                                np.DoubleValues[j] = index_changed.DoubleValues[cid];
                            }
                            if (has_deleted /*&& deleted_bloom[cid % dbloomsize]*/ && index_deleted.Ids.Contains(cid))
                            {
                                if (removed == null)
                                {
                                    removed = new HashSet<int>();
                                }
                                removed.Add(cid);
                            }
                        }
                        if (removed != null)
                        {
                            if (np == null)
                            {
                                np = new IndexPart()
                                {
                                    Ids = new List<int>(p.Ids),
                                    DoubleValues = new List<double>(p.DoubleValues)
                                };
                            }
                            var npp = new IndexPart()
                            {
                                Ids = new List<int>(),
                                DoubleValues = new List<double>()
                            };
                            for (int z = 0; z < np.Ids.Count; z++)
                            {
                                if (!removed.Contains((int)np.Ids[z]))
                                {
                                    npp.Ids.Add(np.Ids[z]);
                                    npp.DoubleValues.Add(np.DoubleValues[z]);
                                }
                            }
                            np = npp;
                        }
                        if (np != null)
                        {
                            p = np; //wow, finally!
                        }
                        parts.Add(p);
                    }
                    if (has_new_double)
                    {
                        IndexPart lastp = null;
                        if (parts.Any())
                        {
                            lastp = parts.LastOrDefault();
                        }
                        if (lastp == null)
                        {
                            lastp = new IndexPart()
                            {
                                Ids = new List<int>(),
                                DoubleValues = new List<double>()
                            };
                            parts.Add(lastp);
                        }
                        else
                        {
                            lastp = new IndexPart()
                            {
                                Ids = new List<int>(lastp.Ids),
                                DoubleValues = new List<double>(lastp.DoubleValues)
                            };
                            parts[parts.Count() - 1] = lastp;
                        }
                        for (int i = 0; i < index_new.DoubleValues.Count; i++)
                        {
                            if (lastp.Ids.Count == 1024)
                            {
                                lastp = new IndexPart()
                                {
                                    Ids = new List<int>(),
                                    DoubleValues = new List<double>()
                                };
                                parts.Add(lastp);
                            }
                            lastp.DoubleValues.Add(index_new.DoubleValues[i]);
                            lastp.Ids.Add(index_new.Ids[i]);
                        }
                    }
                    for (int i = 0; i < index.Parts.Count(); i++)
                    {
                        index.Parts[i].Ids = null;
                    }
                    break;
                case LinqDbTypes.decimal_:
                    for (int i = 0; i < index.Parts.Count; i++)
                    {
                        var p = index.Parts[i];
                        p.Ids = ids_index.Parts[i].IntValues;
                        IndexPart np = null;
                        HashSet<int> removed = null;
                        int icount = p.DecimalValues.Count;
                        for (int j = 0; j < icount; j++)
                        {
                            int cid = p.Ids[j];
                            if (has_changed_decimal /*&& changed_double_bloom[cid % dbloomsize]*/ && index_changed.DecimalValues.ContainsKey(cid))
                            {
                                if (np == null)
                                {
                                    np = new IndexPart()
                                    {
                                        Ids = new List<int>(p.Ids),
                                        DecimalValues = new List<decimal>(p.DecimalValues)
                                    };
                                }
                                np.DecimalValues[j] = index_changed.DecimalValues[cid];
                            }
                            if (has_deleted /*&& deleted_bloom[cid % dbloomsize]*/ && index_deleted.Ids.Contains(cid))
                            {
                                if (removed == null)
                                {
                                    removed = new HashSet<int>();
                                }
                                removed.Add(cid);
                            }
                        }
                        if (removed != null)
                        {
                            if (np == null)
                            {
                                np = new IndexPart()
                                {
                                    Ids = new List<int>(p.Ids),
                                    DecimalValues = new List<decimal>(p.DecimalValues)
                                };
                            }
                            var npp = new IndexPart()
                            {
                                Ids = new List<int>(),
                                DecimalValues = new List<decimal>()
                            };
                            for (int z = 0; z < np.Ids.Count; z++)
                            {
                                if (!removed.Contains((int)np.Ids[z]))
                                {
                                    npp.Ids.Add(np.Ids[z]);
                                    npp.DecimalValues.Add(np.DecimalValues[z]);
                                }
                            }
                            np = npp;
                        }
                        if (np != null)
                        {
                            p = np; //wow, finally!
                        }
                        parts.Add(p);
                    }
                    if (has_new_decimal)
                    {
                        IndexPart lastp = null;
                        if (parts.Any())
                        {
                            lastp = parts.LastOrDefault();
                        }
                        if (lastp == null)
                        {
                            lastp = new IndexPart()
                            {
                                Ids = new List<int>(),
                                DecimalValues = new List<decimal>()
                            };
                            parts.Add(lastp);
                        }
                        else
                        {
                            lastp = new IndexPart()
                            {
                                Ids = new List<int>(lastp.Ids),
                                DecimalValues = new List<decimal>(lastp.DecimalValues)
                            };
                            parts[parts.Count() - 1] = lastp;
                        }
                        for (int i = 0; i < index_new.DecimalValues.Count; i++)
                        {
                            if (lastp.Ids.Count == 1024)
                            {
                                lastp = new IndexPart()
                                {
                                    Ids = new List<int>(),
                                    DecimalValues = new List<decimal>()
                                };
                                parts.Add(lastp);
                            }
                            lastp.DecimalValues.Add(index_new.DecimalValues[i]);
                            lastp.Ids.Add(index_new.Ids[i]);
                        }
                    }
                    for (int i = 0; i < index.Parts.Count(); i++)
                    {
                        index.Parts[i].Ids = null;
                    }
                    break;
                case LinqDbTypes.long_:
                    for (int i = 0; i < index.Parts.Count; i++)
                    {
                        var p = index.Parts[i];
                        p.Ids = ids_index.Parts[i].IntValues;
                        IndexPart np = null;
                        HashSet<int> removed = null;
                        int lcount = p.LongValues.Count;
                        for (int j = 0; j < lcount; j++)
                        {
                            int cid = p.Ids[j];
                            if (has_changed_long /*&& changed_double_bloom[cid % dbloomsize]*/ && index_changed.LongValues.ContainsKey(cid))
                            {
                                if (np == null)
                                {
                                    np = new IndexPart()
                                    {
                                        Ids = new List<int>(p.Ids),
                                        LongValues = new List<long>(p.LongValues)
                                    };
                                }
                                np.LongValues[j] = index_changed.LongValues[cid];
                            }
                            if (has_deleted /*&& deleted_bloom[cid % dbloomsize]*/ && index_deleted.Ids.Contains(cid))
                            {
                                if (removed == null)
                                {
                                    removed = new HashSet<int>();
                                }
                                removed.Add(cid);
                            }
                        }
                        if (removed != null)
                        {
                            if (np == null)
                            {
                                np = new IndexPart()
                                {
                                    Ids = new List<int>(p.Ids),
                                    LongValues = new List<long>(p.LongValues)
                                };
                            }
                            var npp = new IndexPart()
                            {
                                Ids = new List<int>(),
                                LongValues = new List<long>()
                            };
                            for (int z = 0; z < np.Ids.Count; z++)
                            {
                                if (!removed.Contains((int)np.Ids[z]))
                                {
                                    npp.Ids.Add(np.Ids[z]);
                                    npp.LongValues.Add(np.LongValues[z]);
                                }
                            }
                            np = npp;
                        }
                        if (np != null)
                        {
                            p = np; //wow, finally!
                        }
                        parts.Add(p);
                    }
                    if (has_new_long)
                    {
                        IndexPart lastp = null;
                        if (parts.Any())
                        {
                            lastp = parts.LastOrDefault();
                        }
                        if (lastp == null)
                        {
                            lastp = new IndexPart()
                            {
                                Ids = new List<int>(),
                                LongValues = new List<long>()
                            };
                            parts.Add(lastp);
                        }
                        else
                        {
                            lastp = new IndexPart()
                            {
                                Ids = new List<int>(lastp.Ids),
                                LongValues = new List<long>(lastp.LongValues)
                            };
                            parts[parts.Count() - 1] = lastp;
                        }
                        for (int i = 0; i < index_new.LongValues.Count; i++)
                        {
                            if (lastp.Ids.Count == 1024)
                            {
                                lastp = new IndexPart()
                                {
                                    Ids = new List<int>(),
                                    LongValues = new List<long>()
                                };
                                parts.Add(lastp);
                            }
                            lastp.LongValues.Add(index_new.LongValues[i]);
                            lastp.Ids.Add(index_new.Ids[i]);
                        }
                    }
                    for (int i = 0; i < index.Parts.Count(); i++)
                    {
                        index.Parts[i].Ids = null;
                    }
                    break;
                case LinqDbTypes.string_:
                    for (int i = 0; i < index.Parts.Count; i++)
                    {
                        var p = index.Parts[i];
                        p.Ids = ids_index.Parts[i].IntValues;
                        IndexPart np = null;
                        HashSet<int> removed = null;
                        int icount = p.BitValues.Any() ? p.BitValues.First().Count : 0;
                        for (int j = 0; j < icount; j++)
                        {
                            int cid = p.Ids[j];
                            if (has_changed_bits /*&& changed_double_bloom[cid % dbloomsize]*/ && index_changed.BitValues.ContainsKey(cid))
                            {
                                if (np == null)
                                {
                                    np = new IndexPart()
                                    {
                                        Ids = new List<int>(p.Ids),
                                        BitValues = new List<BitArray>()
                                    };
                                    foreach (var ba in p.BitValues)
                                    { 
                                        np.BitValues.Add(new BitArray(ba));
                                    }
                                }
                                for (int k = 0; k < np.BitValues.Count(); k++)
                                {
                                    np.BitValues[k][j] = index_changed.BitValues[cid][k];
                                }
                            }
                            if (has_deleted /*&& deleted_bloom[cid % dbloomsize]*/ && index_deleted.Ids.Contains(cid))
                            {
                                if (removed == null)
                                {
                                    removed = new HashSet<int>();
                                }
                                removed.Add(cid);
                            }
                        }
                        if (removed != null)
                        {
                            if (np == null)
                            {
                                np = new IndexPart()
                                {
                                    Ids = new List<int>(p.Ids),
                                    BitValues = new List<BitArray>()
                                };
                                foreach (var ba in p.BitValues)
                                {
                                    np.BitValues.Add(new BitArray(ba));
                                }
                            }
                            var npp = new IndexPart()
                            {
                                Ids = new List<int>(),
                                BitValues = new List<BitArray>()
                            };
                            int count = 0;
                            for (int z = 0; z < np.Ids.Count; z++)
                            {
                                if (!removed.Contains((int)np.Ids[z]))
                                {
                                    count++;
                                }
                            }
                            foreach (var ba in np.BitValues)
                            {
                                npp.BitValues.Add(new BitArray(count));
                            }
                            int b = 0;
                            for (int z = 0; z < np.Ids.Count; z++)
                            {
                                if (!removed.Contains((int)np.Ids[z]))
                                {
                                    npp.Ids.Add(np.Ids[z]);
                                    for (int l = 0; l < npp.BitValues.Count(); l++)
                                    {
                                        npp.BitValues[l][b] = np.BitValues[l][z];
                                    }
                                    b++;
                                }
                            }
                            np = npp;
                        }
                        if (np != null)
                        {
                            p = np; //wow, finally!
                        }
                        parts.Add(p);
                    }
                    if (has_new_bits)
                    {
                        IndexPart lastp = null;
                        if (parts.Any())
                        {
                            lastp = parts.LastOrDefault();
                        }
                        if (lastp == null)
                        {
                            lastp = new IndexPart()
                            {
                                Ids = new List<int>(),
                                BitValues = new List<BitArray>()
                            };
                            for (int i = 0; i < bitfunnel_size; i++)
                            {
                                lastp.BitValues.Add(new BitArray(0));
                            }
                            parts.Add(lastp);
                        }
                        else
                        {
                            var bits = lastp.BitValues;
                            lastp = new IndexPart()
                            {
                                Ids = new List<int>(lastp.Ids),
                                BitValues = new List<BitArray>()
                            };
                            foreach (var ba in bits)
                            {
                                lastp.BitValues.Add(new BitArray(ba));
                            }
                            parts[parts.Count() - 1] = lastp;
                        }
                        for (int i = 0; i < index_new.Bits.Count; i++)
                        {
                            if (lastp.Ids.Count == batch_size)
                            {
                                lastp = new IndexPart()
                                {
                                    Ids = new List<int>(),
                                    BitValues = new List<BitArray>()
                                };
                                for (int z = 0; z < bitfunnel_size; z++)
                                {
                                    lastp.BitValues.Add(new BitArray(0));
                                }
                                parts.Add(lastp);
                            }

                            if (lastp.BitValues.First().Length < batch_size)
                            {
                                var old = lastp.BitValues;
                                lastp.BitValues = new List<BitArray>();
                                for (int z = 0; z < bitfunnel_size; z++)
                                {
                                    var ba = new BitArray(batch_size);
                                    for (int k = 0; k < old[z].Length; k++)
                                    {
                                        ba[k] = old[z][k];
                                    }
                                    lastp.BitValues.Add(ba);
                                }
                            }
                            var m = lastp.Ids.Count();
                            for (int z = 0; z < lastp.BitValues.Count(); z++)
                            {
                                lastp.BitValues[z][m] = index_new.Bits[i][z];
                            }
                            lastp.Ids.Add(index_new.Ids[i]);
                        }
                        if (lastp.Ids.Count() < batch_size) //shrink last one
                        {
                            var old = lastp.BitValues;
                            lastp.BitValues = new List<BitArray>();
                            var size = lastp.Ids.Count();
                            for (int z = 0; z < bitfunnel_size; z++)
                            {
                                var ba = new BitArray(size);
                                for (int k = 0; k < size; k++)
                                {
                                    ba[k] = old[z][k];
                                }
                                lastp.BitValues.Add(ba);
                            }
                        }
                    }
                    for (int i = 0; i < index.Parts.Count(); i++)
                    {
                        index.Parts[i].Ids = null;
                    }
                    break;
                default:
                    break;
            }

            //free old snapshots
            if ((DateTime.Now - last_cleanup[type + "|" + prop]).TotalMilliseconds > 30000)
            {
                var alive = snapshots_alive[type + "|" + prop];
                var new_alive = new List<Tuple<bool, string>>(); //bool - schedueled for deletion
                foreach (var sn in alive)
                {
                    if (!sn.Item1)
                    {
                        var nsn = new Tuple<bool, string>(true, sn.Item2);
                        new_alive.Add(nsn);

                    }
                    else
                    {
                        IndexGeneric val = null;
                        indexes.TryRemove(sn.Item2, out val);
                    }
                }
                new_alive.Add(new Tuple<bool, string>(false, type + "|" + prop + "|" + snapshot_id));
                snapshots_alive[type + "|" + prop] = new_alive;
                last_cleanup[type + "|" + prop] = DateTime.Now;
            }
            else
            {
                snapshots_alive[type + "|" + prop].Add(new Tuple<bool, string>(false, type + "|" + prop + "|" + snapshot_id));
            }

            key = type + "|" + prop + "|" + snapshot_id;
            indexes[key] = new_index;
            latest_snapshots[latest_key] = snapshot_id;
        }

        void CompactData(string type)
        {
            if (!compaction_count.ContainsKey(type))
            {
                compaction_count[type] = 0;
            }
            if (compaction_count[type] > 50)
            {
                var props = existing_indexes[type];
                foreach (var prop in props)
                {
                    var snap_id = latest_snapshots[type + "|" + prop];
                    var key = type + "|" + prop + "|" + snap_id;
                    var index = indexes[key];
                    switch (index.ColumnType)
                    {
                        case LinqDbTypes.int_:
                            if (index.IndexType == IndexType.PropertyOnly || index.IndexType == IndexType.Both)
                            {
                                var compacted_parts = new List<IndexPart>();
                                foreach (var p in index.Parts)
                                {
                                    if (p.IntValues.Any())
                                    {
                                        compacted_parts.Add(p);
                                    }
                                }
                                index.Parts = compacted_parts;
                            }
                            else if(index.IndexType == IndexType.GroupOnly)
                            {
                                var compacted_parts = new List<IndexPart>();
                                foreach (var p in index.Parts)
                                {
                                    if (p.GroupValues.Any())
                                    {
                                        compacted_parts.Add(p);
                                    }
                                }
                                index.Parts = compacted_parts;
                            }
                            break;
                        case LinqDbTypes.double_:
                        case LinqDbTypes.DateTime_:
                            var d_compacted_parts = new List<IndexPart>();
                            foreach (var p in index.Parts)
                            {
                                if (p.DoubleValues.Any())
                                {
                                    d_compacted_parts.Add(p);
                                }
                            }
                            index.Parts = d_compacted_parts;
                            break;
                        case LinqDbTypes.decimal_:
                            var dec_compacted_parts = new List<IndexPart>();
                            foreach (var p in index.Parts)
                            {
                                if (p.DecimalValues.Any())
                                {
                                    dec_compacted_parts.Add(p);
                                }
                            }
                            index.Parts = dec_compacted_parts;
                            break;
                        case LinqDbTypes.long_:
                            var long_compacted_parts = new List<IndexPart>();
                            foreach (var p in index.Parts)
                            {
                                if (p.LongValues.Any())
                                {
                                    long_compacted_parts.Add(p);
                                }
                            }
                            index.Parts = long_compacted_parts;
                            break;
                        case LinqDbTypes.string_:
                            var bit_compacted_parts = new List<IndexPart>();
                            foreach (var p in index.Parts)
                            {
                                if (p.BitValues.Any() && p.BitValues.First().Length > 0)
                                {
                                    bit_compacted_parts.Add(p);
                                }
                            }
                            index.Parts = bit_compacted_parts;
                            break;
                        default:
                            break;
                    }
                }
                compaction_count[type] = 0;
            }
            else
            {
                compaction_count[type]++;
            }
        }
    }

    public class IndexGeneric
    {
        public string TypeName { get; set; }
        public string ColumnName { get; set; }
        public LinqDbTypes ColumnType { get; set; }
        public List<IndexPart> Parts { get; set; }
        public ConcurrentDictionary<int, int> GroupListMapping { get; set; }
        public IndexType IndexType { get; set; }
    }

    public enum IndexType : int
    {
        GroupOnly = 0,
        PropertyOnly = 1,
        Both = 2
    }
    public class IndexPart
    {
        public List<int> Ids { get; set; }
        public List<int> IntValues { get; set; }
        public List<long> LongValues { get; set; }
        public List<decimal> DecimalValues { get; set; }
        public List<double> DoubleValues { get; set; }
        public List<int> GroupValues { get; set; }
        public List<BitArray> BitValues { get; set; }
    }

    public class IndexDeletedData
    {
        public IndexDeletedData(string ColumnName, LinqDbTypes Type)
        {
            Ids = new HashSet<int>();
            this.ColumnName = ColumnName;
            this.Type = Type;
        }
        public string ColumnName { get; set; }
        public LinqDbTypes Type { get; set; }
        public HashSet<int> Ids { get; set; }
    }
    public class IndexNewData
    {
        public IndexNewData(string ColumnName, LinqDbTypes Type, int bitfunnel_size = 150)
        {
            Ids = new List<int>();
            IntValues = new List<int>();
            DoubleValues = new List<double>();
            DecimalValues = new List<decimal>();
            LongValues = new List<long>();
            Bits = new List<BitArray>();
            this.ColumnName = ColumnName;
            this.Type = Type;
            this.Bitfunnel_size = bitfunnel_size;
        }
        public string ColumnName { get; set; }
        public LinqDbTypes Type { get; set; }
        public List<int> Ids { get; set; }
        public List<int> IntValues { get; set; }
        public List<double> DoubleValues { get; set; }
        public List<decimal> DecimalValues { get; set; }
        public List<long> LongValues { get; set; }
        public List<BitArray> Bits { get; set; }
        public int Bitfunnel_size { get; set; }
    }
    public class IndexChangedData
    {
        public IndexChangedData(string ColumnName, LinqDbTypes Type)
        {
            IntValues = new Dictionary<int, int>();
            DoubleValues = new Dictionary<int, double>();
            DecimalValues = new Dictionary<int, decimal>();
            LongValues = new Dictionary<int, long>();
            BitValues = new Dictionary<int, BitArray>();
            this.ColumnName = ColumnName;
            this.Type = Type;
        }
        public string ColumnName { get; set; }
        public LinqDbTypes Type { get; set; }
        public Dictionary<int, int> IntValues { get; set; }
        public Dictionary<int, double> DoubleValues { get; set; }
        public Dictionary<int, decimal> DecimalValues { get; set; }
        public Dictionary<int, long> LongValues { get; set; }
        public Dictionary<int, BitArray> BitValues { get; set; }
    }
}
