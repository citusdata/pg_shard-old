#include "postgres.h"
#include "fmgr.h"
#include "access/htup_details.h"
#include "executor/executor.h"
#include "utils/builtins.h"
#include "utils/typcache.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif


Datum pg_shard_hash(PG_FUNCTION_ARGS);
static uint32 HashKeyForTuple(Datum hashKey, int16 attNum, TupleDesc tupDesc);


/*
 * pg_shard_hash accepts a record and attribute number and returns
 * the integer produced by hashing that attribute's value using the hash
 * function associated with the attribute's type. If no hash function can be
 * found for the attribute's type, an error occurs.
 */
PG_FUNCTION_INFO_V1(pg_shard_hash);

Datum
pg_shard_hash(PG_FUNCTION_ARGS) {
  HeapTupleHeader tup = PG_GETARG_HEAPTUPLEHEADER(0);
  int16 attNum = PG_GETARG_INT16(1);

  HeapTupleData tupleData = { 0 };
  Datum hashKey = 0;
  bool isNull = false;

  Oid typeId = HeapTupleHeaderGetTypeId(tup);
  int32 typMod = HeapTupleHeaderGetTypMod(tup);
  TupleDesc tupDesc = lookup_rowtype_tupdesc(typeId, typMod);

  /* Set struct fields: heap_getattr needs more than a bare HeapTupleHeader */
  tupleData.t_len = HeapTupleHeaderGetDatumLength(tup);
  ItemPointerSetInvalid(&(tupleData.t_self));
  tupleData.t_tableOid = InvalidOid;
  tupleData.t_data = tup;

  hashKey = heap_getattr(&tupleData, attNum, tupDesc, &isNull);

  /* Null values hash to zero */
  uint32 hashValue = isNull ? 0 : HashKeyForTuple(hashKey, attNum, tupDesc);

  ReleaseTupleDesc(tupDesc);

  PG_RETURN_UINT32(hashValue);
}

/*
 * HashKeyForTuple determines the type of the specified attribute within the
 * given tuple and uses that type information to look up an associated hash
 * function. The provided Datum is passed to that hash function to arrive at
 * a final hash value.
 */
static uint32
HashKeyForTuple(Datum hashKey, int16 attNum, TupleDesc tupDesc) {
  Oid attType = InvalidOid;
  TypeCacheEntry *typeEntry = NULL;
  FunctionCallInfoData locfcinfo = { 0 };

  attType = tupDesc->attrs[attNum - 1]->atttypid;

  typeEntry = lookup_type_cache(attType, TYPECACHE_HASH_PROC_FINFO);
  if (!OidIsValid(typeEntry->hash_proc_finfo.fn_oid))
  {
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                    errmsg("could not identify a hash function for type %s",
                           format_type_be(attType))));
  }

  InitFunctionCallInfoData(locfcinfo, &typeEntry->hash_proc_finfo, 1,
                           InvalidOid, NULL, NULL);
  locfcinfo.arg[0] = hashKey;
  locfcinfo.argnull[0] = false;
  locfcinfo.isnull = false;

  return DatumGetUInt32(FunctionCallInvoke(&locfcinfo));
}
