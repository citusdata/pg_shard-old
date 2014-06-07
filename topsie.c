#include "postgres.h"

#include "postgres_fdw.h"

#include "fmgr.h"
#include "access/htup_details.h"
#include "executor/executor.h"
#include "utils/builtins.h"
#include "utils/typcache.h"

static uint32 HashKeyForTuple(Datum hashKey, int16 attNum, TupleDesc tupDesc);

/*
 * topsie_hash accepts a record and attribute number and returns
 * the integer produced by hashing that attribute's value using the hash
 * function associated with the attribute's type. If no hash function can be
 * found for the attribute's type, an error occurs.
 */
PG_FUNCTION_INFO_V1(topsie_hash);

Datum
topsie_hash(PG_FUNCTION_ARGS) {
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

FunctionCallInfo
TopsieHashFnCallInfo(Oid typeId)
{
	TypeCacheEntry *typeEntry = NULL;
	FunctionCallInfo fcinfo = (FunctionCallInfo) palloc0(
			sizeof(FunctionCallInfoData));

	typeEntry = lookup_type_cache(typeId, TYPECACHE_HASH_PROC_FINFO);
	if (!OidIsValid(typeEntry->hash_proc_finfo.fn_oid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("could not identify a hash function for type %s",
				 format_type_be(typeId))));
	}

	InitFunctionCallInfoData(*fcinfo, &typeEntry->hash_proc_finfo, 1,
			InvalidOid, NULL, NULL);
	fcinfo->arg[0] = 0;
	fcinfo->argnull[0] = false;
	fcinfo->isnull = false;

	return fcinfo;
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
  FunctionCallInfo fcinfo = NULL;

  attType = tupDesc->attrs[attNum - 1]->atttypid;
  fcinfo = TopsieHashFnCallInfo(attType);

  fcinfo->arg[0] = hashKey;

  return DatumGetUInt32(FunctionCallInvoke(fcinfo));
}
