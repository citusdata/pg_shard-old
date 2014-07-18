/*-------------------------------------------------------------------------
 *
 * ddl_events.c
 *			functions to retrieve and extend ddl events for a table.
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *			ddl_events.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_class.h"
#include "catalog/pg_index.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "ddl_events.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/tqual.h"


/* ddl event construction routines forward declarations */
static char * ExtensionDefinition(Oid tableRelationId);
static Oid get_extension_schema(Oid ext_oid);
static char * ServerDefinition(Oid tableRelationId);
static char * TableSchemaDefinition(Oid tableRelationId);
static char * generate_relation_name(Oid relationId);
static void AppendOptionListToString(StringInfo stringBuffer, List *optionList);
static char * TableColumnOptionsDefinition(Oid tableRelationId);
static char * IndexClusterDefinition(Oid indexRelationId);

/* name extension and deparse routines forward declarations */
static Node * ParseTreeNode(const char *ddlCommand);
static bool check_log_statement(List *stmt_list);
static void DDLEventExtendNames(Node *parseTree, uint64 shardId);
static bool TypeAddIndexConstraint(const AlterTableCmd *command);
static bool TypeDropIndexConstraint(const AlterTableCmd *command, 
									const RangeVar *relation, uint64 shardId);
static void AppendShardIdToConstraintName(AlterTableCmd *command,
										  uint64 shardId);
static void AppendShardIdToName(char **name, uint64 shardId);
static char * DeparseDDLEvent(Node *ddlEventNode, Oid originalRelationId);
static char * DeparseAlterTableStmt(AlterTableStmt *alterTableStmt);
static char * DeparseConstraint(Constraint *constraint);
static char * DeparseCreateExtensionStmt(CreateExtensionStmt *createExtensionStatement);
static char * DeparseForeignServerStmt(CreateForeignServerStmt *foreignServerStmt);
static char * DeparseCreateStmt(CreateStmt *createStmt, Oid originalRelationId);
static Node * TransformRawExpression(Node *rawExpression, Oid originalRelationId,
									 ParseExprKind parseExprKind);
static char * DeparseIndexStmt(IndexStmt *indexStmt, Oid originalRelationId);


/*
 * GetTableDDLEvents takes in a relationId, and returns the list of DDL commands
 * needed to reconstruct the relation. These DDL commands are all palloced; and
 * include the table's schema definition, optional column storage and statistics
 * definitions, and index and constraint defitions.
 */
List *
GetTableDDLEvents(Oid relationId)
{
	List *tableDDLEventList = NIL;
	char tableType = 0;
	char *tableSchemaDef = NULL;
	char *tableColumnOptionsDef = NULL;

	Relation pgIndex = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData	scanKey[1];
	int scanKeyCount = 1;
	HeapTuple heapTuple = NULL;

	/* if foreign table, fetch extension and server definitions */
	tableType = get_rel_relkind(relationId);
	if (tableType == RELKIND_FOREIGN_TABLE)
	{
		char *extensionDef = ExtensionDefinition(relationId);
		char *serverDef = ServerDefinition(relationId);

		if (extensionDef != NULL)
		{
			tableDDLEventList = lappend(tableDDLEventList, extensionDef);
		}
		tableDDLEventList = lappend(tableDDLEventList, serverDef);
	}

	/* fetch table schema and column option definitions */
	tableSchemaDef = TableSchemaDefinition(relationId);
	tableColumnOptionsDef = TableColumnOptionsDefinition(relationId);
	
	tableDDLEventList = lappend(tableDDLEventList, tableSchemaDef);
	if (tableColumnOptionsDef != NULL)
	{
		tableDDLEventList = lappend(tableDDLEventList, tableColumnOptionsDef);
	}
	
	/* open system catalog and scan all indexes that belong to this table */
	pgIndex = heap_open(IndexRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_index_indrelid,
				BTEqualStrategyNumber, F_OIDEQ, relationId);

	scanDescriptor = systable_beginscan(pgIndex,
										IndexIndrelidIndexId, true, /* indexOK */
										SnapshotNow, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_index indexForm = (Form_pg_index) GETSTRUCT(heapTuple);
		Oid indexId = indexForm->indexrelid;
		bool isConstraint = false;
		char *statementDef = NULL;

		/*
		 * A primary key index is always created by a constraint statement.
		 * A unique key index is created by a constraint if and only if the
		 * index has a corresponding constraint entry in pg_depend. Any other
		 * index form is never associated with a constraint.
		 */
		if (indexForm->indisprimary)
		{
			isConstraint = true;
		}
		else if (indexForm->indisunique)
		{
			Oid constraintId = get_index_constraint(indexId);
			isConstraint = OidIsValid(constraintId);
		}
		else
		{
			isConstraint = false;
		}

		/* get the corresponding constraint or index statement */
		if (isConstraint)
		{
			Oid constraintId = get_index_constraint(indexId);
			Assert(constraintId != InvalidOid);

			statementDef = pg_get_constraintdef_string(constraintId);
		}
		else
		{
			statementDef = pg_get_indexdef_string(indexId);
		}
		
		/* append found constraint or index definition to the list */
		tableDDLEventList = lappend(tableDDLEventList, statementDef);

		/* if table is clustered on this index, append definition to the list */
		if (indexForm->indisclustered)
		{
			char *clusteredDef = IndexClusterDefinition(indexId);
			Assert (clusteredDef != NULL);

			tableDDLEventList = lappend(tableDDLEventList, clusteredDef);
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	/* clean up scan and close system catalog */
	systable_endscan(scanDescriptor);
	heap_close(pgIndex, AccessShareLock);

	return tableDDLEventList;
}


/*
 * ExtensionDefinition finds the foreign data wrapper that corresponds to
 * the given foreign tableId, and checks if an extension owns this foreign data
 * wrapper. If it does, the function returns the extension's definition. If not,
 * the function returns null.
 */
static char *
ExtensionDefinition(Oid tableRelationId)
{
	ForeignTable *foreignTable = GetForeignTable(tableRelationId);
	ForeignServer *server = GetForeignServer(foreignTable->serverid);
	ForeignDataWrapper *foreignDataWrapper = GetForeignDataWrapper(server->fdwid);
	StringInfoData buffer = { NULL, 0, 0, 0 };

	Oid classId = ForeignDataWrapperRelationId;
	Oid objectId = server->fdwid;

	Oid extensionId = getExtensionOfObject(classId, objectId);
	if (OidIsValid(extensionId))
	{
		char *extensionName = get_extension_name(extensionId);
		Oid extensionSchemaId = get_extension_schema(extensionId);
		char *extensionSchema = get_namespace_name(extensionSchemaId);

		initStringInfo(&buffer);
		appendStringInfo(&buffer, "CREATE EXTENSION IF NOT EXISTS %s WITH SCHEMA %s",
						 quote_identifier(extensionName),
						 quote_identifier(extensionSchema));
	}
	else
	{
		ereport(NOTICE, (errmsg("foreign-data wrapper \"%s\" does not have an "
								"extension defined", foreignDataWrapper->fdwname)));
	}

	return (buffer.data);
}


/*
 * get_extension_schema - given an extension OID, fetch its extnamespace
 *
 * Returns InvalidOid if no such extension.
 * XXX: copied from postgres backend/commands/extension.c
 */
static Oid
get_extension_schema(Oid ext_oid)
{
	Oid			result;
	Relation	rel;
	SysScanDesc scandesc;
	HeapTuple	tuple;
	ScanKeyData entry[1];

	rel = heap_open(ExtensionRelationId, AccessShareLock);

	ScanKeyInit(&entry[0],
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ext_oid));

	scandesc = systable_beginscan(rel, ExtensionOidIndexId, true,
								  SnapshotNow, 1, entry);

	tuple = systable_getnext(scandesc);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(tuple))
		result = ((Form_pg_extension) GETSTRUCT(tuple))->extnamespace;
	else
		result = InvalidOid;

	systable_endscan(scandesc);

	heap_close(rel, AccessShareLock);

	return result;
}


/*
 * ServerDefinition finds the foreign server that corresponds to the
 * given foreign tableId, and returns this server's definition.
 */
static char *
ServerDefinition(Oid tableRelationId)
{
	ForeignTable *foreignTable = GetForeignTable(tableRelationId);
	ForeignServer *server = GetForeignServer(foreignTable->serverid);
	ForeignDataWrapper *foreignDataWrapper = GetForeignDataWrapper(server->fdwid);

	StringInfoData buffer = { NULL, 0, 0, 0 };
	initStringInfo(&buffer);

	appendStringInfo(&buffer, "CREATE SERVER %s", quote_identifier(server->servername));
	if (server->servertype != NULL)
	{
		appendStringInfo(&buffer, " TYPE %s",
						 quote_literal_cstr(server->servertype));
	}
	if (server->serverversion != NULL)
	{
		appendStringInfo(&buffer, " VERSION %s",
						 quote_literal_cstr(server->serverversion));
	}

	appendStringInfo(&buffer, " FOREIGN DATA WRAPPER %s",
					 quote_identifier(foreignDataWrapper->fdwname));

	/* append server options, if any */
	AppendOptionListToString(&buffer, server->options);

	return (buffer.data);
}


/*
 * TableSchemaDefinition returns the definition of a given table. This
 * definition includes table's schema, default column values, not null and check
 * constraints. The definition does not include constraints that trigger index
 * creations; specifically, unique and primary key constraints are excluded.  
 */
static char *
TableSchemaDefinition(Oid tableRelationId)
{
	Relation relation = NULL;
	char *relationName = NULL;
	char relationKind = 0;
	TupleDesc tupleDescriptor = NULL;
	TupleConstr *tupleConstraints = NULL;
	int  attributeIndex = 0;
	bool firstAttributePrinted = false;
	AttrNumber defaultValueIndex = 0;
	AttrNumber constraintIndex = 0;
	AttrNumber constraintCount = 0;
	StringInfoData buffer = { NULL, 0, 0, 0 };

	/*
	 * Instead of retrieving values from system catalogs as other functions in
	 * ruleutils.c do, we follow an unusual approach here: we open the relation,
	 * and fetch the relation's tuple descriptor. We do this because the tuple
	 * descriptor already contains information harnessed from pg_attrdef,
	 * pg_attribute, pg_constraint, and pg_class; and therefore using the
	 * descriptor saves us from a lot of additional work.
	 */
	relation = relation_open(tableRelationId, AccessShareLock);
	relationName = generate_relation_name(tableRelationId);

	relationKind = relation->rd_rel->relkind;
	if (relationKind != RELKIND_RELATION && relationKind != RELKIND_FOREIGN_TABLE)
	{
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("%s is not a regular or foreign table", relationName)));
	}

	initStringInfo(&buffer);
	if (relationKind == RELKIND_RELATION)
	{
		appendStringInfo(&buffer, "CREATE TABLE %s (", relationName);
	}
	else
	{
		appendStringInfo(&buffer, "CREATE FOREIGN TABLE %s (", relationName);
	}

	/*
	 * Iterate over the table's columns. If a particular column is not dropped
	 * and is not inherited from another table, print the column's name and its
	 * formatted type.
	 */
	tupleDescriptor = RelationGetDescr(relation);
	tupleConstraints = tupleDescriptor->constr;

	for (attributeIndex = 0; attributeIndex < tupleDescriptor->natts; attributeIndex++)
	{
		Form_pg_attribute attributeForm = tupleDescriptor->attrs[attributeIndex];
		const char *attributeName = NULL;
		const char *attributeTypeName = NULL;

		if (!attributeForm->attisdropped && attributeForm->attinhcount == 0)
		{
			if (firstAttributePrinted)
			{
				appendStringInfoString(&buffer, ", ");
			}
			firstAttributePrinted = true;

			attributeName = NameStr(attributeForm->attname);
			appendStringInfo(&buffer, "%s ", quote_identifier(attributeName));

			attributeTypeName = format_type_with_typemod(attributeForm->atttypid,
														 attributeForm->atttypmod);
			appendStringInfoString(&buffer, attributeTypeName);

			/* if this column has a default value, append the default value */
			if (attributeForm->atthasdef)
			{
				AttrDefault *defaultValueList = NULL;
				AttrDefault *defaultValue = NULL;
				
				Node *defaultNode = NULL;
				List *defaultContext = NULL;
				char *defaultString = NULL;

				Assert(tupleConstraints != NULL);

				defaultValueList = tupleConstraints->defval;
				Assert(defaultValueList != NULL);

				defaultValue = &(defaultValueList[defaultValueIndex]);
				defaultValueIndex++;

				Assert(defaultValue->adnum == (attributeIndex + 1));
				Assert(defaultValueIndex <= tupleConstraints->num_defval);
				
				/* convert expression to node tree, and prepare deparse context */
				defaultNode = (Node *) stringToNode(defaultValue->adbin);
				defaultContext = deparse_context_for(relationName, tableRelationId);

				/* deparse default value string */
				defaultString = deparse_expression(defaultNode, defaultContext,
												   false, false);

				appendStringInfo(&buffer, " DEFAULT %s", defaultString);
			}

			/* if this column has a not null constraint, append the constraint */
			if (attributeForm->attnotnull)
			{
				appendStringInfoString(&buffer, " NOT NULL");
			}
		}
	}

	/*
	 * Now check if the table has any constraints. If it does, set the number of
	 * check constraints here. Then iterate over all check constraints and print
	 * them.
	 */
	if (tupleConstraints != NULL)
	{
		constraintCount = tupleConstraints->num_check;
	}

	for (constraintIndex = 0; constraintIndex < constraintCount; constraintIndex++)
	{
		ConstrCheck *checkConstraintList = tupleConstraints->check;
		ConstrCheck *checkConstraint = &(checkConstraintList[constraintIndex]);
		
		Node *checkNode = NULL;
		List *checkContext = NULL;
		char *checkString = NULL;

		/* if an attribute or constraint has been printed, format properly */
		if (firstAttributePrinted || constraintIndex > 0)
		{
			appendStringInfoString(&buffer, ", ");
		}

		appendStringInfo(&buffer, "CONSTRAINT %s CHECK ",
						 quote_identifier(checkConstraint->ccname));

		/* convert expression to node tree, and prepare deparse context */
		checkNode = (Node *) stringToNode(checkConstraint->ccbin);
		checkContext = deparse_context_for(relationName, tableRelationId);

		/* deparse check constraint string */
		checkString = deparse_expression(checkNode, checkContext, false, false);

		appendStringInfoString(&buffer, checkString);
	}

	/* close create table's outer parentheses */
	appendStringInfoString(&buffer, ")");

	/*
	 * If the relation is a foreign table, append the server name and options to
	 * the create table statement.
	 */
	if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		ForeignTable *foreignTable = GetForeignTable(tableRelationId);
		ForeignServer *foreignServer = GetForeignServer(foreignTable->serverid);

		char *serverName = foreignServer->servername;
		appendStringInfo(&buffer, " SERVER %s", quote_identifier(serverName));
		AppendOptionListToString(&buffer, foreignTable->options);
	}

	relation_close(relation, AccessShareLock);

	return (buffer.data);
}


/*
 * XXX: Modified generate_relation_name from postgres ruleutils.c dropping
 * nslist and force qualifying by namespace.
 */
static char *
generate_relation_name(Oid relationId)
{
	HeapTuple	tp;
	Form_pg_class reltup;
	char	   *relname;
	char	   *nspname;
	char	   *result;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relationId));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for relation %u", relationId);
	reltup = (Form_pg_class) GETSTRUCT(tp);
	relname = NameStr(reltup->relname);

	nspname = get_namespace_name(reltup->relnamespace);

	result = quote_qualified_identifier(nspname, relname);

	ReleaseSysCache(tp);

	return result;
}


/*
 * AppendOptionListToString converts the option list to its textual format, and
 * appends this text to the given string buffer.
 */
static void
AppendOptionListToString(StringInfo stringBuffer, List *optionList)
{
	if (optionList != NIL)
	{
		ListCell *optionCell = NULL;
		bool firstOptionPrinted = false;

		appendStringInfo(stringBuffer, " OPTIONS (");

		foreach(optionCell, optionList)
		{
			DefElem *option = (DefElem*) lfirst(optionCell);
			char *optionName = option->defname;
			char *optionValue = defGetString(option);

			if (firstOptionPrinted)
			{
				appendStringInfo(stringBuffer, ", ");
			}
			firstOptionPrinted = true;

			appendStringInfo(stringBuffer, "%s ", quote_identifier(optionName));
			appendStringInfo(stringBuffer, "%s", quote_literal_cstr(optionValue));
		}

		appendStringInfo(stringBuffer, ")");
	}
}


/*
 * TableColumnOptionsDefinition returns column storage type and column
 * statistics definitions for given table, _if_ these definitions differ from
 * their default values. The function returns null if all columns use default
 * values for their storage types and statistics.
 */
static char *
TableColumnOptionsDefinition(Oid tableRelationId)
{
	Relation relation = NULL;
	char *relationName = NULL;
	char relationKind = 0;
	TupleDesc tupleDescriptor = NULL;
	AttrNumber attributeIndex = 0;
	char *columnOptionStatement = NULL;
	List *columnOptionList = NIL;
	ListCell *columnOptionCell = NULL;
	bool firstOptionPrinted = false;
	StringInfoData buffer = { NULL, 0, 0, 0 };

	/*
	 * Instead of retrieving values from system catalogs, we open the relation,
	 * and use the relation's tuple descriptor to access attribute information.
	 * This is primarily to maintain symmetry with pg_get_tableschemadef.
	 */
	relation = relation_open(tableRelationId, AccessShareLock);
	relationName = generate_relation_name(tableRelationId);

	relationKind = relation->rd_rel->relkind;
	if (relationKind != RELKIND_RELATION && relationKind != RELKIND_FOREIGN_TABLE)
	{
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("%s is not a regular or foreign table", relationName)));
	}

	/*
	 * Iterate over the table's columns. If a particular column is not dropped
	 * and is not inherited from another table, check if column storage or
	 * statistics statements need to be printed.
	 */
	tupleDescriptor = RelationGetDescr(relation);

	for (attributeIndex = 0; attributeIndex < tupleDescriptor->natts; attributeIndex++)
	{
		Form_pg_attribute attributeForm = tupleDescriptor->attrs[attributeIndex];
		char *attributeName = NameStr(attributeForm->attname);
		char defaultStorageType = get_typstorage(attributeForm->atttypid);

		if (!attributeForm->attisdropped && attributeForm->attinhcount == 0)
		{
			/*
			 * If the user changed the column's default storage type, create
			 * alter statement and add statement to a list for later processing.
			 */
			if (attributeForm->attstorage != defaultStorageType)
			{
				char *storageName = 0;
				StringInfoData statement = { NULL, 0, 0, 0 };
				initStringInfo(&statement);

				switch (attributeForm->attstorage)
				{
					case 'p':
						storageName = "PLAIN";
						break;
					case 'e':
						storageName = "EXTERNAL";
						break;
					case 'm':
						storageName = "MAIN";
						break;
					case 'x':
						storageName = "EXTENDED";
						break;
					default:
						ereport(ERROR, (errmsg("unrecognized storage type: %c",
											   attributeForm->attstorage)));
						break;
				}

				appendStringInfo(&statement, "ALTER COLUMN %s ",
								 quote_identifier(attributeName));
				appendStringInfo(&statement, "SET STORAGE %s", storageName);
				
				columnOptionList = lappend(columnOptionList, statement.data);
			}

			/*
			 * If the user changed the column's statistics target, create
			 * alter statement and add statement to a list for later processing.
			 */
			if (attributeForm->attstattarget >= 0)
			{
				StringInfoData statement = { NULL, 0, 0, 0 };
				initStringInfo(&statement);

				appendStringInfo(&statement, "ALTER COLUMN %s ",
								 quote_identifier(attributeName));
				appendStringInfo(&statement, "SET STATISTICS %d",
								 attributeForm->attstattarget);

				columnOptionList = lappend(columnOptionList, statement.data);
			}
		}
	}

	/*
	 * Iterate over column storage and statistics statements that we created,
	 * and append them to a single alter table statement. 
	 */
	foreach(columnOptionCell, columnOptionList)
	{
		if (!firstOptionPrinted)
		{
			initStringInfo(&buffer);
			appendStringInfo(&buffer, "ALTER TABLE ONLY %s ",
							 generate_relation_name(tableRelationId));
		}
		else
		{
			appendStringInfoString(&buffer, ", ");
		}
		firstOptionPrinted = true;

		columnOptionStatement = (char *) lfirst(columnOptionCell);
		appendStringInfoString(&buffer, columnOptionStatement);

		pfree(columnOptionStatement);
	}

	list_free(columnOptionList);
	relation_close(relation, AccessShareLock);

	return (buffer.data);
}


/*
 * IndexClusterDefinition returns the definition of a cluster statement for
 * given index. The function returns null if the table is not clustered on given
 * index.
 */
static char *
IndexClusterDefinition(Oid indexRelationId)
{
	HeapTuple indexTuple = NULL;
	Form_pg_index indexForm = NULL;
	Oid tableRelationId = InvalidOid;
	StringInfoData buffer = { NULL, 0, 0, 0 };

	indexTuple = SearchSysCache(INDEXRELID, ObjectIdGetDatum(indexRelationId), 0, 0, 0);
	if (!HeapTupleIsValid(indexTuple))
	{
		ereport(ERROR, (errmsg("cache lookup failed for index %u", indexRelationId)));
	}

	indexForm = (Form_pg_index) GETSTRUCT(indexTuple);
	tableRelationId = indexForm->indrelid;

	/* check if the table is clustered on this index */
	if (indexForm->indisclustered)
	{
		char *tableName = generate_relation_name(tableRelationId);
		char *indexName = get_rel_name(indexRelationId); /* needs to be quoted */

		initStringInfo(&buffer);
		appendStringInfo(&buffer, "ALTER TABLE %s CLUSTER ON %s", 
						 tableName, quote_identifier(indexName));
	}

	ReleaseSysCache(indexTuple);

	return (buffer.data);
}


/*
 * ExtendedTableDDLEvents takes in a list of ddl events and parses and
 * subsequently extends the names in the event with the given shardId. The
 * function then de-parses the extended parsed statement back into an executable
 * sql string and returns them in a list.
 */
List *
ExtendedTableDDLEvents(List *ddlEventList, uint64 shardId, Oid originalRelationId)
{
	List *extendedDDLEventList = NIL;
	ListCell *ddlEventCell = NULL;

	foreach(ddlEventCell, ddlEventList)
	{
		char *ddlEvent = (char *) lfirst(ddlEventCell);
		char *extendedDDLEvent = NULL;

		/* extend names in ddl command and apply extended command */
		Node *ddlEventNode = ParseTreeNode(ddlEvent);
		DDLEventExtendNames(ddlEventNode, shardId);

		extendedDDLEvent = DeparseDDLEvent(ddlEventNode, originalRelationId);
		extendedDDLEventList = lappend(extendedDDLEventList, extendedDDLEvent);
	}

	return extendedDDLEventList;	
}


/*
 * Parses the given DDL command, and returns the tree node for parsed command.
 */
static Node *
ParseTreeNode(const char *ddlCommand)
{
	Node *parseTreeNode = NULL;
	List *parseTreeList = NULL;
	uint32 parseTreeCount = 0;

	parseTreeList = pg_parse_query(ddlCommand);

	/* log immediately if dictated by log statement */
	if (check_log_statement(parseTreeList))
	{
		ereport(LOG, (errmsg("statement: %s", ddlCommand), errhidestmt(true)));
	}

	parseTreeCount = list_length(parseTreeList);
	if (parseTreeCount != 1)
	{
		ereport(ERROR, (errmsg("cannot execute multiple utility events")));
	}

	/*
	 * xact.c rejects certain commands that are unsafe to run inside transaction
	 * blocks. Since we only apply commands that relate to creating tables and
	 * those commands are safe, we can safely set the ProcessUtilityContext to
	 * PROCESS_UTILITY_TOPLEVEL.
	 */
	parseTreeNode = (Node *) linitial(parseTreeList);

	return parseTreeNode;
}


/* XXX: pulled from postgres.c */
static bool
check_log_statement(List *stmt_list)
{
	ListCell   *stmt_item;

	if (log_statement == LOGSTMT_NONE)
		return false;
	if (log_statement == LOGSTMT_ALL)
		return true;

	/* Else we have to inspect the statement(s) to see whether to log */
	foreach(stmt_item, stmt_list)
	{
		Node	   *stmt = (Node *) lfirst(stmt_item);

		if (GetCommandLogLevel(stmt) <= log_statement)
			return true;
	}

	return false;
}


/*
 * DDLEventExtendNames extends relation names in the given parse tree for
 * certain utility commands. The function more specifically extends table,
 * sequence, and index names in the parse tree by appending the given shardId;
 * thereby avoiding name collisions in the database among sharded tables. This
 * function has the side effect of extending relation names in the parse tree.
 */
static void
DDLEventExtendNames(Node *parseTree, uint64 shardId)
{
	/* we don't extend names in extension commands */
	NodeTag nodeType = nodeTag(parseTree);
	if (nodeType == T_CreateExtensionStmt)
	{
		return;
	}

	switch (nodeType)
	{
		case T_AlterTableStmt:
		{
			/*
			 * We append shardId to the very end of table, sequence and index
			 * names to avoid name collisions. We usually do not touch
			 * constraint names, except for cases where they refer to index
			 * names. In those cases, we also append to constraint names.
			 */

			AlterTableStmt *alterTableStmt = (AlterTableStmt *) parseTree;
			char **relationName = &(alterTableStmt->relation->relname);
			RangeVar *relation  = alterTableStmt->relation; /* for constraints */

			List *commandList = alterTableStmt->cmds;
			ListCell *commandCell = NULL;

			/* first append shardId to base relation name */
			AppendShardIdToName(relationName, shardId);

			foreach(commandCell, commandList)
			{
				AlterTableCmd *command = (AlterTableCmd *) lfirst(commandCell);

				if (TypeAddIndexConstraint(command) ||
					TypeDropIndexConstraint(command, relation, shardId))
				{
					AppendShardIdToConstraintName(command, shardId);
				}
				else if (command->subtype == AT_ClusterOn)
				{
					char **indexName = &(command->name);
					AppendShardIdToName(indexName, shardId);
				}
			}

			break;
		}

		case T_ClusterStmt:
		{
			ClusterStmt *clusterStmt = (ClusterStmt *) parseTree;
			char **relationName = NULL;

			/* we do not support clustering the entire database */
			if (clusterStmt->relation == NULL)
			{
				ereport(ERROR, (errmsg("cannot extend name for multi-relation cluster")));
			}

			relationName = &(clusterStmt->relation->relname);
			AppendShardIdToName(relationName, shardId);

			if (clusterStmt->indexname != NULL)
			{
				char **indexName = &(clusterStmt->indexname);
				AppendShardIdToName(indexName, shardId);
			}

			break;
		}

		case T_CreateForeignServerStmt:
		{
			CreateForeignServerStmt *serverStmt = (CreateForeignServerStmt *) parseTree;
			char **serverName = &(serverStmt->servername);

			AppendShardIdToName(serverName, shardId);
			break;
		}

		case T_CreateForeignTableStmt:
		{
			CreateForeignTableStmt *createStmt = (CreateForeignTableStmt *) parseTree;
			char **serverName = &(createStmt->servername);

			AppendShardIdToName(serverName, shardId);

			/*
			 * Since CreateForeignTableStmt inherits from CreateStmt and any change
			 * performed on CreateStmt should be done here too, we simply *fall
			 * through* to avoid code repetition.
			 */
		}

		case T_CreateStmt:
		{
			CreateStmt *createStmt = (CreateStmt *) parseTree;
			char **relationName = &(createStmt->relation->relname);

			AppendShardIdToName(relationName, shardId);
			break;
		}

		case T_IndexStmt:
		{
			IndexStmt *indexStmt = (IndexStmt *) parseTree;
			char **relationName = &(indexStmt->relation->relname);
			char **indexName = &(indexStmt->idxname);

			/*
			 * Concurrent index statements cannot run within a transaction block.
			 * Therefore, we do not support them.
			 */
			if (indexStmt->concurrent)
			{
				ereport(ERROR, (errmsg("cannot extend name for concurrent index")));
			}

			/*
			 * In the regular DDL execution code path (for non-sharded tables),
			 * if the index statement results from a table creation command, the
			 * indexName may be null. For sharded tables however, we intercept
			 * that code path and explicitly set the index name. Therefore, the
			 * index name in here cannot be null.
			 */
			if ((*indexName) == NULL)
			{
				ereport(ERROR, (errmsg("cannot extend name for null index name")));
			}

			AppendShardIdToName(relationName, shardId);
			AppendShardIdToName(indexName, shardId);
			break;
		}

		default:
		{
			ereport(WARNING, (errmsg("unsafe statement type in name extension"),
							  errdetail("Statement type: %u", (uint32) nodeType)));
			break;
		}
	}
}


/*
 * TypeAddIndexConstraint checks if the alter table command adds a constraint
 * and if that constraint also results in an index creation.
 */
static bool
TypeAddIndexConstraint(const AlterTableCmd *command)
{
	if (command->subtype == AT_AddConstraint)
	{
		if (IsA(command->def, Constraint))
		{
			Constraint *constraint = (Constraint *) command->def;
			if (constraint->contype == CONSTR_PRIMARY ||
				constraint->contype == CONSTR_UNIQUE)
			{
				return true;
			}
		}
	}

	return false;
}


/*
 * TypeDropIndexConstraint checks if the alter table command drops a constraint
 * and if that constraint also results in an index drop. Note that drop
 * constraints do not have access to constraint type information; this is in
 * contrast with add constraint commands. This function therefore performs
 * additional system catalog lookups to determine if the drop constraint is
 * associated with an index.
 */
static bool
TypeDropIndexConstraint(const AlterTableCmd *command, 
						const RangeVar *relation, uint64 shardId)
{
	Relation pgConstraint = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData	scanKey[1];
	int scanKeyCount = 1;
	HeapTuple heapTuple = NULL;

	char *searchedConstraintName = NULL;
	bool indexConstraint = false;	
	Oid  relationId = InvalidOid;
	bool failOK = true;

	if (command->subtype != AT_DropConstraint)
	{
		return false;
	}

	/*
	 * At this stage, our only option is performing a relationId lookup. We
	 * first find the relationId, and then scan the pg_constraints system
	 * catalog using this relationId. Finally, we check if the passed in
	 * constraint is for a primary key or unique index.
	 */
	relationId = RangeVarGetRelid(relation, NoLock, failOK);
	if (!OidIsValid(relationId))
	{
		/* overlook this error, it should be signaled later in the pipeline */
		return false;
	}

	searchedConstraintName = pnstrdup(command->name, NAMEDATALEN);
	AppendShardIdToName(&searchedConstraintName, shardId);

	pgConstraint = heap_open(ConstraintRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relationId));
	
	scanDescriptor = systable_beginscan(pgConstraint, 
										ConstraintRelidIndexId, true, /* indexOK */
										SnapshotNow, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);
		char *constraintName = NameStr(constraintForm->conname);
		
		if (strncmp(constraintName, searchedConstraintName, NAMEDATALEN) == 0)
		{
			/* we found the constraint, now check if it is for an index */
			if (constraintForm->contype == CONSTRAINT_PRIMARY ||
				constraintForm->contype == CONSTRAINT_UNIQUE)
			{
				indexConstraint = true;
			}
			
			break;
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgConstraint, AccessShareLock);
	
	pfree(searchedConstraintName);

	return indexConstraint;
}


/*
 * AppendShardIdToConstraintName extends given constraint name with given
 * shardId. Note that we only extend constraint names if they correspond to
 * indexes, and the caller should verify that index correspondence before
 * calling this function.
 */
static void
AppendShardIdToConstraintName(AlterTableCmd *command, uint64 shardId)
{
	if (command->subtype == AT_AddConstraint)
	{
		Constraint *constraint = (Constraint *) command->def;
		char **constraintName = &(constraint->conname);
		AppendShardIdToName(constraintName, shardId);
	}
	else if (command->subtype == AT_DropConstraint)
	{
		char **constraintName = &(command->name);
		AppendShardIdToName(constraintName, shardId);
	}
}


/*
 * AppendShardIdToName appends shardId to the given name. The function takes in
 * the name's address in order to reallocate memory for the name in the same
 * memory context the name was originally created in.
 */
static void
AppendShardIdToName(char **name, uint64 shardId)
{
	char   extendedName[NAMEDATALEN];
	uint32 extendedNameLength = 0;

	snprintf(extendedName, NAMEDATALEN, "%s%c" UINT64_FORMAT, 
			 (*name), SHARD_NAME_SEPARATOR, shardId);

	/*
	 * Parser should have already checked that the table name has enough space
	 * reserved for appending shardIds. Nonetheless, we perform an additional
	 * check here to verify that the appended name does not overflow.
	 */
	extendedNameLength = strlen(extendedName) + 1;
	if (extendedNameLength >= NAMEDATALEN)
	{
		ereport(ERROR, (errmsg("shard name too long to extend: \"%s\"", (*name))));
	}

	(*name) = (char *) repalloc((*name), extendedNameLength);
	snprintf((*name), extendedNameLength, "%s", extendedName);
}


/*
 * DeparseDDLEvent converts the parsed ddl event node back into an executable
 * SQL string.
 */
static char *
DeparseDDLEvent(Node *ddlEventNode, Oid originalRelationId)
{
	char *deparsedEventString = NULL;
	NodeTag nodeType = nodeTag(ddlEventNode);

	switch (nodeType)
	{
		case T_AlterTableStmt:
		{
			AlterTableStmt *alterTableStmt = (AlterTableStmt *) ddlEventNode;
			deparsedEventString = DeparseAlterTableStmt(alterTableStmt);
			break;
		}

	    case T_CreateExtensionStmt:
		{
			CreateExtensionStmt *createExtensionStmt =
				(CreateExtensionStmt *) ddlEventNode;
			deparsedEventString = DeparseCreateExtensionStmt(createExtensionStmt);
			break;
		}

	    case T_CreateForeignServerStmt:
		{
			CreateForeignServerStmt *foreignServerStmt =
				(CreateForeignServerStmt *) ddlEventNode;
			deparsedEventString = DeparseForeignServerStmt(foreignServerStmt);
			break;
		}

	    case T_CreateStmt:
	    case T_CreateForeignTableStmt:
		{
			CreateStmt *createStmt = (CreateStmt *) ddlEventNode;
			deparsedEventString = DeparseCreateStmt(createStmt, originalRelationId);
			break;
		}
	    case T_IndexStmt:
		{
			IndexStmt *createIndexStmt = (IndexStmt *) ddlEventNode;
			deparsedEventString = DeparseIndexStmt(createIndexStmt,
												   originalRelationId);
			break;
		}

	    default:
		{
			ereport(ERROR, (errmsg("XXX")));
			break;
		}
	}

	return deparsedEventString;
}


/*
 * DeparseAlterTableStmt converts a parsed alterTable statement back into its
 * SQL form.
 */
static char *
DeparseAlterTableStmt(AlterTableStmt *alterTableStmt)
{
	StringInfo alterTableString = makeStringInfo();
	char *relationName = NULL;
	char *schemaName = NULL;
	char *qualifiedTableName = NULL;
	List *commandList = alterTableStmt->cmds;
	ListCell *commandCell = NULL;
	List *optionList = NIL;
	ListCell *optionCell = NULL;

	bool firstOptionPrinted = false;

	foreach(commandCell, commandList)
	{
		AlterTableCmd *alterTableCommand = (AlterTableCmd *) lfirst(commandCell);
		AlterTableType alterTableType = alterTableCommand->subtype;

		switch (alterTableType)
		{
	        case AT_AddConstraint:
			{
				Constraint *constraint = (Constraint *) alterTableCommand->def;
				char *deparsedConstraint = DeparseConstraint(constraint);
				optionList = lappend(optionList, deparsedConstraint);
				break;
			}

	        case AT_ClusterOn:
			{
				StringInfo clusterOnString = makeStringInfo();
				char *indexName = alterTableCommand->name;

				appendStringInfo(clusterOnString, "CLUSTER ON %s", indexName);
				optionList = lappend(optionList, clusterOnString->data);
				break;
			}

		    case AT_SetStorage:
			{
				StringInfo setStorageString = makeStringInfo();
				char *storageType = NULL;
				char *columnName = alterTableCommand->name;
				Assert(IsA(alterTableCommand->def, String));
				storageType = strVal(alterTableCommand->def);

				appendStringInfo(setStorageString, "ALTER COLUMN %s ",
								 quote_identifier(columnName));
				appendStringInfo(setStorageString, " SET STORAGE %s", 
								 storageType);
				optionList = lappend(optionList, setStorageString->data);
				break;
			}

		    case AT_SetStatistics:
			{
				StringInfo setStatisticsString = makeStringInfo();
				int newStatsTarget = 0;
				char *columnName = alterTableCommand->name;
				Assert(IsA(alterTableCommand->def, Integer));
				newStatsTarget = intVal(alterTableCommand->def);

				appendStringInfo(setStatisticsString, "ALTER COLUMN %s ",
								 quote_identifier(columnName));
				appendStringInfo(setStatisticsString, " SET STATISTICS %d", 
								 newStatsTarget);
				optionList = lappend(optionList, setStatisticsString->data);
				break;
			}

	        default:
			{
				ereport(ERROR, (errmsg("unsupported alter table type:%d", alterTableType)));
			}
		}
	}

	/* iterate over the options and append them to a single alter table	statement */
	relationName = alterTableStmt->relation->relname;
	schemaName = alterTableStmt->relation->schemaname;
	qualifiedTableName = quote_qualified_identifier(schemaName, relationName);
	foreach(optionCell, optionList)
	{
		char *optionString = (char *) lfirst(optionCell);
		if (!firstOptionPrinted)
		{
			appendStringInfo(alterTableString, "ALTER TABLE ONLY %s ",
							 qualifiedTableName);
		}
		else
		{
			appendStringInfoString(alterTableString, ", ");
		}
		firstOptionPrinted = true;

		appendStringInfoString(alterTableString, optionString);
	}

	return alterTableString->data;
}


/*
 * DeparseConstraint converts the parsed constraint into its SQL form. Currently
 * the function only handles PRIMARY KEY and UNIQUE constraints.
*/
static char *
DeparseConstraint(Constraint *constraint)
{
	StringInfo buffer = makeStringInfo();
	ConstrType constraintType = constraint->contype;
	List *columnNameList = constraint->keys;
	ListCell *columnNameCell = NULL;
	bool firstAttributePrinted = false;

	appendStringInfo(buffer, "ADD CONSTRAINT %s ",
					 quote_identifier(constraint->conname));

	if (constraintType == CONSTR_PRIMARY ||
		constraintType == CONSTR_UNIQUE)
	{
		if (constraintType == CONSTR_PRIMARY)
		{
			appendStringInfo(buffer, "PRIMARY KEY (");
		}
		else
		{
			appendStringInfo(buffer, "UNIQUE (");
		}

		/* add the column names */
		foreach(columnNameCell, columnNameList)
		{
			Value *columnNameValue = (Value *) lfirst(columnNameCell);
			char *columnName = strVal(columnNameValue);

			if (firstAttributePrinted)
			{
				appendStringInfo(buffer, ", ");
			}
			firstAttributePrinted = true;

			appendStringInfo(buffer, "%s", quote_identifier(columnName));
		}

		appendStringInfo(buffer, ")");
	}
	else
	{
		ereport(ERROR, (errmsg("unsupported constraint type:%d", constraintType)));
	}

	return buffer->data;
}


/*
 * DeparseCreateExtensionStmt converts the given parsed createExtension
 * statement back into its SQL form.
 */
static char *
DeparseCreateExtensionStmt(CreateExtensionStmt *createExtensionStatement)
{
	StringInfo buffer = makeStringInfo();

	char *extensionName = createExtensionStatement->extname;
	List *optionsList = createExtensionStatement->options;
	ListCell *optionsCell = NULL;

	appendStringInfoString(buffer, "CREATE EXTENSION ");
	if (createExtensionStatement->if_not_exists)
	{
		appendStringInfoString(buffer, "IF NOT EXISTS ");
	}
	appendStringInfo(buffer, "%s", extensionName);

	/* append options if any */
	foreach(optionsCell, optionsList)
	{
		DefElem *option = (DefElem *) lfirst(optionsCell);
		char *optionName = option->defname;
		char *optionValue = strVal(option->arg);

		appendStringInfo(buffer, " %s %s", optionName, optionValue);
	}

	return buffer->data;
}


/*
 * DeparseForeignServerStmt converts the parsed createForeignServer statement
 * back into its SQL form.
 */
static char *
DeparseForeignServerStmt(CreateForeignServerStmt *foreignServerStmt)
{
	StringInfo buffer = makeStringInfo();

	appendStringInfo(buffer, "CREATE SERVER %s",
					 quote_identifier(foreignServerStmt->servername));

	if (foreignServerStmt->servertype != NULL)
	{
		appendStringInfo(buffer, " TYPE %s",
						 quote_literal_cstr(foreignServerStmt->servertype));
	}
	if (foreignServerStmt->version != NULL)
	{
		appendStringInfo(buffer, " VERSION %s",
						 quote_literal_cstr(foreignServerStmt->version));
	}

	appendStringInfo(buffer, " FOREIGN DATA WRAPPER %s",
					 quote_identifier(foreignServerStmt->fdwname));

	/* append server options, if any */
	AppendOptionListToString(buffer, foreignServerStmt->options);

	return buffer->data;
}


/*
 * DeparseCreateStmt converts the given parsed create table statement back into
 * its SQL form. The function also handles create statements for foreign tables.
 */
static char *
DeparseCreateStmt(CreateStmt *createStmt, Oid originalRelationId)
{
	StringInfo buffer = makeStringInfo();
	List *tableElementList = NIL;
	ListCell *tableElementCell = NULL;
	bool firstAttributePrinted = false;
	char *originalRelationName = get_rel_name(originalRelationId);
	char *schemaName = createStmt->relation->schemaname;
	char *relationName = createStmt->relation->relname;
	char *qualifiedTableName = quote_qualified_identifier(schemaName, relationName);

	if (IsA(createStmt, CreateForeignTableStmt))
	{
		appendStringInfo(buffer, "CREATE FOREIGN TABLE %s (", qualifiedTableName);
	}
	else
	{
		appendStringInfo(buffer, "CREATE TABLE %s (", qualifiedTableName);
	}

	/* first iterate over the columns and print the name and type */
	tableElementList = createStmt->tableElts;
	foreach(tableElementCell, tableElementList)
	{
		Node *tableElement = (Node *) lfirst(tableElementCell);

		ColumnDef *columnDef = NULL;
		List *columnConstraintList = NIL;
		ListCell *columnConstraintCell = NULL;
		char *columnName = NULL;

		Oid typeOid = InvalidOid;
		int32 typeMod = 0;
		char *typeName = NULL;

		/*
		 * After raw parsing the ColumnDef and Constraint nodes are intermixed
		 * in the table elements. In this loop we first only process the columns.
		 */
		if (!IsA(tableElement, ColumnDef))
		{
			continue;
		}

		columnDef = (ColumnDef *) tableElement;
		columnName = columnDef->colname;

		/* build the type definition */
		typenameTypeIdAndMod(NULL, columnDef->typeName, &typeOid, &typeMod);
		typeName = format_type_with_typemod(typeOid, typeMod);

		if (firstAttributePrinted)
		{
			appendStringInfoString(buffer, ", ");
		}
		firstAttributePrinted = true;

		appendStringInfo(buffer, "%s ", quote_identifier(columnName));
		appendStringInfoString(buffer, typeName);

		/* if this column has a constraint, append the constraint */
		columnConstraintList = columnDef->constraints;
		foreach(columnConstraintCell, columnConstraintList)
		{
			Constraint *columnConstraint =
				(Constraint *) lfirst(columnConstraintCell);
			if (columnConstraint->contype == CONSTR_NOTNULL)
			{
				appendStringInfoString(buffer, " NOT NULL");
			}
			if (columnConstraint->contype == CONSTR_DEFAULT)
			{
				Node *rawExpression = columnConstraint->raw_expr;
				Node *transformedExpression = TransformRawExpression(rawExpression,
																	 originalRelationId,
																	 EXPR_KIND_COLUMN_DEFAULT);

				List *defaultContext = deparse_context_for(originalRelationName,
														   originalRelationId);
				char *defaultValueString =
					deparse_expression(transformedExpression, defaultContext,
									   false, false);
				appendStringInfo(buffer, " DEFAULT %s ", defaultValueString);
			}
		}
	}

	/* after raw parsing the constraints should be part of the table elements */
	Assert(createStmt->constraints == NIL);

	/* now iterate over the elements again to process the constraints */
	tableElementCell = NULL;
	foreach(tableElementCell, tableElementList)
	{
		Node *tableElement = (Node *) lfirst(tableElementCell);
		Constraint *constraint = NULL;
		List *checkContext = NULL;
		char *checkString = NULL;
		Node *transformedExpression = NULL;

		/* skip over table elements which are ColumnDefs */
		if (!IsA(tableElement, Constraint))
		{
			continue;
		}

		constraint = (Constraint *) tableElement;
		if (constraint->contype != CONSTR_CHECK)
		{
			ereport(ERROR, (errmsg("unsupported constraint type %d",
								   constraint->contype)));
		}

		appendStringInfo(buffer, " CONSTRAINT %s CHECK ",
						 quote_identifier(constraint->conname));

		Assert(constraint->cooked_expr == NULL);

		/* convert the expression from its raw parsed form */
		transformedExpression = TransformRawExpression(constraint->raw_expr,
													   originalRelationId,
													   EXPR_KIND_CHECK_CONSTRAINT);

		/* deparse check constraint string */
		checkContext = deparse_context_for(originalRelationName,
										   originalRelationId);
 		checkString = deparse_expression(transformedExpression, checkContext,
										 false, false);

		appendStringInfoString(buffer, checkString);
	}

	/* close create table's outer parentheses */
	appendStringInfoString(buffer, ")");

	/*
	 * If the relation is a foreign table, append the server name and options to
	 * the create table statement.
	 */
	if (IsA(createStmt, CreateForeignTableStmt))
	{
		CreateForeignTableStmt *createForeignTableStmt = 
			(CreateForeignTableStmt	*) createStmt;

		char *serverName = createForeignTableStmt->servername;
		appendStringInfo(buffer, " SERVER %s", quote_identifier(serverName));
		AppendOptionListToString(buffer, createForeignTableStmt->options);
	}

	return buffer->data;
}


/*
 * TransformRawExpression analyzes and transforms the raw expression generated
 * by the parse tree.
 */
static Node *
TransformRawExpression(Node *rawExpression, Oid originalRelationId,
					   ParseExprKind parseExprKind)
{
	Node *transformedExpression = NULL;

	/*
	 * Create a dummy parse state and insert the original relation as its sole
	 * range table entry. We need this for resolving columns in transformExpr
	 */
	Relation originalRelation = heap_open(originalRelationId,
										  AccessShareLock);

	ParseState *parseState = make_parsestate(NULL);
	RangeTblEntry *rte = addRangeTableEntryForRelation(parseState, originalRelation,
													   NULL, false, true);
	addRTEtoQuery(parseState, rte, false, true, true);

	transformedExpression = transformExpr(parseState, rawExpression, parseExprKind);

	heap_close(originalRelation, AccessShareLock);

	return transformedExpression;
}


/*
 * DeparseIndexStmt converts the parsed indexStmt back into its SQL form.
 */
static char *
DeparseIndexStmt(IndexStmt *indexStmt, Oid originalRelationId)
{
	StringInfo buffer = makeStringInfo();
	List *indexParamsList = NIL;
	ListCell *indexParamsCell = NULL;
	bool firstOptionPrinted = false;
	char *relationName = indexStmt->relation->relname;
	char *schemaName = indexStmt->relation->schemaname;
	char *qualifiedTableName = quote_qualified_identifier(schemaName, relationName);

	appendStringInfoString(buffer, "CREATE");
	if (indexStmt->unique)
	{
		appendStringInfoString(buffer, " UNIQUE");
	}

	Assert(indexStmt->idxname != NULL);
	appendStringInfo(buffer, " INDEX %s ON %s USING %s (",
					 quote_identifier(indexStmt->idxname),
					 qualifiedTableName,
					 quote_identifier(indexStmt->accessMethod));

	/* add the columns */
	indexParamsList = indexStmt->indexParams;
	foreach(indexParamsCell, indexParamsList)
	{
		IndexElem *indexElem = (IndexElem *) lfirst(indexParamsCell);
		if (firstOptionPrinted)
		{
			appendStringInfoString(buffer, ", ");
		}
		firstOptionPrinted = true;

		if (indexElem->name != NULL)
		{
			appendStringInfoString(buffer, quote_identifier(indexElem->name));
		}
		else
		{
			Node *expression = indexElem->expr;
			char *originalRelationName = get_rel_name(originalRelationId);
			List *exprContext = deparse_context_for(originalRelationName,
													originalRelationId);
			char *exprString = deparse_expression(expression,
												  exprContext,
												  false, false);
			
			/* Need parens if it's not a bare function call */
			if (IsA(expression, FuncExpr) &&
				((FuncExpr *) expression)->funcformat == COERCE_EXPLICIT_CALL)
			{
				appendStringInfoString(buffer, exprString);
			}
			else
			{
				appendStringInfo(buffer, "(%s)", exprString);
			}
		}

		/* add collation if present */
		if (indexElem->collation != NIL)
		{
			char *collationName = NULL;
			char *schemaName = NULL;
			DeconstructQualifiedName(indexElem->collation, &schemaName, &collationName);
			appendStringInfo(buffer, " COLLATE %s", collationName);
		}

		/* add opclass if present */
		if (indexElem->opclass != NIL)
		{
			char *opClassName = NULL;
			char *schemaName = NULL;
			DeconstructQualifiedName(indexElem->opclass, &schemaName, &opClassName);
			appendStringInfo(buffer, " %s", opClassName);
		}

		/* if sort ordering specified, add it */
		if (indexElem->ordering != SORTBY_DEFAULT)
		{
			if (indexElem->ordering == SORTBY_ASC)
			{
				appendStringInfoString(buffer, " ASC");
			}
			else if (indexElem->ordering == SORTBY_DESC)
			{
				appendStringInfoString(buffer, " DESC");
			}
		}

		/* if nulls ordering is specified, add it */
		if (indexElem->nulls_ordering != SORTBY_NULLS_DEFAULT)
		{
			appendStringInfoString(buffer, " NULLS");
			if (indexElem->nulls_ordering == SORTBY_NULLS_FIRST)
			{
				appendStringInfoString(buffer, " FIRST");
			}
			else if (indexElem->nulls_ordering == SORTBY_NULLS_LAST)
			{
				appendStringInfoString(buffer, " LAST");
			}
		}

		/* XXX exclusion operator */		
	}

	appendStringInfoChar(buffer, ')');

	/* append options if any */
	AppendOptionListToString(buffer, indexStmt->options);

	/* add tablespace if specified */
	if (indexStmt->tableSpace != NULL)
	{
		appendStringInfo(buffer, " TABLESPACE %s",
						 quote_identifier(indexStmt->tableSpace));
	}

	/* XXX partial index */

	return buffer->data;
}
