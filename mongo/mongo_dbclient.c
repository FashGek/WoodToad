#include "mongo_query.h"
#include "mongo_cursor.h"

#include "common.h"
#include "utils.h"

#include "driver/mongo.h"

#include <lua.h>
#include <lauxlib.h>

#include <luacompat52.h>

typedef struct {
	mongo conn;

} MongoDbClient;

mongo*
userdata_to_dbclient(lua_State *L, int stackpos) {
	// adapted from http://www.lua.org/source/5.1/lauxlib.c.html#luaL_checkudata
	void *ud = lua_touserdata(L, stackpos);
	if (ud == NULL )
		luaL_typerror(L, stackpos, "userdata");

	// try Connection
	lua_getfield(L, LUA_REGISTRYINDEX, LUAMONGO_CONNECTION);
	if (lua_getmetatable(L, stackpos)) {
		if (lua_rawequal(L, -1, -2)) {
			mongo *connection = *((mongo **) ud);
			lua_pop(L, 2);
			return connection;
		}
		lua_pop(L, 2);
	} else
		lua_pop(L, 1);

	// try ReplicaSet
	lua_getfield(L, LUA_REGISTRYINDEX, LUAMONGO_REPLICASET); // get correct metatable
	if (lua_getmetatable(L, stackpos)) {
		if (lua_rawequal(L, -1, -2)) {
			mongo *replicaset = *((mongo **) ud);
			lua_pop(L, 2);
			// remove both metatables
			return replicaset;
		}
		lua_pop(L, 2);
	} else
		lua_pop(L, 1);

	// luaL_typerror(L, stackpos, LUAMONGO_DBCLIENT);
	return NULL ; // should never get here
}
//
////////////////////////////////////////////

/*
 * ok,err = db:auth({})
 *    accepts a table of parameters:
 *       dbname           database to authenticate (required)
 *       username         username to authenticate against (required)
 *       password         password to authenticate against (required)
 *       digestPassword   set to true if password is pre-digested (default = true)
 *
 */
static int dbclient_auth(lua_State *L) {

	mongo *dbclient = userdata_to_dbclient(L, 1);

	luaL_checktype(L, 2, LUA_TTABLE);
	lua_getfield(L, 2, "dbname");
	const char *dbname = luaL_checkstring(L, -1);
	lua_getfield(L, 2, "username");
	const char *username = luaL_checkstring(L, -1);
	lua_getfield(L, 2, "password");
	const char *password = luaL_checkstring(L, -1);
//    lua_getfield(L, 2, "digestPassword");
//    bool digestPassword = lua_isnil(L, -1) ? true : lua_toboolean(L, -1);
	lua_pop(L, 3);

//    char* errmsg;
	int success = mongo_cmd_authenticate(dbclient, dbname, username, password); // dbclient->auth(dbname, username, password, errmsg, digestPassword);
	if (MONGO_ERROR == success) {
		lua_pushnil(L);
		lua_pushfstring(L, LUAMONGO_ERR_CONNECTION_FAILED,
				"couldnot autenticate");
		return 2;
	}
	lua_pushboolean(L, 1);
	return 1;
}

/*
 * count,err = db:count(ns, lua_table or json_str)
 */
static int dbclient_count(lua_State *L) {
	mongo *dbclient = userdata_to_dbclient(L, 1);
	const char *db = luaL_checkstring(L, 2);
	const char *ns = luaL_checkstring(L, 3);
	char *msg = NULL;
	bson query;

	bson_init(&query);
	int type = lua_type(L, 3);
	if (type == LUA_TSTRING) {
		const char *jsonstr = luaL_checkstring(L, 4);
		fromjson(jsonstr, &query);
	} else if (type == LUA_TTABLE) {
		fromtable_with_lua(L, 4, &query);
	}

	int ret = mongo_count(dbclient, db, ns, &query);
	if (MONGO_ERROR == ret) {
		lua_pushnil(L);
		switch (dbclient->err) {
		default:
			msg = "Unknown error occured when counting";
			break;
		}
		lua_pushfstring(L, LUAMONGO_ERR_COUNT_FAILED, msg);
		return 2;
	} else {
		lua_pushinteger(L, ret);
	}

	return 1;
}

/*
 * ok,err = db:drop_collection(ns)
 */
static int dbclient_drop_collection(lua_State *L) {
    mongo *dbclient = userdata_to_dbclient(L, 1);
    const char *ns = luaL_checkstring(L, 2);

    int status = mongo_cmd_drop_collection(dbclient, ns, NULL);

    if (MONGO_ERROR == status) {
    	lua_pushboolean(L, 0);
		lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "drop_collection", "unknow error");
		return 2;
    }

    lua_pushboolean(L, true);
    return 1;
}


/*
 * created = db:ensure_index(ns, json_str or lua_table[, unique[, name]])
 */
static int dbclient_ensure_index(lua_State *L) {
    mongo *dbclient = userdata_to_dbclient(L, 1);
    const char *ns = luaL_checkstring(L, 2);
    bson fields;

    int type = lua_type(L, 3);
	if (type == LUA_TSTRING) {
		fromjson_with_lua(L, 3, &fields);
	} else if (type == LUA_TTABLE) {
		fromtable_with_lua(L, 3, &fields);
	} else {
		lua_pushboolean(L, 0);
		lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "ensure_index", "unknowen reason");
		return 2;
	}

    bool unique = lua_toboolean(L, 4);
    // const char *name = luaL_optstring(L, 5, "");

    int flags = 0;
    if (unique) { flags |= MONGO_INDEX_UNIQUE; }
    int status = mongo_create_index(dbclient, ns, &fields, flags, NULL);

    bool res = true;
    if (MONGO_ERROR == status) {
    	res = false;
    }

    lua_pushboolean(L, res);
    return 1;
}

///*
// * ok,err = db:drop_indexes(ns)
// */
//static int dbclient_drop_indexes(lua_State *L) {
//    mongo *dbclient = userdata_to_dbclient(L, 1);
//    const char *ns = luaL_checkstring(L, 2);
//
//    int status =
//
//    try {
//        dbclient->dropIndexes(ns);
//    } catch (std::exception &e) {
//        lua_pushboolean(L, 0);
//        lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "drop_indexes", e.what());
//        return 2;
//    }
//
//    lua_pushboolean(L, 1);
//    return 1;
//}

/**
 * lua_table,err = db:find_one(ns, lua_table or json_str or query_obj, lua_table or json_str, options)
 */
static int dbclient_find_one(lua_State *L) {
	int n = lua_gettop(L);
	mongo *dbclient = userdata_to_dbclient(L, 1);
	const char *ns = luaL_checkstring(L, 2);
	bool is_query_ud = false;

	// get the query
	bson *query = NULL;
	if (!lua_isnoneornil(L, 3)) {
		int type = lua_type(L, 3);

		if (LUA_TSTRING == type) {
			query = (bson*)malloc(sizeof(bson));
			bson_init(query);
			fromjson_with_lua(L, 3, query);
		} else if (LUA_TTABLE == type) {
			query = (bson*)malloc(sizeof(bson));
			bson_init(query);
			fromtable_with_lua(L, 3, query);
		} else if (LUA_TUSERDATA == type) {
			void *uq = 0;
			uq = luaL_checkudata(L, 3, LUAMONGO_QUERY);
			Query tmp = *(*((Query **) uq));
			query = tmp.condition;
			is_query_ud = true;
		} else {
			// error hanppened
			lua_pushnil(L);
			lua_pushstring(L, "query must be jsonstr or table or query");
			return 2;
		}
	}

	// get the fields
	bson fieldsToReturn;
	bson_init(&fieldsToReturn);
	if (!lua_isnoneornil(L, 4)) {
		int type = lua_type(L, 4);

		if (type == LUA_TSTRING) {
			fromjson_with_lua(L, 4, &fieldsToReturn);
		} else if (type == LUA_TTABLE) {
			fromtable_with_lua(L, 4, &fieldsToReturn);
		} else {
			// error happened
			lua_pushnil(L);
			lua_pushstring(L, "fields must be jsonstr or query");
			return 2;
		}
	}

	int queryOptions = luaL_optint(L, 5, 0);
	bson result;
	int status = mongo_find_one(dbclient, ns, query, &fieldsToReturn, &result);

	int retval = 1;
	if (MONGO_OK == status) {
		bson_to_lua(L, &result);
	} else {
		lua_pushnil(L);
		lua_pushfstring(L, LUAMONGO_ERR_FIND_ONE_FAILED, "unknown error");
		retval = 2;
	}

	// release data
	bson_destroy(&fieldsToReturn);
	if (!is_query_ud) { bson_destroy(query); free(query); }

	return retval;
}

/*
 * ok,err = db:insert(ns, lua_table or json_str)
 */
static int dbclient_insert(lua_State *L) {
	mongo *dbclient = userdata_to_dbclient(L, 1);
	const char *ns = luaL_checkstring(L, 2);

	bson obj;
	bson_init(&obj);
	int type = lua_type(L, 3);

	if (type == LUA_TSTRING) {
		const char *jsonstr = luaL_checkstring(L, 3);
		fromjson(jsonstr, &obj);
	} else if (type == LUA_TTABLE) {
		fromtable_with_lua(L, 3, &obj);
	}

	int write_status = mongo_insert(dbclient, ns, &obj, NULL );
	char *msg = NULL;
	bool expection = false;

	if (MONGO_ERROR == write_status) {
		expection = true;
		switch (dbclient->err) {
		default:
			msg = "Unknown error occured when writing";
			break;
		}
	}

	if (expection) {
		lua_pushboolean(L, 0);
		lua_pushfstring(L, LUAMONGO_ERR_INSERT_FAILED, msg);
		return 2;
	}

	lua_pushboolean(L, 1);
	return 1;
}

/*
 * ok,err = db:insert_batch(ns, lua_array_of_tables)
 */
static int dbclient_insert_batch(lua_State *L) {
	mongo *dbclient = userdata_to_dbclient(L, 1);
	char *msg = NULL;
	const char *ns = luaL_checkstring(L, 2);
	luaL_checktype(L, 3, LUA_TTABLE);
	int flags = 0;

	size_t i = 0;
	size_t tlen = lua_rawlen(L, 3);        // get the lenth of the table
	// bson *vdata = (bson *)malloc(tlen * sizeof(bson));
	bson vdata[tlen];
	for (i = 1; i < tlen + 1; ++i) { // it seems like the first index of table is 1
		int vdata_index = i - 1;
		bson_init(&vdata[vdata_index]);
		lua_rawgeti(L, 3, i);
		fromtable_with_lua(L, 4, &vdata[vdata_index]);
		bson_print(&vdata[vdata_index]);
		lua_pop(L, 1);
	}
	bson *p_vdata = &vdata[0];
	int ret = mongo_insert_batch(dbclient, ns, (const bson **) &vdata, tlen,
			NULL, flags);
	int result = 0;
	if (MONGO_ERROR == ret) {
		lua_pushboolean(L, false);
		switch (dbclient->err) {
		default:
			msg = "Unknown error occured when insert_batch";
			break;
		}
		lua_pushfstring(L, LUAMONGO_ERR_INSERT_FAILED, msg, dbclient->err);
		result = 2;
	} else {
		lua_pushboolean(L, true);
		result = 1;
	}

	// release the data
	size_t j = 0;
	//for (j = 1; j < tlen; ++j) { bson_destroy(&vdata[j]); }
	// free(vdata);

	return result;
}

/*
 * cursor,err = db:query(ns, lua_table or json_str or query_obj, limit, skip, lua_table or json_str, options, batchsize)
 */
static int dbclient_query(lua_State *L) {
	int n = lua_gettop(L);
	mongo *dbclient = userdata_to_dbclient(L, 1);
	const char *ns = luaL_checkstring(L, 2);

	bool expection = false;
	char *err = NULL;
	Query *query = NULL;
	if (!lua_isnoneornil(L, 3)) {
		int type = lua_type(L, 3);
		if (type == LUA_TSTRING) {
			query = init_query();
			if (query->fromjson(query, L, 3)) {
				expection = true;
				err = "json string parse error";
			}
		} else if (type == LUA_TTABLE) {
			query = init_query();
			if (query->frombson(query, L, 3)) {
				expection = true;
				err = "bson table is wrong";
			}
		} else if (type == LUA_TUSERDATA) {
			void *uq = 0;
			uq = luaL_checkudata(L, 3, LUAMONGO_QUERY);
			query = (*((Query **) uq));
		} else {
			expection = true;
			err = LUAMONGO_REQUIRES_QUERY;
		}
	}

	if (expection) {
		lua_pushnil(L);
		lua_pushstring(L, err);
		if (query)
			query->release(query);
		return 2;
	}

	int nToReturn = luaL_optint(L, 4, 0);
	int nToSkip = luaL_optint(L, 5, 0);

	// const bson *fieldsToReturn = NULL;
	QueryCursor *fieldsToReturn = InitQueryCursor();

	expection = false;
	if (!lua_isnoneornil(L, 6)) {
		int type = lua_type(L, 6);

		if (type == LUA_TSTRING) {
			fieldsToReturn->fromjson(fieldsToReturn, L, 6); // new BSONObj(luaL_checkstring(L, 6));
		} else if (type == LUA_TTABLE) {
			fieldsToReturn->frombson(fieldsToReturn, L, 6);
		} else {
			expection = true;
			err = LUAMONGO_REQUIRES_CURSOR;
		}
	}

	if (expection) {
		lua_pushnil(L);
		lua_pushstring(L, err);
		if (fieldsToReturn)
			fieldsToReturn->release(fieldsToReturn);
		return 2;
	}

	int queryOptions = luaL_optint(L, 7, 0);
	int batchSize = 0; // luaL_optint(L, 8, 0);

	int ret = cursor_create(L, dbclient, ns, query, nToReturn, nToSkip,
			fieldsToReturn, queryOptions, batchSize);

	query->release(query);
	fieldsToReturn->release(fieldsToReturn);
	// return res == 0 ? 1 : res;
	return ret;
}


/*
 * ok,err = db:remove(ns, lua_table or json_str or query_obj)
 */
static int dbclient_remove(lua_State *L) {
    mongo *dbclient = userdata_to_dbclient(L, 1);
    const char *ns = luaL_checkstring(L, 2);

    int type = lua_type(L, 3);
	// bool justOne = lua_toboolean(L, 4);

	bson *query = NULL;
	if (type == LUA_TSTRING) {
		query = (bson *)malloc(sizeof(bson)); bson_init(query);
		fromjson_with_lua(L, 3, query);

	} else if (type == LUA_TTABLE) {
		query = (bson *)malloc(sizeof(bson)); bson_init(query);
		fromtable_with_lua(L, 3, query);
	} else if (type == LUA_TUSERDATA) {
		Query tmp;
		void *uq = 0;

		uq = luaL_checkudata(L, 3, LUAMONGO_QUERY);
		tmp = *(*((Query **) uq));
		query = tmp.condition;
	} else {
		lua_pushboolean(L, 0);
		lua_pushfstring(L, LUAMONGO_ERR_REMOVE_FAILED, "unknow error");
		return 2;
	}

	int status = mongo_remove(dbclient, ns, query, NULL);
	if (MONGO_ERROR == status) {
		lua_pushboolean(L, 0);
		lua_pushfstring(L, LUAMONGO_ERR_REMOVE_FAILED, "unknow error");
		return 2;
	}

    lua_pushboolean(L, 1);
    return 1;
}


/*
 * ok,err = db:update(ns, lua_table or json_str or query_obj, lua_table or json_str, upsert, multi)
 */
static int dbclient_update(lua_State *L) {
    mongo *dbclient = userdata_to_dbclient(L, 1);
    const char *ns = luaL_checkstring(L, 2);

    int type_query = lua_type(L, 3);
	int type_obj = lua_type(L, 4);

	bool upsert = lua_toboolean(L, 5);
	bool multi = lua_toboolean(L, 6);
	bool is_query_ud = false;

	bson *query;
	bson obj;

	if (type_query == LUA_TSTRING) {
		query = (bson *)malloc(sizeof(bson)); bson_init(query);
		fromjson_with_lua(L, 3, query);
	} else if (type_query == LUA_TTABLE) {
		query = (bson *)malloc(sizeof(bson)); bson_init(query);
		fromtable_with_lua(L, 3, query);
	} else if (type_query == LUA_TUSERDATA) {
		void *uq = 0;

		uq = luaL_checkudata(L, 3, LUAMONGO_QUERY);
		Query tmp = *(*((Query **) uq));
		query = tmp.condition;
		is_query_ud = true;
	} else {
		lua_pushboolean(L, 0);
		lua_pushfstring(L, LUAMONGO_ERR_UPDATE_FAILED, "unknown reason");
		return 2;
	}

	bson_init(&obj);
	if (type_obj == LUA_TSTRING) {
		fromjson_with_lua(L, 4, &obj);
	} else if (type_obj == LUA_TTABLE) {
		fromtable_with_lua(L, 4, &obj);
	} else {

	}

	int flag = 0;
	if (upsert) { flag = flag | MONGO_UPDATE_UPSERT; }
	if (multi) { flag = flag | MONGO_UPDATE_MULTI; }

	int status = mongo_update(dbclient, ns, query, &obj, flag, NULL);
	if (MONGO_ERROR== status) {
		lua_pushboolean(L, 0);
		lua_pushfstring(L, LUAMONGO_ERR_UPDATE_FAILED, "unknown reason");
		return 2;
	}

	if(!is_query_ud) { bson_destroy(query); free(query); }

    lua_pushboolean(L, true);
    return 1;
}

const luaL_Reg dbclient_methods[] = {
		{ "auth", dbclient_auth },
		{ "count", dbclient_count },
		{"drop_collection", dbclient_drop_collection},
//    {"drop_index_by_fields", dbclient_drop_index_by_fields},
//    {"drop_index_by_name", dbclient_drop_index_by_name},
//		{"drop_indexes", dbclient_drop_indexes},
		{"ensure_index", dbclient_ensure_index},
//    {"eval", dbclient_eval},
//    {"exists", dbclient_exists},
		{ "find_one", dbclient_find_one },
//    {"gen_index_name", dbclient_gen_index_name},
//    {"get_indexes", dbclient_get_indexes},
//    {"get_last_error", dbclient_get_last_error},
//    {"get_last_error_detailed", dbclient_get_last_error_detailed},
//    {"get_server_address", dbclient_get_server_address},
		{ "insert", dbclient_insert },
		{ "insert_batch", dbclient_insert_batch },
//    {"is_failed", dbclient_is_failed},
//    {"mapreduce", dbclient_mapreduce},
		{ "query", dbclient_query },
//    {"reindex", dbclient_reindex},
		{"remove", dbclient_remove},
//    {"reset_index_cache", dbclient_reset_index_cache},
//    {"run_command", dbclient_run_command},
		{"update", dbclient_update},
		{ NULL, NULL } };

