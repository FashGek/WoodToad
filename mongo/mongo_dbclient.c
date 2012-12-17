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

}MongoDbClient;

mongo*
userdata_to_dbclient(lua_State *L, int stackpos)
{
    // adapted from http://www.lua.org/source/5.1/lauxlib.c.html#luaL_checkudata
    void *ud = lua_touserdata(L, stackpos);
    if (ud == NULL)
         luaL_typerror(L, stackpos, "userdata");

    // try Connection
    lua_getfield(L, LUA_REGISTRYINDEX, LUAMONGO_CONNECTION);
    if (lua_getmetatable(L, stackpos))
    {
        if (lua_rawequal(L, -1, -2))
        {
            mongo *connection = *((mongo **)ud);
            lua_pop(L, 2);
            return connection;
        }
        lua_pop(L, 2);
    }
    else
        lua_pop(L, 1);

    // try ReplicaSet
    lua_getfield(L, LUA_REGISTRYINDEX, LUAMONGO_REPLICASET);  // get correct metatable
    if (lua_getmetatable(L, stackpos)) {
        if (lua_rawequal(L, -1, -2))
        {
            mongo *replicaset = *((mongo **)ud);
            lua_pop(L, 2); // remove both metatables
            return replicaset;
        }
        lua_pop(L, 2);
    }
    else
        lua_pop(L, 1);

    // luaL_typerror(L, stackpos, LUAMONGO_DBCLIENT);
    return NULL; // should never get here
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
static int
dbclient_auth(lua_State *L) {

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
        lua_pushfstring(L, LUAMONGO_ERR_CONNECTION_FAILED, "couldnot autenticate");
        return 2;
    }
    lua_pushboolean(L, 1);
    return 1;
}

/*
 * count,err = db:count(ns, lua_table or json_str)
 */
//static int
//dbclient_count(lua_State *L) {
//    mongo *dbclient = userdata_to_dbclient(L, 1);
//    const char *ns = luaL_checkstring(L, 2);
//
//    int count = 0;
//
////    try {
////        BSONObj query;
////        int type = lua_type(L, 3);
////        if (type == LUA_TSTRING) {
////            const char *jsonstr = luaL_checkstring(L, 3);
////            query = fromjson(jsonstr);
////        } else if (type == LUA_TTABLE) {
////            lua_to_bson(L, 3, query);
////        }
////        count = dbclient->count(ns, query);
////    } catch (std::exception &e) {
////        lua_pushnil(L);
////        lua_pushfstring(L, LUAMONGO_ERR_COUNT_FAILED, e.what());
////        return 2;
////    }
//
//    lua_pushinteger(L, count);
//    return 1;
//}

/*
 * ok,err = db:insert(ns, lua_table or json_str)
 */
static int
dbclient_insert(lua_State *L) {
    mongo *dbclient = userdata_to_dbclient(L, 1);
    const char *ns = luaL_checkstring(L, 2);

    bson obj;
    bson_init(&obj);
    int type = lua_type(L, 3);

    if (type == LUA_TSTRING) {
		const char *jsonstr = luaL_checkstring(L, 3);

		fromjson(jsonstr, &obj);
    } else if (type == LUA_TTABLE) {
//		BSONObj data;
//		lua_to_bson(L, 3, data);
//
//		dbclient->insert(ns, data);
    }

    int write_status = mongo_insert( dbclient, ns, &obj, NULL );
    char msg; bool expection = false;
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
static int
dbclient_insert_batch(lua_State *L) {
    mongo *dbclient = userdata_to_dbclient(L, 1);
    const char *ns = luaL_checkstring(L, 2);
    luaL_checktype(L, 3, LUA_TTABLE);
//
//    try {
//        std::vector<BSONObj> vdata;
//        size_t tlen = lua_rawlen(L, 3) + 1;
//        for (size_t i = 1; i < tlen; ++i) {
//            vdata.push_back(BSONObj());
//            lua_rawgeti(L, 3, i);
//            lua_to_bson(L, 4, vdata.back());
//            lua_pop(L, 1);
//        }
//        dbclient->insert(ns, vdata);
//    } catch (std::exception &e) {
//        lua_pushboolean(L, 0);
//        lua_pushfstring(L, LUAMONGO_ERR_INSERT_FAILED, e.what());
//        return 2;
//    } catch (const char *err) {
//        lua_pushboolean(L, 0);
//        lua_pushstring(L, err);
//        return 2;
//    }
//
//    lua_pushboolean(L, 1);
    return 1;
}


/*
 * cursor,err = db:query(ns, lua_table or json_str or query_obj, limit, skip, lua_table or json_str, options, batchsize)
 */
static int
dbclient_query(lua_State *L) {
    int n = lua_gettop(L);
    mongo *dbclient = userdata_to_dbclient(L, 1);
    const char *ns = luaL_checkstring(L, 2);

    bool expection = false; char *err;
    Query *query = NULL;
    if (!lua_isnoneornil(L, 3))  {
    	int type = lua_type(L, 3);
    	if (type == LUA_TSTRING) {
    		query = init_query();
    		if (query->fromjson(query, L, 3)) {
    			expection = true;
    			err = "json string parse error";
    		}
    		// fromjson(lua_checkstring(L, 3), &query->condition);
    	} else if (type == LUA_TTABLE) {
    		query = init_query();
    		if (query->frombson(query, L, 3)) {
    			expection = true;
    			err = "bson table is wrong";
    		}
    		// lua_to_bson(L, 3, &query->condition);
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
		if (query) query->release(query);
		return 2;
    }

    int nToReturn = luaL_optint(L, 4, 0);
    int nToSkip = luaL_optint(L, 5, 0);

    // const bson *fieldsToReturn = NULL;
    QueryCursor *fieldsToReturn = InitQueryCursor();
    if (!lua_isnoneornil(L, 6)) {
        int type = lua_type(L, 6);

        if (type == LUA_TSTRING) {
            fieldsToReturn->fromjson(fieldsToReturn, luaL_checkstring(L, 6)); // new BSONObj(luaL_checkstring(L, 6));
        } else if (type == LUA_TTABLE) {
//            BSONObj obj;
//            lua_to_bson(L, 6, obj);
//            fieldsToReturn = new BSONObj(obj);
        } else {
//            throw(LUAMONGO_REQUIRES_JSON_OR_TABLE);s
        }
    }

    int queryOptions = luaL_optint(L, 7, 0);
    int batchSize = 0; // luaL_optint(L, 8, 0);

    int res = cursor_create(L, dbclient, ns, query, nToReturn, nToSkip, fieldsToReturn, queryOptions, batchSize);

    query->release(query);
    fieldsToReturn->release(fieldsToReturn);
    return res == 0 ? 1 : res;
}

const luaL_Reg dbclient_methods[] = {
    {"auth", dbclient_auth},
//    {"count", dbclient_count},
//    {"drop_collection", dbclient_drop_collection},
//    {"drop_index_by_fields", dbclient_drop_index_by_fields},
//    {"drop_index_by_name", dbclient_drop_index_by_name},
//    {"drop_indexes", dbclient_drop_indexes},
//    {"ensure_index", dbclient_ensure_index},
//    {"eval", dbclient_eval},
//    {"exists", dbclient_exists},
//    {"find_one", dbclient_find_one},
//    {"gen_index_name", dbclient_gen_index_name},
//    {"get_indexes", dbclient_get_indexes},
//    {"get_last_error", dbclient_get_last_error},
//    {"get_last_error_detailed", dbclient_get_last_error_detailed},
//    {"get_server_address", dbclient_get_server_address},
    {"insert", dbclient_insert},
    {"insert_batch", dbclient_insert_batch},
//    {"is_failed", dbclient_is_failed},
//    {"mapreduce", dbclient_mapreduce},
    {"query", dbclient_query},
//    {"reindex", dbclient_reindex},
//    {"remove", dbclient_remove},
//    {"reset_index_cache", dbclient_reset_index_cache},
//    {"run_command", dbclient_run_command},
//    {"update", dbclient_update},
    {NULL, NULL}
};

