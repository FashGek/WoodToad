#include "mongo_query.h"

#include "common.h"
#include "utils.h"
#include "mongo_query.h"
#include "driver/mongo.h"
#include "cjson/json.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

// it is just a set of function which operate bson object
//typedef struct BSONObjBuilder {
//	bson *obj;
//	bool (*appendOID)(struct BSONObjBuilder *self);
//	bool (*appendBool)(struct BSONObjBuilder *self);
//	bool (*appendInt)(struct BSONObjBuilder *self, int number);
//	bool (*appendArray)(struct BSONObjBuilder *self);
//	bool (*appendBson)(struct BSONObjBuilder *self);
//	bool (*appendDate)(struct BSONObjBuilder *self);
//	bool (*appendNull)(struct BSONObjBuilder *self);
//	bool (*appendTimestamp)(struct BSONObjBuilder *self);
//	bool (*appendBinData)(struct BSONObjBuilder *self);
//	bool (*appendFloat)(struct BSONObjBuilder *self);
//	bool (*appendString)(struct BSONObjBuilder *self);
//	bool (*appendLong)(struct BSONObjBuilder *self);
//	bool (*appendRegex)(struct BSONObjBuilder *self);
//
//	void (*release)(struct BSONObjBuilder *self); // should not release the pointer of obj
//	void (*forceRelease)(struct BSONObjBuilder *self);
//}BSONObjBuilder;
//
//BSONObjBuilder*
//create_bsonObjBuilder(bson *obj) {
//	BSONObjBuilder *builder = (BSONObjBuilder *)malloc(sizeof(BSONObjBuilder));
//	builder->obj = obj;
//	return builder;
//}

//static BSONObjBuilder


/***********************begin***************************/

/**
 * json util json string to bson
 */

//static void json_to_bson_append_element( bson *bb , const char *k , struct json_object *v );
//
//static void
//json_to_bson_append( bson *bb , struct json_object *o ) {
//	char *key; struct json_object *val;
//	struct lh_entry *entry = NULL;
//	for (entry = json_object_get_object(o)->head; entry; entry = entry->next) {
//		if (entry) {
//			key = (char*) entry->k;
//			val = (struct json_object*) entry->v;
//			json_to_bson_append_element( bb , key , val );
//		}
//	}

//		json_object_object_foreach( o,k,v ) {
//        json_to_bson_append_element( bb , k , v );
//    }
//}

/**
   should already have called start_array
   this will not call start/finish
 */

//static void
//json_to_bson_append_array( bson *bb , struct json_object *a ) {
//    int i;
//    char buf[10];
//    for ( i=0; i<json_object_array_length( a ); i++ ) {
//        sprintf( buf , "%d" , i );
//        json_to_bson_append_element( bb , buf , json_object_array_get_idx( a , i ) );
//    }
//}

//static void
//json_to_bson_append_element( bson *bb , const char *k , struct json_object *v ) {
//    if ( ! v ) {
//        bson_append_null( bb , k );
//        return;
//    }
//
//    switch ( json_object_get_type( v ) ) {
//    case json_type_int:
//        bson_append_int( bb , k , json_object_get_int( v ) );
//        break;
//    case json_type_boolean:
//        bson_append_bool( bb , k , json_object_get_boolean( v ) );
//        break;
//    case json_type_double:
//        bson_append_double( bb , k , json_object_get_double( v ) );
//        break;
//    case json_type_string:
//        bson_append_string( bb , k , json_object_get_string( v ) );
//        break;
//    case json_type_object:
//        bson_append_start_object( bb , k );
//        json_to_bson_append( bb , v );
//        bson_append_finish_object( bb );
//        break;
//    case json_type_array:
//        bson_append_start_array( bb , k );
//        json_to_bson_append_array( bb , v );
//        bson_append_finish_object( bb );
//        break;
//    default:
//         fprintf( stderr , "can't handle type for : %s\n" , json_object_to_json_string( v ) );
//         break;
//    }
//
//}




/*
 * end
 */

//static void
//lua_append_bson(lua_State *L, const char *key, int stackpos, bson *builder, int ref) {
//    int type = lua_type(L, stackpos);
//
//    if (type == LUA_TTABLE) {
//        if (stackpos < 0) stackpos = lua_gettop(L) + stackpos + 1;
//        lua_checkstack(L, 3);
//
//        int bsontype_found = luaL_getmetafield(L, stackpos, "__bsontype");
//        if (!bsontype_found) {
//            // not a special bsontype
//            // handle as a regular table, iterating keys
//
//            lua_rawgeti(L, LUA_REGISTRYINDEX, ref);
//            lua_pushvalue(L, stackpos);
//            lua_rawget(L, -2);
//            if (lua_toboolean(L, -1)) { // do nothing if the same table encountered
//                lua_pop(L, 2);
//            } else {
//                lua_pop(L, 1);
//                lua_pushvalue(L, stackpos);
//                lua_pushboolean(L, 1);
//                lua_rawset(L, -3);
//                lua_pop(L, 1);
//
//                bson subObj;
//                bson_init(&subObj);
//                BSONObjBuilder *b = create_bsonObjBuilder(&subObj);
//
//                bool dense = true;
//                int len = 0;
//                for (lua_pushnil(L); lua_next(L, stackpos); lua_pop(L, 1)) {
//                    ++len;
//                    if ((lua_type(L, -2) != LUA_TNUMBER) || (lua_tointeger(L, -2) != len)) {
//                        lua_pop(L, 2);
//                        dense = false;
//                        break;
//                    }
//                }
//
//                char ss[256] = {0};
//                if (dense) {
//                    for (int i = 0; i < len; i++) {
//                        lua_rawgeti(L, stackpos, i+1);
////                        stringstream ss;
////                        ss << i;
//                        sprintf(ss, "%d", i);
//                        lua_append_bson(L, ss, -1, b, ref);
//                        lua_pop(L, 1);
//                        memset(ss, 0, sizeof(ss));
//                    }
//
//                    builder->appendArray(key, b->obj);
//                } else {
//                    for (lua_pushnil(L); lua_next(L, stackpos); lua_pop(L, 1)) {
//                        switch (lua_type(L, -2)) { // key type
//                            case LUA_TNUMBER: {
////                                stringstream ss;
////                                ss << lua_tonumber(L, -2);
//                            	sprintf(ss, "%d", lua_tonumber(L, -2));
//                                lua_append_bson(L, ss, -1, b, ref);
//                                memset(ss, 0, sizeof(ss));
//                                break;
//                            }
//                            case LUA_TSTRING: {
//                                lua_append_bson(L, lua_tostring(L, -2), -1, b, ref);
//                                break;
//                            }
//                        }
//                    }
//
//                    builder->appendBson(key, b->obj);
//                }
//            }
//        } else {
//            int bson_type = lua_tointeger(L, -1);
//            lua_pop(L, 1);
//            lua_rawgeti(L, -1, 1);
//            switch (bson_type) {
//            case BSON_DATE:
//                builder->appendDate(key, lua_tonumber(L, -1));
//                break;
//            case BSON_TIMESTAMP:
//                builder->appendTimestamp(key);
//                break;
//            case BSON_REGEX: {
//                const char* regex = lua_tostring(L, -1);
//                lua_rawgeti(L, -2, 2); // options
//                const char* options = lua_tostring(L, -1);
//                lua_pop(L, 1);
//                if (regex && options) builder->appendRegex(key, regex, options);
//                break;
//            }
//            case BSON_INT:
//                builder->appendInt(key, static_cast<int32_t>(lua_tointeger(L, -1)));
//                break;
//            case BSON_LONG:
//                builder->appendLong(key, static_cast<long long int>(lua_tonumber(L, -1)));
//                break;
//            case BSON_SYMBOL: {
//                const char* c = lua_tostring(L, -1);
//                if (c) builder->appendSymbol(key, c);
//                break;
//            }
//            case BSON_BINDATA: {
//                size_t l;
//                const char* c = lua_tolstring(L, -1, &l);
//                if (c) builder->appendBinData(key, l, mongo::BinDataGeneral, c);
//                break;
//            }
//            case BSON_OID: {
//                OID oid;
//                const char* c = lua_tostring(L, -1);
//                if (c) {
//                    oid.init(c);
//                    builder->appendOID(key, &oid);
//                }
//                break;
//            }
//            case BSON_NULL:
//                builder->appendNull(key);
//                break;
//
//            /*default:
//                luaL_error(L, LUAMONGO_UNSUPPORTED_BSON_TYPE, luaL_typename(L, stackpos));*/
//            }
//            lua_pop(L, 1);
//        }
//    } else if (type == LUA_TNIL) {
//        builder->appendNull(builder, key);
//    } else if (type == LUA_TNUMBER) {
//        double numval = lua_tonumber(L, stackpos);
//        if (numval == floor(numval)) {
//            // The numeric value looks like an integer, treat it as such.
//            // This is closer to how JSON datatypes behave.
//            int intval = lua_tointeger(L, stackpos);
//            builder->appendInt(key, static_cast<int32_t>(intval));
//        } else {
//            builder->appendFloat(key, numval);
//        }
//    } else if (type == LUA_TBOOLEAN) {
//        builder->appendBool(key, lua_toboolean(L, stackpos));
//    } else if (type == LUA_TSTRING) {
//        builder->appendString(key, lua_tostring(L, stackpos));
//    } else {
//        luaL_error(L, LUAMONGO_UNSUPPORTED_LUA_TYPE, luaL_typename(L, stackpos));
//    }
//}

/**
 * a table style bson object to bson
 */
// stackpos must be relative to the bottom, i.e., not negative
//int
//lua_to_bson(lua_State *L, int stackpos, bson *obj) {
//    // BSONObjBuilder builder;
//    bson builder;
//    bson_init(&builder);
//
//    lua_newtable(L);
//    int ref = luaL_ref(L, LUA_REGISTRYINDEX);
//    char buffer[256];
//
//    for (lua_pushnil(L); lua_next(L, stackpos); lua_pop(L, 1)) {
//        switch (lua_type(L, -2)) { // key type
//            case LUA_TNUMBER: {
//                //ostringstream ss;
//                //ss << lua_tonumber(L, -2);
//            	  sprintf(buffer, "%d", lua_tonumber(L, -2));
//                lua_append_bson(L, buffer, -1, &builder, ref);
//                break;
//            }
//            case LUA_TSTRING: {
//                lua_append_bson(L, lua_tostring(L, -2), -1, &builder, ref);
//                break;
//            }
//        }
//    }
//    luaL_unref(L, LUA_REGISTRYINDEX, ref);
//
//    obj = builder.obj();
//}
/**
 * end
 */

static int
_fromjson(Query* self, lua_State *L, int stack_pos) {
	return fromjson_with_lua(L, stack_pos, self->condition);
}

static int
_frombson(Query* self, lua_State *L, int stack_pos) {
	return fromtable_with_lua(L, stack_pos, self->condition);
}

static void
_release(struct Query* self) {
	bson_destroy(self->condition);
	free(self);
	self = NULL;
}

Query*
init_query() {
	Query *query = (Query*)malloc(sizeof(Query));
	query->condition = (bson*)malloc(sizeof(bson));
	bson_init(query->condition);

	query->fromjson = _fromjson;
	query->frombson = _frombson;
	query->release = _release;
	return query;
}


/***********************end****************************/


inline Query*
userdata_to_query(lua_State* L, int index) {
    void *ud = 0;
    ud = luaL_checkudata(L, index, LUAMONGO_QUERY);
    Query *query = *((Query **)ud);
    return query;
}

/*
 * query,err = mongo.Query.New(lua_table or json_str)
 */
static int
query_new(lua_State *L) {
    int n = lua_gettop(L);
    bool expection = false;
    char *msg;

    Query **query = (Query **) lua_newuserdata(L, sizeof(Query *));
	*query = init_query();
	if (n >= 1) {
		int type = lua_type(L, 1);
		if (type == LUA_TSTRING) {
//			const char *jsonstr = luaL_checkstring(L, 1);
//			*query = new Query(fromjson( jsonstr));
			if ((*query)->fromjson(*query, L, 1)) {
				expection = true;
				msg = "json string parse error";
			}
		} else if (type == LUA_TTABLE) {
//                    BSONObj data;
//                    lua_to_bson(L, 1, data);
//                    *query = new Query(data);
		} else {
//                    throw(LUAMONGO_REQUIRES_JSON_OR_TABLE);
			expection = true;
			msg = "not supported query type";
		}
	} else {
		// (*query)->fromjson("{}");
	}

	if (expection) {
		lua_pushnil(L);
		lua_pushfstring(L, LUAMONGO_ERR_QUERY_FAILED, msg);
		return 2;
	}
	luaL_getmetatable(L, LUAMONGO_QUERY);
	lua_setmetatable(L, -2);

	return 1;
}

/*
 * __gc
 */
static int
query_gc(lua_State *L) {
    Query *query = userdata_to_query(L, 1);
    query->release(query);
    return 0;
}

/*
 * __tostring
 */
static int
query_tostring(lua_State *L) {
    void *ud = 0;

    ud = luaL_checkudata(L, 1, LUAMONGO_QUERY);
    Query *query = *((Query **)ud);

    lua_pushstring(L, "Not Supported Yet");

    return 1;
}

int
mongo_query_register(lua_State *L) {
    static const luaL_Reg query_methods[] = {
//        {"explain", query_explain},
//        {"hint", query_hint},
//        {"is_explain", query_is_explain},
//        {"max_key", query_max_key},
//        {"min_key", query_min_key},
//        {"snapshot", query_snapshot},
//        {"sort", query_sort},
//        {"where", query_where},
        {NULL, NULL}
    };

    static const luaL_Reg query_class_methods[] = {
        {"New", query_new},
        {NULL, NULL}
    };

    luaL_newmetatable(L, LUAMONGO_QUERY);
    luaL_setfuncs(L, query_methods, 0);

    lua_pushvalue(L,-1);
    lua_setfield(L, -2, "__index");

    lua_pushcfunction(L, query_gc);
    lua_setfield(L, -2, "__gc");

    lua_pushcfunction(L, query_tostring);
    lua_setfield(L, -2, "__tostring");

    lua_remove(L, -1);
    lua_newtable(L); // Query
    luaL_setfuncs(L, query_class_methods, 0);
    lua_setfield(L, -2, "Query");
//
//    lua_pushstring(L, "Options");
//    lua_newtable(L);
//
//    lua_pushstring(L, "CursorTailable");
//    lua_pushinteger(L, QueryOption_CursorTailable);
//    lua_rawset(L, -3);
//
//    lua_pushstring(L, "SlaveOk");
//    lua_pushinteger(L, QueryOption_SlaveOk);
//    lua_rawset(L, -3);
//
//    lua_pushstring(L, "OplogReplay");
//    lua_pushinteger(L, QueryOption_OplogReplay);
//    lua_rawset(L, -3);
//
//    lua_pushstring(L, "NoCursorTimeout");
//    lua_pushinteger(L, QueryOption_NoCursorTimeout);
//    lua_rawset(L, -3);
//
//    lua_pushstring(L, "AwaitData");
//    lua_pushinteger(L, QueryOption_AwaitData);
//    lua_rawset(L, -3);
//
//    lua_pushstring(L, "Exhaust");
//    lua_pushinteger(L, QueryOption_Exhaust);
//    lua_rawset(L, -3);
//
//    lua_pushstring(L, "AllSupported");
//    lua_pushinteger(L, QueryOption_AllSupported);
//    lua_rawset(L, -3);
//
//    lua_rawset(L, -3);

    return 1;
}
