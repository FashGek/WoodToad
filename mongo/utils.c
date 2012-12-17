
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "luacompat52.h"


#include "utils.h"
#include "common.h"

#include "cjson/json.h"


extern void push_bsontype_table(lua_State* L, bson_type bsontype);
void lua_push_value(lua_State *L, bson_iterator *elem);
const char *bson_name(int type);


static void
bson_to_array(lua_State *L, const char* array_data) {
	bson_iterator it;

	bson_iterator_from_buffer( &it, array_data );

	lua_newtable(L);
	int n = 1;
	while ( bson_iterator_next( &it ) ) {
		lua_push_value(L, &it);
		lua_rawseti(L, -2, n++);
	}
}

static void
bson_to_table(lua_State *L, const bson *obj) {
	bson_iterator it;
	bson_iterator_init( &it, obj);

    lua_newtable(L);

    while ( bson_iterator_next(&it) ) {
    	const char *key = bson_iterator_key(&it);

    	lua_pushstring(L, key);
    	lua_push_value(L, &it);
    	lua_rawset(L, -3);
    }

}

void
lua_push_value(lua_State *L, bson_iterator *elem) {
    lua_checkstack(L, 2);
    bson_type type = bson_iterator_type(elem);

	 bson sub; char oidhex[25];

    switch(type) {
    case BSON_UNDEFINED:
        lua_pushnil(L);
        break;
    case BSON_INT:
        lua_pushinteger(L, bson_iterator_int(elem));
        break;
    case BSON_LONG:
    case BSON_DOUBLE:
        lua_pushnumber(L, bson_iterator_double(elem));
        break;
    case BSON_BOOL:
        lua_pushboolean(L, bson_iterator_bool(elem));
        break;
    case BSON_STRING:
        lua_pushstring(L, bson_iterator_string(elem));
        break;
    case BSON_ARRAY:
        bson_to_array(L, bson_iterator_value(elem));
        break;
    case BSON_OBJECT:
    	 bson_iterator_subobject(elem, &sub);
        bson_to_table(L, &sub);
        break;
    case BSON_DATE:
        push_bsontype_table(L, BSON_DATE);
        lua_pushnumber(L, bson_iterator_date(elem));
        lua_rawseti(L, -2, 1);
        break;
    case BSON_TIMESTAMP:
        push_bsontype_table(L, BSON_TIMESTAMP);
        lua_pushnumber(L, bson_iterator_timestamp_time(elem));
        lua_rawseti(L, -2, 1);
        break;
    case BSON_SYMBOL:
//        push_bsontype_table(L, BSON_SYMBOL);
//        // lua_pushstring(L, );
//        lua_rawseti(L, -2, 1);
        break;
    case BSON_BINDATA: {
        push_bsontype_table(L, BSON_BINDATA);
        int l = bson_iterator_bin_len(elem);
        const char* c = bson_iterator_bin_data(elem);
        lua_pushlstring(L, c, l);
        lua_rawseti(L, -2, 1);
        break;
    }
    case BSON_REGEX:
        push_bsontype_table(L, BSON_REGEX);
        lua_pushstring(L, bson_iterator_regex(elem));
        lua_rawseti(L, -2, 1);
        lua_pushstring(L, bson_iterator_regex_opts(elem));
        lua_rawseti(L, -2, 2);
        break;
    case BSON_OID:
        push_bsontype_table(L, BSON_OID);
        bson_oid_to_string(bson_iterator_oid(elem), oidhex);
        lua_pushstring(L, &oidhex[0]);
        lua_rawseti(L, -2, 1);
        break;
    case BSON_NULL:
        push_bsontype_table(L, BSON_NULL);
        break;
    case BSON_EOO:
        break;
    default:
    	break;
        // luaL_error(L, LUAMONGO_UNSUPPORTED_BSON_TYPE, bson_name(type));
    }
}

//static void
//lua_append_bson(lua_State *L, const char *key, int stackpos, BSONObjBuilder *builder, int ref) {
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
//                BSONObjBuilder b;
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
//                if (dense) {
//                    for (int i = 0; i < len; i++) {
//                        lua_rawgeti(L, stackpos, i+1);
//                        stringstream ss;
//                        ss << i;
//
//                        lua_append_bson(L, ss.str().c_str(), -1, &b, ref);
//                        lua_pop(L, 1);
//                    }
//
//                    builder->appendArray(key, b.obj());
//                } else {
//                    for (lua_pushnil(L); lua_next(L, stackpos); lua_pop(L, 1)) {
//                        switch (lua_type(L, -2)) { // key type
//                            case LUA_TNUMBER: {
//                                stringstream ss;
//                                ss << lua_tonumber(L, -2);
//                                lua_append_bson(L, ss.str().c_str(), -1, &b, ref);
//                                break;
//                            }
//                            case LUA_TSTRING: {
//                                lua_append_bson(L, lua_tostring(L, -2), -1, &b, ref);
//                                break;
//                            }
//                        }
//                    }
//
//                    builder->append(key, b.obj());
//                }
//            }
//        } else {
//            int bson_type = lua_tointeger(L, -1);
//            lua_pop(L, 1);
//            lua_rawgeti(L, -1, 1);
//            switch (bson_type) {
//            case mongo::Date:
//                builder->appendDate(key, lua_tonumber(L, -1));
//                break;
//            case mongo::Timestamp:
//                builder->appendTimestamp(key);
//                break;
//            case mongo::RegEx: {
//                const char* regex = lua_tostring(L, -1);
//                lua_rawgeti(L, -2, 2); // options
//                const char* options = lua_tostring(L, -1);
//                lua_pop(L, 1);
//                if (regex && options) builder->appendRegex(key, regex, options);
//                break;
//            }
//            case mongo::NumberInt:
//                builder->append(key, static_cast<int32_t>(lua_tointeger(L, -1)));
//                break;
//            case mongo::NumberLong:
//                builder->append(key, static_cast<long long int>(lua_tonumber(L, -1)));
//                break;
//            case mongo::Symbol: {
//                const char* c = lua_tostring(L, -1);
//                if (c) builder->appendSymbol(key, c);
//                break;
//            }
//            case mongo::BinData: {
//                size_t l;
//                const char* c = lua_tolstring(L, -1, &l);
//                if (c) builder->appendBinData(key, l, mongo::BinDataGeneral, c);
//                break;
//            }
//            case mongo::jstOID: {
//                OID oid;
//                const char* c = lua_tostring(L, -1);
//                if (c) {
//                    oid.init(c);
//                    builder->appendOID(key, &oid);
//                }
//                break;
//            }
//            case mongo::jstNULL:
//                builder->appendNull(key);
//                break;
//
//            /*default:
//                luaL_error(L, LUAMONGO_UNSUPPORTED_BSON_TYPE, luaL_typename(L, stackpos));*/
//            }
//            lua_pop(L, 1);
//        }
//    } else if (type == LUA_TNIL) {
//        builder->appendNull(key);
//    } else if (type == LUA_TNUMBER) {
//        double numval = lua_tonumber(L, stackpos);
//        if (numval == floor(numval)) {
//            // The numeric value looks like an integer, treat it as such.
//            // This is closer to how JSON datatypes behave.
//            int intval = lua_tointeger(L, stackpos);
//            builder->append(key, static_cast<int32_t>(intval));
//        } else {
//            builder->append(key, numval);
//        }
//    } else if (type == LUA_TBOOLEAN) {
//        builder->appendBool(key, lua_toboolean(L, stackpos));
//    } else if (type == LUA_TSTRING) {
//        builder->append(key, lua_tostring(L, stackpos));
//    }/* else {
//        luaL_error(L, LUAMONGO_UNSUPPORTED_LUA_TYPE, luaL_typename(L, stackpos));
//    }*/
//}

void
bson_to_lua(lua_State *L, const bson *obj) {
	bson_to_table(L, obj);
//    if (obj.isEmpty()) {
//        lua_pushnil(L);
//    } else {
//        bson_to_table(L, obj);
//    }
}

// stackpos must be relative to the bottom, i.e., not negative
//void
//lua_to_bson(lua_State *L, int stackpos, bson *obj) {
//    BSONObjBuilder builder;
//
//    lua_newtable(L);
//    int ref = luaL_ref(L, LUA_REGISTRYINDEX);
//
//    for (lua_pushnil(L); lua_next(L, stackpos); lua_pop(L, 1)) {
//        switch (lua_type(L, -2)) { // key type
//            case LUA_TNUMBER: {
//                ostringstream ss;
//                ss << lua_tonumber(L, -2);
//                lua_append_bson(L, ss.str().c_str(), -1, &builder, ref);
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

const char *bson_name(int type) {
    const char *name;

    switch(type) {
        case BSON_EOO:
            name = "EndOfObject";
            break;
        case BSON_DOUBLE:
            name = "NumberDouble";
            break;
        case BSON_STRING:
            name = "String";
            break;
        case BSON_OBJECT:
            name = "Object";
            break;
        case BSON_ARRAY:
            name = "Array";
            break;
        case BSON_BINDATA:
            name = "BinData";
            break;
        case BSON_UNDEFINED:
            name = "Undefined";
            break;
        case BSON_OID:
            name = "ObjectID";
            break;
        case BSON_BOOL:
            name = "Bool";
            break;
        case BSON_DATE:
            name = "Date";
            break;
        case BSON_NULL:
            name = "NULL";
            break;
        case BSON_REGEX:
            name = "RegEx";
            break;
        case BSON_DBREF:
            name = "DBRef";
            break;
        case BSON_CODE:
            name = "Code";
            break;
        case BSON_SYMBOL:
            name = "Symbol";
            break;
        case BSON_CODEWSCOPE:
            name = "CodeWScope";
            break;
        case BSON_INT:
            name = "NumberInt";
            break;
        case BSON_TIMESTAMP:
            name = "Timestamp";
            break;
        case BSON_LONG:
            name = "NumberLong";
            break;
        default:
            name = "UnknownType";
            break;
    }

    return name;
}

int
luaL_typerror(lua_State *L, int narg, const char *tname){
	const char *msg = lua_pushfstring( L, "%s expected, got %s", tname, luaL_typename(L, narg) );
	return luaL_argerror(L, narg, msg);
}

static void json_to_bson_append_element( bson *bb , const char *k , struct json_object *v );
static void json_to_bson_append( bson *bb , struct json_object *o );

static void
json_to_bson_append_array( bson *bb , struct json_object *a ) {
    int i;
    char buf[10];
    for ( i=0; i<json_object_array_length( a ); i++ ) {
        sprintf( buf , "%d" , i );
        json_to_bson_append_element( bb , buf , json_object_array_get_idx( a , i ) );
    }
}

static void
json_to_bson_append_element( bson *bb , const char *k , struct json_object *v ) {
    if ( ! v ) {
        bson_append_null( bb , k );
        return;
    }

    switch ( json_object_get_type( v ) ) {
    case json_type_int:
        bson_append_int( bb , k , json_object_get_int( v ) );
        break;
    case json_type_boolean:
        bson_append_bool( bb , k , json_object_get_boolean( v ) );
        break;
    case json_type_double:
        bson_append_double( bb , k , json_object_get_double( v ) );
        break;
    case json_type_string:
        bson_append_string( bb , k , json_object_get_string( v ) );
        break;
    case json_type_object:
        bson_append_start_object( bb , k );
        json_to_bson_append( bb , v );
        bson_append_finish_object( bb );
        break;
    case json_type_array:
        bson_append_start_array( bb , k );
        json_to_bson_append_array( bb , v );
        bson_append_finish_object( bb );
        break;
    default:
         fprintf( stderr , "can't handle type for : %s\n" , json_object_to_json_string( v ) );
         break;
    }
}

static void
json_to_bson_append( bson *bb , struct json_object *o ) {
	char *key; struct json_object *val;
	struct lh_entry *entry = NULL;
	for (entry = json_object_get_object(o)->head; entry; entry = entry->next) {
		if (entry) {
			key = (char*) entry->k;
			val = (struct json_object*) entry->v;
			json_to_bson_append_element( bb , key , val );
		}
	}
}

static int
json_to_bson( const char* jsonstring, bson *bb ) {
    struct json_object *o = json_tokener_parse(jsonstring);

    if ( is_error( o ) ) {
        return 2;
    }

    if ( !json_object_is_type( o , json_type_object ) ) {
        return 2;
    }

    json_to_bson_append( bb , o );
    return 0;
}

int fromjson(const char* jsonstr, bson *out) {
	int ret = json_to_bson(jsonstr, out);
	bson_finish(out);
	return ret;
}
