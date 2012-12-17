#include "mongo_cursor.h"
#include "mongo_query.h"

#include "utils.h"
#include "common.h"
#include "driver/mongo.h"
#include "cjson/json.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

extern void bson_to_lua(lua_State *L, const bson *obj);


/******
 * **/

static void json_to_bson_append_element( bson *bb , const char *k , struct json_object *v );

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

/**
   should already have called start_array
   this will not call start/finish
 */

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
        break;
    case json_type_array:
        break;
    default:
         break;
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
/****
 *
 * *****/
static int
_frombson(struct QueryCursor* self, lua_State *L, int stack_pos){
	return 2;
}

static int
_fromjson(struct QueryCursor* self, const char *jsonstring) {
	int ret = json_to_bson(jsonstring, self->condition);
	bson_finish(self->condition);
	bson_print(self->condition);
	return ret;
}

static void
_release(struct QueryCursor* self) {
	bson_destroy(self->condition);
	free(self);
	self = NULL;
}

QueryCursor*
InitQueryCursor() {
	QueryCursor *query_cursor = (QueryCursor*)malloc(sizeof(QueryCursor));
	query_cursor->condition = (bson *)malloc(sizeof(bson));
	bson_init(query_cursor->condition);

	query_cursor->fromjson = _fromjson;
	query_cursor->release = _release;

	return query_cursor;
}

/************************************************************************/
inline
MongoCursor* userdata_to_cursor(lua_State* L, int index) {
    void *ud = luaL_checkudata(L, index, LUAMONGO_CURSOR);
    MongoCursor *cursor = *((MongoCursor **)ud);

    return cursor;
}

/*
 * cursor,err = db:query(ns, query)
 */
int
cursor_create(lua_State *L, mongo *connection, const char *ns,
                  const Query *query, int nToReturn, int nToSkip,
                  const QueryCursor *fieldsToReturn, int queryOptions, int batchSize) {

	printf("[%s %d]namespace is: %s, nToRetur:%d nToSkip:%d queryOptions:%d\n", __FILE__, __LINE__, ns, nToSkip, nToReturn, queryOptions);
	MongoCursor **cursor = (MongoCursor **) lua_newuserdata(L, sizeof(MongoCursor *));
	*cursor = (MongoCursor *)malloc(sizeof(MongoCursor));

	mongo_cursor_init( &(*cursor)->cursor, connection, ns );


//	mongo_cursor_set_options(*cursor, queryOptions);
	mongo_cursor_set_skip( &(*cursor)->cursor, nToSkip );
	mongo_cursor_set_limit( &(*cursor)->cursor, nToReturn );

	bson_copy( &(*cursor)->fields, fieldsToReturn->condition);
	// mongo_cursor_set_fields( (*cursor)->cursor, fieldsToReturn->condition );

	bson_copy( &(*cursor)->query, query->condition);
	// mongo_cursor_set_query( (*cursor)->cursor, (*cursor)->query );
//

	luaL_getmetatable(L, LUAMONGO_CURSOR);
	lua_setmetatable(L, -2);

	return 0;
}

/*
 * res = cursor:next()
 */
static int
cursor_next(lua_State *L) {
	MongoCursor *cursor = userdata_to_cursor(L, 1);

    if (MONGO_OK == mongo_cursor_next(&(cursor->cursor))) {
    	bson_to_lua(L, &(cursor->cursor).current);
    } else {
    	lua_pushnil(L);
    }

    return 1;
}

static int
result_iterator(lua_State *L) {
    MongoCursor *cursor = userdata_to_cursor(L, lua_upvalueindex(1));
    mongo_cursor_set_fields(&cursor->cursor, &cursor->fields);
    mongo_cursor_set_query(&cursor->cursor, &cursor->query);

    bool has_more = true;
    while (MONGO_OK == mongo_cursor_next(&(cursor->cursor))) {
    	has_more = false;
		bson_to_lua(L, &(cursor->cursor).current);
	}
    if (has_more) lua_pushnil(L);

    return 1;
}

/*
 * iter_func = cursor:results()
 */
static int
cursor_results(lua_State *L) {
    lua_pushcclosure(L, result_iterator, 1);
    return 1;
}

/*
 * has_more = cursor:has_more(in_current_batch)
 *    pass true to call moreInCurrentBatch (mongo >=1.5)
 */
static int
cursor_has_more(lua_State *L) {
    MongoCursor *cursor = userdata_to_cursor(L, 1);

    bool in_current_batch = lua_toboolean(L, 2);
    if (in_current_batch)
    	lua_pushboolean(L, 0);
        // lua_pushboolean(L, cursor->moreInCurrentBatch());
    else
        lua_pushboolean(L, cursor->cursor.current.cur ? 0 : 1);

    return 1;
}

/*
 * __gc
 */
static int
cursor_gc(lua_State *L) {
    MongoCursor *cursor = userdata_to_cursor(L, 1);
    mongo_cursor_destroy(&cursor->cursor);
    bson_destroy(&cursor->fields);
    bson_destroy(&cursor->query);
    return 0;
}

/*
 * __tostring
 */
static int
cursor_tostring(lua_State *L) {
    MongoCursor *cursor = userdata_to_cursor(L, 1);
    lua_pushfstring(L, "%s: %p", LUAMONGO_CURSOR, cursor);
    return 1;
}

int
mongo_cursor_register(lua_State *L) {
    static const luaL_Reg cursor_methods[] = {
        {"next", cursor_next},
        {"results", cursor_results},
        {"has_more", cursor_has_more},
//        {"itcount", cursor_itcount},
//        {"is_dead", cursor_is_dead},
//        {"is_tailable", cursor_is_tailable},
//        {"has_result_flag", cursor_has_result_flag},
//        {"get_id", cursor_get_id},
        {NULL, NULL}
    };

    static const luaL_Reg cursor_class_methods[] = {
        {NULL, NULL}
    };

    luaL_newmetatable(L, LUAMONGO_CURSOR);
    luaL_setfuncs(L, cursor_methods, 0);

    lua_pushvalue(L,-1);
    lua_setfield(L, -2, "__index");

    lua_pushcfunction(L, cursor_gc);
    lua_setfield(L, -2, "__gc");

    lua_pushcfunction(L, cursor_tostring);
    lua_setfield(L, -2, "__tostring");

    lua_remove(L, -1);
    lua_newtable(L); // Cursor
	luaL_setfuncs(L, cursor_class_methods, 0);
	lua_setfield(L, -2, "Cursor");

    return 1;
}
