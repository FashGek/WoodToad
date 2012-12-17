


#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>


#include "luacompat52.h"


//#include <iostream>
//#include <client/dbclient.h>

#include "utils.h"
#include "common.h"

extern int mongo_bsontypes_register(lua_State *L);
extern int mongo_connection_register(lua_State *L);
extern int mongo_replicaset_register(lua_State *L);
extern int mongo_cursor_register(lua_State *L);
extern int mongo_query_register(lua_State *L);
//extern int mongo_gridfs_register(lua_State *L);
//extern int mongo_gridfile_register(lua_State *L);
//extern int mongo_gridfschunk_register(lua_State *L);

/*
 *
 * library entry point
 *
 */

 int luaopen_mongo_c(lua_State *L) {
    mongo_bsontypes_register(L);
    mongo_connection_register(L);
    mongo_replicaset_register(L);
    mongo_cursor_register(L);
    mongo_query_register(L);
//
//    mongo_gridfs_register(L);
//    mongo_gridfile_register(L);
//    mongo_gridfschunk_register(L);
//
//    /*
//     * push the created table to the top of the stack
//     * so "mongo = require('mongo')" works
//     */
//    lua_getglobal(L, LUAMONGO_ROOT);

    return 1;
}
