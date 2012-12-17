#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "common.h"
#include "driver/mongo.h"

inline mongo*
userdata_to_connection(lua_State* L, int index) {
    void *ud = luaL_checkudata(L, index, LUAMONGO_CONNECTION);
    mongo *connection = *((mongo **)ud);
    return connection;
}

// defined in mongo_dbclinet.c
extern const luaL_Reg dbclient_methods[];

/*
 * db,err = mongo.Connection.New({})
 *    accepts an optional table of features:
 *       auto_reconnect   (default = false)
 *       rw_timeout       (default = 0) (mongo >= v1.5)
 */
static int
connection_new(lua_State *L) {
    int resultcount = 1;

	// bool auto_reconnect;
	double rw_timeout = 0;
	if (lua_type(L, 1) == LUA_TTABLE) {
		// extract arguments from table
		/*
		 * mongo-c-driver 不支持
		 * lua_getfield(L, 1, "auto_reconnect");
		 auto_reconnect = lua_toboolean(L, -1);*/
		lua_getfield(L, 1, "rw_timeout");
		rw_timeout = lua_tonumber(L, -1);
		lua_pop(L, 2);
	} else {
		// auto_reconnect = false;
		rw_timeout = 0;
	}

	/*
	 * @describe 利用指针的指针来节省lua栈的空间
	 * */
	mongo **connection = (mongo **)lua_newuserdata(L, sizeof(mongo *));
	*connection = (mongo *)malloc(sizeof(mongo));
	mongo_init(*connection);
	mongo_set_op_timeout(*connection, rw_timeout);

	// int status = mongo_connect( conn, );

	// DBClientConnection **connection = (DBClientConnection **)lua_newuserdata(L, sizeof(DBClientConnection *));
	// *connection = new DBClientConnection(auto_reconnect, 0, rw_timeout);

	luaL_getmetatable(L, LUAMONGO_CONNECTION);
	lua_setmetatable(L, -2);

//    try {
//
//    } catch (std::exception &e) {
//        lua_pushnil(L);
//        lua_pushfstring(L, LUAMONGO_ERR_CONNECTION_FAILED, e.what());
//        resultcount = 2;
//    }

	return resultcount;
}

/*
 * ok,err = connection:connect(connection_str)
 */
static int
connection_connect(lua_State *L) {
    mongo *connection = userdata_to_connection(L, 1);
    const char *connectstr = luaL_checkstring(L, 2);
    int port = luaL_checkint(L, 3);
    bool expection = false;
    char *msg = NULL;


//  谁这样用，完全是找死
//    if (port <= 0) {
//
//    }

    int status = mongo_client(connection, connectstr, port);

    if (MONGO_OK != status) {
    	switch (connection->err) {
    	case MONGO_CONN_NO_SOCKET:
    		expection = true;
    		msg = "no mongo socket";
    		break;
    	case MONGO_CONN_FAIL:
    		expection = true;
    		msg = "connect mongo failed";
    		break;
    	case MONGO_CONN_NOT_MASTER:
    		expection = true;
    		msg = "no mongo master";
    		break;
    	default:
    		expection = true;
    		msg = "unknowen error occured";
    		break;
    	}
    }

    if (expection) {
    	lua_pushnil(L);
    	lua_pushfstring(L, LUAMONGO_ERR_CONNECT_FAILED, connectstr, msg);
    	return 2;
    }

    lua_pushboolean(L, true);
    return 1;
}

/*
 * __gc
 */
static int
connection_gc(lua_State *L) {
    mongo *connection = userdata_to_connection(L, 1);
    mongo_destroy(connection);
    return 0;
}

/*
 * __tostring
 */
static int
connection_tostring(lua_State *L) {
    mongo *connection = userdata_to_connection(L, 1);
    // lua_pushfstring(L, "%s: %s", LUAMONGO_CONNECTION,  connection->toString().c_str());
    lua_pushfstring(L, "%s: %s", LUAMONGO_CONNECTION,  "MongoConnectionString Not Supported");
    return 1;
}

int
mongo_connection_register(lua_State *L) {
	static const luaL_Reg connection_methods[] = {
			{ "connect",connection_connect },
			{ NULL, NULL }
	};

	static const luaL_Reg connection_class_methods[] = {
			{ "New", connection_new },
			{ NULL, NULL }
	};

	// 创建一个metatable,然后设置为userdata的mt,这样userdata就能表现得像一个object
	// 或者说是一个类的成员
	luaL_newmetatable(L, LUAMONGO_CONNECTION);
	luaL_setfuncs(L, dbclient_methods, 0);
	luaL_setfuncs(L, connection_methods, 0);

	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");

	lua_pushcfunction(L, connection_gc);
	lua_setfield(L, -2, "__gc");

	lua_pushcfunction(L, connection_tostring);
	lua_setfield(L, -2, "__tostring");

	// 现在要把位于栈顶的弹出
	lua_remove(L, -1);
	lua_newtable(L);	// Connection
	luaL_setfuncs(L, connection_class_methods, 0);
	lua_setfield(L, -2, "Connection");
//	lua_remove(L, -1);

	return 1;
}

