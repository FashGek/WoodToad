#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "common.h"
#include "driver/mongo.h"

extern const luaL_Reg dbclient_methods[];

inline mongo*
userdata_to_replicaset(lua_State* L, int index) {
    void *ud = luaL_checkudata(L, index, LUAMONGO_REPLICASET);
    mongo *replicasetConn = *((mongo **)ud);
    return replicasetConn;
}


/*
 * ok,err = replicaset:connect()
 */
static int
replicaset_connect(lua_State *L) {
	bool expection = false;
	char *msg = NULL;
    mongo *replicasetConn = userdata_to_replicaset(L, 1);

    int status = mongo_replica_set_client( replicasetConn );

    if (MONGO_OK != status) {
        	switch (replicasetConn->err) {
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
        	case MONGO_CONN_NO_PRIMARY:
        		expection = true;
        		msg = "no primary";
        		break;
        	default:
        		expection = true;
        		msg = "unknowen error occured";
        		break;
        	}
        }

        if (expection) {
        	lua_pushnil(L);
        	lua_pushfstring(L, LUAMONGO_ERR_CONNECT_FAILED, LUAMONGO_REPLICASET, msg);
        	return 2;
        }

		lua_pushboolean(L, true);
		return 1;
}

/*
 * db,err = mongo.ReplicaSet.New(name, {hostAndPort1, ...)
 */
static int
replicaset_new(lua_State *L) {
    luaL_checktype(L, 1, LUA_TSTRING);
	luaL_checktype(L, 2, LUA_TTABLE);

    int ports[256] = {0};
    char hostString[256][17];
    int numRs = 0;

    int i = 1;
    while (1 && numRs < 256) {
    	int pathlen;

		lua_pushinteger(L, i++);
		lua_gettable(L, 2);
		const char* hostAndPort = lua_tostring(L, -1);
		if (NULL == hostAndPort) break;

		char * pathend = strchr(hostAndPort,':');

		if (pathend) {
			pathlen = pathend - hostAndPort;

			strncpy(hostString[numRs], hostAndPort, pathlen);
			hostAndPort += (pathlen + 1);
			ports[numRs] = atoi(hostAndPort);
			numRs++;
		} else {
			break;
		}
	}

    mongo **connection = (mongo **) lua_newuserdata(L, sizeof(mongo *));
	*connection = (mongo *) malloc(sizeof(mongo));

	const char *rs_name = luaL_checkstring(L, 1);
	mongo_replica_set_init(*connection, rs_name);

	int j = 0;
	for(j = 0; j < numRs && numRs < 256; j++) {
		mongo_replica_set_add_seed(*connection, hostString[j], ports[j]);
	}

    luaL_getmetatable(L, LUAMONGO_REPLICASET);
	lua_setmetatable(L, -2);

    return 1;
}


/*
 * __gc
 */
static int
replicaset_gc(lua_State *L) {
    mongo *replicasetConn = userdata_to_replicaset(L, 1);
    mongo_destroy(replicasetConn);
    return 0;
}

/*
 * __tostring
 */
static int
replicaset_tostring(lua_State *L) {
    mongo *replicasetConn = userdata_to_replicaset(L, 1);
    lua_pushfstring(L, "%s: %s", LUAMONGO_REPLICASET, "NotImplemention!Next Version");
    return 1;
}

int
mongo_replicaset_register(lua_State *L) {
	static const luaL_Reg replicaset_methods[] = {
			{"connect", replicaset_connect},
			{NULL, NULL}
	};

	static const luaL_Reg replicaset_class_methods[] = {
			{ "New", replicaset_new },
			{ NULL, NULL }
	};

	luaL_newmetatable(L, LUAMONGO_REPLICASET);
	luaL_setfuncs(L, dbclient_methods, 0);
	luaL_setfuncs(L, replicaset_methods, 0);

	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");

	lua_pushcfunction(L, replicaset_gc);
	lua_setfield(L, -2, "__gc");

	lua_pushcfunction(L, replicaset_tostring);
	lua_setfield(L, -2, "__tostring");

	lua_remove(L, -1);
	lua_newtable(L);	// ReplicaSet
	luaL_setfuncs(L, replicaset_class_methods, 0);
	lua_setfield(L, -2, "ReplicaSet");

	return 1;
}


