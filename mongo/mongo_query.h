
#ifndef _MONGO_QUERY_H__
#define _MONGO_QUERY_H__
// forward declaration
#include "driver/bson.h"
#include "lua.h"

typedef struct Query {
	bson *condition;
	int (*fromjson)(struct Query* self, lua_State *L, int stack_pos);
	int (*frombson)(struct Query* self, lua_State *L, int stack_pos);
	void (*release)(struct Query* self);
}Query;

Query* init_query();

#endif //_MONGO_QUERY_H__
