#ifndef _MONGO_CURSOR_H__
#define _MONGO_CURSOR_H__

#include "mongo_query.h"

#include "lua.h"
#include "driver/mongo.h"

typedef struct QueryCursor {
	bson *condition;
	int (*fromjson)(struct QueryCursor* self, lua_State *L, int stack_pos);
	int (*frombson)(struct QueryCursor* self, lua_State *L, int stack_pos);
	void (*release)(struct QueryCursor* self);
}QueryCursor;

typedef struct {
	mongo_cursor cursor;
	bson fields;
	bson query;
}MongoCursor;

QueryCursor*
InitQueryCursor();

int
cursor_create(lua_State *L, mongo *connection, const char *ns,
                  const Query *query, int nToReturn, int nToSkip,
                  const QueryCursor *fieldsToReturn, int queryOptions, int batchSize);

#endif // _MONGO_CURSOR_H__
