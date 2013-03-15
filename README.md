WoodToad
========

WoodToad is a Lua Library that wraps the mongo-c-driver api. Meet in the connection between the lua logic and pure c mongodb demand, and offer another choise of luamongo 

HowTo Use WoodToad
# import WoodToad
local mongo = require "mongo.c"

# New A Connection
local db = assert(mongo.Connection.New())

# Connect the Db
assert(db:connect('localhost', 27017))

# Authnicate the Db
auth_info['dbname'] = 'test'
auth_info['username'] = 'root'
auth_info['password'] = 'root'
assert(db:auth(auth_info))

# Query
local q = assert(db:query('test.persons', '{}'))

# Insert
local data = {}
data['a'] = 72 * 1
data['b'] = 72 * 2
data['c'] = {}
data['c']['c1'] = 7
data['c']['c2'] = 29
assert(db:insert('test.persons', data))
assert(db:insert_batch('test.persons', {{name = 'tom', age = 120}, {name = 'jack', age = 72}, {name = 'lucy', age = 24}}))
assert(db:insert('test.persons', data))

# Loop
local uq = db:find_one('test.persons', '{}', '{}')
for k,v in pairs(uq) do
	print(k)
	print(v)
end

# Update
db:update('test.persons', '{}', {a=72, b=123}, false, false)
for result in q:results() do
	print(result.a)
	print(result.b)
end
