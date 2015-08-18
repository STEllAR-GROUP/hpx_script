#ifndef XLUA_PROTOTYPES_HPP
#define XLUA_PROTOTYPES_HPP

#include "xlua.hpp"
namespace hpx {

int call(lua_State *L);
int dataflow(lua_State *L);
int make_ready_future(lua_State *L);
int async(lua_State *L);
int luax_wait_all(lua_State *L);
int luax_when_all(lua_State *L);
int luax_when_any(lua_State *L);
int unwrap(lua_State *L);
int find_here(lua_State *L);
int all_localities(lua_State *L);
int remote_localities(lua_State *L);
int root_locality(lua_State *L);

int hpx_reg(lua_State *L);

int isfuture(lua_State *L);
int istable(lua_State *L);
int islocality(lua_State *L);

int hpx_run(lua_State *L);


int luax_run_guarded(lua_State *L);

int open_table(lua_State *L);
int open_table_iter(lua_State *L);
int open_future(lua_State *L);
int open_guard(lua_State *L);
int open_locality(lua_State *L);

int new_future(lua_State *L);
int new_table(lua_State *L);
}

#endif
