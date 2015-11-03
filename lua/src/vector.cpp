#include "xlua.hpp"
#include "xlua_prototypes.hpp"

namespace hpx {

int new_vector(lua_State *L) {
  size_t nbytes = sizeof(vector_ptr);
  char *vector = (char *)lua_newuserdata(L,nbytes);
  new (vector) vector_ptr(new std::vector<double>());
  luaL_setmetatable(L,vector_metatable_name);
  return 1;
}

int vlinspace(lua_State *L) {
  double lo = lua_tonumber(L,1);
  double hi = lua_tonumber(L,2);
  double sz = lua_tonumber(L,3);
  size_t nbytes = sizeof(table_ptr);
  char *vector = (char *)lua_newuserdata(L,nbytes);
  luaL_setmetatable(L,vector_metatable_name);
  new (vector) vector_ptr(new std::vector<double>());
  vector_ptr& v = *(vector_ptr*)vector;
  if(v->size() < sz+1)
    v->resize(sz+1);
  double delta = (hi-lo)/(sz-1);
  for(int i=1;i<=sz;i++) {
    (*v)[i] = lo + (i-1)*delta;
  }
  return 1;
}

int hpx_vector_clean(lua_State *L) {
    if(cmp_meta(L,-1,vector_metatable_name)) {
      vector_ptr *fnc = (vector_ptr *)lua_touserdata(L,-1);
      dtor(fnc);
    }
    return 1;
}

int vector_len(lua_State *L) {
    vector_ptr *fnc_p = (vector_ptr *)lua_touserdata(L,-1);
    vector_ptr& fnc = *fnc_p;
    int sz = fnc->size();
    if(sz > 0) sz--;
    lua_pushnumber(L,sz);
    return 1;
}

/**
 * Implements __ipairs for the vector class.
 */
int vector_clos_iter(lua_State *L) {
  int index = 0;
  if(lua_isnumber(L,-1))
    index = lua_tonumber(L,-1);
  int next_index = index+1;
  vector_ptr *fnc_p = (vector_ptr*)lua_touserdata(L,lua_upvalueindex(1));
  vector_ptr& fnc = *fnc_p;
  lua_pop(L,lua_gettop(L));
  if(next_index >= fnc->size())
    return 0;
  lua_pushnumber(L,next_index);
  lua_pushnumber(L,(*fnc)[next_index]);
  return 2;
}

int vector_ipairs(lua_State *L) {
  lua_pushcclosure(L,&vector_clos_iter,1);
  return 1;
}

int vector_name(lua_State *L) {
  lua_pushstring(L,vector_metatable_name);
  return 1;
}

int vector_pop(lua_State *L) {
  vector_ptr *fnc_p = (vector_ptr *)lua_touserdata(L,1);
  vector_ptr& fnc = *fnc_p;
  lua_pop(L,lua_gettop(L));
  int n = fnc->size()-1;
  if(n < 1)
    return 0;
  lua_pushnumber(L,(*fnc)[n]);
  fnc->pop_back();
  return 1;
}

int vector_new_index(lua_State *L) {
  vector_ptr *fnc_p = (vector_ptr *)lua_touserdata(L,1);
  vector_ptr& fnc = *fnc_p;
  if(lua_gettop(L)==3) { // set
    int key = lua_tonumber(L,2);
    if(key >= fnc->size())
      fnc->resize(key+1);
    (*fnc)[key] = lua_tonumber(L,3);
    return 0;
  } else { // get
    if(!lua_isnumber(L,2)) {
      lua_pop(L,lua_gettop(L));
      lua_pushcfunction(L,vector_name);
      return 1;
    }
    int key = lua_tonumber(L,2);
    if(0 <= key && key < fnc->size()) {
      lua_pushnumber(L,(*fnc)[key]);
    } else {
      lua_pushnil(L);
    }
    return 1;
  }
  return 1;
}

int open_vector(lua_State *L) {
    static const struct luaL_Reg vector_meta_funcs [] = {
        {NULL,NULL},
    };

    static const struct luaL_Reg vector_funcs [] = {
        {"new", &new_vector},
        {"linspace", &vlinspace},
        {NULL, NULL}
    };

    luaL_newlib(L,vector_funcs);

    luaL_newmetatable(L,vector_metatable_name);
    //luaL_newlib(L, vector_meta_funcs);
    //lua_setfield(L,-2,"__index");

    lua_pushstring(L,"__gc");
    lua_pushcfunction(L,hpx_vector_clean);
    lua_settable(L,-3);

    lua_pushstring(L,"__len");
    lua_pushcfunction(L,vector_len);
    lua_settable(L,-3);

    lua_pushstring(L,"__newindex");
    lua_pushcfunction(L,vector_new_index);
    lua_settable(L,-3);

    lua_pushstring(L,"__index");
    lua_pushcfunction(L,vector_new_index);
    lua_settable(L,-3);

    lua_pushstring(L,"__ipairs");
    lua_pushcfunction(L,vector_ipairs);
    lua_settable(L,-3);

    /*
    lua_pushstring(L,"name");
    lua_pushstring(L,vector_metatable_name);
    lua_settable(L,-3);
    */

    lua_pop(L,1);

    return 1;
}
}
