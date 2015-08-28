#include "xlua.hpp"
#include "xlua_prototypes.hpp"

namespace hpx {
//---table_iter structure--//

int new_table_iter(lua_State *L) {
  size_t nbytes = sizeof(table_iter_type);
  char *table_iter = (char *)lua_newuserdata(L,nbytes);
  luaL_setmetatable(L,table_iter_metatable_name);
  new (table_iter) table_iter_type();
  return 1;
}

int hpx_table_iter_clean(lua_State *L) {
    if(cmp_meta(L,-1,table_iter_metatable_name)) {
      table_iter_type *fnc = (table_iter_type *)lua_touserdata(L,-1);
      dtor(fnc);
    }
    return 1;
}

int hpx_table_iter_call(lua_State *L) {
  if(true) {//cmp_meta(L,1,table_iter_metatable_name)) {
    table_iter_type *fnc = (table_iter_type *)lua_touserdata(L,1);
    if(fnc->ready && fnc->begin != fnc->end) {

      key_type kt = fnc->begin->first;
      if(kt.which() == 0)
        lua_pushnumber(L,boost::get<double>(kt));
      else
        lua_pushstring(L,boost::get<std::string>(kt).c_str());
      lua_replace(L,2);

      Holder h = fnc->begin->second;
      h.unpack(L);
      lua_replace(L,3);

      ++fnc->begin;
      return 2;
    } else {
      lua_pushnil(L);
    }
  }
  return 1;
}

int open_table_iter(lua_State *L) {
    static const struct luaL_Reg table_iter_meta_funcs [] = {
        {NULL,NULL},
    };

    static const struct luaL_Reg table_iter_funcs [] = {
        {"new", &new_table_iter},
        {NULL, NULL}
    };

    luaL_newlib(L,table_iter_funcs);

    luaL_newmetatable(L,table_iter_metatable_name);
    luaL_newlib(L, table_iter_meta_funcs);
    lua_setfield(L,-2,"__index");

    lua_pushstring(L,"__gc");
    lua_pushcfunction(L,hpx_table_iter_clean);
    lua_settable(L,-3);

    /*
    lua_pushstring(L,"name");
    lua_pushstring(L,table_metatable_name);
    lua_settable(L,-3);
    */

    lua_pushstring(L,"__call");
    lua_pushcfunction(L,hpx_table_iter_call);
    lua_settable(L,-3);

    lua_pop(L,1);

    return 1;
}
//---table structure--//

int new_table(lua_State *L) {
  size_t nbytes = sizeof(table_ptr);
  char *table = (char *)lua_newuserdata(L,nbytes);
  luaL_setmetatable(L,table_metatable_name);
  new (table) table_ptr(new table_inner());
  return 1;
}
int linspace(lua_State *L) {
  double lo = lua_tonumber(L,1);
  double hi = lua_tonumber(L,2);
  double sz = lua_tonumber(L,3);
  size_t nbytes = sizeof(table_ptr);
  char *table = (char *)lua_newuserdata(L,nbytes);
  luaL_setmetatable(L,table_metatable_name);
  new (table) table_ptr(new table_inner());
  table_ptr& t = *(table_ptr*)table;
  double delta = (hi-lo)/(sz-1);
  for(int i=1;i<=sz;i++) {
    (t->t)[i].var = lo + (i-1)*delta;
  }
  t->size=sz;
  return 1;
}

int hpx_table_clean(lua_State *L) {
    if(cmp_meta(L,-1,table_metatable_name)) {
      table_ptr *fnc = (table_ptr *)lua_touserdata(L,-1);
      dtor(fnc);
    }
    return 1;
}

int table_len(lua_State *L) {
    if(cmp_meta(L,-1,table_metatable_name)) {
      table_ptr *fnc_p = (table_ptr *)lua_touserdata(L,-1);
      table_ptr& fnc = *fnc_p;
      int sz = fnc->size;
      lua_pushnumber(L,sz);
    }
    return 1;
}

/**
 * Implements __ipairs for the table class.
 */
int table_clos_iter(lua_State *L) {
  int index = 0;
  if(lua_isnumber(L,-1))
    index = lua_tonumber(L,-1);
  int next_index = index+1;
  table_ptr *fnc_p = (table_ptr*)lua_touserdata(L,lua_upvalueindex(1));
  table_ptr& fnc = *fnc_p;
  lua_pop(L,lua_gettop(L));
  lua_pushnumber(L,next_index);
  auto ptr = fnc->t.find(next_index);
  if(ptr == fnc->t.end())
    return 0;
  Holder h = (fnc->t)[next_index];
  h.unpack(L);
  return 2;
}

int table_pairs(lua_State *L) {
  if(cmp_meta(L,1,table_metatable_name)) {
    table_ptr *fnc_p = (table_ptr *)lua_touserdata(L,-1);
    table_ptr& fnc = *fnc_p;
    new_table_iter(L);
    table_iter_type *fc =
      (table_iter_type *)lua_touserdata(L,-1);
    fc->ready = true;
    fc->begin = fnc->t.begin();
    fc->end   = fnc->t.end();
  }
  return 1;
}

int table_ipairs(lua_State *L) {
  lua_pushcclosure(L,&table_clos_iter,1);
  return 1;
}

void push_key(lua_State *L,const key_type& kt) {
  if(kt.which() == 0) {
    lua_pushnumber(L,boost::get<double>(kt));
  } else {
    lua_pushstring(L,boost::get<std::string>(kt).c_str());
  }
}

int table_name(lua_State *L) {
  lua_pushstring(L,table_metatable_name);
  return 1;
}

int table_new_index(lua_State *L) {
  table_ptr *fnc_p = (table_ptr *)lua_touserdata(L,1);
  table_ptr& fnc = *fnc_p;
  Holder h;
  if(lua_gettop(L)==3) { // set
    h.pack(L,3);
    if(lua_isnumber(L,2)) {
      double key = lua_tonumber(L,2);
      (fnc->t)[key] = h;
      if(key == 1 + fnc->size)
        fnc->size = key;
    } else {
      std::string key = lua_tostring(L,2);
      (fnc->t)[key] = h;
    }
    return 0;
  } else {// get
    if(lua_isnumber(L,2)) {
      double key = lua_tonumber(L,2);
      auto ptr = fnc->t.find(key);
      if(ptr == fnc->t.end())
        return 0;
      h = ptr->second;
    } else {
      std::string key = lua_tostring(L,2);
      if(key == "Name") {
        lua_pop(L,2);
        lua_pushcfunction(L,table_name);
        return 1;
      }
      auto ptr = fnc->t.find(key);
      if(ptr == fnc->t.end())
        return 0;
      h = ptr->second;
    }
    lua_pop(L,2);
    h.unpack(L);
  }
  return 1;
}

int open_table(lua_State *L) {
    static const struct luaL_Reg table_meta_funcs [] = {
        {NULL,NULL},
    };

    static const struct luaL_Reg table_funcs [] = {
        {"new", &new_table},
        {"linspace", &linspace},
        {NULL, NULL}
    };

    luaL_newlib(L,table_funcs);

    luaL_newmetatable(L,table_metatable_name);
    //luaL_newlib(L, table_meta_funcs);
    //lua_setfield(L,-2,"__index");

    lua_pushstring(L,"__gc");
    lua_pushcfunction(L,hpx_table_clean);
    lua_settable(L,-3);

    lua_pushstring(L,"__len");
    lua_pushcfunction(L,table_len);
    lua_settable(L,-3);

    lua_pushstring(L,"__newindex");
    lua_pushcfunction(L,table_new_index);
    lua_settable(L,-3);

    lua_pushstring(L,"__index");
    lua_pushcfunction(L,table_new_index);
    lua_settable(L,-3);

    lua_pushstring(L,"__pairs");
    lua_pushcfunction(L,table_pairs);
    lua_settable(L,-3);

    lua_pushstring(L,"__ipairs");
    lua_pushcfunction(L,table_ipairs);
    lua_settable(L,-3);

    lua_pushstring(L,"name");
    lua_pushstring(L,table_metatable_name);
    lua_settable(L,-3);

    lua_pop(L,1);

    return 1;
}
}
