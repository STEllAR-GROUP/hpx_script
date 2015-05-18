#ifndef XLUA_HPP
#define XLUA_HPP

#include <iostream>
#include <hpx/hpx.hpp>
#include <hpx/hpx_init.hpp>
#include <lua.hpp>
#include <map>
#include <sstream>
#include <hpx/include/lcos.hpp>
#include <atomic>
#include <memory>
#include <sstream>
#include <boost/bind.hpp>
#include <boost/variant.hpp>
#include <hpx/lcos/future.hpp>
#include <hpx/lcos/local/composable_guard.hpp>
#include <hpx/include/actions.hpp>
#include <hpx/runtime/serialization/shared_ptr.hpp>
#include <hpx/runtime/serialization/map.hpp>
#include <hpx/runtime/serialization/vector.hpp>
#include <hpx/runtime/serialization/variant.hpp>

#define SHOW_ERROR(L) do { std::cout \
    << "Error: " << __FILE__ << ":" << __LINE__ << " " \
    << lua_tostring(L,-1) << std::endl; lua_pop(L,1); } while(false)

#define HERE std::cout << "HERE: " << __FILE__ << ":" << __LINE__ << std::endl

#define OUT(I,V) do { o << I << "] " << V << std::endl; } while(false)

#define STACK show_stack(std::cout,L,__LINE__,true)

namespace hpx {

std::ostream& show_stack(std::ostream& o,lua_State *L,int line,bool recurse=true);

/*
 * This class is present because of a bug in boost (?) which
 * prevents the serialization of shared_ptr<string>.
 */
struct string_wrap {
  std::string str;
  const char *c_str() const { return str.c_str(); }
  operator const std::string&() const { return str; }
  string_wrap() {}
  string_wrap(const char *s) : str(s) {}
  ~string_wrap() {}
private:
    friend class hpx::serialization::access;
    template<class Archive>
    void serialize(Archive & ar, const unsigned int version)
    {
      ar & str;
    }
};
inline std::ostream& operator<<(std::ostream& o,const string_wrap& sw) {
  return o << sw.str;
}

int dataflow(lua_State *L);
int async(lua_State *L);
int luax_wait_all(lua_State *L);
int luax_when_any(lua_State *L);
int unwrap(lua_State *L);

int hpx_reg(lua_State *L);

int isfuture(lua_State *L);

int hpx_run(lua_State *L);


int luax_run_guarded(lua_State *L);

int open_future(lua_State *L);
int open_guard(lua_State *L);
int open_locality(lua_State *L);

int new_future(lua_State *L);

const char *lua_read(lua_State *L,void *data,size_t *size);

// metatables
extern const char *future_metatable_name;
extern const char *guard_metatable_name;
extern const char *locality_metatable_name;

typedef hpx::naming::id_type locality_type;
 
class Holder;

typedef boost::shared_ptr<std::vector<Holder> > ptr_type;
typedef hpx::shared_future<ptr_type> future_type;
typedef boost::variant<double,std::string> key_type;
typedef std::map<key_type,Holder> table_type;
typedef std::vector<Holder> array_type;

struct Guard {
  boost::shared_ptr<hpx::lcos::local::guard> g;
  ptr_type g_data;
  Guard() : g(new hpx::lcos::local::guard()), g_data(new std::vector<Holder>()) {}
  ~Guard() {}
};
typedef boost::shared_ptr<Guard> guard_type;

typedef boost::shared_ptr<string_wrap> string_ptr;

int hpx_srun(lua_State *L,std::string& fname,ptr_type p);
void hpx_srun(string_ptr fname,ptr_type p,guard_type*,int);

extern guard_type global_guarded;

struct Empty {
private:
    friend class hpx::serialization::access;
    template<class Archive>
    void serialize(Archive & ar, const unsigned int version)
    {
    }
};

//--- Generic holder for a Lua data object
class Holder {
private:
    friend class hpx::serialization::access;
    template<class Archive>
    void serialize(Archive & ar, const unsigned int version)
    {
      ar & var;
    }
public:
  enum utype { empty_t, num_t, fut_t, str_t, ptr_t, table_t, guard_t };

  boost::variant<
    Empty,
    double,
    future_type,
    std::string,
    ptr_type,
    table_type
    > var;

  void set(double num_) {
    var = num_;
  }
  void set(std::string& str_) {
    var = str_;
  }
  void set(const char *str_) {
    std::string s(str_);
    var = s;
  }
  void unpack(lua_State *L) {
    if(var.which() == num_t) {
      lua_pushnumber(L,boost::get<double>(var));
    } else if(var.which() == str_t) {
      lua_pushstring(L,boost::get<std::string>(var).c_str());
    } else if(var.which() == ptr_t) {
      auto ptr = boost::get<ptr_type >(var);
      for(auto i=ptr->begin();i != ptr->end();++i)
        i->unpack(L);
    } else if(var.which() == fut_t) {
      // Shouldn't ever happen
      //std::cout << "ERROR: Unrealized future in arg list" << std::endl;
      //abort();
      new_future(L);
      future_type *fc = (future_type *)lua_touserdata(L,-1);
      *fc = boost::get<future_type>(var);
    } else if(var.which() == table_t) {
      table_type& table = boost::get<table_type>(var);
      lua_createtable(L,0,table.size());
      for(auto i=table.begin();i != table.end();++i) {
        int which = i->first.which();
        i->second.unpack(L);
        if(which == 0) {
          lua_pushnumber(L,(int)boost::get<double>(i->first));
          lua_settable(L,-3);
        } else {
          std::string str = boost::get<std::string>(i->first);
          lua_setfield(L,-2,str.c_str());
        }
      }
    } else {
      std::cout << "ERROR: Unknown type: " << var.which() << std::endl;
    }
  }
  void push(ptr_type& vec) {
    if(var.which() != empty_t)
      vec->push_back(*this);
  }
  void pack(lua_State *L,int index) {
    if(lua_isnumber(L,index)) {
      set(lua_tonumber(L,index));
    } else if(lua_isstring(L,index)) {
      set(lua_tostring(L,index));
    } else if(lua_isuserdata(L,index) && luaL_checkudata(L,index,future_metatable_name) != nullptr) {
      var = *(future_type *)lua_touserdata(L,index);
    } else if(lua_istable(L,index)) {
      int nn = lua_gettop(L);
      lua_pushvalue(L,index);
      lua_pushnil(L);
      var = table_type();
      table_type& table = boost::get<table_type>(var);
      while(lua_next(L,-2) != 0) {
        lua_pushvalue(L,-2);
        if(lua_isstring(L,-1)) {
          const char *keys = lua_tostring(L,-1);
          if(keys == nullptr)
            continue;
          std::string key{keys};
          Holder h;
          h.pack(L,-2);
          if(h.var.which() != empty_t) {
            table[key] = h;
          }
        } else if(lua_isnumber(L,-1)) {
          double key = lua_tonumber(L,-1);
          Holder h;
          h.pack(L,-2);
          if(h.var.which() != empty_t) {
            table[key] = h;
          }
        }
        lua_pop(L,2);
      }
      if(lua_gettop(L) > nn)
        lua_pop(L,lua_gettop(L)-nn);
    }
  }
};

class Lua;

extern std::map<std::string,std::string> function_registry;

//--- A wrapper for the Lua object. Allows us to add state.
class Lua {
public:
  std::atomic<bool> busy;
private:
  lua_State *L;
  public:
  Lua() : busy(true), L(luaL_newstate()) {
    luaL_openlibs(L);
    lua_pushcfunction(L,dataflow);
    lua_setglobal(L,"dataflow");
    lua_pushcfunction(L,async);
    lua_setglobal(L,"async");
    lua_pushcfunction(L,luax_wait_all);
    lua_setglobal(L,"wait_all");
    lua_pushcfunction(L,luax_when_any);
    lua_setglobal(L,"when_any");
    lua_pushcfunction(L,unwrap);
    lua_setglobal(L,"unwrap");
    lua_pushcfunction(L,isfuture);
    lua_setglobal(L,"isfuture");
    lua_pushcfunction(L,hpx_reg);
    lua_setglobal(L,"HPX_PLAIN_ACTION");
    lua_pushcfunction(L,hpx_run);
    lua_setglobal(L,"hpx_run");
    lua_pushcfunction(L,luax_run_guarded);
    lua_setglobal(L,"run_guarded");
    open_future(L);
    luaL_requiref(L, "future", &open_future, 1);
    open_guard(L);
    luaL_requiref(L, "guard",&open_guard, 1);
    open_locality(L);
    luaL_requiref(L, "locality",&open_locality, 1);
    lua_pop(L,lua_gettop(L));
    for(auto i=function_registry.begin();i != function_registry.end();++i) {
      // Insert into table
      if(lua_load(L,(lua_Reader)lua_read,(void *)&i->second,i->first.c_str(),"b") != 0) {
        std::cout << "function " << i->first << " size=" << i->second.size() << std::endl;
        SHOW_ERROR(L);
      } else {
        lua_setglobal(L,i->first.c_str());
      }
    }
    busy = false;
  }
  ~Lua() {
    lua_close(L);
  }
  lua_State *get_state() {
    return L;
  }
};
Lua *get_lua_ptr();
void set_lua_ptr(Lua *lua);

//--- Thread-specific ptr deletes objects on reset. Circumvent this.
struct LuaHolder {
  Lua *held;
  LuaHolder() { held = new Lua(); }
  ~LuaHolder() {}
};

//--- Safeguard the use of a Lua VM
class LuaEnv {
  Lua *ptr;
  lua_State *L;
public:
  lua_State *get_state() {
    return L;
  }
  LuaEnv();
  ~LuaEnv();
  operator lua_State *() {
    return L;
  }
};

// END Header file
}
#endif
