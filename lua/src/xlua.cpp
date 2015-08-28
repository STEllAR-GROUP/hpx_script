#include "xlua.hpp"
#include "xlua_prototypes.hpp"
#include <hpx/lcos/broadcast.hpp>

const int max_output_args = 10;

#define CHECK_STRING(INDEX,NAME) \
  if(!lua_isstring(L,INDEX)) { \
    luai_writestringerror("Argument to '%s' is not a string ",NAME);\
    return 0; \
  }

namespace hpx {

const char *table_metatable_name = "table";
const char *table_iter_metatable_name = "table_iter";
const char *future_metatable_name = "hpx_future";
const char *guard_metatable_name = "hpx_guard";
const char *locality_metatable_name = "hpx_locality";
const char *hpx_metatable_name = "hpx";

const char *lua_read(lua_State *L,void *data,size_t *size);
int lua_write(lua_State *L,const char *str,unsigned long len,std::string *buf);
bool cmp_meta(lua_State *L,int index,const char *meta_name);

  Lua::Lua() : busy(true), L(luaL_newstate()) {
    luaL_openlibs(L);
    lua_pushcfunction(L,xlua_stop);
    lua_setglobal(L,"stop");
    lua_pushcfunction(L,xlua_start);
    lua_setglobal(L,"start");
    lua_pushcfunction(L,xlua_get_value);
    lua_setglobal(L,"get_value");
    lua_pushcfunction(L,xlua_get_counter);
    lua_setglobal(L,"get_counter");
    lua_pushcfunction(L,discover);
    lua_setglobal(L,"discover_counter_types");
    lua_pushcfunction(L,make_ready_future);
    lua_setglobal(L,"make_ready_future");
    lua_pushcfunction(L,dataflow);
    lua_setglobal(L,"dataflow");
    lua_pushcfunction(L,xlua_unwrapped);
    lua_setglobal(L,"unwrapped");
    lua_pushcfunction(L,call);
    lua_setglobal(L,"call");
    lua_pushcfunction(L,async);
    lua_setglobal(L,"async");
    lua_pushcfunction(L,luax_wait_all);
    lua_setglobal(L,"wait_all");
    lua_pushcfunction(L,luax_when_all);
    lua_setglobal(L,"when_all");
    lua_pushcfunction(L,luax_when_any);
    lua_setglobal(L,"when_any");
    lua_pushcfunction(L,unwrap);
    lua_setglobal(L,"unwrap");
    lua_pushcfunction(L,isfuture);
    lua_setglobal(L,"isfuture");
    lua_pushcfunction(L,islocality);
    lua_setglobal(L,"islocality");
    lua_pushcfunction(L,istable);
    lua_setglobal(L,"istable");
    lua_pushcfunction(L,hpx_reg);
    lua_setglobal(L,"HPX_PLAIN_ACTION");
    lua_pushcfunction(L,hpx_run);
    lua_setglobal(L,"hpx_run");
    lua_pushcfunction(L,luax_run_guarded);
    lua_setglobal(L,"run_guarded");
    lua_pushcfunction(L,find_here);
    lua_setglobal(L,"find_here");
    lua_pushcfunction(L,all_localities);
    lua_setglobal(L,"find_all_localities");
    lua_pushcfunction(L,remote_localities);
    lua_setglobal(L,"find_remote_localities");
    lua_pushcfunction(L,root_locality);
    lua_setglobal(L,"find_root_locality");

    //open_hpx(L);
    luaL_requiref(L, "hpx",&open_hpx, 1);
    open_table(L);
    luaL_requiref(L, "table_t", &open_table, 1);
    open_table_iter(L);
    luaL_requiref(L, "table_iter_t", &open_table_iter, 1);
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
    /*
    luaL_dostring(L,
"function __hpx_nextvalue(obj)"
"  local t = setmetatable({object=obj},{"
"    __call = function(self,v,k)"
"      local nk = self.obj.find(k)"
"      if nk != nil then"
"        return nk,self.obj[nk]"
"      end"
"    end"
"  })"
"  return t"
"end");
*/
    busy = false;
  }
  void Holder::unpack(lua_State *L) {
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
      new_table(L);
      table_ptr *tp = (table_ptr *)lua_touserdata(L,-1);
      *tp = boost::get<table_ptr>(var);
      /*
      try {
        table_ptr& table = boost::get<table_ptr>(var);
        lua_createtable(L,0,table->size());
        for(auto i=table->begin();i != table->end();++i) {
          const int which = i->first.which();
          if(which == 0) { // number
            double d = boost::get<double>(i->first);
            lua_pushnumber(L,d);
            if(d == 0) {
              std::cout << "unpack:PRINT=" << (*this) << std::endl;
              abort();
            }
          } else if(which == 1) { // string
            std::string str = boost::get<std::string>(i->first);
            lua_pushstring(L,str.c_str());
          } else {
            std::cout << "ERROR: Unknown key type: " << which << std::endl;
            abort();
          }
          i->second.unpack(L);
          lua_settable(L,-3);
        }
      } catch(std::exception e) {
        std::cout << "EX2=" << e.what() << std::endl;
      }
      */
    } else if(var.which() == bytecode_t) {
      Bytecode& bc = boost::get<Bytecode>(var);
      lua_load(L,(lua_Reader)lua_read,(void *)&bc.data,"func","b");
    } else if(var.which() == empty_t) {
      lua_pushnil(L);
    } else {
      std::cout << "ERROR: Unknown type: " << var.which() << std::endl;
      abort();
    }
  }

  void Holder::pack(lua_State *L,int index) {
    if(lua_isnumber(L,index)) {
      set(lua_tonumber(L,index));
    } else if(lua_isstring(L,index)) {
      set(lua_tostring(L,index));
    } else if(cmp_meta(L,index,future_metatable_name)) {
      var = *(future_type *)lua_touserdata(L,index);
    } else if(cmp_meta(L,index,table_metatable_name)) {
      var = *(table_ptr *)lua_touserdata(L,index);
    } else if(lua_istable(L,index)) {
      try {
        int nn = lua_gettop(L);
        lua_pushvalue(L,index);
        lua_pushnil(L);
        var = table_ptr(new table_inner());
        table_ptr& table = boost::get<table_ptr>(var);
        while(lua_next(L,-2) != 0) {
          lua_pushvalue(L,-2);
          if(lua_isnumber(L,-1)) {
            double key = lua_tonumber(L,-1);
            if(key==table->size+1)
              table->size = key;
            Holder h;
            h.pack(L,-2);
            if(h.var.which() != empty_t) {
              (table->t)[key] = h;
              //STACK;
              if(key == 0) {
                std::cout << "pack0:PRINT=" << (*this) << std::endl;
                abort();
              }
            } else {
              std::cout << "pack1:PRINT=" << (*this) << std::endl;
              abort();
            }
          } else if(lua_isstring(L,-1)) {
            const char *keys = lua_tostring(L,-1);
            if(keys == nullptr)
              continue;
            std::string key{keys};
            Holder h;
            h.pack(L,-2);
            if(h.var.which() != empty_t) {
              (table->t)[key] = h;
            } else {
              std::cout << "pack1:PRINT=" << (*this) << std::endl;
              abort();
            }
          } else {
            std::cerr << "Can't pack key value!" << lua_type(L,-1) << std::endl;
            abort();
          }
          lua_pop(L,2);
        }
        if(lua_gettop(L) > nn)
          lua_pop(L,lua_gettop(L)-nn);
        //std::cout << "pack:PRINT=" << (*this) << std::endl;
      } catch(std::exception e) {
        std::cout << "EX=" << e.what() << std::endl;
      }
    } else if(lua_isfunction(L,index)) {
      lua_pushvalue(L,index);
      assert(lua_isfunction(L,-1));
      Bytecode b;
      lua_dump(L,(lua_Writer)lua_write,&b.data);
      var = b;
      lua_pop(L,1);
    } else if(lua_isnil(L,index)) {
      Empty e;
      var = e;
    } else if(lua_isuserdata(L,index)) {
      std::cout << "Can't pack unknown user data!" << std::endl;
    } else {
      int t = lua_type(L,index);
      std::cerr << "Can't pack value! " << t << std::endl;
      //abort();
    }
  }

inline bool is_bytecode(const std::string& s) {
  return s.size() > 4 && s[0] == 27 && s[1] == 'L' && s[2] == 'u' && s[3] == 'a';
}

LuaEnv::LuaEnv() {
  ptr = get_lua_ptr();
  if(ptr->busy) {
    std::cout << "Busy" << std::endl;
    abort();
  }
  ptr->busy = true;
  L = ptr->get_state();
}
LuaEnv::~LuaEnv() {
  ptr->busy = false;
  set_lua_ptr(ptr);
}
const char *metatables[] = {table_metatable_name, table_iter_metatable_name, future_metatable_name,
  guard_metatable_name, locality_metatable_name,0};

bool cmp_meta(lua_State *L,int index,const char *name) {
  if(lua_isnil(L,index))
    return false;
  if(!lua_isuserdata(L,index))
    return false;
  int ss1 = lua_gettop(L);
  lua_getfield(L,index,"Name");
  int ss2 = lua_gettop(L);
  if(ss1+1 != ss2)
    return false;
  if(!lua_iscfunction(L,-1))
    return false;
  lua_CFunction f = lua_tocfunction(L,-1);
  lua_pop(L,1);
  if(f == nullptr)
    return false;
  (*f)(L);
  if(lua_isstring(L,-1)) {
    std::string nm = lua_tostring(L,-1);
    lua_pop(L,1);
    return nm == name;
  }
  return false;
}

guard_type global_guarded{new Guard()};

std::ostream& operator<<(std::ostream& out,const key_type& kt) {
  if(kt.which() == 0)
    out << boost::get<double>(kt) << "{f}";
  else
    out << boost::get<std::string>(kt) << "{s}";
  return out;
}
std::ostream& operator<<(std::ostream& out,const Holder& holder) {
  switch(holder.var.which()) {
    case Holder::empty_t:
      break;
    case Holder::num_t:
      out << boost::get<double>(holder.var) << "{f}";
      break;
    case Holder::str_t:
      out << boost::get<std::string>(holder.var) << "{s}";
      break;
    case Holder::table_t:
      {
        table_ptr t = boost::get<table_ptr>(holder.var);
        out << "{";
        for(auto i=t->t.begin(); i != t->t.end(); ++i) {
          if(i != t->t.begin()) out << ", ";
          out << i->first << ":" << i->second;
        }
        out << "}";
      }
      break;
    case Holder::fut_t:
      out << "Fut()";
      break;
    case Holder::ptr_t:
      {
        ptr_type p = boost::get<ptr_type>(holder.var);
        out << "{";
        for(auto i=p->begin();i != p->end();++i) {
          if(i != p->begin()) out << ", ";
          out << *i;
        }
        out << "}";
      }
      break;
    default:
      out << "Unk(" << holder.var.which() << ")";
      break;
  }
  return out;
}

//--- Transfer lua bytecode to/from a std:string
int lua_write(lua_State *L,const char *str,unsigned long len,std::string *buf) {
    std::string b(str,len);
    *buf += b;
    return 0;
}

const char *lua_read(lua_State *L,void *data,size_t *size) {
    std::string *rbuf = (std::string*)data;
    (*size) = rbuf->size();
    return rbuf->c_str();
}

//--- Debugging utility, print the Lua stack
std::ostream& show_stack(std::ostream& o,lua_State *L,const char *fname,int line,bool recurse) {
    int n = lua_gettop(L);
    if(!recurse)
      o << "RESTACK:n=" << n << std::endl;
    else
      o << "STACK:n=" << n << " src: " << fname << ":" << line << std::endl;
    for(int i=1;i<=n;i++) {
        if(lua_isnil(L,i)) OUT(i,"nil");
        else if(lua_isnumber(L,i)) OUT2(i,lua_tonumber(L,i),"num");
        else if(lua_isstring(L,i)) OUT2(i,lua_tostring(L,i),"str");
        else if(lua_isboolean(L,i)) OUT2(i,lua_toboolean(L,i),"bool");
        else if(lua_isfunction(L,i)) {
          lua_pushvalue(L,i);
          std::string bytecode;
			    lua_dump(L,(lua_Writer)lua_write,&bytecode);
          lua_pop(L,1);
          std::ostringstream msg;
          msg << "function ";
          msg << bytecode.size();
          std::string s = msg.str();
          OUT(i,s.c_str());
        } else if(lua_iscfunction(L,i)) OUT(i,"c-function");
        else if(lua_isthread(L,i)) OUT(i,"thread");
        else if(lua_isuserdata(L,i)) {
          bool found = false;
          /*
          for(int j=0;metatables[j] != nullptr;j++) {
            if(luaL_checkudata(L,i,metatables[j]) != nullptr) {
              OUT(i,metatables[j]);
            }
          }
          */
          if(!found)
            OUT(i,"userdata");
        }
        else if(lua_istable(L,i)) {
          o << i << "] table" << std::endl;
          if(recurse) {
            int nn = lua_gettop(L);
            lua_pushvalue(L,i);
            lua_pushnil(L);
            while(lua_next(L,-2) != 0) {
              lua_pushvalue(L,-2);
              std::string value = "?";
              if(lua_isnumber(L,-2)) {
                value = "num: ";
                value += lua_tostring(L,-2);
              } else if(lua_isstring(L,-2)) {
                value = "string: ";
                value += lua_tostring(L,-2);
              } else if(lua_isboolean(L,-2)) {
                value = "bool: ";
                value += lua_tostring(L,-2);
              } else if(lua_iscfunction(L,-2)) {
                value = "c-function";
              }
              const char *key = lua_tostring(L,-1);
              lua_pop(L,2);
              o << "  key=" << key << " value=" << value << std::endl;
            }
            if(lua_gettop(L) > nn)
              lua_pop(L,lua_gettop(L)-nn);
          }
        } else OUT(i,"other");
    }
    if(!recurse)
      o << "END-RESTACK" << std::endl;
    else
      o << "END-STACK" << std::endl;
    o << std::endl;
    return o;
}

//--- Synchronization for the function registry process
std::map<std::string,std::string> function_registry;

#include <hpx/util/thread_specific_ptr.hpp>
struct lua_interpreter_tag {};
hpx::util::thread_specific_ptr<
    LuaHolder,
    lua_interpreter_tag
> lua_ptr;

//--- Methods for getting/setting the Lua ptr. Ensures
//--- that no two user threads has the same Lua VM.
Lua *get_lua_ptr() {
    LuaHolder *h = lua_ptr.get();
    if(h == nullptr)
      lua_ptr.reset(h = new LuaHolder());
    Lua *lua = h->held;
    if(lua == nullptr) {
        lua = new Lua();
    } else {
      h->held = nullptr;
    }
    return lua;
}

void set_lua_ptr(Lua *lua) {
  LuaHolder *h = lua_ptr.get();
  if(h == nullptr)
    lua_ptr.reset(h = new LuaHolder());
  Lua *l = h->held;
  if(l == nullptr) {
    h->held = lua;
  } else {
    delete lua;
  }
}

template<typename T>
void dtor(T *t) {
  t->T::~T();
}

//---future data structure---//

int new_future(lua_State *L) {
    size_t nbytes = sizeof(future_type); 
    char *mem = (char *)lua_newuserdata(L, nbytes);
    luaL_setmetatable(L,future_metatable_name);
    new (mem) future_type(); // initialize with placement new
    return 1;
}

int hpx_future_clean(lua_State *L) {
    if(cmp_meta(L,-1,future_metatable_name)) {
      future_type *fnc = (future_type *)lua_touserdata(L,-1);
      dtor(fnc);
    }
    return 1;
}

int hpx_future_get(lua_State *L) {
  if(cmp_meta(L,-1,future_metatable_name)) {
    future_type *fnc = (future_type *)lua_touserdata(L,-1);
    lua_pop(L,1);
    ptr_type result = fnc->get();
    for(auto i=result->begin();i!=result->end();++i) {
      i->unpack(L);
      if(cmp_meta(L,-1,future_metatable_name)) {
        hpx_future_get(L);
      }
    }
    // Need to make sure something is returned
    if(result->size()==0)
      return 0;
  }
  return 1;
}

ptr_type luax_async2(
    string_ptr func,
    ptr_type args);

int luax_wait_all(lua_State *L) {
  int nargs = lua_gettop(L);
  std::vector<future_type> v;
  for(int i=1;i<=nargs;i++) {
    if(lua_istable(L,i) && nargs==1) {
      int top = lua_gettop(L);
      lua_pushvalue(L,i);
      lua_pushnil(L);
      int n = 0;
      while(lua_next(L,-2)) {
        lua_pushvalue(L,-2);
        n++;
        const int ix = -2;
        if(!cmp_meta(L,ix,future_metatable_name)) {
          luai_writestringerror("Argument %d to wait_all() is not a future ",n);
          return 0;
        }
        future_type *fnc = (future_type *)lua_touserdata(L,ix);
        v.push_back(*fnc);
        lua_pop(L,2);
      }
      if(lua_gettop(L) > top)
        lua_pop(L,lua_gettop(L)-top);
    } else if(cmp_meta(L,i,future_metatable_name)) {
      future_type *fnc = (future_type *)lua_touserdata(L,i);
      v.push_back(*fnc);
    }
  }

  new_future(L);
  future_type *fc =
    (future_type *)lua_touserdata(L,-1);

  hpx::wait_all(v);

  return 1;
}

ptr_type luax_when_all2(std::vector<future_type> result) {
  ptr_type pt{new std::vector<Holder>};
  table_ptr t{new table_inner()};
  int n = 1;
  for(auto i=result.begin();i != result.end();++i) {
    Holder h;
    h.var = *i;
    (t->t)[n++] = h;
  }
  Holder h;
  h.var = t;
  pt->push_back(h);

  return pt;
}

int luax_when_all(lua_State *L) {
  int nargs = lua_gettop(L);
  std::vector<future_type> v;
  for(int i=1;i<=nargs;i++) {
    if(lua_istable(L,i) && nargs==1) {
      int top = lua_gettop(L);
      lua_pushvalue(L,i);
      lua_pushnil(L);
      int n = 0;
      while(lua_next(L,-2)) {
        lua_pushvalue(L,-2);
        n++;
        const int ix = -2;
        if(!cmp_meta(L,ix,future_metatable_name)) {
          luai_writestringerror("Argument %d to wait_all() is not a future ",n);
          return 0;
        }
        future_type *fnc = (future_type *)lua_touserdata(L,ix);
        v.push_back(*fnc);
        lua_pop(L,2);
      }
      if(lua_gettop(L) > top)
        lua_pop(L,lua_gettop(L)-top);
    } else if(cmp_meta(L,i,future_metatable_name)) {
      future_type *fnc = (future_type *)lua_touserdata(L,i);
      v.push_back(*fnc);
    }
  }

  new_future(L);
  future_type *fc =
    (future_type *)lua_touserdata(L,-1);

  hpx::shared_future<std::vector<future_type>> result = hpx::when_all(v);
  *fc = result.then(hpx::util::unwrapped(boost::bind(luax_when_all2,_1)));

  return 1;
}

ptr_type get_when_any_result(hpx::when_any_result< std::vector< future_type > > result) {
  ptr_type p{new std::vector<Holder>()};
  //Holder h;
  //h.var = result.index;
  //p->push_back(h);
  table_ptr t{new table_inner()};
  (t->t)["index"].var = result.index+1;
  table_ptr t2{new table_inner()};
  for(int i=0;i<result.futures.size();i++) {
    (t2->t)[i+1].var = result.futures[i];
  }
  (t->t)["futures"].var = t2;
  Holder h;
  h.var = t;
  p->push_back(h);
  return p;
}

int luax_when_any(lua_State *L) {
  int nargs = lua_gettop(L);
  std::vector<future_type> v;
  for(int i=1;i<=nargs;i++) {
    if(lua_istable(L,i) && nargs==1) {
      int top = lua_gettop(L);
      lua_pushvalue(L,i);
      lua_pushnil(L);
      int n = 0;
      while(lua_next(L,-2)) {
        lua_pushvalue(L,-2);
        n++;
        const int ix = -2;
        if(!cmp_meta(L,ix,future_metatable_name)) {
          luai_writestringerror("Argument %d to when_any() is not a future ",n);
          return 0;
        }
        future_type *fnc = (future_type *)lua_touserdata(L,ix);
        v.push_back(*fnc);
        lua_pop(L,2);
      }
      if(lua_gettop(L) > top)
        lua_pop(L,lua_gettop(L)-top);
    } else if(cmp_meta(L,i,future_metatable_name)) {
      future_type *fnc = (future_type *)lua_touserdata(L,i);
      v.push_back(*fnc);
    }
  }

  new_future(L);
  future_type *fc =
    (future_type *)lua_touserdata(L,-1);

  hpx::future< hpx::when_any_result< std::vector< future_type > > > result = hpx::when_any(v);
  *fc = result.then(hpx::util::unwrapped(boost::bind(get_when_any_result,_1)));

  return 1;
}

const char *unwrapped_str = "**unwrapped**";

std::string getfunc(lua_State *L,int index) {
  std::string func;
  if(lua_isstring(L,index)) {
    func = lua_tostring(L,index);
  } else if(lua_isfunction(L,index)) {
    lua_pushvalue(L,index);
    assert(lua_isfunction(L,-1));
    lua_dump(L,(lua_Writer)lua_write,&func);
  } else if(lua_istable(L,index)) {
    // this is intended to be used with unwrapped
    func = unwrapped_str;
  } else {
    func = "**error**";
    abort();
  }
  return func;
}

int hpx_future_then(lua_State *L) {
  if(cmp_meta(L,1,future_metatable_name)) {
    future_type *fnc = (future_type *)lua_touserdata(L,1);
    
    CHECK_STRING(2,"Future:Then()")
    string_ptr fname{new std::string};
    *fname = getfunc(L,2);

    // Package up the arguments
    ptr_type args(new std::vector<Holder>());
    int nargs = lua_gettop(L);
    for(int i=3;i<=nargs;i++) {
      Holder h;
      h.pack(L,i);
      h.push(args);
    }
    Holder h;
    h.var = *fnc;
    h.push(args);

    new_future(L);
    future_type *fc =
      (future_type *)lua_touserdata(L,-1);
    *fc = fnc->then(boost::bind(luax_async2,fname,args));
  }
  return 1;
}

int future_name(lua_State *L) {
  lua_pushstring(L,future_metatable_name);
  return 1;
}

int open_future(lua_State *L) {
    static const struct luaL_Reg future_meta_funcs [] = {
        {"Get",&hpx_future_get},
        {"Then",&hpx_future_then},
        {"Name",future_name},
        {NULL,NULL},
    };

    static const struct luaL_Reg future_funcs [] = {
        {"new", &new_future},
        {NULL, NULL}
    };

    luaL_newlib(L,future_funcs);

    luaL_newmetatable(L,future_metatable_name);
    luaL_newlib(L, future_meta_funcs);
    lua_setfield(L,-2,"__index");

    lua_pushstring(L,"__gc");
    lua_pushcfunction(L,hpx_future_clean);
    lua_settable(L,-3);

    lua_pop(L,1);

    return 1;
}

//---guard structure--//

int new_guard(lua_State *L) {
  size_t nbytes = sizeof(guard_type);
  char *guard = (char *)lua_newuserdata(L,nbytes);
  luaL_setmetatable(L,guard_metatable_name);
  new (guard) guard_type(new Guard());
  return 1;
}

int hpx_guard_clean(lua_State *L) {
    if(cmp_meta(L,-1,guard_metatable_name)) {
      guard_type *fnc = (guard_type *)lua_touserdata(L,-1);
      dtor(fnc);
    }
    return 1;
}

int open_guard(lua_State *L) {
    static const struct luaL_Reg guard_meta_funcs [] = {
        {NULL,NULL},
    };

    static const struct luaL_Reg guard_funcs [] = {
        {"new", &new_guard},
        {NULL, NULL}
    };

    luaL_newlib(L,guard_funcs);

    luaL_newmetatable(L,guard_metatable_name);
    luaL_newlib(L, guard_meta_funcs);
    lua_setfield(L,-2,"__index");

    lua_pushstring(L,"__gc");
    lua_pushcfunction(L,hpx_guard_clean);
    lua_settable(L,-3);

    lua_pushstring(L,"name");
    lua_pushstring(L,guard_metatable_name);
    lua_settable(L,-3);
    lua_pop(L,1);

    return 1;
}
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

//---locality structure--//

int new_locality(lua_State *L) {
  size_t nbytes = sizeof(locality_type);
  char *locality = (char *)lua_newuserdata(L,nbytes);
  luaL_setmetatable(L,locality_metatable_name);
  new (locality) locality_type();
  return 1;
}

int find_here(lua_State *L) {
  new_locality(L);
  locality_type *loc = (locality_type*)lua_touserdata(L,-1);
  *loc = hpx::find_here();
  return 1;
}

int root_locality(lua_State *L) {
  new_locality(L);
  locality_type *loc = (locality_type*)lua_touserdata(L,-1);
  *loc = hpx::find_root_locality();
  return 1;
}

int all_localities(lua_State *L) {
  std::vector<hpx::naming::id_type> all_localities = hpx::find_all_localities();
  lua_createtable(L,all_localities.size(),0); 
  int n = 1;
  for(auto i = all_localities.begin();i != all_localities.end();++i) {
    lua_pushnumber(L,n);
    new_locality(L);
    locality_type *loc = (locality_type*)lua_touserdata(L,-1);
    *loc = *i;
    lua_settable(L,-3);
    n++;
  }
  return 1;
}

int remote_localities(lua_State *L) {
  std::vector<hpx::naming::id_type> remote_localities = hpx::find_all_localities();
  lua_createtable(L,remote_localities.size(),0); 
  int n = 1;
  for(auto i = remote_localities.begin();i != remote_localities.end();++i) {
    lua_pushnumber(L,n);
    new_locality(L);
    locality_type *loc = (locality_type*)lua_touserdata(L,-1);
    *loc = *i;
    lua_settable(L,-3);
    n++;
  }
  return 1;
}

int loc_str(lua_State *L) {
  std::ostringstream msg;
  int n = lua_gettop(L);
  for(int i=1;i<=n;i++) {
    if(lua_isstring(L,i)) {
      msg << lua_tostring(L,i);
    } else if(cmp_meta(L,i,locality_metatable_name)) {
      locality_type *loc = (locality_type*)lua_touserdata(L,i);
      msg << *loc;
    } else {
      return 0;
    }
  }
  lua_pop(L,n);
  lua_pushstring(L,msg.str().c_str());
  return 1;
}

int hpx_locality_clean(lua_State *L) {
    if(cmp_meta(L,-1,locality_metatable_name)) {
      locality_type *fnc = (locality_type *)lua_touserdata(L,-1);
      dtor(fnc);
    }
    return 1;
}

int loc_name(lua_State *L) {
  lua_pushstring(L,locality_metatable_name);
  return 1;
}

int hello(lua_State *L) {
  std::cout << "Hello" << std::endl;
}

int open_hpx(lua_State *L) {
    /*
    static const struct luaL_Reg hpx_meta_funcs [] = {
        {NULL,NULL},
    };
    */

    static const struct luaL_Reg hpx_funcs [] = {
        {"hello",hello},
        {"async",async},
        {"start",xlua_start},
        {"stop",xlua_stop},
        {"discover_counter_types",discover},
        {"get_counter",xlua_get_counter},
        {"get_value",xlua_get_value}, // xxx
        {NULL, NULL}
    };

    luaL_newlib(L,hpx_funcs);

    return 1;
}

int open_locality(lua_State *L) {
    static const struct luaL_Reg locality_meta_funcs [] = {
        {"str",&loc_str},
        {"Name",&loc_name},
        {NULL,NULL},
    };

    static const struct luaL_Reg locality_funcs [] = {
        {"new", &new_locality},
        {NULL, NULL}
    };

    luaL_newlib(L,locality_funcs);

    luaL_newmetatable(L,locality_metatable_name);
    luaL_newlib(L, locality_meta_funcs);
    lua_setfield(L,-2,"__index");

    lua_pushstring(L,"__gc");
    lua_pushcfunction(L,hpx_locality_clean);
    lua_settable(L,-3);

    lua_pushstring(L,"__concat");
    lua_pushcfunction(L,loc_str);
    lua_settable(L,-3);

    lua_pop(L,1);

    return 1;
}

//--- Alternative implementation of when_all
#define WHEN_ALL hpx::when_all
//#define WHEN_ALL my_when_all

template<typename Future>
void my_when_all2(
    hpx::lcos::local::promise<std::vector<Future> > *pr,
    std::vector<Future>& futs,
    int index)
{
  const int n = futs.size();
  for(;index < n;index++) {
    if(!futs[index].is_ready()) {
      auto shared_state = hpx::traits::detail::get_shared_state(futs[index]);
      auto f = hpx::util::bind(my_when_all2<Future>,pr,futs,index+1);
      shared_state->set_on_completed(f);
      return;
    }
  }
  pr->set_value(futs);
  delete pr;
}

template<typename Future>
hpx::future<std::vector<Future> > my_when_all(
    std::vector<Future>& futs)
{
  const int n = futs.size();
  for(int index=0;index < n;index++) {
    if(!futs[index].is_ready()) {
      auto p = new hpx::lcos::local::promise<std::vector<Future> >();
      auto fut = p->get_future();
      auto shared_state = hpx::traits::detail::get_shared_state(futs[index]);
      auto f = hpx::util::bind(my_when_all2<Future>,p,futs,index+1);
      shared_state->set_on_completed(f);
      return fut;
    }
  }
  return make_ready_future(futs);
}

template<typename Future>
hpx::future<std::vector<Future> > my_when_all(
    std::vector<Future>&& futs_)
{
  std::vector<Future> futs;
  futs.swap(futs_);
  const int n = futs.size();
  for(int index=0;index < n;index++) {
    if(!futs[index].is_ready()) {
      auto p = new hpx::lcos::local::promise<std::vector<Future> >();
      auto fut = p->get_future();
      auto shared_state = hpx::traits::detail::get_shared_state(futs[index]);
      auto f = hpx::util::bind(my_when_all2<Future>,p,futs,index+1);
      shared_state->set_on_completed(f);
      return fut;
    }
  }
  return make_ready_future(futs);
}

//--- Use these methods to process function inputs
boost::shared_ptr<std::vector<ptr_type> > realize_when_all_inputs_step2(ptr_type args,std::vector<future_type> results) {
  boost::shared_ptr<std::vector<ptr_type> > results_step2(new std::vector<ptr_type>());
  for(auto i=results.begin();i != results.end();++i) {
    results_step2->push_back(i->get());
  }
  #if 0
  std::vector<future_type> futs;
  for(auto j=results_step2->begin();j != results_step2->end();++j) {
    ptr_type& p = *j;
    for(auto i=p->begin();i != p->end();++i) {
      if(i->var.which() == Holder::fut_t) {
        futs.push_back(boost::get<future_type>(i->var));
      }
    }
  }
  if(futs.size() == 0)
  hpx::future<std::vector<future_type> > result = WHEN_ALL(futs);
  return result.then(hpx::util::unwrapped(boost::bind(realize_when_all_inputs_step2,args,_1)));
  #endif
  return results_step2;
}

hpx::future<boost::shared_ptr<std::vector<ptr_type> > > realize_when_all_inputs(ptr_type args) {
  std::vector<future_type> futs;
  for(auto i=args->begin();i != args->end();++i) {
    int w = i->var.which();
    if(w == Holder::fut_t) {
      futs.push_back(boost::get<future_type>(i->var));
    }
  }
  hpx::future<std::vector<future_type> > result = WHEN_ALL(futs);
  return result.then(hpx::util::unwrapped(boost::bind(realize_when_all_inputs_step2,args,_1)));
}

//--- Use these methods to process function outputs
ptr_type realize_when_all_outputs_step2(ptr_type args,std::vector<future_type> results) {
  auto j = results.begin();
  for(auto i=args->begin();i != args->end();++i) {
    int w = i->var.which();
    if(w == Holder::fut_t) {
      i->var = j->get();
      j++;
    }
  }
  return args;
}

future_type realize_when_all_outputs(ptr_type args) {
  std::vector<future_type> futs;
  for(auto i=args->begin();i != args->end();++i) {
    int w = i->var.which();
    if(w == Holder::fut_t) {
      futs.push_back(boost::get<future_type>(i->var));
    }
  }
  hpx::future<std::vector<future_type> > result = WHEN_ALL(std::move(futs));
  return result.then(hpx::util::unwrapped(boost::bind(realize_when_all_outputs_step2,args,_1)));
}

int xlua_unwrapped(lua_State *L) {
  int n = lua_gettop(L);
  lua_createtable(L,0,2);
  lua_pushstring(L,"func");
  lua_pushvalue(L,1);
  lua_settable(L,-3);
  lua_remove(L,1);
  lua_pushstring(L,"args");
  lua_createtable(L,n,0);
  for(int i=1;i<n;i++) {
    lua_pushnumber(L,i);
    lua_pushvalue(L,1);
    lua_settable(L,-3);
    lua_remove(L,1);
  }
  lua_settable(L,-3);
  return 1;
}

int call(lua_State *L) {
  int argn = 1;
  if(cmp_meta(L,1,table_metatable_name)) {
    table_ptr& tp = *(table_ptr *)lua_touserdata(L,-1);
    Holder hfunc = (tp->t)["func"];
    hfunc.unpack(L);
    Holder hargs = (tp->t)["args"];
    table_ptr tpargs = boost::get<table_ptr>(hargs.var);
    for(int i=1;i<=tpargs->size;i++) {
      (tpargs->t)[i].unpack(L);
      while(cmp_meta(L,-1,future_metatable_name)) {
        future_type *fc =
          (future_type *)lua_touserdata(L,-1);
        ptr_type p = fc->get();
        for(int i=0;i<p->size();i++) {
          (*p)[i].unpack(L);
          lua_remove(L,-2);
        }
      }
    }
    lua_remove(L,1);
    argn = lua_gettop(L);
  } else {
    std::string func;
    if(lua_istable(L,-1)) {
      // Func
      lua_pushstring(L,"func");
      lua_gettable(L,-2);
      if(lua_isfunction(L,-1)) {
        lua_insert(L,1);
      } else if(lua_isstring(L,-1)) {
        func = lua_tostring(L,-1);
        lua_getglobal(L,func.c_str());
        if(lua_isfunction(L,-1)) {
          lua_insert(L,1);
        } else {
          std::string msg = func+" is not a function";
          lua_pushstring(L,msg.c_str());
          return 0;
        }
      } else {
        lua_pushstring(L,"No function supplied to call");
        return 0;
      }
      // Args
      lua_pushstring(L,"args");
      lua_gettable(L,-2);
      if(lua_istable(L,-1)) {
        lua_pushnil(L);
        while(lua_next(L,-2) != 0) {
          while(cmp_meta(L,-1,future_metatable_name)) {
            future_type *fc =
              (future_type *)lua_touserdata(L,-1);
            ptr_type p = fc->get();
            for(int i=0;i<p->size();i++) {
              (*p)[i].unpack(L);
              lua_remove(L,-2);
            }
          }
          lua_insert(L,++argn);
          lua_pushvalue(L,-1);
          lua_remove(L,-2);
        }
      } else {
        lua_pushstring(L,"args is not a table");
        return 0;
      }
    }
    lua_remove(L,-1);
    lua_remove(L,-1);
  }
  lua_pcall(L,argn-1,100,0);
  return lua_gettop(L);
}

//--- Handle dataflow calling from Lua
ptr_type luax_dataflow2(
    string_ptr fname,
    ptr_type args,
    boost::shared_ptr<std::vector<ptr_type> > futs) {
  ptr_type answers(new std::vector<Holder>());

  {
    LuaEnv lenv;

    lua_State *L = lenv.get_state();

    bool found = false;

    lua_pop(L,lua_gettop(L));

    if(is_bytecode(*fname)) {
      if(lua_load(L,(lua_Reader)lua_read,(void *)fname.get(),0,"b") != 0) {
        std::cout << "Error in function: size=" << fname->size() << std::endl;
        SHOW_ERROR(L);
      }
    } else {
      lua_getglobal(L,fname->c_str());
      if(lua_isfunction(L,-1)) {
        found = true;
      }

      if(!found) {
        if(function_registry.find(*fname) == function_registry.end()) {
          std::cout << "Function '" << *fname << "' is not defined." << std::endl;
          return answers;
        }

        std::string bytecode = function_registry[*fname];
        if(lua_load(L,(lua_Reader)lua_read,(void *)&bytecode,fname->c_str(),"b") != 0) {
          std::cout << "Error in function: '" << *fname << "' size=" << bytecode.size() << std::endl;
          SHOW_ERROR(L);
          return answers;
        }

        lua_setglobal(L,fname->c_str());
      }
    }

    // Push data from the concrete values and ready futures onto the Lua stack
    auto f = futs->begin();
    for(auto i=args->begin();i!=args->end();++i) {
      int w = i->var.which();
      if(w == Holder::fut_t) {
        for(auto j=(*f)->begin();j != (*f)->end();++j)
          j->unpack(L);
        f++;
      } else {
        i->unpack(L);
      }
    }

    lua_getglobal(L,fname->c_str());
    lua_insert(L,1);

    //std::ostringstream msg;
    //show_stack(msg,L,__LINE__);
    // Provide a maximum number output args
    if(lua_pcall(L,args->size(),max_output_args,0) != 0) {
      SHOW_ERROR(L);
      return answers;
    }

    // Trim stack
    int nargs = lua_gettop(L);
    while(nargs > 0 && lua_isnil(L,-1)) {
      lua_pop(L,1);
      nargs--;
    }

    for(int i=1;i<=nargs;i++) {
      Holder h;
      h.pack(L,i);
      h.push(answers);
    }
    lua_pop(L,nargs);
  }

  return answers;
}

//--- Handle async calling from Lua
ptr_type luax_async2(
    string_ptr fname,
    ptr_type args) {
  ptr_type answers(new std::vector<Holder>());

  {
    LuaEnv lenv;

    lua_State *L = lenv.get_state();

    bool found = false;

    lua_pop(L,lua_gettop(L));

    if(is_bytecode(*fname)) {
      if(lua_load(L,(lua_Reader)lua_read,(void *)fname.get(),0,"b") != 0) {
        std::cout << "Error in function: size=" << fname->size() << std::endl;
        SHOW_ERROR(L);
      }
    } else {
      lua_getglobal(L,fname->c_str());
      if(lua_isfunction(L,-1)) {
        found = true;
      }

      if(!found) {
        if(function_registry.find(*fname) == function_registry.end()) {
          std::cout << "Function '" << *fname << "' is not defined." << std::endl;
          return answers;
        }

        std::string bytecode = function_registry[*fname];
        if(lua_load(L,(lua_Reader)lua_read,(void *)&bytecode,fname->c_str(),"b") != 0) {
          std::cout << "Error in function: '" << *fname << "' size=" << bytecode.size() << std::endl;
          SHOW_ERROR(L);
          return answers;
        }

        lua_setglobal(L,fname->c_str());
      }
    }

    // Push data from the concrete values and ready futures onto the Lua stack
    for(auto i=args->begin();i!=args->end();++i) {
      i->unpack(L);
    }

    const int max_output_args = 10;
    if(lua_pcall(L,args->size(),max_output_args,0) != 0) {
      //std::cout << msg.str();
      SHOW_ERROR(L);
      return answers;
    }

    // Trim stack
    int nargs = lua_gettop(L);
    while(nargs > 0 && lua_isnil(L,-1)) {
      lua_pop(L,1);
      nargs--;
    }

    for(int i=1;i<=nargs;i++) {
      Holder h;
      h.pack(L,i);
      h.push(answers);
    }
    lua_pop(L,nargs);
  }

  return answers;
}

//--- Realize futures in inputs, call dataflow function, realize futures in outputs
future_type luax_dataflow(
    string_ptr fname,
    ptr_type args) {
    // wait for all futures in input
    hpx::future<boost::shared_ptr<std::vector<ptr_type> > > f1 = realize_when_all_inputs(args);
    // pass values of all futures along with args
    future_type f2 = f1.then(hpx::util::unwrapped(boost::bind(luax_dataflow2,fname,args,_1)));
    // clean all futures out of returns
    return f2.then(hpx::util::unwrapped(boost::bind(realize_when_all_outputs,_1)));
}

int remote_reg(std::map<std::string,std::string> registry);

}

HPX_PLAIN_ACTION(hpx::luax_dataflow,luax_dataflow_action);
HPX_PLAIN_ACTION(hpx::luax_async2,luax_async_action);
HPX_PLAIN_ACTION(hpx::remote_reg,remote_reg_action);
HPX_REGISTER_BROADCAST_ACTION_DECLARATION(remote_reg_action);
HPX_REGISTER_BROADCAST_ACTION(remote_reg_action);

namespace hpx {

int luax_run_guarded(lua_State *L) {
  int n = lua_gettop(L);
  CHECK_STRING(-1,"run_guarded")
  string_ptr fname(new std::string(lua_tostring(L,-1)));
  guard_type g;
  if(n == 1) {
    g = global_guarded;
  } else if(n == 2) {
    g = *(guard_type *)lua_touserdata(L,-2);
  } else if(n > 2) {
    boost::shared_ptr<hpx::lcos::local::guard_set> gs{new hpx::lcos::local::guard_set()};
    ptr_type all_data{new std::vector<Holder>()};

    guard_type *gv = new guard_type[n];
    for(int i=1;i<n;i++) {
      guard_type g2 = *(guard_type *)lua_touserdata(L,i);
      gv[i-1]=g2;
      gs->add(g2->g);
      Holder h;
      h.var = g2->g_data;
      all_data->push_back(h);
    }
    boost::function<void()> func = boost::bind(hpx_srun,fname,all_data,gv,n);
    run_guarded(*gs,func);
    return 1;
  }
  lua_pop(L,n);
  guard_type *gv = new guard_type[1];
  gv[0] = g;
  boost::function<void()> func = boost::bind(hpx_srun,fname,g->g_data,gv,1);
  run_guarded(*g->g,func);
  return 1;
}

int isfuture(lua_State *L) {
    if(cmp_meta(L,-1,future_metatable_name)) {
      lua_pop(L,1);
      lua_pushboolean(L,1);
    } else {
      lua_pop(L,1);
      lua_pushboolean(L,0);
    }
    return 1;
}

int istable(lua_State *L) {
    if(cmp_meta(L,-1,table_metatable_name)) {
      lua_pop(L,1);
      lua_pushboolean(L,1);
    } else {
      lua_pop(L,1);
      lua_pushboolean(L,0);
    }
    return 1;
}

int islocality(lua_State *L) {
    if(cmp_meta(L,-1,locality_metatable_name)) {
      lua_pop(L,1);
      lua_pushboolean(L,1);
    } else {
      lua_pop(L,1);
      lua_pushboolean(L,0);
    }
    return 1;
}

int dataflow(lua_State *L) {

    locality_type *loc = nullptr;
    if(cmp_meta(L,1,locality_metatable_name)) {
      loc = (locality_type *)lua_touserdata(L,1);
      lua_remove(L,1);
    }

    // Package up the arguments
    ptr_type args(new std::vector<Holder>());
    int nargs = lua_gettop(L);
    for(int i=2;i<=nargs;i++) {
      Holder h;
      h.pack(L,i);
      h.push(args);
    }
    
    string_ptr fname(new std::string);
    *fname = getfunc(L,1);

    // Launch the thread
    future_type f =
      (loc == nullptr) ?
        hpx::async(luax_dataflow,fname,args) :
        hpx::async<luax_dataflow_action>(*loc,fname,args);

    new_future(L);
    future_type *fc =
      (future_type *)lua_touserdata(L,-1);
    *fc = f;
    return 1;
}

int async(lua_State *L) {

    locality_type *loc = nullptr;
    if(cmp_meta(L,1,locality_metatable_name)) {
      loc = (locality_type *)lua_touserdata(L,1);
      lua_remove(L,1);
    }

    // Package up the arguments
    ptr_type args(new std::vector<Holder>());
    int nargs = lua_gettop(L);
    
    //CHECK_STRING(1,"async")
    string_ptr fname{new std::string};
    *fname = getfunc(L,1);
    if(*fname == unwrapped_str) {
      Holder h;
      h.pack(L,1);
      h.push(args);
      *fname = "call";
    }
    for(int i=2;i<=nargs;i++) {
      Holder h;
      h.pack(L,i);
      h.push(args);
    }

    // Launch the thread
    future_type f =
      (loc == nullptr) ?
        hpx::async(luax_async2,fname,args) :
        hpx::async<luax_async_action>(*loc,fname,args);

    new_future(L);
    future_type *fc =
      (future_type *)lua_touserdata(L,-1);
    *fc = f;
    return 1;
}

void unwrap_future(lua_State *L,int index,future_type& f) {
  ptr_type p = f.get();
  if(p->size() == 1) {
    if((*p)[0].var.which() == Holder::fut_t) {
      future_type& f2 = boost::get<future_type>((*p)[0].var);
      unwrap_future(L,index,f2);
    } else {
      (*p)[0].unpack(L);
      STACK;
      lua_replace(L,index);
      STACK;
    }
  }
}

int unwrap(lua_State *L) {
    int nargs = lua_gettop(L);
    for(int i=1;i<=nargs;i++) {
      if(cmp_meta(L,i,future_metatable_name) ) {
        future_type *fc = (future_type *)lua_touserdata(L,i);
        unwrap_future(L,i,*fc);
      }
    }
    return 1;
}

int remote_reg(std::map<std::string,std::string> registry) {
	LuaEnv lenv;
    lua_State *L = lenv.get_state();
	function_registry = registry;
	for(auto i = registry.begin();i != registry.end();++i) {
		std::string& bytecode = i->second;
		if(lua_load(L,(lua_Reader)lua_read,(void *)&bytecode,i->first.c_str(),"b") != 0) {
			std::cout << "Error in function: " << i->first << " size=" << bytecode.size() << std::endl;
			SHOW_ERROR(L);
			return -1;
		}

		lua_setglobal(L,i->first.c_str());
	}
	return 0;
}

// TODO: add wrappers to conveniently get and use tables?

// TODO: Extend to include loading of libraries from .so files and running scripts.
// Example: hpx_reg('init.lua','power','pr()')
// The first is a script, the second a lib (named either power.so or libpower.so),
// the third a function. 
int hpx_reg(lua_State *L) {
	while(lua_gettop(L)>0) {
    CHECK_STRING(-1,"HPX_PLAIN_ACTION")
		if(lua_isstring(L,-1)) {
			const int n = lua_gettop(L);
			std::string fname = lua_tostring(L,-1);
			lua_getglobal(L,fname.c_str());
      std::string bytecode;
			lua_dump(L,(lua_Writer)lua_write,&bytecode);
			function_registry[fname]=bytecode;
			//std::cout << "register(" << fname << "):size=" << len << std::endl;
			const int nf = lua_gettop(L);
			if(nf > n) {
				lua_pop(L,nf-n);
			}
		}
		lua_pop(L,1);
	}

	std::vector<hpx::naming::id_type> remote_localities = hpx::find_remote_localities();
  if(remote_localities.size() > 0) {
    auto f = hpx::lcos::broadcast<remote_reg_action>(remote_localities,function_registry);
    f.get(); // in case there are exceptions
  }
  
	return 1;
}

void hpx_srun(string_ptr fname,ptr_type gdata,guard_type *gv,int ng) {
    LuaEnv lenv;
    lua_State *L = lenv.get_state();
    int n = lua_gettop(L);
    lua_pop(L,n);
    for(auto i=gdata->begin();i!=gdata->end();++i) {
      i->unpack(L);
    }
    hpx_srun(L,*fname,gdata);
    gdata->clear();

    // Trim stack
    int nargs = lua_gettop(L);
    while(nargs > 0 && lua_isnil(L,-1)) {
      lua_pop(L,1);
      nargs--;
    }

    for(int i=1;i<=nargs;i++) {
      Holder h;
      h.pack(L,i);
      guard_type g;
      int n = i <= ng ? i-1 : ng-1;
      g = gv[n];
      g->g_data->clear();
      h.push(g->g_data);
    }
    delete[] gv;
}

int hpx_run(lua_State *L) {
  CHECK_STRING(1,"hpx_run")
  std::string fname = lua_tostring(L,1);
  lua_remove(L,1);
  return hpx_srun(L,fname,global_guarded->g_data);
}

int hpx_srun(lua_State *L,std::string& fname,ptr_type gdata) {
  int n = lua_gettop(L);
  if(function_registry.find(fname) == function_registry.end()) {
    std::cout << "Function '" << fname << "' is not defined." << std::endl;
    return 0;
  }

  std::string bytecode = function_registry[fname];
  if(lua_load(L,(lua_Reader)lua_read,(void *)&bytecode,0,"b") != 0) {
    std::cout << "Error in function: " << fname << " size=" << bytecode.size() << std::endl;
    SHOW_ERROR(L);
    return 0;
  }

  if(!lua_isfunction(L,-1)) {
    std::cout << "Failed to load byte code for " << fname << std::endl;
    return 0;
  }

  lua_insert(L,1);
  if(lua_pcall(L,n,10,0) != 0) {
    SHOW_ERROR(L);
    return 0;
  }
  return 1;
}

int make_ready_future(lua_State *L) {
  ptr_type pt{new std::vector<Holder>};
  int nargs = lua_gettop(L);
  for(int i=1;i<=nargs;i++) {
    Holder h;
    h.pack(L,i);
    h.push(pt);
  }
  lua_pop(L,nargs);
  new_future(L);
  future_type *fc =
    (future_type *)lua_touserdata(L,-1);
  *fc = hpx::make_ready_future(pt);
  return 1;
}

}
