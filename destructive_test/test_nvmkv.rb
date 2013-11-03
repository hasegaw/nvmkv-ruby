# #!ruby -rnvmkv_driver
require 'test/unit'
require 'nvmkv'

class TC_NVMKV < Test::Unit::TestCase
  include NVMKV

  def store_open
    store = NVMKVStore.new
    store.open('/dev/fioa', 100, Nvmkv::KV_ARBITRARY_EXPIRY)
    return store
  end

  # NVMKVStore Basic test.
  #
  # Test NVMKV Basic operations. 
  def test_NVMKV_Store
    print "test_NVMKV_Store\n"

    store = self.store_open
    pool = store.new_pool('store_test')

    # store_info
    si = store.store_info
    assert(si.version >= 0, "kv_get_store_info (version)")
    assert(si.num_pools >= 0, "kv_get_store_info (num_pools)")
    assert(si.max_pools >= 0, "kv_get_store_info (max_pools)")
    assert(si.expiry_mode >= 0, "kv_get_store_info (expiry_mode)")
    assert(si.global_expiry >= 0, "kv_get_store_info (global_expiry)")
    assert(si.num_keys >= 0, "kv_get_store_info (num_keys)")
    assert(si.free_space >= 0, "kv_get_store_info (free_space)")

    store.close
  end

  def test_NVMKV_Pool
    print "test_NVMKV_Pool\n"

    store = self.store_open
    pool = store.new_pool('pool_test')
 
    # pool_info
    pi = pool.pool_info
    assert(pi.version, "kv_get_pool_info (version)")
    assert(pi.pool_status, "kv_get_pool_info (pool_status)")

    pool.delete("ruby")
    assert(!pool.is_exist("ruby"), "pre-condition failed (key exists)")

    r = pool.put("ruby", "sapphire")
    assert(r > 0, "put")

    r = pool.get("ruby")
    assert(r.value == "sapphire", "get")
    assert(r.value_len == 8, "NVMKVValue: value_len")
    assert(r.gen_count >= 0, "NVMKVValue: gen_count")
    #assert(r.expiry >= 0, "NVMKVValue: expiry")

    r = pool.is_exist("ruby")
    assert(r, "is_exist should be true after put")

    r = pool.delete("ruby")
    assert(r == 0, "delete failed")

    r = pool.is_exist("ruby")
    assert(!r, "is_exist should be false after delete")

    pool.put("one", "1")
    pool.put("two", "2")
    pool.put("three", "3")

    ki = pool.key_info("one")
    assert(ki.value_len > 0, "get_key_info broken");

    store.close
  end

  def test_NVM_Pool_expiry
    print "test_NVMKV_expiry\n"

    store = self.store_open
    pool = store.new_pool('expiry')
    printf("expiry mode check: %d == %d\n", store.store_info.expiry_mode,
    Nvmkv::KV_ARBITRARY_EXPIRY)
    assert(store.store_info.expiry_mode == Nvmkv::KV_ARBITRARY_EXPIRY,
        "KV_ARBITARY_EXPIRY mode is off")

    pool.put('expiry0', 'data', 0)
    pool.put('expiry10', 'data', 10)
    pool.put('expiry20', 'data', 20)

    r = pool.get('expiry0')
    assert(r.expiry == 0, 'pool.get().expiry == 0')
    r = pool.get('expiry10')
    assert(r.expiry > 0, 'pool.get().expiry > 0')
    r = pool.get('expiry20')
    assert(r.expiry > 0, 'pool.get().expiry > 0')

    assert(pool.is_exist('expiry0') != 0, 'expiry test: key expiry0 ')
    assert(pool.is_exist('expiry10') != 0, 'expiry test: key expiry10 ')
    assert(pool.is_exist('expiry20') != 0, 'expiry test: key expiry20 ')
    sleep 5
    assert(pool.is_exist('expiry0'), 'expiry test: key expiry0 ')
    assert(pool.is_exist('expiry10'), 'expiry test: key expiry10 ')
    assert(pool.is_exist('expiry20'), 'expiry test: key expiry20 ')
    sleep 10
    assert(pool.is_exist('expiry0'), 'expiry test: key expiry0 (15)')
    assert(! pool.is_exist('expiry10'), 'expiry test: key expiry10 (15)')
    assert(pool.is_exist('expiry20'), 'expiry test: key expiry20 (15)')
    sleep 10
    assert(pool.is_exist('expiry0'), 'expiry test: key expiry0  (25)')
    assert(! pool.is_exist('expiry10'), 'expiry test: key expiry10  (25)')
    assert(! pool.is_exist('expiry20'), 'expiry test: key expiry20 (25) ')

    if false then 
      # Don't test global_expiry today.
      store.global_expiry = 0
      assert(store.global_expiry == 0, "setter/getter of global_expiry == 0") 
      assert(store.store_info.global_expiry == 0, "global_expiry == 0")
  
      store.global_expiry = 100
      assert(store.global_expiry == 100, "setter/getter of global_expiry == 100")
      assert(store.store_info.global_expiry == 100, "global_expiry == 100")
  
      store.global_expiry = 0
      assert(store.expiry_mode >= 0, "getter of expiry_mode")
    end
    store.close
  end 
    

  # NVMKV Persistence test.
  #
  # The data on the pool should be remained during NVMKV session.
  def test_NVMKV_Pool_persistence
    return
    print "test_NVMKV_Pool_persistence\n"

    store = self.store_open
    store.delete_all
    pool = store.new_pool('hello')
    pool.put("persistence", "non-volatile")
    assert(pool.get("persistence").value == "non-volatile", "persistence test (get - 1)")
    store.close

    store = self.store_open
    pool = store.new_pool('hello')
    assert(pool.get("persistence").value == "non-volatile", "persistence test (get - 1)")
    store.close
  end
    
  # batch operation test 
  def test_NVMKV_Pool_batch
    print "test_NVMKV_Pool_batch\n"

    store = self.store_open
    pool = store.new_pool('pool_test')
 
    # batch_put
    batch = [
      {"key"=>"hello", "value"=>"world"}, 
      {"key"=>"this is ", "value"=>"a pen", "expiry"=>3600},
    ];

    batch.each{|entry|
      pool.delete(entry.fetch('key'))
      assert(! pool.is_exist(entry.fetch('key')), "batch_put pre-condition test")
    }

    pool.batch_put(batch)

    batch.each{|entry|
      assert(pool.get(entry.fetch('key')).value == entry.fetch('value'), "batch_put post-condition test")
    }

    batch = [
      {"key"=>"hello", "value"=>"world_2_"}, 
      {"key"=>"this is ", "value"=>"a pen_2_", "expiry"=>3600},
    ];
    pool.batch_put(batch)

    itr_count1 = 0
    pool.each_ex{|e|
      assert(pool.get(e.key).value == e.value, "batch_test (each_ex)")
      # printf("each_ex(): %s %s\n", e.key, e.value)
      itr_count1 += 1
    }

    itr_count2 = 0
    pool.each{|k,v|
      assert(pool.get(k).value == v, "batch_test (each)")
      # printf("each(): %s %s\n", k, v)
      itr_count2 += 1
    }

    itr_count3 = 0
    batch.each{|entry|
      assert(pool.get(entry.fetch('key')).value == entry.fetch('value'), "batch_put post-condition test")
      pool.delete(entry.fetch('key'))
      itr_count3 += 1
    }
    # printf("iterator count: %d %d %d\n", itr_count1, itr_count2, itr_count3);
    assert(itr_count1 == itr_count2,
        "iterator count check")

    store.close
 
  end 
end
