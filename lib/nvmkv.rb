#!/usr/bin/ruby -rnvmkv_driver
require 'nvmkv_driver'
module NVMKV

  # Entity class for NVMKV.
  class NVMKVValue
    def initialize(value_hash)
      @key = value_hash.fetch('key', nil)
      @key_len = value_hash.fetch('key_len', nil)
      @value = value_hash.fetch('value', nil)
      @value_len = value_hash.fetch('value_len', nil)
      @expiry = value_hash.fetch('expiry', nil)
      @gen_count = value_hash.fetch('gen_count', nil)
      @reserved1 = value_hash.fetch('reserved1', nil)
    end

    def value
      return @value if @value != nil
      raise IndexError('the field unavailable')
    end

    def value_len
      return @value_len if @value_len != nil
      raise IndexError('the field unavailable')
    end

    def key
      return @key if @key != nil
      raise IndexError('the field unavailable')
    end

    def key_len
      return @key_len if @key_len != nil
      raise IndexError('the field unavailable')
    end

    def expiry
      return @expiry if @expiry != nil
      raise IndexError('the field unavailable')
    end

    def gen_count
      return @gen_count if @gen_count != nil
      raise IndexError('the field unavailable')
    end

    def reserved1
      return @reserved1 if @reserved1 != nil
      raise IndexError('the field unavailable')
    end
  end

  # Interface to NVMKV Store information.
  #
  # +version+ -  Version of NVMKV Store
  # +num_pools+ -  Number of pools on the Key-Value Store 
  # +max_pools+ -  Maximum number of pools on the Key-Value Store 
  # +expiry_mode+ -  Expiry mode
  # +global_expiry+ -  Global Expiry (in second). Valid only for KV_GLOBAL_EXPIRY mode
  # +num_keys+ -  Count of Key-Value pairs on the store
  # +free_space+ -  Free (available) space
  class NVMKVStoreInfo

    # Internal constructor. Should not be called by user application
    # directly.
    def initialize(value_hash)
      @version = value_hash.fetch('version')
      @num_pools = value_hash.fetch('num_pools')
      @max_pools = value_hash.fetch('max_pools')
      @expiry_mode = value_hash.fetch('expiry_mode')
      @global_expiry = value_hash.fetch('global_expiry')
      @num_keys = value_hash.fetch('num_keys')
      @free_space = value_hash.fetch('free_space')
    end

    attr_reader :version, :num_pools, :max_pools, :expiry_mode, :global_expiry, :num_keys, :free_space
  end

  class NVMKVPoolInfo
    def initialize(value_hash)
      @version  = value_hash.fetch("version")
      @pool_status = value_hash.fetch("pool_status")
    end

    attr_reader :version, :pool_status
  end


  # Interface for OpenNVM NVMKV Pool. 
  class NVMKVPool
    # KV store id
    @kv_id = 0

    # KV pool id
    @pool_id = 0

    # create a NVMKVPool instance. Internally used by NVMKV.
    def initialize(_kv_id, _pool_id)
      @kv_id = _kv_id
      @pool_id = _pool_id
    end
  
    # Put a key-value pair into the pool.
    #
    # key :: Key (string, <= 128 Bytes) 
    # value :: Value (string, <= 1MB-1KB)
    # expiry :: expiry, in seconds relative to the insertion time (default: 0)
    # replace :: replace the value if the key already exists (default: true)
    # gen_count :: the generation count for the key(default: 0)
    def put(key, value, expiry=0, replace=1, gen_count=0) 
      return Nvmkv::kv_put(@kv_id, @pool_id, key, value, expiry, replace, gen_count)
    end
 
    # Get a key-value pair from the pool.
    #
    # key :: Key (string, <= 128 Bytes) 
    def get(key)
      v = Nvmkv::kv_get(@kv_id, @pool_id, key)
      return (v != nil ? NVMKVValue.new(v) : nil)
    end
  
    # Delete a key-value pair from the pool.
    #
    # key :: Key (string, <= 128 Bytes) 
    def delete(key)
      return Nvmkv::kv_delete(@kv_id, @pool_id, key)
    end
  
  
    def is_exist(key)
      return Nvmkv::kv_exists(@kv_id, @pool_id, key) != 0
    end
 
    # Batch-put key-value pairs into the pool.
    #
    # batch_put() allows two types of argument:
    # 
    # 1. Hash with string value.  
    #      pool.batch_put({ "key1" => "value1", "key2" => "value2" })
    # 2. Hash with hash value.  
    #      pool.batch_put({
    #         "key1" => { value => "value1", "expiry" => 100 },
    #         "key2" => { value => "value2", "expiry" => 100 }
    #      )
    #      available fields: value, expiry, gen_count, replace, reserved1
    #
    def batch_put(keys, expiry=0, gen_count=0, replace=1, reserved1=0)
      batch = []
      if keys.is_a?(Array) then
        keys.each{|entry| batch.push([
            entry.fetch('key'),
            entry.fetch('value'),
            entry.fetch('expiry', 0),
            entry.fetch('gen_count', 0),
            entry.fetch('replace', 1),
            entry.fetch('reserved1', 0)
        ])}
      elsif keys.is_a?(Hash) then
        keys.each{|key| batch.push([
            key,
            keys[key],
            expiry,
            gen_count,
            replace,
            reserved1
        ])}
      else
        raise "invalid data"
      end
  
      Nvmkv.kv_batch_put(@kv_id, @pool_id, batch)
    end
 
    # Return pool information.
    def pool_info
      pi = Nvmkv::kv_get_pool_info(@kv_id, @pool_id)
      return (pi != nil ? NVMKVPoolInfo.new(pi) : nil)
    end
 
    # Retrieve a information of key-value pair.
    #
    # key :: Key to retrieve the information
    def key_info(key)
      ki = Nvmkv::kv_get_key_info(@kv_id, @pool_id, key)
      return (ki != nil ? NVMKVValue.new(ki) : nil)
    end
 
    # Iterator. Return each key-value pairs as string variables.
    # 
    # Usage:
    #   pool.each({|key,value| printf("key:%s value:%s\n", key, value)}
    def each
      self.each_ex{|key_info| yield(key_info.key, key_info.value)} 
    end
  
    # Iterator. Return each key-value pairs as a hash objcet.
    # 
    # Usage:
    #   pool.each({|e| printf("key:%s value:%s\n", e.key), e.value)}
    def each_ex
      itr_id = Nvmkv.kv_begin(@kv_id, @pool_id)
      if itr_id == -1 then
        # FixMe: error
        return
      end
  
      r = 0
      while r == 0 do
        kv = Nvmkv.kv_get_current(@kv_id, itr_id)
        yield NVMKVValue.new(kv) if kv != nil
        # FIXME: RuntimeError if kv == nil
  
        r = Nvmkv.kv_next(@kv_id, itr_id)
        # FIXME: if kv == -1 then error-handling
      end
      Nvmkv.kv_iteration_end(@kv_id, itr_id)
    end
  
    # Delete the pool.
    def pool_delete
      Nvmkv.kv_pool_delete(@kv_id, @pool_id)
      # FIXME: object should be disposed
    end
  end
  
  # NMVKV Store
  # Interface to access NVMKV Key-Value Store Library.
  class NVMKVStore
    # NVMKVStore implementation allows the access to OpenNVM NVMKV
    # Key-Value store library.
    #
    # example:
    #   store = NVMKVStore.new
    #   store.open('/dev/fioa') # path to the store
    #   pool  = store.new_pool('inventry')
    #
    #   pool.put('key1', 'value1')
    #   if (pool.is_exist('key1') then
    #     ....
    #   end
    #   pool.get('key1', 'value1')
    #   pool.each do |entry| .... end
    #
    #   store.close

    # File descriptor (fd) of NVMKV store.
    @file_fd = 0

    # KV Store ID of NVMKV Store.
    @kv_id = 0

    # Open a key-value store library.
    #
    # path :: path to OpenNVM device. eg) /dev/fioa
    # max_pools :: Maximum pool size in this KV Store (valid only for KV Store initialization)
    # expiry_mode :: Expiry mode (vaild only for KV Store initialization)
    def open(path, max_pools = Nvmkv::MAX_POOLS, expiry_mode = 0)
       @file_fd = Nvmkv::file_open(path);
      kv_version = 0
      @kv_id = Nvmkv.kv_open(@file_fd, kv_version, max_pools, expiry_mode);
      @kv_id
    end
 
    # Close the key-value store.  
    def close
      Nvmkv.kv_close(@kv_id)
      f= IO.open(@file_fd).close
    end
 
    # Retrive KV Store information. 
    def store_info
      si = Nvmkv::kv_get_store_info(@kv_id)
      if si != nil then
        NVMKVStoreInfo.new(si)
      else
        nil
      end
    end
  
    # Retrive the pool (NVMKVPool object) specified with pool_id.
    # 
    # pool_id: KV Pool ID
    def pool(pool_id)
      NVMKVPool.new(@kv_id, pool_id)
    end
  
    # Create or open a new pool (NVMKVPool object) specified with tag.
    # 
    # tag :: tag (<=16 bytes)
    def new_pool(tag)
      pool_id = Nvmkv.kv_pool_create(@kv_id, tag)
      NVMKVPool.new(@kv_id, pool_id)
    end
 
    # specify global expiry timer (in second).
    # global_expiry is valid only for arbitrary expiry mode.
    # 
    # expiry :: expiry, in seconds relative to the insertion time
    def global_expiry=(expiry)
      r = Nvmkv.kv_set_global_expiry(@kv_id, expiry)
      printf("k_set_global_expiry(%d): %d\n", expiry, r)
    end
  
    # Retrive global_expiry timer (in second).
    # global_expiry is valid only for arbitrary expiry mode.
    def global_expiry
      Nvmkv.kv_get_store_info(@kv_id).fetch("global_expiry")
    end
  
    # Retrive expiry mode.
    def expiry_mode
      Nvmkv.kv_get_store_info(@kv_id).fetch("expiry_mode")
    end
  
    # Delete all pools (including key-value pairs) from the store. 
    def delete_all
      Nvmkv.kv_delete_all(@kv_id)
    end
  
    # Delete the specified pool from the store.
    def delete_pool(pool_id)
      # FIXME: tag validation
      Nvmkv.kv_pool_delete(@kv_id, pool_id)
    end
  end
end
