# #!ruby -rnvmkv_driver
require 'test/unit'
require 'nvmkv'

class TC_NVMKV < Test::Unit::TestCase
  include NVMKV
  def get_thread(pool, start, count)
    ok = 0
    i = start
    batch = Array.new
    count.times do |m|
      n = i + m
      k = sprintf('key%d', n)
      v = pool.get(k)
      if (v.value != sprintf("value%d", n)) then
        printf("comparsion error: key %s value\n", k)
      else 
        ok += 1
      end
    end
    printf("get_thread: count %d ok %d\n", count, ok);
  end

  def put_thread(pool, start, count)
    i = start
    batch = Array.new
    count.times do |m|
      n = i + m
      pool.put(sprintf('key%d', n), sprintf('value%d', n))
    end
  end

  def batch_put_thread(pool, start, count)
    i = start
    batch = Array.new
    count.times do |m|
      n = i + m
      h = {'key' => sprintf('key%d', n), 'value' => sprintf('value%d', n) }
      batch.push h
      if (batch.length >= 64) then
        pool.batch_put(batch)
        batch.clear
      end
    end
    if (batch.length > 0) then
      pool.batch_put(batch)
    end
  end

  def store_open
    store = NVMKVStore.new
    store.open('/dev/fioa', 100, Nvmkv::KV_ARBITRARY_EXPIRY)
    return store
  end

  # NVMKV bulk opertion test
  #
  #
  def test_NVMKV_bulkops
    print "test_NVMKV_bulkops\n"

    store = self.store_open
    pool = store.new_pool('bulk')

    total_kv_count = 1000000
    num_threads = 24
    count = total_kv_count / num_threads
    
    start = 1
    keys_before_batch = store.store_info.num_keys;

    threads = Array.new
    num_threads.times do |thread_id|
      t = Thread.new(start, count) do |start,count|
        batch_put_thread(pool, start, count)
      end
      threads.push t
      start += count
    end

    num_threads.times do |thread_id|
      threads.pop.join
    end

    keys_after_batch = store.store_info.num_keys;
    printf("batch result: num_threads %d count %d total %d | before %d after %d total %d\n",
        num_threads, count, num_threads * count,
        keys_before_batch, keys_after_batch, keys_after_batch - keys_before_batch);
    start = 1
    num_threads.times do |thread_id|
      t = Thread.new(start, count) do |start,count|
        get_thread(pool, start, count)
      end
      threads.push t
      start += count
    end

    num_threads.times do |thread_id|
      threads.pop.join
    end

#    assert(num_threads * count == keys_after_batch - keys_before_batch, "batch_put comfirmation");
#    pool.each {|value| p value}
    store.close
  end
end
