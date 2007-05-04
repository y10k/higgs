#!/usr/local/bin/ruby

require 'higgs/cache'
require 'higgs/thread'
require 'test/unit'
require 'timeout'

module Higgs::CacheTest
  # for ident(1)
  CVS_ID = '$Id$'

  class LRUCacheTest < Test::Unit::TestCase
    include Higgs::Cache

    CACHE_LIMIT_SIZE = 10

    def setup
      @cache = LRUCache.new(CACHE_LIMIT_SIZE)
    end

    def test_store_and_fetch
      @cache[:foo] = 'apple'
      @cache[:bar] = 'banana'
      @cache[:baz] = 'orange'
      assert_equal('apple',  @cache[:foo])
      assert_equal('banana', @cache[:bar])
      assert_equal('orange', @cache[:baz])
    end

    def test_fetch_not_defined_value
      assert_nil(@cache[:foo])
      assert_nil(@cache[:bar])
      assert_nil(@cache[:baz])
    end

    def test_delete
      @cache[:foo] = 'apple'
      @cache[:bar] = 'banana'
      @cache[:baz] = 'orange'
      @cache.delete(:bar)
      assert_equal('apple',  @cache[:foo])
      assert_equal(nil,      @cache[:bar])
      assert_equal('orange', @cache[:baz])
    end

    def test_LRU_read
      CACHE_LIMIT_SIZE.times do |i|
	@cache[i] = i.to_s
      end
      CACHE_LIMIT_SIZE.times do |i|
	assert_equal(i.to_s, @cache[i], "#{i}")
      end

      old_key = 0
      last_key = CACHE_LIMIT_SIZE - 1
      new_key = CACHE_LIMIT_SIZE

      @cache[new_key] = new_key.to_s
      assert_equal(nil,           @cache[old_key])
      assert_equal(last_key.to_s, @cache[last_key])
      assert_equal(new_key.to_s,  @cache[new_key])
    end

    def test_LRU_write
      CACHE_LIMIT_SIZE.times do |i|
	@cache[i] = i.to_s
      end
      (0...CACHE_LIMIT_SIZE).to_a.reverse_each do |i|
	@cache[i] = i.to_s
      end

      old_key = CACHE_LIMIT_SIZE - 1
      last_key = 0
      new_key = CACHE_LIMIT_SIZE

      @cache[new_key] = new_key.to_s
      assert_equal(nil,           @cache[old_key])
      assert_equal(last_key.to_s, @cache[last_key])
      assert_equal(new_key.to_s,  @cache[new_key])
    end
  end

  class SharedWorkCacheTest < Test::Unit::TestCase
    include Higgs::Cache

    def setup
      @calc_calls = 0
      @cache = SharedWorkCache.new{|key| calc(key) }
    end

    def calc(n)
      @calc_calls += 1
      @s = 0                    # @s's scope is over multi-threading
      for i in 1..n
        @s += i
      end
      @s
    end

    def test_calc
      assert_equal(1, calc(1))
      assert_equal(1, @calc_calls)

      assert_equal(3, calc(2))
      assert_equal(2, @calc_calls)

      assert_equal(6, calc(3))
      assert_equal(3, @calc_calls)

      assert_equal(10, calc(4))
      assert_equal(4, @calc_calls)

      assert_equal(15, calc(5))
      assert_equal(5, @calc_calls)
    end

    def test_fetch
      100.times do |i|
        assert_equal(1,  @cache[1], "loop(#{i})")
        assert_equal(3,  @cache[2], "loop(#{i})")
        assert_equal(6,  @cache[3], "loop(#{i})")
        assert_equal(10, @cache[4], "loop(#{i})")
        assert_equal(15, @cache[5], "loop(#{i})")
        assert_equal(5, @calc_calls)
      end
    end

    def test_delete
      assert_equal(false, @cache.delete(5))
      assert_equal(15, @cache[5])
      assert_equal(1, @calc_calls)

      assert_equal(true, @cache.delete(5))
      assert_equal(15, @cache[5])
      assert_equal(2, @calc_calls, 'reload')
    end

    NUM_OF_THREADS = 10
    WORK_COUNT = 10000

    def calc_race_condition
      barrier = Higgs::Thread::Barrier.new(NUM_OF_THREADS + 1)
      th_grp = ThreadGroup.new

      result_list = [ nil ] * NUM_OF_THREADS
      NUM_OF_THREADS.times{|i|	# `i' should be local scope of thread block
	th_grp.add Thread.new{
	  barrier.wait
	  result_list[i] = calc(WORK_COUNT)
	}
      }

      barrier.wait
      for t in th_grp.list
	t.join
      end

      expected_value = calc(WORK_COUNT)
      result_list.find{|v| v != expected_value }
    end
    private :calc_race_condition

    def test_multi_thread_fetch
      count = 0
      timeout(10) {
	begin
	  count += 1
	end until (calc_race_condition)
      }

      @calc_calls = 0
      expected_result = calc(WORK_COUNT)
      assert_equal(1, @calc_calls)

      count.times do |n|
	barrier = Higgs::Thread::Barrier.new(NUM_OF_THREADS + 1)
	th_grp = ThreadGroup.new
	NUM_OF_THREADS.times{|i|  # `i' should be local scope of thread block
	  th_grp.add Thread.new{
	    barrier.wait
	    assert_equal(expected_result, @cache[WORK_COUNT], "#{n}th: th#{i}")
	  }
	}

	barrier.wait
	for t in th_grp.list
	  t.join
	end
	assert_equal(2, @calc_calls, "#{n}th")
      end
    end
  end

  class SharedWorkCacheNoWorkBlockTest < Test::Unit::TestCase
    include Higgs::Cache

    def test_no_work_block
      assert_raise(RuntimeError) {
        SharedWorkCache.new
      }
    end
  end
end
