#!/usr/local/bin/ruby
# -*- coding: utf-8 -*-

require 'higgs/cache'
require 'higgs/thread'
require 'test/unit'
require 'timeout'

Thread.abort_on_exception = true if $DEBUG

module Higgs::Test
  class LRUCacheTest < Test::Unit::TestCase
    include Higgs

    def setup
      @cache = LRUCache.new(3)
    end

    def test_empty
      assert_nil(@cache['apple'])
    end

    def test_store_fetch
      @cache[:a] = 1
      assert_equal(1, @cache[:a])
    end

    def test_store_fetch_full
      @cache[:a] = 1
      @cache[:b] = 2
      @cache[:c] = 3
      assert_equal(1, @cache[:a])
      assert_equal(2, @cache[:b])
      assert_equal(3, @cache[:c])
    end

    def test_fetch_not_defined_value
      assert_nil(@cache[:a])
      assert_nil(@cache[:b])
      assert_nil(@cache[:c])
    end

    def test_delete
      @cache[:a] = 1
      @cache[:b] = 2
      @cache[:c] = 3
      @cache.delete(:b)
      assert_equal(1,  @cache[:a])
      assert_equal(nil, @cache[:b])
      assert_equal(3, @cache[:c])
    end

    def test_store_fetch_overflow
      @cache['a'] = 1
      @cache['b'] = 2
      @cache['c'] = 3
      @cache['d'] = 4
      assert_equal(nil, @cache['a'])
      assert_equal(2, @cache['b'])
      assert_equal(3, @cache['c'])
      assert_equal(4, @cache['d'])
    end

    def test_store_fetch_reorder_read
      @cache['a'] = 1
      @cache['b'] = 2
      @cache['c'] = 3
      @cache['b']
      @cache['d'] = 4
      @cache['e'] = 5
      assert_equal(nil, @cache['a'])
      assert_equal(2, @cache['b'])
      assert_equal(nil, @cache['c'])
      assert_equal(4, @cache['d'])
      assert_equal(5, @cache['e'])
    end

    def test_store_fetch_reorder_write
      @cache['a'] = 1
      @cache['b'] = 2
      @cache['c'] = 3
      @cache['b'] = 4
      @cache['d'] = 5
      @cache['e'] = 6
      assert_equal(nil, @cache['a'])
      assert_equal(4, @cache['b'])
      assert_equal(nil, @cache['c'])
      assert_equal(5, @cache['d'])
      assert_equal(6, @cache['e'])
    end
  end

  class SharedWorkCacheTest < Test::Unit::TestCase
    include Higgs

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
      barrier = Higgs::Barrier.new(NUM_OF_THREADS + 1)
      th_list = []

      result_list = [ nil ] * NUM_OF_THREADS
      NUM_OF_THREADS.times{|i|  # `i' should be local scope of thread block
        th_list << Thread.new{
          barrier.wait
          result_list[i] = calc(WORK_COUNT)
        }
      }

      barrier.wait
      for t in th_list
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
        barrier = Higgs::Barrier.new(NUM_OF_THREADS + 1)
        th_list = []
        NUM_OF_THREADS.times{|i|  # `i' should be local scope of thread block
          th_list << Thread.new{
            barrier.wait
            assert_equal(expected_result, @cache[WORK_COUNT], "#{n}th: th#{i}")
          }
        }

        barrier.wait
        for t in th_list
          t.join
        end
        assert_equal(2, @calc_calls, "#{n}th")
      end
    end

    def test_store
      @cache[WORK_COUNT] = 0
      assert_equal(0, @cache[WORK_COUNT])
      assert_equal(0, @calc_calls, 'no call')
    end

    def test_store_overwrite
      assert(@cache[WORK_COUNT] != 0)
      assert_equal(1, @calc_calls)
      @cache[WORK_COUNT] = 0
      assert_equal(0, @cache[WORK_COUNT])
      assert_equal(1, @calc_calls)
    end
  end

  class SharedWorkCacheNoWorkBlockTest < Test::Unit::TestCase
    include Higgs

    def test_no_work_block
      assert_raise(ArgumentError) {
        SharedWorkCache.new
      }
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
