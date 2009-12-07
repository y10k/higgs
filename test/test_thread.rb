#!/usr/local/bin/ruby
# -*- coding: utf-8 -*-

require 'higgs/thread'
require 'test/unit'
require 'timeout'

Thread.abort_on_exception = true if $DEBUG

module Higgs::Test
  module ThreadParams
    COUNT_OF_THREADS = (ENV['THREADS'] || '10').to_i
    WORK_COUNT = (ENV['WORK'] || '100').to_i
    DELTA_T = (ENV['DELTA_T'] || '0.1').to_f

    if ($DEBUG) then
      puts 'thread test parameters...'
      for name in constants
        puts "#{name} = #{const_get(name)}"
      end
      puts ''
    end
  end

  class LatchTest < Test::Unit::TestCase
    include Higgs
    include ThreadParams
    include Timeout

    def test_start_wait
      latch = Latch.new

      lock = Mutex.new
      count = 0
      th_list = []
      COUNT_OF_THREADS.times do
        th_list << Thread.new{
          latch.wait
          lock.synchronize{ count += 1 }
        }
      end

      sleep(DELTA_T)
      assert_equal(0, lock.synchronize{ count })

      latch.start
      timeout(10) {
        for t in th_list
          t.join
        end
      }
      assert_equal(COUNT_OF_THREADS, lock.synchronize{ count })
    end
  end

  class CountDownLatchTest < Test::Unit::TestCase
    include Higgs
    include ThreadParams
    include Timeout

    def test_count_down_wait
      latch = CountDownLatch.new(3)

      lock = Mutex.new
      count = 0
      th_list = []
      COUNT_OF_THREADS.times do
        th_list << Thread.new{
          latch.wait
          lock.synchronize{ count += 1 }
        }
      end

      sleep(DELTA_T)
      assert_equal(0, lock.synchronize{ count })

      latch.count_down
      sleep(DELTA_T)
      assert_equal(0, lock.synchronize{ count })

      latch.count_down
      sleep(DELTA_T)
      assert_equal(0, lock.synchronize{ count })

      latch.count_down
      timeout(10) {
        for t in th_list
          t.join
        end
      }
      assert_equal(COUNT_OF_THREADS, lock.synchronize{ count })
    end
  end

  class BarrierTest < Test::Unit::TestCase
    include Higgs
    include ThreadParams
    include Timeout

    def test_wait
      barrier = Barrier.new(COUNT_OF_THREADS)

      lock = Mutex.new
      count = 0
      th_new = proc{
        Thread.new{
          barrier.wait
          lock.synchronize{ count += 1 }
        }
      }

      th_list = []
      (COUNT_OF_THREADS - 1).times do
        th_list << th_new.call
      end

      sleep(DELTA_T)
      assert_equal(0, lock.synchronize{ count })

      th_list << th_new.call
      timeout(10) {
        for t in th_list
          t.join
        end
      }
      assert_equal(COUNT_OF_THREADS, lock.synchronize{ count })
    end

    def test_not_recycle
      barrier = Barrier.new(1)
      barrier.wait
      assert_raise(RuntimeError) { barrier.wait }
    end
  end

  class SharedWorkTest < Test::Unit::TestCase
    include Higgs
    include ThreadParams
    include Timeout

    def calc
      @s = 0                    # @s's scope is over multi-threading
      for i in 1..WORK_COUNT
        @s += i
      end
      @s
    end

    def test_calc_single_thread
      a = calc
      b = calc
      assert_equal(a, b)
    end

    def test_calc_race_condition
      a = nil
      b = nil
      begin
        barrier = Barrier.new(3)

        th1 = Thread.new{
          barrier.wait
          a = calc
        }

        th2 = Thread.new{
          barrier.wait
          b = calc
        }

        barrier.wait
        th1.join
        th2.join
      end until (a != b)        # race condition
    end

    def test_result
      expected_result = calc

      latch = Latch.new
      work = SharedWork.new{
        latch.wait
        calc
      }

      barrier = Barrier.new(COUNT_OF_THREADS + 1)
      lock = Mutex.new
      count = 0

      th_list = []
      COUNT_OF_THREADS.times{|i|  # `i' should be local scope of thread block
        th_list << Thread.new{
          barrier.wait
          assert_equal(expected_result, work.result, "th#{i}")
          lock.synchronize{ count += 1 }
        }
      }

      barrier.wait
      assert_equal(0, lock.synchronize{ count })

      latch.start
      timeout(10) {
        for t in th_list
          t.join
        end
      }
      assert_equal(COUNT_OF_THREADS, lock.synchronize{ count })
      assert_equal(expected_result, work.result)
    end

    def test_no_work_block
      assert_raise(ArgumentError) { SharedWork.new }
    end

    def test_set_result
      work = SharedWork.new{ :foo }
      work.result = :bar
      assert_equal(:bar, work.result)
    end

    def test_set_result_after_work
      work = SharedWork.new{ :foo }
      assert_equal(:foo, work.result)
      work.result = :bar
      assert_equal(:bar, work.result)
    end

    def test_set_result_after_work_multithread
      barrier = Barrier.new(3)

      work = SharedWork.new{
        barrier.wait
        sleep(DELTA_T)
        :foo
      }

      th_set_result = Thread.new{
        barrier.wait
        work.result = :bar
      }

      th_work = Thread.new{
        assert_equal(:foo, work.result)
      }

      barrier.wait
      th_set_result.join
      th_work.join

      assert_equal(:bar, work.result)
    end

    def test_abort_work
      work = SharedWork.new{
        raise ThreadError, 'abort'
      }

      barrier = Barrier.new(2 + 1)
      latch = Latch.new

      t1 = Thread.new{
        barrier.wait
        assert_raise(ThreadError) {
          work.result
        }
        latch.start
      }

      t2 = Thread.new{
        barrier.wait
        latch.wait
        timeout(10) {
          assert_raise(RuntimeError) {
            work.result
          }
        }
      }

      barrier.wait
      t1.join
      t2.join

      assert_raise(RuntimeError) {
        work.result = nil
      }
    end

    def test_abort_work_many_threads
      work = SharedWork.new{
        raise RuntimeError, 'abort'
      }

      barrier = Barrier.new(COUNT_OF_THREADS + 1)
      th_list = []
      COUNT_OF_THREADS.times{|i|# `i' should be local scope of thread block
        th_list << Thread.new{
          barrier.wait
          assert_raise(RuntimeError, "thread: #{i}") {
            work.result
          }
        }
      }

      barrier.wait
      for t in th_list
        t.join
      end
    end
  end

  class ReadWriteLockTest < Test::Unit::TestCase
    include Higgs
    include ThreadParams

    def setup
      @rw_lock = ReadWriteLock.new
    end

    def test_read_lock_single_thread
      v = "foo"
      r_lock = @rw_lock.read_lock
      WORK_COUNT.times do
        assert_equal("foo", r_lock.synchronize{ v })
      end
    end

    def test_write_lock_single_thread
      count = 0
      w_lock = @rw_lock.write_lock
      WORK_COUNT.times do
        w_lock.synchronize{
          count += 1
        }
      end
      assert_equal(WORK_COUNT, count)
    end

    def test_read_lock_multithread
      v = "foo"
      th_list = []
      barrier = Barrier.new(COUNT_OF_THREADS + 1)

      COUNT_OF_THREADS.times{|i| # `i' should be local scope of thread block
        th_list << Thread.new{
          r_lock = @rw_lock.read_lock
          r_lock.synchronize{
            barrier.wait
            WORK_COUNT.times do |j|
              assert_equal("foo", v, "read_lock: #{i}.#{j}")
            end
          }
        }
      }

      barrier.wait
      for t in th_list
        t.join
      end
    end

    def test_write_lock_multithread
      count = 0
      th_list = []
      barrier = Barrier.new(COUNT_OF_THREADS + 1)

      COUNT_OF_THREADS.times do
        th_list << Thread.new{
          w_lock = @rw_lock.write_lock
          barrier.wait
          WORK_COUNT.times do
            w_lock.synchronize{
              count += 1
            }
          end
        }
      end

      barrier.wait
      for t in th_list
        t.join
      end
      assert_equal(COUNT_OF_THREADS * WORK_COUNT, count)
    end

    def test_read_write_lock_multithread
      count = 0
      th_list = []
      barrier = Barrier.new(COUNT_OF_THREADS * 2 + 1)

      COUNT_OF_THREADS.times{|i| # `i' should be local scope of thread block
        th_list << Thread.new{
          r_lock = @rw_lock.read_lock
          r_lock.synchronize{
            barrier.wait
            WORK_COUNT.times do |j|
              assert_equal(0, count, "read_lock: #{i}.#{j}")
            end
          }
        }
      }

      COUNT_OF_THREADS.times do
        th_list << Thread.new{
          w_lock = @rw_lock.write_lock
          barrier.wait
          WORK_COUNT.times do
            w_lock.synchronize{
              count += 1
            }
          end
        }
      end

      barrier.wait
      for t in th_list
        t.join
      end
      assert_equal(COUNT_OF_THREADS * WORK_COUNT, count)
    end

    def test_write_read_lock_multithread
      count = 0
      th_list = []
      barrier = Barrier.new(COUNT_OF_THREADS + 2)

      th_list << Thread.new{
        w_lock = @rw_lock.write_lock
        w_lock.synchronize{
          barrier.wait
          COUNT_OF_THREADS.times do
            WORK_COUNT.times do
              count += 1
            end
          end
        }
      }

      COUNT_OF_THREADS.times{|i| # `i' should be local scope of thread block
        th_list << Thread.new{
          r_lock = @rw_lock.read_lock
          barrier.wait
          r_lock.synchronize{
            assert_equal(COUNT_OF_THREADS * WORK_COUNT, count, "read_lock: #{i}")
          }
        }
      }

      barrier.wait
      for t in th_list
        t.join
      end
    end

    def test_read_write_race
      count = 0
      value = true
      th_list = []
      barrier = Barrier.new(COUNT_OF_THREADS + 2)

      COUNT_OF_THREADS.times{|i| # `i' should be local scope of thread block
        th_list << Thread.new{
          r_lock = @rw_lock.read_lock
          barrier.wait
          WORK_COUNT.times do
            r_lock.synchronize{
              p "#{i}: #{count}" if $DEBUG
              assert_equal(true, value, "read_lock: #{i}")
            }
          end
        }
      }

      th_list << Thread.new{
        w_lock = @rw_lock.write_lock
        barrier.wait
        WORK_COUNT.times do
          w_lock.synchronize{
            count += 1
            value = false
            value = true
          }
        end
      }

      barrier.wait
      for t in th_list
        t.join
      end
    end
  end

  class PoolTest < Test::Unit::TestCase
    include Higgs
    include ThreadParams

    class Counter
      def initialize
        @value = 0
      end

      attr_reader :value

      def count
        @value += 1
      end
    end

    def setup
      @pool = Pool.new(2) { Counter.new }
    end

    def test_transaction
      th_list = []
      barrier = Barrier.new(COUNT_OF_THREADS + 1)

      COUNT_OF_THREADS.times{|i| # `i' should be local scope of thread block
        th_list << Thread.new{
          barrier.wait
          WORK_COUNT.times do |j|
            @pool.transaction{|c|
              v = c.value
              c.count
              assert_equal(v + 1, c.value, "thread: #{i}.#{j}")
            }
          end
        }
      }

      barrier.wait
      for t in th_list
        t.join
      end

      n = 0
      s = 0
      @pool.shutdown{|c|
        n += 1
        s += c.value
      }
      assert_equal(@pool.size, n)
      assert_equal(WORK_COUNT * COUNT_OF_THREADS, s)
    end

    def test_shutdown
      th_list = []
      barrier = Barrier.new(COUNT_OF_THREADS + 1)
      latch = CountDownLatch.new(COUNT_OF_THREADS)

      COUNT_OF_THREADS.times{|i| # `i' should be local scope of thread block
        th_list << Thread.new{
          barrier.wait
          assert_raise(Pool::ShutdownException) {
            j = 0
            loop do
              @pool.transaction{|c|
                v = c.value
                c.count
                assert_equal(v + 1, c.value, "thread: #{i}.#{j}")
                latch.count_down if (j > WORK_COUNT)
              }
              j += 1
            end
          }
        }
      }

      barrier.wait
      latch.wait
      @pool.shutdown
      for t in th_list
        t.join
      end
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
