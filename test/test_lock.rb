#!/usr/local/bin/ruby

require 'rubyunit'
require 'tank/lock'
require 'tank/thread'

module Tank::Test
  class ReadWriteLockTest < RUNIT::TestCase
    WORK_COUNT = 100
    THREAD_COUNT = 10

    def setup
      @rw_lock = Tank::Lock::ReadWriteLock.new
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
      th_grp = ThreadGroup.new
      barrier = Tank::Thread::Barrier.new(THREAD_COUNT + 1)

      THREAD_COUNT.times{|i|
        th_grp.add Thread.new{
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
      for t in th_grp.list
        t.join
      end
    end

    def test_write_lock_multithread
      count = 0
      th_grp = ThreadGroup.new
      barrier = Tank::Thread::Barrier.new(THREAD_COUNT + 1)

      THREAD_COUNT.times do
        th_grp.add Thread.new{
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
      for t in th_grp.list
        t.join
      end
      assert_equal(THREAD_COUNT * WORK_COUNT, count)
    end

    def test_read_write_lock_multithread
      count = 0
      th_grp = ThreadGroup.new
      barrier = Tank::Thread::Barrier.new(THREAD_COUNT * 2 + 1)

      THREAD_COUNT.times{|i|
        th_grp.add Thread.new{
          r_lock = @rw_lock.read_lock
          r_lock.synchronize{
            barrier.wait
            WORK_COUNT.times do |j|
              assert_equal(0, count, "read_lock: #{i}.#{j}")
            end
          }
        }
      }

      THREAD_COUNT.times do
        th_grp.add Thread.new{
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
      for t in th_grp.list
        t.join
      end
      assert_equal(THREAD_COUNT * WORK_COUNT, count)
    end

    def test_write_read_lock_multithread
      count = 0
      th_grp = ThreadGroup.new
      barrier = Tank::Thread::Barrier.new(THREAD_COUNT + 2)

      th_grp.add Thread.new{
        w_lock = @rw_lock.write_lock
        w_lock.synchronize{
          barrier.wait
          THREAD_COUNT.times do
            WORK_COUNT.times do
              count += 1
            end
          end
        }
      }

      THREAD_COUNT.times{|i|
        th_grp.add Thread.new{
          r_lock = @rw_lock.read_lock
          barrier.wait
          r_lock.synchronize{
            assert_equal(THREAD_COUNT * WORK_COUNT, count, "read_lock: #{i}")
          }
        }
      }

      barrier.wait
      for t in th_grp.list
        t.join
      end
    end
  end

  module LockManagerTest
    WORK_COUNT = 100
    THREAD_COUNT = 10

    def test_read_transaction_single_thread
      v = "foo"
      WORK_COUNT.times do
        @lock_manager.transaction(true) {|lock_handler|
          lock_handler.lock(:foo)
          assert_equal("foo", v)
        }
      end
    end

    def test_write_transaction_single_thread
      count = 0
      WORK_COUNT.times do
        @lock_manager.transaction{|lock_handler|
          lock_handler.lock(:foo)
          count += 1
        }
      end
      assert_equal(WORK_COUNT, count)
    end

    def test_read_transaction_multithread
      v = "foo"
      th_grp = ThreadGroup.new
      barrier = Tank::Thread::Barrier.new(THREAD_COUNT + 1)

      THREAD_COUNT.times{|i|
        th_grp.add Thread.new{
          @lock_manager.transaction(true) {|lock_handler|
            lock_handler.lock(:foo)
            barrier.wait
            WORK_COUNT.times do |j|
              assert_equal("foo", v, "read transaction: #{i}.#{j}")
            end
          }
        }
      }

      barrier.wait
      for t in th_grp.list
        t.join
      end
    end

    def test_write_transaction_multithread
      count = 0
      th_grp = ThreadGroup.new
      barrier = Tank::Thread::Barrier.new(THREAD_COUNT + 1)

      THREAD_COUNT.times do
        th_grp.add Thread.new{
          barrier.wait
          WORK_COUNT.times do
            @lock_manager.transaction{|lock_handler|
              lock_handler.lock(:foo)
              count += 1
            }
          end
        }
      end

      barrier.wait
      for t in th_grp.list
        t.join
      end
      assert_equal(THREAD_COUNT * WORK_COUNT, count)
    end

    def test_read_write_transaction_multithread
      count = 0
      th_grp = ThreadGroup.new
      barrier = Tank::Thread::Barrier.new(THREAD_COUNT * 2 + 1)

      THREAD_COUNT.times{|i|
        th_grp.add Thread.new{
          @lock_manager.transaction(true) {|lock_handler|
            lock_handler.lock(:foo)
            barrier.wait
            WORK_COUNT.times do |j|
              assert_equal(0, count, "read transaction: #{i}.#{j}")
            end
          }
        }
      }

      THREAD_COUNT.times do
        th_grp.add Thread.new{
          barrier.wait
          WORK_COUNT.times do
            @lock_manager.transaction{|lock_handler|
              lock_handler.lock(:foo)
              count += 1
            }
          end
        }
      end

      barrier.wait
      for t in th_grp.list
        t.join
      end
      assert_equal(THREAD_COUNT * WORK_COUNT, count)
    end
  end

  class GiantLockManagerTest < RUNIT::TestCase
    include LockManagerTest

    def setup
      @lock_manager = Tank::Lock::GiantLockManager.new
    end
  end

  class FineGrainLockManagerTest < RUNIT::TestCase
    include LockManagerTest

    def setup
      @lock_manager = Tank::Lock::FineGrainLockManager.new
    end
  end
end